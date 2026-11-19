// Package okx adapts the OKX v5 public websocket
// (wss://ws.okx.com:443/ws/v5/public) to the normalized model. OKX
// instrument IDs (BTC-USDT) are already canonical.
//
// OKX closes a connection after 30 s without traffic from the client, so the
// feed asks the runner to send "ping" (answered with a bare "pong").
//
// The trades channel reports one trade per price level of a match: tradeId
// is the last trade ID of the level and count how many it covers, so IDs
// advance by count and the gap tracker gets the whole range.
//
// Book checksums are no longer computed (always 0, checked live); each
// update instead carries prevSeqId, which must equal the previous message's
// seqId.
package okx

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/book"
	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const Name = "okx"

// bookDepth is the depth of the "books" channel.
const bookDepth = 400

// resyncEvery limits book resubscriptions per symbol.
const resyncEvery = 30 * time.Second

// argsPerRequest keeps subscribe frames well below OKX's size limit.
const argsPerRequest = 40

type Feed struct {
	// mu guards everything below: Handle runs on the reader goroutine while
	// SetSymbols is called by the subscription controller.
	mu      sync.Mutex
	url     string
	symbols []string
	ids     *exchange.TradeIDs
	books   *book.Tracker
	// lastSeq is the seqId of the last applied book message per symbol.
	lastSeq map[string]int64
	resyncs *exchange.Resyncs
	outbox  [][]byte
}

var (
	_ exchange.Feed    = (*Feed)(nil)
	_ exchange.Dynamic = (*Feed)(nil)
	_ exchange.Outbox  = (*Feed)(nil)
	_ exchange.Pinger  = (*Feed)(nil)
)

// New takes canonical symbols (BTC-USDT).
func New(url string, symbols []string) (*Feed, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("okx: no symbols")
	}
	for _, s := range symbols {
		if _, _, err := exchange.SplitSymbol(s); err != nil {
			return nil, fmt.Errorf("okx: %w", err)
		}
	}
	return &Feed{
		url: url, symbols: slices.Clone(symbols), ids: exchange.NewTradeIDs(Name), books: book.NewTracker(Name),
		lastSeq: map[string]int64{}, resyncs: exchange.NewResyncs(resyncEvery),
	}, nil
}

type arg struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

func args(symbols []string, channels ...string) []arg {
	out := make([]arg, 0, len(symbols)*len(channels))
	for _, s := range symbols {
		for _, c := range channels {
			out = append(out, arg{Channel: c, InstID: s})
		}
	}
	return out
}

func requests(op string, as []arg) [][]byte {
	var out [][]byte
	for chunk := range slices.Chunk(as, argsPerRequest) {
		b, _ := json.Marshal(map[string]any{"op": op, "args": chunk})
		out = append(out, b)
	}
	return out
}

func (f *Feed) Name() string { return Name }
func (f *Feed) URL() string  { return f.url }

func (f *Feed) Ping() ([]byte, time.Duration) { return []byte("ping"), 20 * time.Second }

func (f *Feed) Subscriptions() ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return requests("subscribe", args(f.symbols, "trades", "books")), nil
}

func (f *Feed) SetSymbols(symbols []string) ([][]byte, error) {
	for _, s := range symbols {
		if _, _, err := exchange.SplitSymbol(s); err != nil {
			return nil, fmt.Errorf("okx: %w", err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	added, removed := exchange.Diff(f.symbols, symbols)
	f.symbols = slices.Clone(symbols)
	for _, s := range removed {
		delete(f.lastSeq, s)
		f.ids.Forget(s)
		f.books.Forget(s)
		f.resyncs.Forget(s)
	}
	frames := requests("subscribe", args(added, "trades", "books"))
	return append(frames, requests("unsubscribe", args(removed, "trades", "books"))...), nil
}

func (f *Feed) Outbox() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.outbox
	f.outbox = nil
	return out
}

type message struct {
	Event  string          `json:"event"` // subscribe | unsubscribe | error | notice
	Msg    string          `json:"msg"`
	Code   string          `json:"code"`
	Arg    arg             `json:"arg"`
	Action string          `json:"action"` // books: snapshot | update
	Data   json.RawMessage `json:"data"`
}

type trade struct {
	InstID  string          `json:"instId"`
	TradeID string          `json:"tradeId"`
	Price   decimal.Decimal `json:"px"`
	Size    decimal.Decimal `json:"sz"`
	Side    string          `json:"side"` // taker side
	TS      string          `json:"ts"`
	Count   string          `json:"count"`
}

type bookData struct {
	// Levels are [price, size, deprecated, order count].
	Asks      [][]decimal.Decimal `json:"asks"`
	Bids      [][]decimal.Decimal `json:"bids"`
	TS        string              `json:"ts"`
	SeqID     int64               `json:"seqId"`
	PrevSeqID int64               `json:"prevSeqId"`
}

func (f *Feed) Handle(frame []byte, received time.Time, dst []model.Event) ([]model.Event, error) {
	if string(frame) == "pong" {
		return dst, nil
	}
	var m message
	if err := json.Unmarshal(frame, &m); err != nil {
		return dst, fmt.Errorf("okx: decode frame: %w", err)
	}
	switch m.Event {
	case "":
	case "error":
		// "...channel:trades,instId:FOO-USDT doesn't exist...": only that
		// instrument failed.
		if _, rest, ok := strings.Cut(m.Msg, "instId:"); ok {
			id, _, _ := strings.Cut(rest, " ")
			return dst, &exchange.SymbolError{Symbol: id, Reason: "okx " + m.Code + ": " + m.Msg}
		}
		return dst, fmt.Errorf("okx: error %s: %s", m.Code, m.Msg)
	default:
		return dst, nil // subscribe acks, notices
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if !slices.Contains(f.symbols, m.Arg.InstID) {
		return f.books.Flush(received, dst), nil // in flight when we unsubscribed
	}
	var err error
	switch m.Arg.Channel {
	case "trades":
		dst, err = f.handleTrades(m, received, dst)
	case "books":
		dst, err = f.handleBook(m, received, dst)
	}
	for _, s := range f.resyncs.Due(received) {
		f.resubscribe(s)
	}
	return f.books.Flush(received, dst), err
}

func (f *Feed) handleTrades(m message, received time.Time, dst []model.Event) ([]model.Event, error) {
	var trades []trade
	if err := json.Unmarshal(m.Data, &trades); err != nil {
		return dst, fmt.Errorf("okx: decode trades: %w", err)
	}
	for _, t := range trades {
		id, err := strconv.ParseInt(t.TradeID, 10, 64)
		if err != nil {
			return dst, fmt.Errorf("okx: trade id %q: %w", t.TradeID, err)
		}
		count, err := strconv.ParseInt(t.Count, 10, 64)
		if err != nil || count < 1 {
			count = 1
		}
		var dup bool
		if dst, dup = f.ids.ObserveRange(t.InstID, id-count+1, id, received, dst); dup {
			continue
		}
		side, err := parseSide(t.Side)
		if err != nil {
			return dst, err
		}
		ms, err := strconv.ParseInt(t.TS, 10, 64)
		if err != nil {
			return dst, fmt.Errorf("okx: trade time %q: %w", t.TS, err)
		}
		dst = append(dst, model.Trade{
			Exchange: Name, Symbol: t.InstID, TradeID: t.TradeID, Price: t.Price, Size: t.Size, TakerSide: side,
			ExchangeTime: time.UnixMilli(ms).UTC(), ReceiveTime: received,
		})
	}
	return dst, nil
}

// handleBook applies a snapshot or update. An update whose prevSeqId is not
// the last applied seqId means a message was lost: the book is reset and
// resubscribed for a fresh snapshot. (An update without changes repeats the
// seqId, which passes the check.)
func (f *Feed) handleBook(m message, received time.Time, dst []model.Event) ([]model.Event, error) {
	var ds []bookData
	if err := json.Unmarshal(m.Data, &ds); err != nil {
		return dst, fmt.Errorf("okx: decode book: %w", err)
	}
	symbol := m.Arg.InstID
	for _, d := range ds {
		switch {
		case m.Action == "snapshot":
			f.books.Reset(symbol)
		case !f.books.Ready(symbol):
			continue // waiting for the snapshot after a resync
		case d.PrevSeqID != f.lastSeq[symbol]:
			prev := f.lastSeq[symbol]
			f.books.Reset(symbol)
			delete(f.lastSeq, symbol)
			if f.resyncs.Request(symbol, received) {
				f.resubscribe(symbol)
			}
			dst = append(dst, model.BookResync{Exchange: Name, Symbol: symbol,
				Reason: fmt.Sprintf("prevSeqId %d, last applied %d", d.PrevSeqID, prev)})
			continue
		}
		f.lastSeq[symbol] = d.SeqID
		b := f.books.Book(symbol)
		for _, l := range d.Bids {
			if len(l) >= 2 {
				b.SetBid(l[0], l[1])
			}
		}
		for _, l := range d.Asks {
			if len(l) >= 2 {
				b.SetAsk(l[0], l[1])
			}
		}
		b.Truncate(bookDepth)
		if m.Action == "snapshot" {
			f.books.Snapshot(symbol)
		}
		ms, _ := strconv.ParseInt(d.TS, 10, 64)
		dst = f.books.Changed(symbol, time.UnixMilli(ms).UTC(), received, d.SeqID, dst)
	}
	return dst, nil
}

func (f *Feed) resubscribe(symbol string) {
	if !slices.Contains(f.symbols, symbol) {
		return
	}
	a := args([]string{symbol}, "books")
	f.outbox = append(f.outbox, requests("unsubscribe", a)...)
	f.outbox = append(f.outbox, requests("subscribe", a)...)
}

func parseSide(s string) (model.Side, error) {
	switch s {
	case "buy":
		return model.SideBuy, nil
	case "sell":
		return model.SideSell, nil
	default:
		return model.SideUnknown, fmt.Errorf("okx: unknown side %q", s)
	}
}
