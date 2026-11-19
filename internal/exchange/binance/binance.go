// Package binance adapts the Binance spot combined-stream websocket
// (wss://stream.binance.com:9443/stream) to the normalized model.
//
// Streams are selected in the URL (?streams=btcusdt@trade/...), so nothing is
// sent after connecting. Binance has no application-level heartbeat and stays
// silent for unknown symbols, so the read timeout is what detects both a
// frozen feed and a typo in the symbol list.
package binance

import (
	"encoding/json"
	"fmt"
	"net/url"
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

const Name = "binance"

type Feed struct {
	// mu guards everything below: Handle runs on the reader goroutine while
	// SetSymbols is called by the subscription controller.
	mu        sync.Mutex
	base      *url.URL
	symbols   []string
	canonical map[string]string // BTCUSDT -> BTC-USDT
	ids       *exchange.TradeIDs
	reqID     int
	books     *book.Tracker
}

var (
	_ exchange.Feed    = (*Feed)(nil)
	_ exchange.Dynamic = (*Feed)(nil)
)

// New takes canonical symbols (BTC-USDT) and a base URL ending in /stream.
func New(baseURL string, symbols []string) (*Feed, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("binance: no symbols")
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("binance: url: %w", err)
	}
	f := &Feed{base: u, canonical: map[string]string{}, ids: exchange.NewTradeIDs(Name), books: book.NewTracker(Name)}
	if _, err := f.SetSymbols(symbols); err != nil {
		return nil, err
	}
	f.reqID = 0 // the initial set goes in the URL, no request was sent
	return f, nil
}

func native(symbol string) (string, error) {
	base, quote, err := exchange.SplitSymbol(symbol)
	if err != nil {
		return "", fmt.Errorf("binance: %w", err)
	}
	return base + quote, nil
}

// Per symbol: individual trades, and the top 20 book levels every 100 ms
// (a full partial snapshot each time: no local diff sync needed).
const depthSuffix = "@depth20@100ms"

func streams(native string) []string {
	n := strings.ToLower(native)
	return []string{n + "@trade", n + depthSuffix}
}

func (f *Feed) Name() string { return Name }

// URL selects the current streams; a reconnect starts with all of them.
func (f *Feed) URL() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	all := make([]string, 0, 2*len(f.symbols))
	for _, s := range f.symbols {
		n, _ := native(s) // validated in SetSymbols
		all = append(all, streams(n)...)
	}
	u := *f.base
	// Stream names are [a-z0-9@_], safe in a query without escaping; Binance
	// documents them unescaped.
	u.RawQuery = "streams=" + strings.Join(all, "/")
	return u.String()
}

func (f *Feed) Subscriptions() ([][]byte, error) { return nil, nil }

// SetSymbols changes streams on the open connection with SUBSCRIBE /
// UNSUBSCRIBE requests (Binance allows 5 requests per second).
func (f *Feed) SetSymbols(symbols []string) ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range symbols {
		if _, err := native(s); err != nil {
			return nil, err
		}
	}
	added, removed := exchange.Diff(f.symbols, symbols)
	f.symbols = slices.Clone(symbols)
	var frames [][]byte
	for _, op := range []struct {
		method  string
		symbols []string
	}{{"SUBSCRIBE", added}, {"UNSUBSCRIBE", removed}} {
		if len(op.symbols) == 0 {
			continue
		}
		params := make([]string, 0, 2*len(op.symbols))
		for _, s := range op.symbols {
			n, _ := native(s)
			params = append(params, streams(n)...)
			if op.method == "SUBSCRIBE" {
				f.canonical[n] = s
			} else {
				delete(f.canonical, n)
				f.ids.Forget(s)
				f.books.Forget(s)
			}
		}
		f.reqID++
		b, err := json.Marshal(map[string]any{"method": op.method, "params": params, "id": f.reqID})
		if err != nil {
			return nil, err
		}
		frames = append(frames, b)
	}
	return frames, nil
}

type envelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
	// Replies to SUBSCRIBE/UNSUBSCRIBE.
	ID    int `json:"id"`
	Error *struct {
		Msg string `json:"msg"`
	} `json:"error"`
}

// trade mirrors the @trade payload. Binance uses keys that differ only in
// case (e/E, t/T, m/M); encoding/json matches keys case-insensitively, so each
// one must be declared explicitly or the wrong value lands in a field.
type trade struct {
	EventType  string          `json:"e"`
	EventTime  int64           `json:"E"`
	Symbol     string          `json:"s"`
	ID         int64           `json:"t"`
	Price      decimal.Decimal `json:"p"`
	Qty        decimal.Decimal `json:"q"`
	TradeTime  int64           `json:"T"`
	BuyerMaker bool            `json:"m"`
	Ignore     bool            `json:"M"`
}

func (f *Feed) Handle(frame []byte, received time.Time, dst []model.Event) ([]model.Event, error) {
	var env envelope
	if err := json.Unmarshal(frame, &env); err != nil {
		return dst, fmt.Errorf("binance: decode frame: %w", err)
	}
	if env.Error != nil {
		return dst, &exchange.SymbolError{Symbol: fmt.Sprintf("request %d", env.ID), Reason: env.Error.Msg}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if prefix, ok := strings.CutSuffix(env.Stream, depthSuffix); ok {
		return f.handleDepth(strings.ToUpper(prefix), env.Data, received, dst)
	}
	if !strings.HasSuffix(env.Stream, "@trade") {
		return dst, nil
	}
	var t trade
	if err := json.Unmarshal(env.Data, &t); err != nil {
		return dst, fmt.Errorf("binance: decode trade: %w", err)
	}
	symbol, ok := f.canonical[t.Symbol]
	if !ok {
		return dst, nil // in flight when we unsubscribed
	}

	var dup bool
	dst, dup = f.ids.Observe(symbol, t.ID, received, dst)
	if dup {
		return dst, nil
	}

	// m=true: the buyer was the maker, so the aggressor sold.
	side := model.SideBuy
	if t.BuyerMaker {
		side = model.SideSell
	}
	return append(dst, model.Trade{
		Exchange:     Name,
		Symbol:       symbol,
		TradeID:      strconv.FormatInt(t.ID, 10),
		Price:        t.Price,
		Size:         t.Qty,
		TakerSide:    side,
		ExchangeTime: time.UnixMilli(t.TradeTime).UTC(),
		ReceiveTime:  received,
	}), nil
}

// handleDepth applies a depth20 partial snapshot: the whole top of the
// book, so the local book is replaced each time. lastUpdateId orders them.
func (f *Feed) handleDepth(native string, data []byte, received time.Time, dst []model.Event) ([]model.Event, error) {
	symbol, ok := f.canonical[native]
	if !ok {
		return dst, nil // in flight when we unsubscribed
	}
	var d struct {
		LastUpdateID int64                `json:"lastUpdateId"`
		Bids         [][2]decimal.Decimal `json:"bids"`
		Asks         [][2]decimal.Decimal `json:"asks"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return dst, fmt.Errorf("binance: decode depth: %w", err)
	}
	f.books.Reset(symbol)
	b := f.books.Book(symbol)
	for _, l := range d.Bids {
		b.SetBid(l[0], l[1])
	}
	for _, l := range d.Asks {
		b.SetAsk(l[0], l[1])
	}
	f.books.Snapshot(symbol)
	// The partial depth stream has no event time; receipt time stands in.
	return f.books.Changed(symbol, received, received, d.LastUpdateID, dst), nil
}
