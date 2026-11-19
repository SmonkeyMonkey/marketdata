// Package bybit adapts the Bybit v5 public spot websocket
// (wss://stream.bybit.com/v5/public/spot) to the normalized model.
//
// Bybit closes connections without client pings, so the feed asks the
// runner to send {"op":"ping"} every 20 s. One bad symbol fails the whole
// subscribe request it is in, so every symbol is subscribed by its own
// request.
package bybit

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

const Name = "bybit"

// bookDepth is the subscribed depth. Bybit sends changes within it only,
// so levels pushed out of it must be dropped locally.
const bookDepth = 50

// resyncEvery limits book resubscriptions per symbol.
const resyncEvery = 30 * time.Second

type Feed struct {
	// mu guards everything below: Handle runs on the reader goroutine while
	// SetSymbols is called by the subscription controller.
	mu        sync.Mutex
	url       string
	symbols   []string          // canonical: BTC-USDT
	canonical map[string]string // BTCUSDT -> BTC-USDT, for subscribed symbols
	ids       *exchange.TradeIDs
	books     *book.Tracker
	// lastU is the update id of the last applied book message per native
	// symbol; Bybit increments it by one per message.
	lastU   map[string]int64
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
		return nil, fmt.Errorf("bybit: no symbols")
	}
	f := &Feed{
		url: url, canonical: map[string]string{}, ids: exchange.NewTradeIDs(Name), books: book.NewTracker(Name),
		lastU: map[string]int64{}, resyncs: exchange.NewResyncs(resyncEvery),
	}
	if _, err := f.SetSymbols(symbols); err != nil {
		return nil, err
	}
	return f, nil
}

func native(symbol string) (string, error) {
	base, quote, err := exchange.SplitSymbol(symbol)
	if err != nil {
		return "", fmt.Errorf("bybit: %w", err)
	}
	return base + quote, nil
}

func topics(native string) []string {
	return []string{"publicTrade." + native, "orderbook." + strconv.Itoa(bookDepth) + "." + native}
}

func request(op string, args []string) []byte {
	b, _ := json.Marshal(map[string]any{"op": op, "args": args})
	return b
}

func (f *Feed) Name() string { return Name }
func (f *Feed) URL() string  { return f.url }

func (f *Feed) Ping() ([]byte, time.Duration) { return []byte(`{"op":"ping"}`), 20 * time.Second }

func (f *Feed) Subscriptions() ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]byte
	for _, s := range f.symbols {
		n, _ := native(s) // validated in SetSymbols
		out = append(out, request("subscribe", topics(n)))
	}
	return out, nil
}

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
	for _, s := range added {
		n, _ := native(s)
		f.canonical[n] = s
		frames = append(frames, request("subscribe", topics(n)))
	}
	for _, s := range removed {
		n, _ := native(s)
		delete(f.canonical, n)
		delete(f.lastU, n)
		f.ids.Forget(s)
		f.books.Forget(s)
		f.resyncs.Forget(n)
		frames = append(frames, request("unsubscribe", topics(n)))
	}
	return frames, nil
}

func (f *Feed) Outbox() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.outbox
	f.outbox = nil
	return out
}

type message struct {
	Topic string          `json:"topic"`
	Type  string          `json:"type"` // snapshot | delta
	TS    int64           `json:"ts"`
	CTS   int64           `json:"cts"` // matching engine time (books)
	Data  json.RawMessage `json:"data"`

	// Replies to subscribe/unsubscribe/ping.
	Op      string `json:"op"`
	Success *bool  `json:"success"`
	RetMsg  string `json:"ret_msg"`
}

type trade struct {
	ID     string          `json:"i"`
	Time   int64           `json:"T"`
	Price  decimal.Decimal `json:"p"`
	Size   decimal.Decimal `json:"v"`
	Side   string          `json:"S"` // taker side
	Symbol string          `json:"s"`
}

type bookData struct {
	Symbol string               `json:"s"`
	Bids   [][2]decimal.Decimal `json:"b"`
	Asks   [][2]decimal.Decimal `json:"a"`
	U      int64                `json:"u"`   // update id, +1 per message
	Seq    int64                `json:"seq"` // cross sequence, orders snapshots
}

func (f *Feed) Handle(frame []byte, received time.Time, dst []model.Event) ([]model.Event, error) {
	var m message
	if err := json.Unmarshal(frame, &m); err != nil {
		return dst, fmt.Errorf("bybit: decode frame: %w", err)
	}
	if m.Op != "" {
		if m.Success != nil && !*m.Success {
			return dst, f.rejected(m)
		}
		return dst, nil // subscribed, pong
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	switch {
	case strings.HasPrefix(m.Topic, "publicTrade."):
		dst, err = f.handleTrades(m, received, dst)
	case strings.HasPrefix(m.Topic, "orderbook."):
		dst, err = f.handleBook(m, received, dst)
	}
	for _, n := range f.resyncs.Due(received) {
		f.resubscribe(n)
	}
	return f.books.Flush(received, dst), err
}

// rejected turns "Invalid symbol :[publicTrade.FOOUSDT]" into a SymbolError.
func (f *Feed) rejected(m message) error {
	sym := m.RetMsg
	if _, rest, ok := strings.Cut(m.RetMsg, "["); ok {
		topic, _, _ := strings.Cut(rest, "]")
		sym = topic[strings.LastIndexByte(topic, '.')+1:]
		f.mu.Lock()
		if c, ok := f.canonical[sym]; ok {
			sym = c
		}
		f.mu.Unlock()
	}
	return &exchange.SymbolError{Symbol: sym, Reason: "bybit " + m.Op + ": " + m.RetMsg}
}

func (f *Feed) handleTrades(m message, received time.Time, dst []model.Event) ([]model.Event, error) {
	var trades []trade
	if err := json.Unmarshal(m.Data, &trades); err != nil {
		return dst, fmt.Errorf("bybit: decode trades: %w", err)
	}
	for _, t := range trades {
		symbol, ok := f.canonical[t.Symbol]
		if !ok {
			continue // in flight when we unsubscribed
		}
		// Trade IDs come from counters shared by the symbols of one matching
		// shard (checked live: the "holes" of ETHUSDC are BTCUSDC's IDs), so
		// they grow per symbol but skip values: good for dropping repeats,
		// useless for gap detection.
		if id, err := strconv.ParseInt(t.ID, 10, 64); err == nil && f.ids.Repeat(symbol, id) {
			continue
		}
		side, err := parseSide(t.Side)
		if err != nil {
			return dst, err
		}
		dst = append(dst, model.Trade{
			Exchange: Name, Symbol: symbol, TradeID: t.ID, Price: t.Price, Size: t.Size, TakerSide: side,
			ExchangeTime: time.UnixMilli(t.Time).UTC(), ReceiveTime: received,
		})
	}
	return dst, nil
}

// handleBook applies a snapshot or delta. Deltas carry an update id that
// grows by one per message: a jump means a message was lost, so the book is
// reset and resubscribed for a fresh snapshot.
func (f *Feed) handleBook(m message, received time.Time, dst []model.Event) ([]model.Event, error) {
	var d bookData
	if err := json.Unmarshal(m.Data, &d); err != nil {
		return dst, fmt.Errorf("bybit: decode book: %w", err)
	}
	symbol, ok := f.canonical[d.Symbol]
	if !ok {
		return dst, nil // in flight when we unsubscribed
	}
	switch {
	case m.Type == "snapshot":
		f.books.Reset(symbol)
	case !f.books.Ready(symbol):
		return dst, nil // waiting for the snapshot after a resync
	case d.U != f.lastU[d.Symbol]+1:
		prev := f.lastU[d.Symbol]
		f.books.Reset(symbol)
		delete(f.lastU, d.Symbol)
		if f.resyncs.Request(d.Symbol, received) {
			f.resubscribe(d.Symbol)
		}
		return append(dst, model.BookResync{Exchange: Name, Symbol: symbol,
			Reason: fmt.Sprintf("update id %d after %d", d.U, prev)}), nil
	}
	f.lastU[d.Symbol] = d.U
	b := f.books.Book(symbol)
	for _, l := range d.Bids {
		b.SetBid(l[0], l[1])
	}
	for _, l := range d.Asks {
		b.SetAsk(l[0], l[1])
	}
	b.Truncate(bookDepth)
	if m.Type == "snapshot" {
		f.books.Snapshot(symbol)
	}
	at := m.CTS
	if at == 0 {
		at = m.TS
	}
	return f.books.Changed(symbol, time.UnixMilli(at).UTC(), received, d.Seq, dst), nil
}

func (f *Feed) resubscribe(native string) {
	if _, ok := f.canonical[native]; !ok {
		return
	}
	topic := []string{"orderbook." + strconv.Itoa(bookDepth) + "." + native}
	f.outbox = append(f.outbox, request("unsubscribe", topic), request("subscribe", topic))
}

func parseSide(s string) (model.Side, error) {
	switch s {
	case "Buy":
		return model.SideBuy, nil
	case "Sell":
		return model.SideSell, nil
	default:
		return model.SideUnknown, fmt.Errorf("bybit: unknown side %q", s)
	}
}
