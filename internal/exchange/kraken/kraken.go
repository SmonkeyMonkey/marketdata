// Package kraken adapts the Kraken websocket v2 API (wss://ws.kraken.com/v2)
// to the normalized model.
//
// Kraken sends prices and quantities as JSON numbers, not strings. They are
// decoded straight from their text into decimals, never through float64.
package kraken

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

const Name = "kraken"

type Feed struct {
	// mu guards everything below: Handle runs on the reader goroutine while
	// SetSymbols is called by the subscription controller.
	mu      sync.Mutex
	url     string
	symbols []string // native form: BTC/USD
	ids     *exchange.TradeIDs
	books   *book.Tracker
	// precision per native symbol, from the instrument channel; needed to
	// format prices and sizes for the book checksum.
	precision map[string]precision
	outbox    [][]byte
	// resync rate limit per native symbol (see handleBook).
	resyncs *exchange.Resyncs
}

var (
	_ exchange.Feed    = (*Feed)(nil)
	_ exchange.Dynamic = (*Feed)(nil)
	_ exchange.Outbox  = (*Feed)(nil)
)

// New takes canonical symbols (BTC-USD).
func New(url string, symbols []string) (*Feed, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("kraken: no symbols")
	}
	nat, err := toNative(symbols)
	if err != nil {
		return nil, err
	}
	return &Feed{
		url: url, symbols: nat, ids: exchange.NewTradeIDs(Name),
		books: book.NewTracker(Name), precision: map[string]precision{},
		resyncs: exchange.NewResyncs(resyncEvery),
	}, nil
}

func toNative(symbols []string) ([]string, error) {
	out := make([]string, 0, len(symbols))
	for _, s := range symbols {
		base, quote, err := exchange.SplitSymbol(s)
		if err != nil {
			return nil, fmt.Errorf("kraken: %w", err)
		}
		out = append(out, base+"/"+quote)
	}
	return out, nil
}

func (f *Feed) SetSymbols(symbols []string) ([][]byte, error) {
	nat, err := toNative(symbols)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	added, removed := exchange.Diff(f.symbols, nat)
	f.symbols = nat
	var frames [][]byte
	for _, op := range []struct {
		method  string
		symbols []string
	}{{"subscribe", added}, {"unsubscribe", removed}} {
		if len(op.symbols) == 0 {
			continue
		}
		b, err := requests(op.method, op.symbols)
		if err != nil {
			return nil, err
		}
		frames = append(frames, b...)
	}
	for _, s := range removed {
		f.ids.Forget(canonical(s))
		f.books.Forget(canonical(s))
		f.resyncs.Forget(s)
	}
	return frames, nil
}

func canonical(native string) string { return strings.Replace(native, "/", "-", 1) }

// requests builds (un)subscribe frames for the trade and book channels.
func requests(method string, symbols []string) ([][]byte, error) {
	trade := map[string]any{"channel": "trade", "symbol": symbols}
	books := map[string]any{"channel": "book", "symbol": symbols, "depth": bookDepth}
	if method == "subscribe" {
		trade["snapshot"] = true
	}
	var out [][]byte
	for _, params := range []map[string]any{trade, books} {
		b, err := json.Marshal(map[string]any{"method": method, "params": params})
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// Outbox returns frames the feed wants sent (book resubscriptions).
func (f *Feed) Outbox() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.outbox
	f.outbox = nil
	return out
}

func (f *Feed) Name() string { return Name }
func (f *Feed) URL() string  { return f.url }

// Subscriptions requests instrument info (price/size precision, for book
// checksums), recent trades (after a reconnect the trade snapshot overlaps
// what we already saw, so the ID tracker can tell whether anything was
// missed) and the books.
func (f *Feed) Subscriptions() ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.symbols) == 0 {
		return nil, nil
	}
	instrument, err := json.Marshal(map[string]any{"method": "subscribe", "params": map[string]any{"channel": "instrument", "snapshot": true}})
	if err != nil {
		return nil, err
	}
	rest, err := requests("subscribe", f.symbols)
	if err != nil {
		return nil, err
	}
	return append([][]byte{instrument}, rest...), nil
}

type message struct {
	Channel string          `json:"channel"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`

	// Request acknowledgements.
	Method  string `json:"method"`
	Success *bool  `json:"success"`
	Error   string `json:"error"`
	Symbol  string `json:"symbol"`
}

type trade struct {
	Symbol    string          `json:"symbol"`
	Side      string          `json:"side"`
	Price     decimal.Decimal `json:"price"`
	Qty       decimal.Decimal `json:"qty"`
	TradeID   int64           `json:"trade_id"`
	Timestamp time.Time       `json:"timestamp"`
}

func (f *Feed) Handle(frame []byte, received time.Time, dst []model.Event) ([]model.Event, error) {
	var m message
	if err := json.Unmarshal(frame, &m); err != nil {
		return dst, fmt.Errorf("kraken: decode frame: %w", err)
	}

	if m.Method != "" {
		if m.Success != nil && !*m.Success {
			// One rejected pair must not take down the others.
			return dst, &exchange.SymbolError{Symbol: m.Symbol, Reason: fmt.Sprintf("kraken %s: %s", m.Method, m.Error)}
		}
		return dst, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	var err error
	switch m.Channel {
	case "trade":
		dst, err = f.handleTrades(m, received, dst)
	case "book":
		dst, err = f.handleBook(m, received, dst)
	case "instrument":
		err = f.handleInstrument(m)
	}
	f.retryResyncs(received)
	// Any frame (heartbeats arrive every second) publishes books whose last
	// change waited for the rate limit.
	return f.books.Flush(received, dst), err
}

func (f *Feed) handleTrades(m message, received time.Time, dst []model.Event) ([]model.Event, error) {
	var trades []trade
	if err := json.Unmarshal(m.Data, &trades); err != nil {
		return dst, fmt.Errorf("kraken: decode trades: %w", err)
	}
	for _, t := range trades {
		if !slices.Contains(f.symbols, t.Symbol) {
			continue // in flight when we unsubscribed
		}
		symbol := canonical(t.Symbol)
		var dup bool
		dst, dup = f.ids.Observe(symbol, t.TradeID, received, dst)
		if dup {
			continue
		}
		side, err := parseSide(t.Side)
		if err != nil {
			return dst, err
		}
		dst = append(dst, model.Trade{
			Exchange:     Name,
			Symbol:       symbol,
			TradeID:      strconv.FormatInt(t.TradeID, 10),
			Price:        t.Price,
			Size:         t.Qty,
			TakerSide:    side, // Kraken reports the taker side
			ExchangeTime: t.Timestamp,
			ReceiveTime:  received,
			Snapshot:     m.Type == "snapshot",
		})
	}
	return dst, nil
}

func parseSide(s string) (model.Side, error) {
	switch s {
	case "buy":
		return model.SideBuy, nil
	case "sell":
		return model.SideSell, nil
	default:
		return model.SideUnknown, fmt.Errorf("kraken: unknown side %q", s)
	}
}
