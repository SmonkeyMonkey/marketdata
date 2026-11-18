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

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const Name = "kraken"

type Feed struct {
	// mu guards symbols and ids: Handle runs on the reader goroutine while
	// SetSymbols is called by the subscription controller.
	mu      sync.Mutex
	url     string
	symbols []string // native form: BTC/USD
	ids     *exchange.TradeIDs
}

var (
	_ exchange.Feed    = (*Feed)(nil)
	_ exchange.Dynamic = (*Feed)(nil)
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
	return &Feed{url: url, symbols: nat, ids: exchange.NewTradeIDs(Name)}, nil
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
		b, err := request(op.method, op.symbols)
		if err != nil {
			return nil, err
		}
		frames = append(frames, b)
	}
	for _, s := range removed {
		f.ids.Forget(strings.Replace(s, "/", "-", 1))
	}
	return frames, nil
}

func request(method string, symbols []string) ([]byte, error) {
	params := map[string]any{"channel": "trade", "symbol": symbols}
	if method == "subscribe" {
		params["snapshot"] = true
	}
	return json.Marshal(map[string]any{"method": method, "params": params})
}

func (f *Feed) Name() string { return Name }
func (f *Feed) URL() string  { return f.url }

// Subscriptions requests a snapshot of recent trades: after a reconnect the
// snapshot overlaps what we already saw, which lets the ID tracker tell
// whether anything was missed in between.
func (f *Feed) Subscriptions() ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.symbols) == 0 {
		return nil, nil
	}
	msg, err := request("subscribe", f.symbols)
	if err != nil {
		return nil, err
	}
	return [][]byte{msg}, nil
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
	if m.Channel != "trade" { // heartbeat, status
		return dst, nil
	}

	var trades []trade
	if err := json.Unmarshal(m.Data, &trades); err != nil {
		return dst, fmt.Errorf("kraken: decode trades: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range trades {
		if !slices.Contains(f.symbols, t.Symbol) {
			continue // in flight when we unsubscribed
		}
		symbol := strings.Replace(t.Symbol, "/", "-", 1)
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
