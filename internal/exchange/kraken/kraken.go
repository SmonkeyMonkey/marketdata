// Package kraken adapts the Kraken websocket v2 API (wss://ws.kraken.com/v2)
// to the normalized model.
//
// Kraken sends prices and quantities as JSON numbers, not strings. They are
// decoded straight from their text into decimals, never through float64.
package kraken

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const Name = "kraken"

type Feed struct {
	url     string
	symbols []string // native form: BTC/USD
	ids     *exchange.TradeIDs
}

var _ exchange.Feed = (*Feed)(nil)

// New takes canonical symbols (BTC-USD).
func New(url string, symbols []string) (*Feed, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("kraken: no symbols")
	}
	native := make([]string, 0, len(symbols))
	for _, s := range symbols {
		base, quote, err := exchange.SplitSymbol(s)
		if err != nil {
			return nil, fmt.Errorf("kraken: %w", err)
		}
		native = append(native, base+"/"+quote)
	}
	return &Feed{url: url, symbols: native, ids: exchange.NewTradeIDs(Name)}, nil
}

func (f *Feed) Name() string { return Name }
func (f *Feed) URL() string  { return f.url }

// Subscriptions requests a snapshot of recent trades: after a reconnect the
// snapshot overlaps what we already saw, which lets the ID tracker tell
// whether anything was missed in between.
func (f *Feed) Subscriptions() ([][]byte, error) {
	msg, err := json.Marshal(map[string]any{
		"method": "subscribe",
		"params": map[string]any{
			"channel":  "trade",
			"symbol":   f.symbols,
			"snapshot": true,
		},
	})
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
			return dst, fmt.Errorf("kraken: %s %s failed: %s", m.Method, m.Symbol, m.Error)
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
	for _, t := range trades {
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
