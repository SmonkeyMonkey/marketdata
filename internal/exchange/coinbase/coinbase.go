// Package coinbase adapts the Coinbase Exchange websocket feed
// (wss://ws-feed.exchange.coinbase.com) to the normalized model.
//
// Only public channels are used: "matches" (trades) and "heartbeat".
// level2/level3/full require authentication on this endpoint.
package coinbase

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const Name = "coinbase"

// Feed implements exchange.Feed. Coinbase product IDs (BTC-USD) are already
// in canonical form.
type Feed struct {
	url      string
	products []string
	// Trade IDs are sequential per product; kept across reconnects so the
	// last_match after a reconnect reveals what was missed.
	ids *exchange.TradeIDs
}

var _ exchange.Feed = (*Feed)(nil)

func New(url string, products []string) *Feed {
	return &Feed{url: url, products: products, ids: exchange.NewTradeIDs(Name)}
}

func (f *Feed) Name() string { return Name }
func (f *Feed) URL() string  { return f.url }

func (f *Feed) Subscriptions() ([][]byte, error) {
	msg, err := json.Marshal(map[string]any{
		"type":        "subscribe",
		"product_ids": f.products,
		"channels":    []string{"matches", "heartbeat"},
	})
	if err != nil {
		return nil, err
	}
	return [][]byte{msg}, nil
}

// message is a union of the fields we need from all handled message types.
type message struct {
	Type        string          `json:"type"`
	ProductID   string          `json:"product_id"`
	TradeID     int64           `json:"trade_id"`
	Side        string          `json:"side"`
	Price       decimal.Decimal `json:"price"`
	Size        decimal.Decimal `json:"size"`
	Time        time.Time       `json:"time"`
	LastTradeID int64           `json:"last_trade_id"`
	Message     string          `json:"message"`
	Reason      string          `json:"reason"`
}

func (f *Feed) Handle(frame []byte, received time.Time, dst []model.Event) ([]model.Event, error) {
	var m message
	if err := json.Unmarshal(frame, &m); err != nil {
		return dst, fmt.Errorf("coinbase: decode frame: %w", err)
	}

	switch m.Type {
	case "match", "last_match":
		// last_match is sent once after subscribing: the latest trade before
		// our session. After a reconnect it reveals trades we missed.
		var dup bool
		dst, dup = f.ids.Observe(m.ProductID, m.TradeID, received, dst)
		if dup {
			return dst, nil
		}
		side, err := parseSide(m.Side)
		if err != nil {
			return dst, err
		}
		return append(dst, model.Trade{
			Exchange: Name,
			Symbol:   m.ProductID,
			TradeID:  strconv.FormatInt(m.TradeID, 10),
			Price:    m.Price,
			Size:     m.Size,
			// Coinbase reports the MAKER order side; the taker is the opposite.
			TakerSide:    side.Opposite(),
			ExchangeTime: m.Time,
			ReceiveTime:  received,
			Snapshot:     m.Type == "last_match",
		}), nil

	case "heartbeat":
		// Heartbeats carry the last trade ID, so gaps are visible even when
		// no new trades arrive.
		return f.ids.AdvanceTo(m.ProductID, m.LastTradeID, received, dst), nil

	case "error":
		return dst, fmt.Errorf("coinbase: server error: %s: %s", m.Message, m.Reason)

	default: // subscriptions, ticker, etc.
		return dst, nil
	}
}

func parseSide(s string) (model.Side, error) {
	switch s {
	case "buy":
		return model.SideBuy, nil
	case "sell":
		return model.SideSell, nil
	default:
		return model.SideUnknown, fmt.Errorf("coinbase: unknown side %q", s)
	}
}
