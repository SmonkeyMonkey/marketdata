// Package model holds the exchange-agnostic market data types.
// Every exchange adapter converts its wire format into these types, so
// downstream services never deal with exchange-specific quirks.
package model

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// Side is the aggressor (taker) side of a trade.
type Side uint8

const (
	SideUnknown Side = iota
	SideBuy
	SideSell
)

func (s Side) String() string {
	switch s {
	case SideBuy:
		return "buy"
	case SideSell:
		return "sell"
	default:
		return "unknown"
	}
}

func (s Side) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

func (s *Side) UnmarshalText(b []byte) error {
	switch string(b) {
	case "buy":
		*s = SideBuy
	case "sell":
		*s = SideSell
	default:
		*s = SideUnknown
	}
	return nil
}

// Opposite returns the other side; useful for exchanges that report the maker side.
func (s Side) Opposite() Side {
	switch s {
	case SideBuy:
		return SideSell
	case SideSell:
		return SideBuy
	default:
		return SideUnknown
	}
}

// Event is any normalized market data event. The set is closed: only types
// in this package implement it.
type Event interface {
	Kind() string
	event()
}

// Trade is a single execution.
type Trade struct {
	Exchange  string          `json:"exchange"`
	Symbol    string          `json:"symbol"` // canonical BASE-QUOTE, e.g. BTC-USD
	TradeID   string          `json:"trade_id"`
	Price     decimal.Decimal `json:"price"`
	Size      decimal.Decimal `json:"size"`
	TakerSide Side            `json:"taker_side"`
	// ExchangeTime is when the exchange matched the trade; ReceiveTime is when
	// we read the frame. The difference is the feed latency.
	ExchangeTime time.Time `json:"exchange_time"`
	ReceiveTime  time.Time `json:"receive_time"`
	// Snapshot marks a historical trade replayed on subscribe (not live).
	// It is valid data, but must not count towards feed latency.
	Snapshot bool `json:"snapshot,omitempty"`
}

func (Trade) Kind() string { return "trade" }
func (Trade) event()       {}

// Gap reports trades that were missed between two observed trade IDs
// (e.g. during a reconnect). A backfiller can later fetch them via REST.
type Gap struct {
	Exchange   string    `json:"exchange"`
	Symbol     string    `json:"symbol"`
	AfterID    string    `json:"after_id"`  // last trade ID seen before the gap
	BeforeID   string    `json:"before_id"` // first trade ID seen after the gap
	Missing    int64     `json:"missing"`
	DetectedAt time.Time `json:"detected_at"`
	// Source is the ingester instance that missed the trades. With redundant
	// ingesters a gap in one is usually covered by another.
	Source string `json:"source"`
}

func (Gap) Kind() string { return "gap" }
func (Gap) event()       {}

// Candle aggregates the trades of one symbol on one exchange over
// [Start, Start+Interval). Open and Close are the first and last trades in
// exchange order. Volumes are in base units; QuoteVolume is sum(price*size),
// so VWAP = QuoteVolume / Volume.
type Candle struct {
	Exchange     string          `json:"exchange"`
	Symbol       string          `json:"symbol"`
	Interval     time.Duration   `json:"interval"`
	Start        time.Time       `json:"start"`
	Open         decimal.Decimal `json:"open"`
	High         decimal.Decimal `json:"high"`
	Low          decimal.Decimal `json:"low"`
	Close        decimal.Decimal `json:"close"`
	Volume       decimal.Decimal `json:"volume"`
	QuoteVolume  decimal.Decimal `json:"quote_volume"`
	BuyVolume    decimal.Decimal `json:"buy_volume"`  // taker buys
	SellVolume   decimal.Decimal `json:"sell_volume"` // taker sells
	Trades       int64           `json:"trades"`
	FirstTradeID string          `json:"first_trade_id"`
	LastTradeID  string          `json:"last_trade_id"`
}

func (Candle) Kind() string { return "candle" }
func (Candle) event()       {}

// PeriodName formats an interval the way traders write it: 1s, 5m, 1h.
func PeriodName(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	default:
		return fmt.Sprintf("%ds", d/time.Second)
	}
}

type Direction uint8

const (
	DirectionUnknown Direction = iota
	Above
	Below
)

func (d Direction) String() string {
	switch d {
	case Above:
		return "above"
	case Below:
		return "below"
	default:
		return "unknown"
	}
}

func (d Direction) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Direction) UnmarshalText(b []byte) error {
	switch string(b) {
	case "above":
		*d = Above
	case "below":
		*d = Below
	default:
		return fmt.Errorf("direction %q: want above or below", b)
	}
	return nil
}

// AlertRule is a one-shot user price alert.
type AlertRule struct {
	ID        string          `json:"id"`
	Exchange  string          `json:"exchange"`
	Symbol    string          `json:"symbol"`
	Direction Direction       `json:"direction"`
	Price     decimal.Decimal `json:"price"`
	CreatedAt time.Time       `json:"created_at"`
	Note      string          `json:"note,omitempty"`
}

type AlertType uint8

const (
	AlertUnknown AlertType = iota
	AlertPriceRule
	AlertWhaleTrade
	AlertPriceMove
)

func (t AlertType) String() string {
	switch t {
	case AlertPriceRule:
		return "price_rule"
	case AlertWhaleTrade:
		return "whale_trade"
	case AlertPriceMove:
		return "price_move"
	default:
		return "unknown"
	}
}

func (t AlertType) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// Alert is a fired alert. ID is deterministic so replays can be deduplicated.
type Alert struct {
	ID        string          `json:"id"`
	Type      AlertType       `json:"type"`
	Exchange  string          `json:"exchange"`
	Symbol    string          `json:"symbol"`
	Message   string          `json:"message"`
	Price     decimal.Decimal `json:"price"`
	Time      time.Time       `json:"time"` // exchange time of the triggering trade
	RuleID    string          `json:"rule_id,omitempty"`
	TradeID   string          `json:"trade_id,omitempty"`
	Notional  decimal.Decimal `json:"notional,omitzero"`
	ChangePct decimal.Decimal `json:"change_pct,omitzero"`
	Window    time.Duration   `json:"-"`
}

func (Alert) Kind() string { return "alert" }
func (Alert) event()       {}
