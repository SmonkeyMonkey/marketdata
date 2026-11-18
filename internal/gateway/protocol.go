package gateway

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

// Channel names:
//
//	trades:<exchange>:<symbol>             trades:binance:BTC-USDT
//	candles:<exchange>:<symbol>:<period>   candles:coinbase:BTC-USD:1m
func TradesChannel(exchange, symbol string) string { return "trades:" + exchange + ":" + symbol }

// AlertsChannel carries every alert of a symbol; RuleChannel only the alert
// of one user rule.
func AlertsChannel(exchange, symbol string) string { return "alerts:" + exchange + ":" + symbol }
func RuleChannel(id string) string                 { return "alerts:rule:" + id }

func CandlesChannel(c model.Candle) string {
	return "candles:" + c.Exchange + ":" + c.Symbol + ":" + model.PeriodName(c.Interval)
}

// ValidChannel checks the shape of a channel name. Unknown symbols are
// allowed: the channel simply stays silent.
func ValidChannel(ch string) error {
	parts := strings.Split(ch, ":")
	switch {
	case len(ch) > 64:
		return fmt.Errorf("channel too long")
	case (parts[0] == "trades" || parts[0] == "alerts") && len(parts) == 3 && parts[1] != "" && parts[2] != "":
		return nil
	case parts[0] == "candles" && len(parts) == 4 && parts[1] != "" && parts[2] != "" && parts[3] != "":
		return nil
	default:
		return fmt.Errorf("invalid channel %q: want trades:<exchange>:<symbol>, candles:<exchange>:<symbol>:<period>, alerts:<exchange>:<symbol> or alerts:rule:<id>", ch)
	}
}

// Client -> server.
type request struct {
	Op       string   `json:"op"` // subscribe | unsubscribe | ping
	Channels []string `json:"channels"`
}

// Server -> client. TS is the gateway's send time in Unix milliseconds, so
// clients can measure fan-out latency.
type envelope struct {
	Type    string `json:"type"`
	Channel string `json:"channel,omitempty"`
	TS      int64  `json:"ts"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
}

type tradeMsg struct {
	Exchange string          `json:"exchange"`
	Symbol   string          `json:"symbol"`
	ID       string          `json:"id"`
	Price    decimal.Decimal `json:"price"`
	Size     decimal.Decimal `json:"size"`
	Side     model.Side      `json:"side"` // taker side
	Time     time.Time       `json:"time"` // exchange match time
	// IngestTime is when our ingester received the trade, for end-to-end
	// latency measurement.
	IngestTime time.Time `json:"ingest_time"`
}

type candleMsg struct {
	Exchange   string          `json:"exchange"`
	Symbol     string          `json:"symbol"`
	Period     string          `json:"period"`
	Start      time.Time       `json:"start"`
	Open       decimal.Decimal `json:"open"`
	High       decimal.Decimal `json:"high"`
	Low        decimal.Decimal `json:"low"`
	Close      decimal.Decimal `json:"close"`
	Volume     decimal.Decimal `json:"volume"`
	VWAP       decimal.Decimal `json:"vwap"`
	BuyVolume  decimal.Decimal `json:"buy_volume"`
	SellVolume decimal.Decimal `json:"sell_volume"`
	Trades     int64           `json:"trades"`
}

func encodeTrade(t model.Trade, now time.Time) ([]byte, error) {
	return json.Marshal(envelope{Type: "trade", Channel: TradesChannel(t.Exchange, t.Symbol), TS: now.UnixMilli(), Data: tradeMsg{
		Exchange: t.Exchange, Symbol: t.Symbol, ID: t.TradeID, Price: t.Price, Size: t.Size,
		Side: t.TakerSide, Time: t.ExchangeTime, IngestTime: t.ReceiveTime,
	}})
}

func encodeCandle(c model.Candle, now time.Time) ([]byte, error) {
	vwap := decimal.Zero
	if !c.Volume.IsZero() {
		vwap = c.QuoteVolume.DivRound(c.Volume, 8)
	}
	return json.Marshal(envelope{Type: "candle", Channel: CandlesChannel(c), TS: now.UnixMilli(), Data: candleMsg{
		Exchange: c.Exchange, Symbol: c.Symbol, Period: model.PeriodName(c.Interval), Start: c.Start,
		Open: c.Open, High: c.High, Low: c.Low, Close: c.Close, Volume: c.Volume, VWAP: vwap,
		BuyVolume: c.BuyVolume, SellVolume: c.SellVolume, Trades: c.Trades,
	}})
}

type alertMsg struct {
	ID        string          `json:"id"`
	Type      model.AlertType `json:"type"`
	Exchange  string          `json:"exchange"`
	Symbol    string          `json:"symbol"`
	Message   string          `json:"message"`
	Price     decimal.Decimal `json:"price"`
	Time      time.Time       `json:"time"`
	RuleID    string          `json:"rule_id,omitempty"`
	Notional  decimal.Decimal `json:"notional,omitzero"`
	ChangePct decimal.Decimal `json:"change_pct,omitzero"`
}

func encodeAlert(a model.Alert, channel string, now time.Time) ([]byte, error) {
	return json.Marshal(envelope{Type: "alert", Channel: channel, TS: now.UnixMilli(), Data: alertMsg{
		ID: a.ID, Type: a.Type, Exchange: a.Exchange, Symbol: a.Symbol, Message: a.Message,
		Price: a.Price, Time: a.Time, RuleID: a.RuleID, Notional: a.Notional, ChangePct: a.ChangePct,
	}})
}

func control(typ, errMsg string, channels []string) []byte {
	b, _ := json.Marshal(struct {
		envelope
		Channels []string `json:"channels,omitempty"`
	}{envelope{Type: typ, TS: time.Now().UnixMilli(), Error: errMsg}, channels})
	return b
}
