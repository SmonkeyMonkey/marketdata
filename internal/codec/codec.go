// Package codec converts normalized events to and from their protobuf wire form.
package codec

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"

	pb "marketdata/internal/gen/marketdata/v1"
	"marketdata/internal/model"
)

func EncodeTrade(t model.Trade) ([]byte, error) {
	return proto.Marshal(&pb.Trade{
		Exchange:             t.Exchange,
		Symbol:               t.Symbol,
		TradeId:              t.TradeID,
		Price:                t.Price.String(),
		Size:                 t.Size.String(),
		TakerSide:            sideToPB(t.TakerSide),
		ExchangeTimeUnixNano: unixNano(t.ExchangeTime),
		ReceiveTimeUnixNano:  unixNano(t.ReceiveTime),
		Snapshot:             t.Snapshot,
	})
}

func DecodeTrade(b []byte) (model.Trade, error) {
	var m pb.Trade
	if err := proto.Unmarshal(b, &m); err != nil {
		return model.Trade{}, fmt.Errorf("decode trade: %w", err)
	}
	price, err := decimal.NewFromString(m.Price)
	if err != nil {
		return model.Trade{}, fmt.Errorf("decode trade price %q: %w", m.Price, err)
	}
	size, err := decimal.NewFromString(m.Size)
	if err != nil {
		return model.Trade{}, fmt.Errorf("decode trade size %q: %w", m.Size, err)
	}
	return model.Trade{
		Exchange:     m.Exchange,
		Symbol:       m.Symbol,
		TradeID:      m.TradeId,
		Price:        price,
		Size:         size,
		TakerSide:    sideFromPB(m.TakerSide),
		ExchangeTime: fromUnixNano(m.ExchangeTimeUnixNano),
		ReceiveTime:  fromUnixNano(m.ReceiveTimeUnixNano),
		Snapshot:     m.Snapshot,
	}, nil
}

func EncodeGap(g model.Gap) ([]byte, error) {
	return proto.Marshal(&pb.Gap{
		Exchange:           g.Exchange,
		Symbol:             g.Symbol,
		AfterId:            g.AfterID,
		BeforeId:           g.BeforeID,
		Missing:            g.Missing,
		DetectedAtUnixNano: unixNano(g.DetectedAt),
		Source:             g.Source,
	})
}

func DecodeGap(b []byte) (model.Gap, error) {
	var m pb.Gap
	if err := proto.Unmarshal(b, &m); err != nil {
		return model.Gap{}, fmt.Errorf("decode gap: %w", err)
	}
	return model.Gap{
		Exchange:   m.Exchange,
		Symbol:     m.Symbol,
		AfterID:    m.AfterId,
		BeforeID:   m.BeforeId,
		Missing:    m.Missing,
		DetectedAt: fromUnixNano(m.DetectedAtUnixNano),
		Source:     m.Source,
	}, nil
}

func EncodeCandle(c model.Candle) ([]byte, error) {
	return proto.Marshal(&pb.Candle{
		Exchange:      c.Exchange,
		Symbol:        c.Symbol,
		IntervalMs:    c.Interval.Milliseconds(),
		StartUnixNano: unixNano(c.Start),
		Open:          c.Open.String(),
		High:          c.High.String(),
		Low:           c.Low.String(),
		Close:         c.Close.String(),
		Volume:        c.Volume.String(),
		QuoteVolume:   c.QuoteVolume.String(),
		BuyVolume:     c.BuyVolume.String(),
		SellVolume:    c.SellVolume.String(),
		Trades:        c.Trades,
		FirstTradeId:  c.FirstTradeID,
		LastTradeId:   c.LastTradeID,
	})
}

func DecodeCandle(b []byte) (model.Candle, error) {
	var m pb.Candle
	if err := proto.Unmarshal(b, &m); err != nil {
		return model.Candle{}, fmt.Errorf("decode candle: %w", err)
	}
	c := model.Candle{
		Exchange:     m.Exchange,
		Symbol:       m.Symbol,
		Interval:     time.Duration(m.IntervalMs) * time.Millisecond,
		Start:        fromUnixNano(m.StartUnixNano),
		Trades:       m.Trades,
		FirstTradeID: m.FirstTradeId,
		LastTradeID:  m.LastTradeId,
	}
	var err error
	for _, f := range []struct {
		dst *decimal.Decimal
		src string
	}{
		{&c.Open, m.Open}, {&c.High, m.High}, {&c.Low, m.Low}, {&c.Close, m.Close},
		{&c.Volume, m.Volume}, {&c.QuoteVolume, m.QuoteVolume},
		{&c.BuyVolume, m.BuyVolume}, {&c.SellVolume, m.SellVolume},
	} {
		if *f.dst, err = decimal.NewFromString(f.src); err != nil {
			return model.Candle{}, fmt.Errorf("decode candle decimal %q: %w", f.src, err)
		}
	}
	return c, nil
}

func sideToPB(s model.Side) pb.Side {
	switch s {
	case model.SideBuy:
		return pb.Side_SIDE_BUY
	case model.SideSell:
		return pb.Side_SIDE_SELL
	default:
		return pb.Side_SIDE_UNSPECIFIED
	}
}

func sideFromPB(s pb.Side) model.Side {
	switch s {
	case pb.Side_SIDE_BUY:
		return model.SideBuy
	case pb.Side_SIDE_SELL:
		return model.SideSell
	default:
		return model.SideUnknown
	}
}

// unixNano maps the zero time to 0 (time.Time{}.UnixNano() overflows).
func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromUnixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func EncodeRule(r model.AlertRule) ([]byte, error) {
	return proto.Marshal(&pb.AlertRule{
		Id: r.ID, Exchange: r.Exchange, Symbol: r.Symbol,
		Direction: pb.Direction(r.Direction), Price: r.Price.String(),
		CreatedUnixNano: unixNano(r.CreatedAt), Note: r.Note,
	})
}

func DecodeRule(b []byte) (model.AlertRule, error) {
	var m pb.AlertRule
	if err := proto.Unmarshal(b, &m); err != nil {
		return model.AlertRule{}, fmt.Errorf("decode rule: %w", err)
	}
	price, err := decimal.NewFromString(m.Price)
	if err != nil {
		return model.AlertRule{}, fmt.Errorf("decode rule price %q: %w", m.Price, err)
	}
	return model.AlertRule{
		ID: m.Id, Exchange: m.Exchange, Symbol: m.Symbol, Direction: model.Direction(m.Direction),
		Price: price, CreatedAt: fromUnixNano(m.CreatedUnixNano), Note: m.Note,
	}, nil
}

func EncodeAlert(a model.Alert) ([]byte, error) {
	return proto.Marshal(&pb.Alert{
		Id: a.ID, Type: pb.AlertType(a.Type), Exchange: a.Exchange, Symbol: a.Symbol,
		Message: a.Message, Price: a.Price.String(), TimeUnixNano: unixNano(a.Time),
		RuleId: a.RuleID, TradeId: a.TradeID, Notional: a.Notional.String(),
		ChangePct: a.ChangePct.String(), WindowMs: a.Window.Milliseconds(),
	})
}

func DecodeAlert(b []byte) (model.Alert, error) {
	var m pb.Alert
	if err := proto.Unmarshal(b, &m); err != nil {
		return model.Alert{}, fmt.Errorf("decode alert: %w", err)
	}
	a := model.Alert{
		ID: m.Id, Type: model.AlertType(m.Type), Exchange: m.Exchange, Symbol: m.Symbol,
		Message: m.Message, Time: fromUnixNano(m.TimeUnixNano), RuleID: m.RuleId, TradeID: m.TradeId,
		Window: time.Duration(m.WindowMs) * time.Millisecond,
	}
	var err error
	for _, f := range []struct {
		dst *decimal.Decimal
		src string
	}{{&a.Price, m.Price}, {&a.Notional, m.Notional}, {&a.ChangePct, m.ChangePct}} {
		if *f.dst, err = decimal.NewFromString(f.src); err != nil {
			return model.Alert{}, fmt.Errorf("decode alert decimal %q: %w", f.src, err)
		}
	}
	return a, nil
}

func EncodeSubscription(s model.Subscription) ([]byte, error) {
	return proto.Marshal(&pb.Subscription{
		Exchange: s.Exchange, Symbol: s.Symbol, Pinned: s.Pinned, CreatedUnixNano: unixNano(s.CreatedAt),
	})
}

func DecodeSubscription(b []byte) (model.Subscription, error) {
	var m pb.Subscription
	if err := proto.Unmarshal(b, &m); err != nil {
		return model.Subscription{}, fmt.Errorf("decode subscription: %w", err)
	}
	return model.Subscription{Exchange: m.Exchange, Symbol: m.Symbol, Pinned: m.Pinned, CreatedAt: fromUnixNano(m.CreatedUnixNano)}, nil
}
