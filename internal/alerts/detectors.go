package alerts

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

// Whale flags single trades whose notional (price*size, in quote currency)
// reaches Threshold.
type Whale struct {
	Threshold decimal.Decimal
}

func (w Whale) Check(t model.Trade) (model.Alert, bool) {
	notional := t.Price.Mul(t.Size)
	if w.Threshold.IsZero() || notional.LessThan(w.Threshold) {
		return model.Alert{}, false
	}
	_, quote := splitQuote(t.Symbol)
	return model.Alert{
		ID:   "whale:" + t.Exchange + ":" + t.Symbol + ":" + t.TradeID,
		Type: model.AlertWhaleTrade, Exchange: t.Exchange, Symbol: t.Symbol,
		Price: t.Price, Time: t.ExchangeTime, TradeID: t.TradeID, Notional: notional.Round(2),
		Message: fmt.Sprintf("%s %s: taker %s %s at %s (%s %s)", t.Exchange, t.Symbol, t.TakerSide,
			t.Size, t.Price, notional.Round(0), quote),
	}, true
}

// Move flags a price change of at least Pct percent within Window (exchange
// time), in either direction, then stays quiet for Cooldown per symbol and
// direction so one move is reported once.
//
// Per symbol it keeps two monotonic deques over the window: one whose front
// is the minimum price, one whose front is the maximum. Each trade is pushed
// and popped at most once, so the check is O(1) amortized.
type Move struct {
	Pct      decimal.Decimal
	Window   time.Duration
	Cooldown time.Duration

	state map[string]*moveState
}

type point struct {
	at time.Time
	px int64
	p  decimal.Decimal
}

type moveState struct {
	mins, maxs []point // monotonic deques
	quietUntil [2]time.Time
}

func (m *Move) Check(t model.Trade) (model.Alert, bool) {
	if m.Pct.IsZero() {
		return model.Alert{}, false
	}
	if m.state == nil {
		m.state = make(map[string]*moveState)
	}
	k := key(t.Exchange, t.Symbol)
	s, ok := m.state[k]
	if !ok {
		s = &moveState{}
		m.state[k] = s
	}
	now := t.ExchangeTime
	p := point{at: now, px: toFixed(t.Price), p: t.Price}

	cutoff := now.Add(-m.Window)
	for len(s.mins) > 0 && s.mins[0].at.Before(cutoff) {
		s.mins = s.mins[1:]
	}
	for len(s.maxs) > 0 && s.maxs[0].at.Before(cutoff) {
		s.maxs = s.maxs[1:]
	}
	for len(s.mins) > 0 && s.mins[len(s.mins)-1].px >= p.px {
		s.mins = s.mins[:len(s.mins)-1]
	}
	s.mins = append(s.mins, p)
	for len(s.maxs) > 0 && s.maxs[len(s.maxs)-1].px <= p.px {
		s.maxs = s.maxs[:len(s.maxs)-1]
	}
	s.maxs = append(s.maxs, p)

	lo, hi := s.mins[0], s.maxs[0]
	hundred := decimal.NewFromInt(100)
	// Up: from the window's low to now. Down: from the window's high to now.
	if up := t.Price.Sub(lo.p).Div(lo.p).Mul(hundred); up.GreaterThanOrEqual(m.Pct) && now.After(s.quietUntil[0]) {
		s.quietUntil[0] = now.Add(m.Cooldown)
		return m.alert(t, "up", up, lo), true
	}
	if down := hi.p.Sub(t.Price).Div(hi.p).Mul(hundred); down.GreaterThanOrEqual(m.Pct) && now.After(s.quietUntil[1]) {
		s.quietUntil[1] = now.Add(m.Cooldown)
		return m.alert(t, "down", down.Neg(), hi), true
	}
	return model.Alert{}, false
}

func (m *Move) alert(t model.Trade, dir string, pct decimal.Decimal, from point) model.Alert {
	return model.Alert{
		ID:   fmt.Sprintf("move:%s:%s:%s:%d", t.Exchange, t.Symbol, dir, t.ExchangeTime.UnixMilli()),
		Type: model.AlertPriceMove, Exchange: t.Exchange, Symbol: t.Symbol,
		Price: t.Price, Time: t.ExchangeTime, TradeID: t.TradeID,
		ChangePct: pct.Round(3), Window: m.Window,
		Message: fmt.Sprintf("%s %s %s %s%% in %s: %s -> %s", t.Exchange, t.Symbol, dir, pct.Abs().Round(2),
			t.ExchangeTime.Sub(from.at).Round(time.Second), from.p, t.Price),
	}
}

func splitQuote(symbol string) (base, quote string) {
	for i := len(symbol) - 1; i >= 0; i-- {
		if symbol[i] == '-' {
			return symbol[:i], symbol[i+1:]
		}
	}
	return symbol, ""
}
