package exchange

import (
	"strconv"
	"time"

	"marketdata/internal/model"
)

// TradeIDs tracks the last seen trade ID per symbol for exchanges whose trade
// IDs are sequential per symbol (Coinbase, Binance, Kraken). It drops
// duplicates and reports jumps as Gap events. A zero value is not usable;
// call NewTradeIDs.
type TradeIDs struct {
	exchange string
	last     map[string]int64
}

func NewTradeIDs(exchange string) *TradeIDs {
	return &TradeIDs{exchange: exchange, last: make(map[string]int64)}
}

// Observe records id for symbol. It returns dst with a Gap appended if IDs
// were skipped, and dup=true if id was already seen (the trade must be dropped).
func (t *TradeIDs) Observe(symbol string, id int64, now time.Time, dst []model.Event) (_ []model.Event, dup bool) {
	last, seen := t.last[symbol]
	switch {
	case !seen:
	case id <= last:
		return dst, true
	case id > last+1:
		dst = append(dst, t.gap(symbol, last, id, now))
	}
	t.last[symbol] = id
	return dst, false
}

// AdvanceTo handles out-of-band knowledge of the latest trade ID (e.g. a
// heartbeat): trades between the last seen ID and id are reported missing.
// Unknown symbols are ignored, there is no baseline to compare against.
func (t *TradeIDs) AdvanceTo(symbol string, id int64, now time.Time, dst []model.Event) []model.Event {
	last, seen := t.last[symbol]
	if !seen || id <= last {
		return dst
	}
	t.last[symbol] = id
	return append(dst, t.gap(symbol, last, id+1, now))
}

func (t *TradeIDs) gap(symbol string, after, before int64, now time.Time) model.Gap {
	return model.Gap{
		Exchange:   t.exchange,
		Symbol:     symbol,
		AfterID:    strconv.FormatInt(after, 10),
		BeforeID:   strconv.FormatInt(before, 10),
		Missing:    before - after - 1,
		DetectedAt: now,
	}
}
