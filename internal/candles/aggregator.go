// Package candles turns the trade stream into OHLCV candles.
//
// Candles are bucketed by exchange time and closed by a per-symbol event-time
// watermark: a bucket closes once a trade at least Lateness past its end
// arrives, or, for quiet symbols, once no trade has come for Idle (wall time).
// A closed bucket is never reopened; trades that arrive for it later are
// counted as late and dropped.
package candles

import (
	"slices"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/dedup"
	"marketdata/internal/model"
)

type Config struct {
	Intervals   []time.Duration
	Lateness    time.Duration
	Idle        time.Duration
	DedupWindow int
}

// Emit receives every closed candle, in start order per symbol and interval.
type Emit func(model.Candle)

// Stats are safe to read concurrently (e.g. from a metrics scrape).
type Stats struct {
	Duplicates  atomic.Int64
	Late        atomic.Int64
	OpenCandles atomic.Int64
}

func (s *Stats) DuplicatesTotal() int64 { return s.Duplicates.Load() }
func (s *Stats) LateTotal() int64       { return s.Late.Load() }
func (s *Stats) Open() int64            { return s.OpenCandles.Load() }

// Aggregator is not safe for concurrent use.
type Aggregator struct {
	cfg   Config
	keys  map[string]*keyState
	seen  *dedup.Window
	known Boundaries
	Stats Stats
}

type keyState struct {
	partition int32
	watermark time.Time // max exchange time seen
	lastWall  time.Time // wall time of the last trade
	// closed[iv] is the start of the newest closed bucket; anything at or
	// before it is final.
	closed map[time.Duration]time.Time
	open   map[time.Duration]map[int64]*openCandle
}

type openCandle struct {
	c model.Candle
	// firstOffset is the Kafka offset of the first trade in the candle. The
	// consumer never commits past it, so after a restart the candle can be
	// rebuilt from Kafka.
	firstOffset int64
}

func New(cfg Config) *Aggregator {
	return &Aggregator{
		cfg:   cfg,
		keys:  make(map[string]*keyState),
		seen:  dedup.NewWindow(cfg.DedupWindow),
		known: Boundaries{},
	}
}

// Restore tells the aggregator which candles were already emitted (e.g.
// before a restart). It applies to symbols not yet in memory.
func (a *Aggregator) Restore(b Boundaries) {
	for key, ivs := range b {
		for iv, start := range ivs {
			a.known.observe(key, iv, start)
		}
	}
}

// Add feeds one trade read from partition at offset.
func (a *Aggregator) Add(t model.Trade, partition int32, offset int64, now time.Time, emit Emit) {
	key := t.Exchange + ":" + t.Symbol
	if a.seen.Seen(key, t.TradeID) {
		a.Stats.Duplicates.Add(1)
		return
	}
	a.seen.Add(key, t.TradeID)

	ks := a.state(key, partition)
	ks.lastWall = now
	if t.ExchangeTime.After(ks.watermark) {
		ks.watermark = t.ExchangeTime
	}

	late := false
	for _, iv := range a.cfg.Intervals {
		start := t.ExchangeTime.Truncate(iv)
		if !start.After(ks.closed[iv]) {
			late = true
			continue
		}
		bucket := ks.open[iv]
		oc, ok := bucket[start.UnixNano()]
		if !ok {
			oc = &openCandle{c: newCandle(t, iv, start), firstOffset: offset}
			bucket[start.UnixNano()] = oc
			a.Stats.OpenCandles.Add(1)
			continue
		}
		addTrade(&oc.c, t)
		oc.firstOffset = min(oc.firstOffset, offset)
	}
	if late {
		a.Stats.Late.Add(1)
	}
	a.closeThrough(key, ks, ks.watermark.Add(-a.cfg.Lateness), emit)
}

// Tick closes candles of symbols that have been quiet for Idle, using wall
// time in place of the watermark. Call it periodically.
func (a *Aggregator) Tick(now time.Time, emit Emit) {
	for key, ks := range a.keys {
		if now.Sub(ks.lastWall) >= a.cfg.Idle {
			a.closeThrough(key, ks, now.Add(-a.cfg.Lateness), emit)
		}
	}
}

// SafeOffsets returns, per partition, the offset of the oldest trade still
// needed by an open candle. Committing beyond it would lose that candle on a
// restart. Partitions without open candles are absent.
func (a *Aggregator) SafeOffsets() map[int32]int64 {
	out := make(map[int32]int64)
	for _, ks := range a.keys {
		for _, bucket := range ks.open {
			for _, oc := range bucket {
				if cur, ok := out[ks.partition]; !ok || oc.firstOffset < cur {
					out[ks.partition] = oc.firstOffset
				}
			}
		}
	}
	return out
}

// Drop forgets symbols of the given partitions, e.g. after they were
// reassigned to another consumer. Their open candles are discarded: the new
// owner rebuilds them from the committed offset.
func (a *Aggregator) Drop(partitions map[int32]bool) {
	for key, ks := range a.keys {
		if !partitions[ks.partition] {
			continue
		}
		for _, bucket := range ks.open {
			a.Stats.OpenCandles.Add(-int64(len(bucket)))
		}
		delete(a.keys, key)
		a.seen.Forget(key)
	}
}

func (a *Aggregator) state(key string, partition int32) *keyState {
	ks, ok := a.keys[key]
	if ok {
		return ks
	}
	ks = &keyState{
		partition: partition,
		closed:    make(map[time.Duration]time.Time, len(a.cfg.Intervals)),
		open:      make(map[time.Duration]map[int64]*openCandle, len(a.cfg.Intervals)),
	}
	for _, iv := range a.cfg.Intervals {
		ks.closed[iv] = a.known[key][iv] // zero time if nothing was emitted before
		ks.open[iv] = make(map[int64]*openCandle)
	}
	a.keys[key] = ks
	return ks
}

// closeThrough emits every candle that ended at or before cutoff and marks
// all buckets up to cutoff as closed, including empty ones.
func (a *Aggregator) closeThrough(key string, ks *keyState, cutoff time.Time, emit Emit) {
	for _, iv := range a.cfg.Intervals {
		lastClosed := cutoff.Add(-iv).Truncate(iv)
		bucket := ks.open[iv]
		var ready []*openCandle
		for start, oc := range bucket {
			if !time.Unix(0, start).After(lastClosed) {
				ready = append(ready, oc)
				delete(bucket, start)
			}
		}
		slices.SortFunc(ready, func(x, y *openCandle) int { return x.c.Start.Compare(y.c.Start) })
		for _, oc := range ready {
			a.Stats.OpenCandles.Add(-1)
			emit(oc.c)
		}
		if lastClosed.After(ks.closed[iv]) {
			ks.closed[iv] = lastClosed
		}
	}
}

func newCandle(t model.Trade, iv time.Duration, start time.Time) model.Candle {
	c := model.Candle{
		Exchange: t.Exchange, Symbol: t.Symbol, Interval: iv, Start: start,
		Open: t.Price, High: t.Price, Low: t.Price, Close: t.Price,
		Volume: t.Size, QuoteVolume: t.Price.Mul(t.Size),
		BuyVolume: decimal.Zero, SellVolume: decimal.Zero,
		Trades: 1, FirstTradeID: t.TradeID, LastTradeID: t.TradeID,
	}
	addSide(&c, t)
	return c
}

// addTrade folds a trade into a candle. Trades arrive in exchange order per
// symbol, so the latest one is the close.
func addTrade(c *model.Candle, t model.Trade) {
	if t.Price.GreaterThan(c.High) {
		c.High = t.Price
	}
	if t.Price.LessThan(c.Low) {
		c.Low = t.Price
	}
	c.Close = t.Price
	c.Volume = c.Volume.Add(t.Size)
	c.QuoteVolume = c.QuoteVolume.Add(t.Price.Mul(t.Size))
	c.Trades++
	c.LastTradeID = t.TradeID
	addSide(c, t)
}

func addSide(c *model.Candle, t model.Trade) {
	switch t.TakerSide {
	case model.SideBuy:
		c.BuyVolume = c.BuyVolume.Add(t.Size)
	case model.SideSell:
		c.SellVolume = c.SellVolume.Add(t.Size)
	}
}

// Boundaries maps exchange:symbol -> interval -> start of the newest candle
// already emitted.
type Boundaries map[string]map[time.Duration]time.Time

func (b Boundaries) observe(key string, iv time.Duration, start time.Time) {
	m, ok := b[key]
	if !ok {
		m = make(map[time.Duration]time.Time)
		b[key] = m
	}
	if start.After(m[iv]) {
		m[iv] = start
	}
}

// Observe records an emitted candle.
func (b Boundaries) Observe(c model.Candle) { b.observe(c.Exchange+":"+c.Symbol, c.Interval, c.Start) }
