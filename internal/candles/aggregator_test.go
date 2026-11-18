package candles

import (
	"strconv"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

var t0 = time.Date(2026, 11, 17, 15, 0, 0, 0, time.UTC)

func cfg() Config {
	return Config{Intervals: []time.Duration{time.Second, time.Minute}, Lateness: 2 * time.Second, Idle: 5 * time.Second, DedupWindow: 1000}
}

type sink struct{ got []model.Candle }

func (s *sink) emit(c model.Candle) { s.got = append(s.got, c) }

func (s *sink) only(iv time.Duration) []model.Candle {
	var out []model.Candle
	for _, c := range s.got {
		if c.Interval == iv {
			out = append(out, c)
		}
	}
	return out
}

func trade(id int, at time.Duration, price, size string, side model.Side) model.Trade {
	return model.Trade{
		Exchange: "coinbase", Symbol: "BTC-USD", TradeID: strconv.Itoa(id),
		Price: decimal.RequireFromString(price), Size: decimal.RequireFromString(size),
		TakerSide: side, ExchangeTime: t0.Add(at),
	}
}

func TestBuildsOHLCVAndClosesOnWatermark(t *testing.T) {
	a, s := New(cfg()), &sink{}
	a.Add(trade(1, 100*time.Millisecond, "100", "1", model.SideBuy), 0, 10, t0, s.emit)
	a.Add(trade(2, 300*time.Millisecond, "105", "2", model.SideSell), 0, 11, t0, s.emit)
	a.Add(trade(3, 600*time.Millisecond, "98", "1", model.SideBuy), 0, 12, t0, s.emit)
	a.Add(trade(4, 900*time.Millisecond, "101", "0.5", model.SideSell), 0, 13, t0, s.emit)
	if len(s.got) != 0 {
		t.Fatalf("emitted %d candles before the watermark passed", len(s.got))
	}

	// 1s bucket [0s,1s) closes when a trade at >= 1s + 2s lateness arrives.
	a.Add(trade(5, 3*time.Second, "102", "1", model.SideBuy), 0, 14, t0, s.emit)
	got := s.only(time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d 1s candles, want 1", len(got))
	}
	c := got[0]
	d := decimal.RequireFromString
	if !c.Start.Equal(t0) || !c.Open.Equal(d("100")) || !c.High.Equal(d("105")) || !c.Low.Equal(d("98")) ||
		!c.Close.Equal(d("101")) || !c.Volume.Equal(d("4.5")) || c.Trades != 4 ||
		!c.BuyVolume.Equal(d("2")) || !c.SellVolume.Equal(d("2.5")) ||
		!c.QuoteVolume.Equal(d("458.5")) || // 100*1 + 105*2 + 98*1 + 101*0.5
		c.FirstTradeID != "1" || c.LastTradeID != "4" {
		t.Errorf("candle = %+v", c)
	}
	if len(s.only(time.Minute)) != 0 {
		t.Error("1m candle closed too early")
	}
}

func TestDuplicatesAreIgnored(t *testing.T) {
	a, s := New(cfg()), &sink{}
	tr := trade(1, 0, "100", "1", model.SideBuy)
	a.Add(tr, 0, 1, t0, s.emit)
	a.Add(tr, 0, 2, t0, s.emit) // second ingester's copy
	a.Add(trade(2, 5*time.Second, "100", "1", model.SideBuy), 0, 3, t0, s.emit)
	if c := s.only(time.Second)[0]; c.Trades != 1 {
		t.Errorf("trades = %d, want 1", c.Trades)
	}
	if a.Stats.Duplicates.Load() != 1 {
		t.Errorf("duplicates = %d", a.Stats.Duplicates.Load())
	}
}

func TestLateTradeDoesNotReopenCandle(t *testing.T) {
	a, s := New(cfg()), &sink{}
	a.Add(trade(1, 0, "100", "1", model.SideBuy), 0, 1, t0, s.emit)
	a.Add(trade(2, 5*time.Second, "100", "1", model.SideBuy), 0, 2, t0, s.emit) // closes [0s,1s)
	a.Add(trade(3, 500*time.Millisecond, "999", "1", model.SideBuy), 0, 3, t0, s.emit)
	a.Add(trade(4, 10*time.Second, "100", "1", model.SideBuy), 0, 4, t0, s.emit)
	for _, c := range s.only(time.Second) {
		if c.Start.Equal(t0) && c.Trades != 1 {
			t.Errorf("closed candle changed: %+v", c)
		}
	}
	if n := len(s.only(time.Second)); n != 2 {
		t.Errorf("1s candles = %d, want 2 (0s and 5s; the late trade must not emit)", n)
	}
	if a.Stats.Late.Load() != 1 {
		t.Errorf("late = %d", a.Stats.Late.Load())
	}
}

func TestEmptyBucketsAreSkipped(t *testing.T) {
	a, s := New(cfg()), &sink{}
	a.Add(trade(1, 0, "100", "1", model.SideBuy), 0, 1, t0, s.emit)
	a.Add(trade(2, 10*time.Second, "100", "1", model.SideBuy), 0, 2, t0, s.emit)
	if n := len(s.only(time.Second)); n != 1 {
		t.Errorf("1s candles = %d, want 1 (no candles for empty seconds)", n)
	}
}

func TestIdleSymbolIsClosedByWallClock(t *testing.T) {
	a, s := New(cfg()), &sink{}
	a.Add(trade(1, 0, "100", "1", model.SideBuy), 0, 1, t0, s.emit)
	a.Tick(t0.Add(4*time.Second), s.emit)
	if len(s.got) != 0 {
		t.Fatal("closed before idle timeout")
	}
	a.Tick(t0.Add(6*time.Second), s.emit)
	if n := len(s.only(time.Second)); n != 1 {
		t.Errorf("1s candles = %d after idle, want 1", n)
	}
}

func TestSafeOffsetsTrackOldestOpenCandle(t *testing.T) {
	a, s := New(cfg()), &sink{}
	a.Add(trade(1, 0, "100", "1", model.SideBuy), 3, 50, t0, s.emit)
	a.Add(trade(2, 5*time.Second, "100", "1", model.SideBuy), 3, 51, t0, s.emit)
	// The 1s candle at 0s is closed, but the 1m candle still needs offset 50.
	if got := a.SafeOffsets()[3]; got != 50 {
		t.Errorf("safe offset = %d, want 50", got)
	}
}

// After a restart the consumer replays from the safe offset; candles already
// emitted (known from the output topic) must not be emitted again.
func TestRestoreSkipsAlreadyEmittedCandles(t *testing.T) {
	a, s := New(cfg()), &sink{}
	a.Restore(Boundaries{"coinbase:BTC-USD": {time.Second: t0}})
	a.Add(trade(1, 500*time.Millisecond, "100", "1", model.SideBuy), 0, 1, t0, s.emit) // belongs to emitted 0s
	a.Add(trade(2, 1500*time.Millisecond, "101", "1", model.SideBuy), 0, 2, t0, s.emit)
	a.Add(trade(3, 5*time.Second, "102", "1", model.SideBuy), 0, 3, t0, s.emit)
	got := s.only(time.Second)
	if len(got) != 1 || !got[0].Start.Equal(t0.Add(time.Second)) {
		t.Errorf("1s candles = %+v, want only the 1s bucket", got)
	}
}

func TestDropForgetsPartition(t *testing.T) {
	a, s := New(cfg()), &sink{}
	tr := trade(1, 0, "100", "1", model.SideBuy)
	a.Add(tr, 7, 1, t0, s.emit)
	a.Drop(map[int32]bool{7: true})
	if len(a.SafeOffsets()) != 0 || a.Stats.OpenCandles.Load() != 0 {
		t.Error("state survived Drop")
	}
	a.Add(tr, 7, 1, t0, s.emit) // replayed by the new owner: not a duplicate
	if a.Stats.Duplicates.Load() != 0 {
		t.Error("dedup state survived Drop")
	}
}
