package candles

import (
	"strconv"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

// One trade into 1s and 1m candles: 20 markets, 50 trades a second each, so
// candles close continuously as event time advances.
func BenchmarkAdd(b *testing.B) {
	a := New(Config{Intervals: []time.Duration{time.Second, time.Minute}, Lateness: 2 * time.Second, Idle: 5 * time.Second, DedupWindow: 20_000})
	const markets = 20
	trades := make([]model.Trade, 100_000)
	for i := range trades {
		trades[i] = model.Trade{
			Exchange: "binance", Symbol: "S" + strconv.Itoa(i%markets) + "-USDT", TradeID: strconv.Itoa(i),
			Price: decimal.NewFromInt(int64(60_000 + i%100)), Size: decimal.RequireFromString("0.01"),
			TakerSide: model.Side(1 + i%2), ExchangeTime: t0.Add(time.Duration(i) * time.Second / (50 * markets)),
		}
	}
	emitted := 0
	emit := func(model.Candle) { emitted++ }
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		t := trades[i%len(trades)]
		// Keep IDs and time moving forward across laps of the slice.
		lap := i / len(trades)
		if lap > 0 {
			t.TradeID = strconv.Itoa(i)
			t.ExchangeTime = t.ExchangeTime.Add(time.Duration(lap) * time.Duration(len(trades)) * time.Second / (50 * markets))
		}
		a.Add(t, int32(i%12), int64(i), t.ExchangeTime, emit)
		i++
	}
	b.ReportMetric(float64(emitted)/float64(b.N), "candles/trade")
}
