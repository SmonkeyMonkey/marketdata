package dedup

import (
	"strconv"
	"testing"
)

// The redundant-ingester check every consumer runs per trade: Seen, then
// Add, with a full 20k window per market.
func BenchmarkSeenAdd(b *testing.B) {
	w := NewWindow(20_000)
	keys := []string{"binance:BTC-USDT", "coinbase:BTC-USD", "kraken:BTC-USD", "okx:BTC-USDT", "bybit:BTC-USDT"}
	ids := make([]string, 1<<16)
	for i := range ids {
		ids[i] = strconv.Itoa(1_000_000_000 + i)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		k, id := keys[i%len(keys)], ids[i%len(ids)]
		if !w.Seen(k, id) {
			w.Add(k, id)
		}
		i++
	}
}
