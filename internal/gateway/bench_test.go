package gateway

import (
	"strconv"
	"testing"
	"time"
)

// nopSub counts deliveries. It must not be zero-sized: pointers to
// zero-sized values may all be equal, and the hub would see one subscriber.
type nopSub struct{ n int }

func (s *nopSub) enqueue([]byte) bool { s.n++; return true }
func (s *nopSub) slow()               {}

// Fan-out of one message: the hub hands the same encoded bytes to every
// subscriber's queue (no copy, no re-encoding).
func BenchmarkPublish(b *testing.B) {
	for _, n := range []int{1, 100, 10_000} {
		b.Run(strconv.Itoa(n)+"_subscribers", func(b *testing.B) {
			h := NewHub()
			for range n {
				h.Subscribe("trades:binance:BTC-USDT", &nopSub{})
			}
			if got := h.Subscribers("trades:binance:BTC-USDT"); got != n {
				b.Fatalf("subscribers = %d, want %d", got, n)
			}
			msg, _ := encodeTrade(trade, time.Now())
			b.ReportAllocs()
			for b.Loop() {
				h.Publish("trades:binance:BTC-USDT", msg)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/delivery")
		})
	}
}

// JSON encoding of a trade for clients, once per trade.
func BenchmarkEncodeTrade(b *testing.B) {
	now := time.Now()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := encodeTrade(trade, now); err != nil {
			b.Fatal(err)
		}
	}
}
