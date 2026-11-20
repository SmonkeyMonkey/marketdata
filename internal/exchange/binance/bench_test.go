package binance

import "testing"

// Decoding one trade frame into a normalized trade. The ID tracker is reset
// each round, so every frame takes the full path instead of the
// duplicate shortcut.
func BenchmarkHandleTrade(b *testing.B) {
	f, _ := New(baseURL, []string{"BTC-USDT"})
	frame := frame(b, "trade_taker_buy.json")
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		f.ids.Forget("BTC-USDT")
		if _, err := f.Handle(frame, now, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// A depth20 partial snapshot replaces the local top of book.
func BenchmarkHandleDepth(b *testing.B) {
	f, _ := New(baseURL, []string{"BTC-USDT"})
	frame := frame(b, "depth20.json")
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.Handle(frame, now, nil); err != nil {
			b.Fatal(err)
		}
	}
}
