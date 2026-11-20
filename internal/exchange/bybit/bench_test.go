package bybit

import (
	"bytes"
	"testing"
)

func firstFrame(b *testing.B, prefix string) []byte {
	for _, fr := range session(b) {
		if bytes.HasPrefix(fr, []byte(prefix)) {
			return fr
		}
	}
	b.Fatalf("no frame starting with %s", prefix)
	return nil
}

func BenchmarkHandleTrades(b *testing.B) {
	f := newFeed(b, "BTC-USDT")
	frame := firstFrame(b, `{"topic":"publicTrade.`)
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		f.ids.Forget("BTC-USDT")
		if _, err := f.Handle(frame, t0, nil); err != nil {
			b.Fatal(err)
		}
	}
}
