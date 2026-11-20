package okx

import (
	"bytes"
	"testing"
)

func BenchmarkHandleTrades(b *testing.B) {
	f := newFeed(b, "BTC-USDT")
	var frame []byte
	for _, fr := range session(b) {
		if bytes.Contains(fr, []byte(`"channel":"trades"`)) && bytes.Contains(fr, []byte(`"data"`)) {
			frame = fr
			break
		}
	}
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		f.ids.Forget("BTC-USDT")
		if _, err := f.Handle(frame, t0, nil); err != nil {
			b.Fatal(err)
		}
	}
}
