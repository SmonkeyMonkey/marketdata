package coinbase

import "testing"

func BenchmarkHandleMatch(b *testing.B) {
	f := New(testURL, []string{"BTC-USD"})
	frame := frame(b, "match.json")
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		f.ids.Forget("BTC-USD")
		if _, err := f.Handle(frame, now, nil); err != nil {
			b.Fatal(err)
		}
	}
}
