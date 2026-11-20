package kraken

import "testing"

// A frame with two trades (Kraken sends numbers, decoded straight into
// decimals).
func BenchmarkHandleTrades(b *testing.B) {
	f := newFeed(b)
	frame := frame(b, "update.json")
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		f.ids.Forget("BTC-USD")
		if _, err := f.Handle(frame, now, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// Applying real book updates, each verified with the CRC32 of the top 10
// levels; the snapshot is reapplied every round so the updates stay valid.
func BenchmarkBookUpdatesWithChecksum(b *testing.B) {
	f := newFeed(b)
	if _, err := f.Handle(instrument, now, nil); err != nil {
		b.Fatal(err)
	}
	snap, updates := bookFrames(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.Handle(snap, now, nil); err != nil {
			b.Fatal(err)
		}
		for _, u := range updates {
			ev, err := f.Handle(u, now, nil)
			if err != nil {
				b.Fatal(err)
			}
			for _, e := range ev {
				if e.Kind() == "book_resync" {
					b.Fatal("checksum mismatch")
				}
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*(len(updates)+1)), "ns/frame")
}
