package book

import (
	"testing"
	"time"

	"marketdata/internal/model"
)

func TestTrackerRateLimitsAndFlushes(t *testing.T) {
	tr := NewTracker("x")
	t0 := time.Unix(1000, 0)
	b := tr.Book("BTC-USD")
	b.SetBid(d("1"), d("1"))
	b.SetAsk(d("2"), d("1"))
	if ev := tr.Changed("BTC-USD", t0, t0, 0, nil); len(ev) != 0 {
		t.Fatal("published before the snapshot was applied")
	}
	tr.Snapshot("BTC-USD")
	if ev := tr.Changed("BTC-USD", t0, t0, 0, nil); len(ev) != 1 {
		t.Fatalf("first change: %d events", len(ev))
	}
	b.SetBid(d("1.5"), d("1"))
	if ev := tr.Changed("BTC-USD", t0, t0.Add(30*time.Millisecond), 0, nil); len(ev) != 0 {
		t.Fatal("published within the rate limit")
	}
	if ev := tr.Flush(t0.Add(60*time.Millisecond), nil); len(ev) != 0 {
		t.Fatal("flushed within the rate limit")
	}
	ev := tr.Flush(t0.Add(120*time.Millisecond), nil)
	if len(ev) != 1 || !ev[0].(model.Book).Bids[0].Price.Equal(d("1.5")) {
		t.Fatalf("pending change not flushed: %+v", ev)
	}
	if ev := tr.Flush(t0.Add(500*time.Millisecond), nil); len(ev) != 0 {
		t.Fatal("unchanged book republished")
	}
}

func TestTrackerHoldsCrossedBook(t *testing.T) {
	tr := NewTracker("x")
	tr.Snapshot("S")
	tr.Book("S").SetBid(d("10"), d("1"))
	tr.Book("S").SetAsk(d("9"), d("1"))
	if ev := tr.Changed("S", time.Unix(1, 0), time.Unix(1, 0), 0, nil); len(ev) != 0 {
		t.Error("published a crossed book")
	}
}
