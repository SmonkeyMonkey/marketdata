package catalog

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"marketdata/internal/exchange"
)

func TestCacheFetchesOnceAndRefreshes(t *testing.T) {
	var calls atomic.Int32
	fail := false
	c := New(time.Minute, map[string]Fetcher{"x": func(context.Context) ([]exchange.Instrument, error) {
		calls.Add(1)
		if fail {
			return nil, errors.New("exchange down")
		}
		return []exchange.Instrument{{Symbol: "B-USD"}, {Symbol: "A-USD"}}, nil
	}})
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 20 { // concurrent first use: one fetch
		wg.Go(func() { _, _ = c.Get(ctx, "x") })
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("fetches = %d, want 1", calls.Load())
	}
	list, _ := c.Get(ctx, "x")
	if list[0].Symbol != "A-USD" {
		t.Error("list not sorted")
	}
	if ok, _ := c.Has(ctx, "x", "B-USD"); !ok {
		t.Error("Has(B-USD) = false")
	}

	now = now.Add(2 * time.Minute)
	fail = true
	if ok, err := c.Has(ctx, "x", "A-USD"); err != nil || !ok {
		t.Errorf("stale list not served on refresh failure: %v %v", ok, err)
	}
	if calls.Load() != 2 {
		t.Errorf("fetches = %d, want a refresh attempt", calls.Load())
	}
	if _, err := c.Get(ctx, "nope"); err == nil {
		t.Error("unknown exchange accepted")
	}
}
