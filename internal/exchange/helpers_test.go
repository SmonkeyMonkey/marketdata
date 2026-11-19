package exchange

import (
	"testing"
	"time"

	"marketdata/internal/model"
)

func TestObserveRange(t *testing.T) {
	ids := NewTradeIDs("okx")
	now := time.Unix(0, 0)
	ev, dup := ids.ObserveRange("X", 1, 3, now, nil)
	if dup || len(ev) != 0 {
		t.Fatal("first range")
	}
	if _, dup := ids.ObserveRange("X", 2, 3, now, nil); !dup {
		t.Error("range already seen not dropped")
	}
	if ev, _ := ids.ObserveRange("X", 4, 4, now, nil); len(ev) != 0 {
		t.Error("contiguous range reported a gap")
	}
	ev, _ = ids.ObserveRange("X", 8, 9, now, nil)
	if len(ev) != 1 || ev[0].(model.Gap).Missing != 3 {
		t.Errorf("gap = %+v, want 5..7 missing", ev)
	}
}

func TestResyncsRateLimit(t *testing.T) {
	r := NewResyncs(30 * time.Second)
	t0 := time.Unix(1000, 0)
	if !r.Request("A", t0) || r.Request("A", t0.Add(time.Second)) {
		t.Fatal("first request must pass, the second wait")
	}
	if due := r.Due(t0.Add(10 * time.Second)); len(due) != 0 {
		t.Errorf("due too early: %v", due)
	}
	if due := r.Due(t0.Add(31 * time.Second)); len(due) != 1 || due[0] != "A" {
		t.Errorf("due = %v", due)
	}
	if due := r.Due(t0.Add(time.Hour)); len(due) != 0 {
		t.Errorf("retried twice: %v", due)
	}
	r.Request("B", t0)
	r.Request("B", t0)
	r.Forget("B")
	if due := r.Due(t0.Add(time.Hour)); len(due) != 0 {
		t.Errorf("forgotten symbol retried: %v", due)
	}
}
