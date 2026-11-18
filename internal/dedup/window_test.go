package dedup

import (
	"strconv"
	"testing"
)

func TestSeenAfterAdd(t *testing.T) {
	w := NewWindow(3)
	if w.Seen("a", "1") {
		t.Fatal("empty window reports seen")
	}
	w.Add("a", "1")
	if !w.Seen("a", "1") {
		t.Error("added id not seen")
	}
	if w.Seen("b", "1") {
		t.Error("keys must be independent")
	}
}

func TestEvictsOldest(t *testing.T) {
	w := NewWindow(3)
	for i := 1; i <= 5; i++ {
		w.Add("k", strconv.Itoa(i))
	}
	for id, want := range map[string]bool{"1": false, "2": false, "3": true, "4": true, "5": true} {
		if got := w.Seen("k", id); got != want {
			t.Errorf("Seen(%s) = %v, want %v", id, got, want)
		}
	}
}

func TestReAddDoesNotEvict(t *testing.T) {
	w := NewWindow(2)
	w.Add("k", "1")
	w.Add("k", "2")
	w.Add("k", "2") // must not push "1" out
	if !w.Seen("k", "1") {
		t.Error("duplicate Add evicted an entry")
	}
}

func TestMemoryIsBounded(t *testing.T) {
	w := NewWindow(100)
	for i := range 10_000 {
		w.Add("k", strconv.Itoa(i))
	}
	r := w.keys["k"]
	if len(r.ids) != 100 || len(r.set) != 100 {
		t.Errorf("ids=%d set=%d, want 100", len(r.ids), len(r.set))
	}
}
