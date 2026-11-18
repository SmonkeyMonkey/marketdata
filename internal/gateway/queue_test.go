package gateway

import (
	"fmt"
	"testing"
)

func TestQueueBoundsAndOrder(t *testing.T) {
	q := newQueue(3)
	for i := range 3 {
		if !q.push([]byte(fmt.Sprint(i))) {
			t.Fatalf("push %d refused", i)
		}
	}
	if q.push([]byte("x")) {
		t.Fatal("push beyond max accepted")
	}
	<-q.ready
	got := q.drain()
	if len(got) != 3 || string(got[0]) != "0" || string(got[2]) != "2" {
		t.Fatalf("drain = %q", got)
	}
	if !q.push([]byte("3")) {
		t.Fatal("push after drain refused")
	}
	<-q.ready
	if got := q.drain(); len(got) != 1 || string(got[0]) != "3" {
		t.Fatalf("second drain = %q", got)
	}
}

func TestQueueSignalsOncePerBacklog(t *testing.T) {
	q := newQueue(10)
	q.push([]byte("a"))
	q.push([]byte("b")) // must not block on the full ready channel
	<-q.ready
	if n := len(q.drain()); n != 2 {
		t.Fatalf("drained %d, want 2", n)
	}
	select {
	case <-q.ready:
		t.Fatal("stale wake-up token")
	default:
	}
}
