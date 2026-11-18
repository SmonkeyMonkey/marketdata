package gateway

import "sync"

// queue is a bounded per-client message queue whose memory follows the
// actual backlog. A buffered channel of the same bound would allocate every
// slot up front for every connection; most clients are almost always empty.
type queue struct {
	mu    sync.Mutex
	items [][]byte
	spare [][]byte // the previously drained slice, reused to avoid reallocating
	max   int
	// ready has capacity 1 and holds a token while items is non-empty.
	ready chan struct{}
}

func newQueue(max int) *queue {
	return &queue{max: max, ready: make(chan struct{}, 1)}
}

// push appends msg unless the queue is full; it never blocks.
func (q *queue) push(msg []byte) bool {
	q.mu.Lock()
	if len(q.items) >= q.max {
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, msg)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default: // a token is already waiting
	}
	return true
}

// drain takes everything queued so far. The returned slice is valid until the
// next drain.
func (q *queue) drain() [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.items
	clear(q.spare)
	q.items, q.spare = q.spare[:0], out
	// After a burst, give the big buffer back instead of keeping it forever.
	if cap(q.items) > 64 {
		q.items = nil
	}
	return out
}
