package exchange

import (
	"maps"
	"slices"
	"time"
)

// Resyncs limits book resubscriptions per symbol. A book that diverged is
// resubscribed for a fresh snapshot; if it kept diverging (a bug on our
// side), resubscribing on every update would flood the exchange and could
// get the connection rate limited or banned. Not safe for concurrent use.
type Resyncs struct {
	every   time.Duration
	last    map[string]time.Time
	pending map[string]bool
}

func NewResyncs(every time.Duration) *Resyncs {
	return &Resyncs{every: every, last: map[string]time.Time{}, pending: map[string]bool{}}
}

// Request reports whether symbol may be resubscribed now. If not, it is
// remembered and returned by Due once the interval has passed.
func (r *Resyncs) Request(symbol string, now time.Time) bool {
	if now.Sub(r.last[symbol]) < r.every {
		r.pending[symbol] = true
		return false
	}
	r.last[symbol] = now
	delete(r.pending, symbol)
	return true
}

// Due returns the waiting symbols that may be resubscribed now, sorted.
func (r *Resyncs) Due(now time.Time) []string {
	var out []string
	for _, s := range slices.Sorted(maps.Keys(r.pending)) {
		if r.Request(s, now) {
			out = append(out, s)
		}
	}
	return out
}

// Forget drops a symbol (unsubscribed): nothing is retried for it.
func (r *Resyncs) Forget(symbol string) {
	delete(r.last, symbol)
	delete(r.pending, symbol)
}
