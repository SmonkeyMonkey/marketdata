// Package exchange defines the contract every exchange integration implements.
//
// Each exchange speaks its own websocket dialect (subscribe format, field
// names, maker vs taker side, string vs float numbers). A Feed converts that
// dialect to the normalized model, while ingest.Runner owns everything
// protocol-independent: dialing, reconnects, timeouts and publishing.
package exchange

import (
	"time"

	"marketdata/internal/model"
)

// Feed converts one exchange websocket stream into normalized events.
//
// Handle is called sequentially from a single goroutine, so implementations
// may keep state (e.g. last seen trade IDs) without locking. That state lives
// across reconnects, which is what makes gap detection after a reconnect work.
type Feed interface {
	// Name is the exchange identifier used in events, logs and metrics.
	Name() string
	// URL is the websocket endpoint.
	URL() string
	// Subscriptions returns the frames to send right after connecting.
	Subscriptions() ([][]byte, error)
	// Handle decodes one frame and appends resulting events to dst.
	// Unknown message types must be ignored; a returned error ends the session.
	Handle(frame []byte, received time.Time, dst []model.Event) ([]model.Event, error)
}

// Dynamic is implemented by feeds whose symbol set can change while the
// websocket is open. SetSymbols replaces the set and returns the frames to
// send on the live connection (subscribe/unsubscribe); the next reconnect
// uses the new set from the start.
type Dynamic interface {
	SetSymbols(symbols []string) ([][]byte, error)
}

// SymbolError is returned by Handle when the exchange rejected one symbol.
// It does not end the session: the other symbols keep streaming.
type SymbolError struct {
	Symbol string
	Reason string
}

func (e *SymbolError) Error() string { return "symbol " + e.Symbol + " rejected: " + e.Reason }

// Instrument is a tradable market in canonical form.
type Instrument struct {
	Symbol string `json:"symbol"` // BASE-QUOTE
	Base   string `json:"base"`
	Quote  string `json:"quote"`
}

// Diff returns the elements of next not in prev (added) and of prev not in
// next (removed).
func Diff(prev, next []string) (added, removed []string) {
	in := func(s []string, v string) bool {
		for _, x := range s {
			if x == v {
				return true
			}
		}
		return false
	}
	for _, v := range next {
		if !in(prev, v) {
			added = append(added, v)
		}
	}
	for _, v := range prev {
		if !in(next, v) {
			removed = append(removed, v)
		}
	}
	return added, removed
}
