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
