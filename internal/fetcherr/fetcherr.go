// Package fetcherr handles Kafka fetch errors the same way in every consumer:
// logs are rate limited, and errors that do not stop end the process.
//
// Some errors never clear on their own. After a topic is deleted and
// recreated, a client keeps fetching the old topic ID and gets
// UNKNOWN_TOPIC_ID back immediately, in a tight loop (it logged ~140k lines
// a minute per service). Exiting lets the container restart policy start the
// process again with a fresh client.
package fetcherr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Watch struct {
	log   *slog.Logger
	limit time.Duration
	// LogEvery is the minimum gap between two logged errors.
	LogEvery time.Duration
	// Quiet is how long without an error ends a failure streak.
	Quiet time.Duration
	now   func() time.Time

	failingSince time.Time
	lastErr      time.Time
	lastLog      time.Time
	suppressed   int
}

// New fails consumers whose fetches keep failing for longer than limit.
func New(log *slog.Logger, limit time.Duration) *Watch {
	return &Watch{log: log, limit: limit, LogEvery: 5 * time.Second, Quiet: 5 * time.Second, now: time.Now}
}

// Check logs the fetch errors of one poll and returns an error once they
// have not stopped for longer than the limit. Cancelled polls (deadline,
// shutdown) are not errors.
func (w *Watch) Check(f kgo.Fetches) error {
	var (
		first error
		topic string
		part  int32
		n     int
	)
	f.EachError(func(t string, p int32, err error) {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return
		}
		if n++; first == nil {
			first, topic, part = err, t, p
		}
	})
	now := w.now()
	if n == 0 {
		if !w.failingSince.IsZero() && now.Sub(w.lastErr) >= w.Quiet {
			w.log.Info("kafka fetches recovered", "failed_for", w.lastErr.Sub(w.failingSince).Round(time.Millisecond), "suppressed", w.suppressed)
			w.failingSince, w.suppressed = time.Time{}, 0
		}
		return nil
	}
	if w.failingSince.IsZero() {
		w.failingSince = now
	}
	w.lastErr = now
	failing := now.Sub(w.failingSince)
	if now.Sub(w.lastLog) >= w.LogEvery {
		w.log.Error("kafka fetch error", "topic", topic, "partition", part, "err", first,
			"errors", n, "suppressed", w.suppressed, "failing_for", failing.Round(time.Millisecond))
		w.lastLog, w.suppressed = now, 0
	} else {
		w.suppressed += n
	}
	if failing > w.limit {
		return fmt.Errorf("kafka fetches failing for %s: %s/%d: %w", failing.Round(time.Second), topic, part, first)
	}
	return nil
}
