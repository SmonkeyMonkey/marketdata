// Package ingest runs exchange feeds: it owns the websocket connection
// lifecycle and hands decoded events to a Publisher.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"marketdata/internal/backoff"
	"marketdata/internal/exchange"
	"marketdata/internal/metrics"
	"marketdata/internal/model"
)

// Publisher receives normalized events. Today it writes to stdout; the Kafka
// implementation will plug in here without changing the runner.
type Publisher interface {
	Publish(ctx context.Context, events []model.Event) error
}

// Runner keeps one Feed connected, reconnecting with backoff on any failure.
type Runner struct {
	Feed      exchange.Feed
	Publisher Publisher
	Log       *slog.Logger
	Backoff   backoff.Policy
	// ReadTimeout detects a stale feed: if no frame (not even a heartbeat)
	// arrives in this window, the connection is considered dead.
	ReadTimeout time.Duration
	DialTimeout time.Duration
	// ReadLimit is the max frame size. Order book snapshots can be megabytes.
	ReadLimit int64
	// StableAfter resets the backoff once a session has lived this long.
	StableAfter time.Duration
	// Metrics is optional; when nil, metrics go to a private registry.
	Metrics *metrics.Ingest
	// Source identifies this ingester instance in Gap events.
	Source string

	live atomic.Bool
}

// Live reports whether the feed is connected and has delivered at least one
// frame since. Used for readiness: a rolling deploy waits for it before
// stopping the next instance.
func (r *Runner) Live() bool { return r.live.Load() }

// Run blocks until ctx is cancelled. It only returns nil: failures are
// retried forever because a market data feed must self-heal.
func (r *Runner) Run(ctx context.Context) error {
	if r.Metrics == nil {
		r.Metrics = metrics.NewIngest(prometheus.NewRegistry())
	}
	r.Metrics.Init(r.Feed.Name())
	log := r.Log.With("exchange", r.Feed.Name())
	attempt := 0
	for {
		started := time.Now()
		err := r.session(ctx, log)
		r.live.Store(false)
		r.Metrics.Connected(r.Feed.Name(), false)
		if ctx.Err() != nil {
			log.Info("feed stopped")
			return nil
		}
		if time.Since(started) >= r.StableAfter {
			attempt = 0
		}
		r.Metrics.Disconnected(r.Feed.Name())
		delay := r.Backoff.Delay(attempt)
		attempt++
		log.Warn("feed disconnected, reconnecting", "err", err, "attempt", attempt, "delay", delay)

		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			log.Info("feed stopped")
			return nil
		case <-t.C:
		}
	}
}

func (r *Runner) session(ctx context.Context, log *slog.Logger) (err error) {
	dialCtx, cancel := context.WithTimeout(ctx, r.DialTimeout)
	conn, _, err := websocket.Dial(dialCtx, r.Feed.URL(), nil)
	cancel()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		if ctx.Err() != nil {
			_ = conn.Close(websocket.StatusNormalClosure, "shutdown")
			return
		}
		_ = conn.CloseNow()
	}()
	conn.SetReadLimit(r.ReadLimit)

	subs, err := r.Feed.Subscriptions()
	if err != nil {
		return fmt.Errorf("build subscriptions: %w", err)
	}
	for _, s := range subs {
		if err := conn.Write(ctx, websocket.MessageText, s); err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
	}
	log.Info("feed connected", "url", r.Feed.URL())
	r.Metrics.Connected(r.Feed.Name(), true)

	name := r.Feed.Name()
	events := make([]model.Event, 0, 64)
	for {
		readCtx, cancel := context.WithTimeout(ctx, r.ReadTimeout)
		_, frame, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return fmt.Errorf("stale feed: no data for %s", r.ReadTimeout)
			}
			return fmt.Errorf("read: %w", err)
		}

		received := time.Now().UTC()
		r.Metrics.Frame(name, len(frame), received)
		r.live.Store(true)

		events, err = r.Feed.Handle(frame, received, events[:0])
		if err != nil {
			return err
		}
		if len(events) == 0 {
			continue
		}
		r.Metrics.Events(events)
		for i, e := range events {
			if g, ok := e.(model.Gap); ok {
				g.Source = r.Source
				events[i] = g
			}
		}
		if err := r.Publisher.Publish(ctx, events); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
	}
}
