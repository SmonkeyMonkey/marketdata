package ingest

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"marketdata/internal/backoff"
	"marketdata/internal/model"
)

// echoFeed turns every text frame into a Trade whose TradeID is the frame body.
type echoFeed struct{ url string }

func (f echoFeed) Name() string                     { return "test" }
func (f echoFeed) URL() string                      { return f.url }
func (f echoFeed) Subscriptions() ([][]byte, error) { return [][]byte{[]byte("subscribe")}, nil }
func (f echoFeed) Handle(b []byte, at time.Time, dst []model.Event) ([]model.Event, error) {
	return append(dst, model.Trade{TradeID: string(b), ReceiveTime: at}), nil
}

type collector struct {
	mu  sync.Mutex
	ids []string
	got chan struct{}
}

func (c *collector) Publish(_ context.Context, events []model.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range events {
		c.ids = append(c.ids, e.(model.Trade).TradeID)
		c.got <- struct{}{}
	}
	return nil
}

func newRunner(url string, pub Publisher, readTimeout time.Duration) *Runner {
	return &Runner{
		Feed:        echoFeed{url: url},
		Publisher:   pub,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Backoff:     backoff.Constant(10 * time.Millisecond),
		ReadTimeout: readTimeout,
		DialTimeout: time.Second,
		ReadLimit:   1 << 20,
		StableAfter: time.Minute,
	}
}

func wsURL(s *httptest.Server) string { return "ws" + strings.TrimPrefix(s.URL, "http") }

func wait(t *testing.T, c *collector, n int) {
	t.Helper()
	for range n {
		select {
		case <-c.got:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out; received %v", c.ids)
		}
	}
}

// The server sends one frame per connection and then drops it, so every
// frame after the first proves a reconnect + resubscribe happened.
func TestRunnerReconnectsAfterDisconnect(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		if _, msg, err := c.Read(r.Context()); err != nil || string(msg) != "subscribe" {
			return
		}
		n := conns.Add(1)
		_ = c.Write(r.Context(), websocket.MessageText, []byte{byte('0' + n)})
	}))
	defer srv.Close()

	pub := &collector{got: make(chan struct{}, 16)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- newRunner(wsURL(srv), pub, time.Second).Run(ctx) }()

	wait(t, pub, 3)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if got := strings.Join(pub.ids[:3], ","); got != "1,2,3" {
		t.Errorf("ids = %s, want 1,2,3", got)
	}
}

// A connection that stays open but goes silent must be treated as dead.
func TestRunnerDetectsStaleFeed(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		_, _, _ = c.Read(r.Context())
		n := conns.Add(1)
		_ = c.Write(r.Context(), websocket.MessageText, []byte{byte('0' + n)})
		<-r.Context().Done() // hang without closing
	}))
	defer srv.Close()

	pub := &collector{got: make(chan struct{}, 16)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = newRunner(wsURL(srv), pub, 100*time.Millisecond).Run(ctx) }()

	wait(t, pub, 2)
	if conns.Load() < 2 {
		t.Errorf("connections = %d, want reconnect after stale read", conns.Load())
	}
}

func TestRunnerStopsWhileWaitingToReconnect(t *testing.T) {
	r := newRunner("ws://127.0.0.1:1", &collector{got: make(chan struct{}, 1)}, time.Second)
	r.Backoff = backoff.Constant(time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop on cancel")
	}
}
