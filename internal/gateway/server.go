package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"marketdata/internal/metrics"
)

type Options struct {
	MaxConns         int64
	MaxSubscriptions int
	// SendQueue is the per-client buffer. A client that falls this many
	// messages behind is disconnected rather than slowing everyone down.
	SendQueue    int
	WriteTimeout time.Duration
	PingInterval time.Duration
	Metrics      *metrics.Gateway
}

// Server accepts websocket clients and serves them from a Hub.
type Server struct {
	hub   *Hub
	opts  Options
	log   *slog.Logger
	conns atomic.Int64

	// mu orders new connections against Shutdown: once closed is set no
	// client goroutine is started, so wg.Wait sees a final count. clients
	// lets Shutdown reach every connection without a watcher goroutine per
	// client (that would be a third goroutine and ~8 KB of stack each).
	mu      sync.Mutex
	closed  bool
	clients map[*client]struct{}
	wg      sync.WaitGroup
}

func NewServer(hub *Hub, opts Options, log *slog.Logger) *Server {
	return &Server{hub: hub, opts: opts, log: log, clients: make(map[*client]struct{})}
}

// Shutdown sends every client a "going away" close and waits for them to
// finish, at most until ctx ends (unresponsive peers must not hold up a
// deploy).
func (s *Server) Shutdown(ctx context.Context) {
	s.mu.Lock()
	s.closed = true
	clients := make([]*client, 0, len(s.clients))
	for c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()
	for _, c := range clients {
		c.close("server")
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	if s.conns.Add(1) > s.opts.MaxConns {
		s.conns.Add(-1)
		s.opts.Metrics.Rejected()
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Compression costs CPU per message per client; market data is small
		// and latency-sensitive.
		CompressionMode:    websocket.CompressionDisabled,
		InsecureSkipVerify: true, // public read-only API: any origin may connect
	})
	if err != nil {
		s.conns.Add(-1)
		return
	}
	conn.SetReadLimit(4096)

	c := &client{
		srv:  s,
		conn: conn,
		send: newQueue(s.opts.SendQueue),
		done: make(chan struct{}),
		subs: make(map[string]struct{}),
	}
	s.mu.Lock()
	if s.closed { // shut down while the handshake was in progress
		s.mu.Unlock()
		s.conns.Add(-1)
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	s.clients[c] = struct{}{}
	s.wg.Add(2) // safe: this handler's own Add keeps the counter above zero
	s.mu.Unlock()
	s.opts.Metrics.Connected()
	go c.writeLoop()
	go c.readLoop()
}

type client struct {
	srv  *Server
	conn *websocket.Conn
	send *queue
	done chan struct{}

	closeOnce sync.Once
	reason    string

	mu   sync.Mutex
	subs map[string]struct{}
}

func (c *client) enqueue(msg []byte) bool { return c.send.push(msg) }

func (c *client) slow() { go c.close("slow") }

// close is idempotent; the first reason wins.
func (c *client) close(reason string) {
	c.closeOnce.Do(func() {
		c.reason = reason
		close(c.done)
		c.mu.Lock()
		for ch := range c.subs {
			c.srv.hub.Unsubscribe(ch, c)
		}
		n := len(c.subs)
		c.subs = nil
		c.mu.Unlock()
		c.srv.opts.Metrics.Subscriptions(-n)

		code, text := websocket.StatusNormalClosure, ""
		switch reason {
		case "slow":
			code, text = websocket.StatusPolicyViolation, "too slow: send queue full"
		case "server":
			code, text = websocket.StatusGoingAway, "server shutting down"
		}
		// Close sends the close frame and waits for the peer's reply, which
		// also ends the pending Read in readLoop.
		go func() { _ = c.conn.Close(code, text) }()
		c.srv.mu.Lock()
		delete(c.srv.clients, c)
		c.srv.mu.Unlock()
		c.srv.conns.Add(-1)
		c.srv.opts.Metrics.Disconnected(reason)
	})
}

func (c *client) readLoop() {
	defer c.srv.wg.Done()
	// No read deadline: liveness is checked by pings from writeLoop. The
	// read ends when the connection is closed, so the close frame is sent
	// first instead of the socket being cut by a cancelled context.
	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			reason := "client"
			if websocket.CloseStatus(err) == -1 {
				reason = "read_error"
			}
			c.close(reason)
			return
		}
		var req request
		if err := json.Unmarshal(data, &req); err != nil {
			c.reply(control("error", "invalid JSON", nil))
			continue
		}
		c.handle(req)
	}
}

func (c *client) handle(req request) {
	switch req.Op {
	case "subscribe":
		for _, ch := range req.Channels {
			if err := ValidChannel(ch); err != nil {
				c.reply(control("error", err.Error(), nil))
				return
			}
		}
		c.mu.Lock()
		if c.subs == nil { // closing
			c.mu.Unlock()
			return
		}
		var added []string
		var snapshots [][]byte
		for _, ch := range req.Channels {
			if _, ok := c.subs[ch]; ok {
				continue
			}
			if len(c.subs) >= c.srv.opts.MaxSubscriptions {
				c.mu.Unlock()
				c.reply(control("error", "subscription limit reached", added))
				return
			}
			c.subs[ch] = struct{}{}
			added = append(added, ch)
			if last := c.srv.hub.Subscribe(ch, c); last != nil {
				snapshots = append(snapshots, last)
			}
		}
		c.mu.Unlock()
		c.srv.opts.Metrics.Subscriptions(len(added))
		c.reply(control("subscribed", "", req.Channels))
		for _, m := range snapshots {
			c.reply(m)
		}
	case "unsubscribe":
		c.mu.Lock()
		n := 0
		for _, ch := range req.Channels {
			if _, ok := c.subs[ch]; ok {
				delete(c.subs, ch)
				c.srv.hub.Unsubscribe(ch, c)
				n++
			}
		}
		c.mu.Unlock()
		c.srv.opts.Metrics.Subscriptions(-n)
		c.reply(control("unsubscribed", "", req.Channels))
	case "ping":
		c.reply(control("pong", "", nil))
	default:
		c.reply(control("error", "unknown op: want subscribe, unsubscribe or ping", nil))
	}
}

// reply queues a control message; a client that can't take even that is slow.
func (c *client) reply(msg []byte) {
	if !c.enqueue(msg) {
		c.slow()
	}
}

func (c *client) writeLoop() {
	defer c.srv.wg.Done()
	ping := time.NewTicker(c.srv.opts.PingInterval)
	defer ping.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-c.send.ready:
			// Everything queued since the last wake-up, in one pass.
			for _, msg := range c.send.drain() {
				ctx, cancel := context.WithTimeout(context.Background(), c.srv.opts.WriteTimeout)
				err := c.conn.Write(ctx, websocket.MessageText, msg)
				cancel()
				if err != nil {
					c.close("write_error")
					return
				}
				c.srv.opts.Metrics.Sent(len(msg))
			}
		case <-ping.C:
			// Detects dead peers that never send anything (half-open TCP).
			ctx, cancel := context.WithTimeout(context.Background(), c.srv.opts.WriteTimeout)
			err := c.conn.Ping(ctx)
			cancel()
			if err != nil {
				c.close("ping_timeout")
				return
			}
		}
	}
}
