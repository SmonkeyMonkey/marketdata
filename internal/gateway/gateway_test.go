package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"

	"marketdata/internal/metrics"
	"marketdata/internal/model"
)

// --- hub

type fakeSub struct {
	room  int
	got   [][]byte
	slows atomic.Int32
}

func (f *fakeSub) enqueue(m []byte) bool {
	if len(f.got) >= f.room {
		return false
	}
	f.got = append(f.got, m)
	return true
}
func (f *fakeSub) slow() { f.slows.Add(1) }

func TestHubRoutesByChannel(t *testing.T) {
	h := NewHub()
	a, b := &fakeSub{room: 10}, &fakeSub{room: 10}
	h.Subscribe("trades:x:A", a)
	h.Subscribe("trades:x:B", b)
	if n := h.Publish("trades:x:A", []byte("1")); n != 1 {
		t.Errorf("delivered = %d", n)
	}
	if len(a.got) != 1 || len(b.got) != 0 {
		t.Errorf("a=%d b=%d", len(a.got), len(b.got))
	}
	h.Unsubscribe("trades:x:A", a)
	h.Publish("trades:x:A", []byte("2"))
	if len(a.got) != 1 {
		t.Error("delivered after unsubscribe")
	}
}

func TestHubSnapshotForNewSubscriber(t *testing.T) {
	h := NewHub()
	h.Publish("candles:x:A:1m", []byte("latest"))
	if last := h.Subscribe("candles:x:A:1m", &fakeSub{room: 1}); string(last) != "latest" {
		t.Errorf("snapshot = %q", last)
	}
}

func TestHubFlagsFullQueues(t *testing.T) {
	h := NewHub()
	full, ok := &fakeSub{room: 0}, &fakeSub{room: 5}
	h.Subscribe("c", full)
	h.Subscribe("c", ok)
	if n := h.Publish("c", []byte("m")); n != 1 {
		t.Errorf("delivered = %d, want 1", n)
	}
	if full.slows.Load() != 1 || ok.slows.Load() != 0 {
		t.Error("slow subscriber not flagged")
	}
}

// --- server, end to end over real websockets

type env struct {
	hub *Hub
	srv *Server
	url string
}

func newEnv(t *testing.T, opts Options) *env {
	t.Helper()
	if opts.Metrics == nil {
		opts.Metrics = metrics.NewGateway(prometheus.NewRegistry())
	}
	if opts.MaxConns == 0 {
		opts.MaxConns = 100
	}
	if opts.MaxSubscriptions == 0 {
		opts.MaxSubscriptions = 10
	}
	if opts.SendQueue == 0 {
		opts.SendQueue = 16
	}
	opts.WriteTimeout, opts.PingInterval = time.Second, time.Minute
	hub := NewHub()
	srv := NewServer(hub, opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return &env{hub: hub, srv: srv, url: "ws" + strings.TrimPrefix(ts.URL, "http")}
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func send(t *testing.T, c *websocket.Conn, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if err := c.Write(context.Background(), websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}

type msg struct {
	Type     string          `json:"type"`
	Channel  string          `json:"channel"`
	TS       int64           `json:"ts"`
	Error    string          `json:"error"`
	Channels []string        `json:"channels"`
	Data     json.RawMessage `json:"data"`
}

func recv(t *testing.T, c *websocket.Conn) msg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m msg
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var trade = model.Trade{
	Exchange: "binance", Symbol: "BTC-USDT", TradeID: "42",
	Price: decimal.RequireFromString("64000.5"), Size: decimal.RequireFromString("0.01"),
	TakerSide: model.SideSell, ExchangeTime: time.Now().UTC(), ReceiveTime: time.Now().UTC(),
}

func TestSubscribeAndReceive(t *testing.T) {
	e := newEnv(t, Options{})
	c := dial(t, e.url)
	send(t, c, request{Op: "subscribe", Channels: []string{"trades:binance:BTC-USDT"}})
	if m := recv(t, c); m.Type != "subscribed" {
		t.Fatalf("got %+v", m)
	}
	waitFor(t, func() bool { return e.hub.Subscribers("trades:binance:BTC-USDT") == 1 })

	b, _ := encodeTrade(trade, time.Now())
	e.hub.Publish("trades:binance:BTC-USDT", b)
	m := recv(t, c)
	if m.Type != "trade" || m.Channel != "trades:binance:BTC-USDT" || m.TS == 0 {
		t.Fatalf("got %+v", m)
	}
	var tr tradeMsg
	if err := json.Unmarshal(m.Data, &tr); err != nil {
		t.Fatal(err)
	}
	if tr.Price.String() != "64000.5" || tr.Side != model.SideSell || tr.ID != "42" {
		t.Errorf("trade = %+v", tr)
	}
	if !strings.Contains(string(m.Data), `"price":"64000.5"`) {
		t.Errorf("decimals must be JSON strings: %s", m.Data)
	}
}

func TestCandleSnapshotOnSubscribe(t *testing.T) {
	e := newEnv(t, Options{})
	c := model.Candle{Exchange: "kraken", Symbol: "ETH-USD", Interval: time.Minute, Start: time.Now().Truncate(time.Minute),
		Open: decimal.NewFromInt(1), High: decimal.NewFromInt(2), Low: decimal.NewFromInt(1), Close: decimal.NewFromInt(2),
		Volume: decimal.NewFromInt(4), QuoteVolume: decimal.NewFromInt(6), Trades: 3}
	b, _ := encodeCandle(c, time.Now())
	e.hub.Publish("candles:kraken:ETH-USD:1m", b) // before anyone subscribed

	conn := dial(t, e.url)
	send(t, conn, request{Op: "subscribe", Channels: []string{"candles:kraken:ETH-USD:1m"}})
	recv(t, conn) // subscribed
	m := recv(t, conn)
	var cm candleMsg
	_ = json.Unmarshal(m.Data, &cm)
	if m.Type != "candle" || cm.Period != "1m" || cm.VWAP.String() != "1.5" {
		t.Errorf("snapshot = %+v / %+v", m, cm)
	}
}

func TestRejectsBadRequests(t *testing.T) {
	e := newEnv(t, Options{MaxSubscriptions: 2})
	c := dial(t, e.url)
	send(t, c, request{Op: "subscribe", Channels: []string{"nope"}})
	if m := recv(t, c); m.Type != "error" {
		t.Errorf("invalid channel: %+v", m)
	}
	send(t, c, request{Op: "subscribe", Channels: []string{"trades:a:X", "trades:a:Y", "trades:a:Z"}})
	if m := recv(t, c); m.Type != "error" || !strings.Contains(m.Error, "limit") {
		t.Errorf("limit: %+v", m)
	}
	send(t, c, request{Op: "dance"})
	if m := recv(t, c); m.Type != "error" {
		t.Errorf("unknown op: %+v", m)
	}
}

func TestSlowClientIsDisconnected(t *testing.T) {
	e := newEnv(t, Options{SendQueue: 4})
	c := dial(t, e.url)
	send(t, c, request{Op: "subscribe", Channels: []string{"trades:x:A"}})
	recv(t, c)
	waitFor(t, func() bool { return e.hub.Subscribers("trades:x:A") == 1 })

	// The client never reads, so its socket buffer and queue fill up.
	big := []byte(`{"type":"trade","data":"` + strings.Repeat("x", 64<<10) + `"}`)
	for range 2000 {
		e.hub.Publish("trades:x:A", big)
		if e.hub.Subscribers("trades:x:A") == 0 {
			break
		}
	}
	waitFor(t, func() bool { return e.hub.Subscribers("trades:x:A") == 0 })
	waitFor(t, func() bool { return e.srv.conns.Load() == 0 })
}

func TestConnectionLimit(t *testing.T) {
	e := newEnv(t, Options{MaxConns: 1})
	dial(t, e.url)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, resp, err := websocket.Dial(ctx, e.url, nil); err == nil || resp == nil || resp.StatusCode != 503 {
		t.Errorf("second connection: err=%v resp=%v, want 503", err, resp)
	}
}

func TestShutdownClosesClientsWithGoingAway(t *testing.T) {
	e := newEnv(t, Options{})
	c := dial(t, e.url)
	send(t, c, request{Op: "ping"})
	recv(t, c)

	// A real client is always reading, which also answers the close frame.
	readErr := make(chan error, 1)
	go func() {
		_, _, err := c.Read(context.Background())
		readErr <- err
	}()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e.srv.Shutdown(ctx)
	if d := time.Since(start); d > time.Second {
		t.Errorf("shutdown took %s with a responsive client", d)
	}
	if err := <-readErr; websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Errorf("close status = %v (err %v), want going away", websocket.CloseStatus(err), err)
	}
}

func TestArbSnapshotRelayed(t *testing.T) {
	e := newEnv(t, Options{})
	c := dial(t, e.url)
	send(t, c, request{Op: "subscribe", Channels: []string{"arb:BTC-USD"}})
	if m := recv(t, c); m.Type != "subscribed" {
		t.Fatalf("got %+v", m)
	}
	waitFor(t, func() bool { return e.hub.Subscribers("arb:BTC-USD") == 1 })
	snap := model.ArbSnapshot{Asset: "BTC-USD", Time: time.Now(),
		Venues: []model.ArbVenue{{Exchange: "binance", Symbol: "BTC-USDT", Bid: decimal.RequireFromString("64000.1"), RateSource: "peg"}},
		Routes: []model.ArbRoute{{BuyExchange: "kraken", BuySymbol: "BTC-USD", SellExchange: "binance", SellSymbol: "BTC-USDT", NetBps: decimal.RequireFromString("-12.5")}}}
	b, _ := encodeArb(snap, time.Now())
	e.hub.Publish(ArbChannel("BTC-USD"), b)
	m := recv(t, c)
	if m.Type != "arb" || !strings.Contains(string(m.Data), `"buy":"kraken:BTC-USD"`) || !strings.Contains(string(m.Data), `"net_bps":"-12.5"`) {
		t.Errorf("got %s %s", m.Type, m.Data)
	}
}
