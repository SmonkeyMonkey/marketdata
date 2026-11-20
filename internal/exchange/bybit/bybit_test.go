package bybit

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const url = "wss://stream.bybit.com/v5/public/spot"

var t0 = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

func newFeed(t testing.TB, symbols ...string) *Feed {
	t.Helper()
	f, err := New(url, symbols)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// session replays frames captured from the live endpoint (subscribe ack,
// trades, a book snapshot and its deltas, a pong).
func session(t testing.TB) [][]byte {
	t.Helper()
	fh, err := os.Open(filepath.Join("testdata", "session.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	var out [][]byte
	sc := bufio.NewScanner(fh)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		out = append(out, append([]byte(nil), sc.Bytes()...))
	}
	return out
}

func replay(t *testing.T, f *Feed, frames [][]byte) []model.Event {
	t.Helper()
	var all []model.Event
	for i, fr := range frames {
		ev, err := f.Handle(fr, t0.Add(time.Duration(i)*30*time.Millisecond), nil)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		all = append(all, ev...)
	}
	return all
}

func TestRealSessionHasNoGapsOrResyncs(t *testing.T) {
	f := newFeed(t, "BTC-USDT")
	var trades, books int
	var last model.Book
	for _, e := range replay(t, f, session(t)) {
		switch ev := e.(type) {
		case model.Trade:
			trades++
			if ev.Symbol != "BTC-USDT" || ev.TakerSide == model.SideUnknown || !ev.Price.IsPositive() || ev.Snapshot {
				t.Errorf("trade = %+v", ev)
			}
		case model.Book:
			books++
			last = ev
		default:
			t.Errorf("unexpected %T %+v (gap or resync on a clean session)", e, e)
		}
	}
	if trades == 0 || books == 0 {
		t.Fatalf("trades %d books %d", trades, books)
	}
	if len(last.Bids) == 0 || !last.Bids[0].Price.LessThan(last.Asks[0].Price) || len(last.Bids) > bookDepth || last.Seq == 0 {
		t.Errorf("last book: %d bids, best %s/%s, seq %d", len(last.Bids), last.Bids[0].Price, last.Asks[0].Price, last.Seq)
	}
}

func TestSkippedTradeIDsAreNotGaps(t *testing.T) {
	f := newFeed(t, "ETH-USDC")
	tr := func(id string) []byte {
		return []byte(`{"topic":"publicTrade.ETHUSDC","type":"snapshot","ts":1,"data":[{"i":"` + id + `","T":1,"p":"1","v":"1","S":"Buy","s":"ETHUSDC"}]}`)
	}
	ev := replay(t, f, [][]byte{tr("2240000000000000100"), tr("2240000000000000107"), tr("2240000000000000107"), tr("2240000000000000103")})
	if len(ev) != 2 || ev[0].Kind() != "trade" || ev[1].Kind() != "trade" {
		t.Errorf("events = %+v, want two trades: no gap for skipped IDs, repeats and older IDs dropped", ev)
	}
}

func TestOneRequestPerSymbol(t *testing.T) {
	f := newFeed(t, "BTC-USDT", "ETH-USDC")
	frames, _ := f.Subscriptions()
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want one per symbol (a bad symbol fails its whole request)", len(frames))
	}
	var req struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}
	_ = json.Unmarshal(frames[1], &req)
	if req.Op != "subscribe" || strings.Join(req.Args, ",") != "publicTrade.ETHUSDC,orderbook.50.ETHUSDC" {
		t.Errorf("request = %+v", req)
	}
}

func TestRejectedSymbol(t *testing.T) {
	f := newFeed(t, "BTC-USDT", "FOOBAR-USDT")
	b, _ := os.ReadFile(filepath.Join("testdata", "subscribe_error.json"))
	_, err := f.Handle(b, t0, nil)
	var se *exchange.SymbolError
	if !errors.As(err, &se) || se.Symbol != "FOOBAR-USDT" {
		t.Errorf("err = %v, want SymbolError for FOOBAR-USDT", err)
	}
}

func bookFrame(typ string, u int64, bid string) []byte {
	return []byte(`{"topic":"orderbook.50.BTCUSDT","type":"` + typ + `","ts":1795100002288,"data":{"s":"BTCUSDT","b":[["` + bid + `","1"]],"a":[["70000","1"]],"u":` +
		itoa(u) + `,"seq":` + itoa(u*10) + `}}`)
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestUpdateIDJumpResyncs(t *testing.T) {
	f := newFeed(t, "BTC-USDT")
	replay(t, f, [][]byte{bookFrame("snapshot", 100, "69000"), bookFrame("delta", 101, "69001")})
	if len(f.Outbox()) != 0 {
		t.Fatal("resync on a contiguous delta")
	}
	ev, _ := f.Handle(bookFrame("delta", 103, "69002"), t0.Add(time.Second), nil)
	if len(ev) != 1 || ev[0].Kind() != "book_resync" {
		t.Fatalf("events = %+v, want one resync", ev)
	}
	out := f.Outbox()
	if len(out) != 2 || !strings.Contains(string(out[0]), `"unsubscribe"`) || !strings.Contains(string(out[1]), `"orderbook.50.BTCUSDT"`) {
		t.Errorf("outbox = %q", out)
	}
	// Deltas are ignored until the fresh snapshot.
	if ev, _ := f.Handle(bookFrame("delta", 104, "69003"), t0.Add(2*time.Second), nil); len(ev) != 0 {
		t.Errorf("delta before snapshot produced %+v", ev)
	}
	// A second divergence within 30 s waits.
	replay(t, f, [][]byte{bookFrame("snapshot", 200, "69000")})
	f.Handle(bookFrame("delta", 205, "69000"), t0.Add(3*time.Second), nil)
	if out := f.Outbox(); len(out) != 0 {
		t.Errorf("resubscribed again within the limit: %q", out)
	}
}

func TestSetSymbols(t *testing.T) {
	f := newFeed(t, "BTC-USDT")
	frames, err := f.SetSymbols([]string{"ETH-USDT"})
	if err != nil || len(frames) != 2 || !strings.Contains(string(frames[0]), `"subscribe"`) || !strings.Contains(string(frames[1]), `"publicTrade.BTCUSDT"`) {
		t.Fatalf("frames = %q err = %v", frames, err)
	}
	// A trade still in flight for the removed symbol is dropped.
	ev, _ := f.Handle([]byte(`{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"i":"5","T":1,"p":"1","v":"1","S":"Buy","s":"BTCUSDT"}]}`), t0, nil)
	if len(ev) != 0 {
		t.Errorf("trade of an unsubscribed symbol: %+v", ev)
	}
	if _, err := f.SetSymbols([]string{"btc-usdt"}); err == nil {
		t.Error("non-canonical symbol accepted")
	}
}

func TestPing(t *testing.T) {
	frame, every := newFeed(t, "BTC-USDT").Ping()
	if string(frame) != `{"op":"ping"}` || every != 20*time.Second {
		t.Errorf("ping %s every %s", frame, every)
	}
}
