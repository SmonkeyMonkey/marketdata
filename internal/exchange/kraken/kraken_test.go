package kraken

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const testURL = "wss://ws.kraken.com/v2"

var now = time.Date(2026, 11, 17, 15, 16, 48, 0, time.UTC)

func frame(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newFeed(t *testing.T) *Feed {
	t.Helper()
	f, err := New(testURL, []string{"BTC-USD", "ETH-USD"})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func handle(t *testing.T, f *Feed, b []byte) []model.Event {
	t.Helper()
	ev, err := f.Handle(b, now, nil)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	return ev
}

func TestSubscriptions(t *testing.T) {
	subs, err := newFeed(t).Subscriptions()
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Method string `json:"method"`
		Params struct {
			Channel  string   `json:"channel"`
			Symbol   []string `json:"symbol"`
			Snapshot bool     `json:"snapshot"`
		} `json:"params"`
	}
	// instrument (precision for book checksums), trade, book.
	if len(subs) != 3 || !strings.Contains(string(subs[0]), `"channel":"instrument"`) ||
		!strings.Contains(string(subs[2]), `"channel":"book","depth":100`) {
		t.Fatalf("subscriptions = %s", subs)
	}
	if err := json.Unmarshal(subs[1], &got); err != nil {
		t.Fatal(err)
	}
	if got.Method != "subscribe" || got.Params.Channel != "trade" || !got.Params.Snapshot ||
		strings.Join(got.Params.Symbol, ",") != "BTC/USD,ETH/USD" {
		t.Errorf("trade subscription = %s", subs[1])
	}
}

func TestUpdateIsNormalized(t *testing.T) {
	ev := handle(t, newFeed(t), frame(t, "update.json"))
	if len(ev) != 2 {
		t.Fatalf("got %d events, want 2 trades", len(ev))
	}
	tr := ev[0].(model.Trade)
	if tr.Exchange != "kraken" || tr.Symbol != "BTC-USD" || tr.TradeID != "116531709" || tr.TakerSide != model.SideBuy {
		t.Errorf("trade = %+v", tr)
	}
	if tr.Price.String() != "64549.1" || tr.Size.String() != "0.00116178" {
		t.Errorf("price/size = %s/%s", tr.Price, tr.Size)
	}
	if want := time.Date(2026, 11, 17, 15, 16, 45, 28548000, time.UTC); !tr.ExchangeTime.Equal(want) {
		t.Errorf("time = %s", tr.ExchangeTime)
	}
	if tr.Snapshot {
		t.Error("update trade marked as snapshot")
	}
}

func TestSnapshotTradesAreMarked(t *testing.T) {
	for _, e := range handle(t, newFeed(t), frame(t, "snapshot.json")) {
		if !e.(model.Trade).Snapshot {
			t.Errorf("snapshot trade not marked: %+v", e)
		}
	}
}

// Numbers arrive as JSON literals; they must be parsed from text, exactly.
func TestNumbersAreExact(t *testing.T) {
	ev := handle(t, newFeed(t), frame(t, "snapshot.json"))
	if got := ev[1].(model.Trade).Size.String(); got != "0.00000051" {
		t.Errorf("size = %s, want 0.00000051", got)
	}
	exp := []byte(`{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"sell","price":64549.1,"qty":5.1e-7,"trade_id":1,"timestamp":"2026-11-17T15:16:45Z"}]}`)
	if got := handle(t, newFeed(t), exp)[0].(model.Trade).Size.String(); got != "0.00000051" {
		t.Errorf("exponent size = %s, want 0.00000051", got)
	}
}

// After a reconnect the snapshot repeats trades we already have; only the new
// ones may be emitted, and a hole between them must show up as a gap.
func TestSnapshotAfterReconnect(t *testing.T) {
	f := newFeed(t)
	if ev := handle(t, f, frame(t, "snapshot.json")); len(ev) != 3 {
		t.Fatalf("first snapshot: %d events, want 3", len(ev))
	}
	if ev := handle(t, f, frame(t, "snapshot.json")); len(ev) != 0 {
		t.Errorf("repeated snapshot: %d events, want 0", len(ev))
	}

	later := strings.Replace(string(frame(t, "snapshot.json")), "69457397", "69457400", 1)
	ev := handle(t, f, []byte(later))
	if len(ev) != 2 {
		t.Fatalf("got %d events, want gap + trade", len(ev))
	}
	if g := ev[0].(model.Gap); g.Missing != 2 || g.Symbol != "ETH-USD" {
		t.Errorf("gap = %+v", g)
	}
}

func TestControlMessagesAreIgnored(t *testing.T) {
	f := newFeed(t)
	for _, name := range []string{"status.json", "subscribe_ok.json", "heartbeat.json"} {
		if ev := handle(t, f, frame(t, name)); len(ev) != 0 {
			t.Errorf("%s produced %d events", name, len(ev))
		}
	}
}

// A rejected pair is reported, but the session (and the other pairs) live on.
func TestSubscribeErrorIsPerSymbol(t *testing.T) {
	_, err := newFeed(t).Handle(frame(t, "subscribe_error.json"), now, nil)
	var se *exchange.SymbolError
	if !errors.As(err, &se) || se.Symbol != "NOPE/USD" || !strings.Contains(se.Reason, "Currency pair not supported") {
		t.Fatalf("err = %v", err)
	}
}

func TestSetSymbols(t *testing.T) {
	f := newFeed(t) // BTC-USD, ETH-USD
	frames, err := f.SetSymbols([]string{"BTC-USD", "DOGE-USD"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"method":"subscribe","params":{"channel":"trade","snapshot":true,"symbol":["DOGE/USD"]}}`,
		`{"method":"subscribe","params":{"channel":"book","depth":100,"symbol":["DOGE/USD"]}}`,
		`{"method":"unsubscribe","params":{"channel":"trade","symbol":["ETH/USD"]}}`,
		`{"method":"unsubscribe","params":{"channel":"book","depth":100,"symbol":["ETH/USD"]}}`,
	}
	if len(frames) != len(want) {
		t.Fatalf("frames = %s", frames)
	}
	for i := range want {
		if string(frames[i]) != want[i] {
			t.Errorf("frame %d = %s, want %s", i, frames[i], want[i])
		}
	}
	// ETH trades still in flight are dropped.
	eth := []byte(strings.ReplaceAll(string(frame(t, "update.json")), "BTC/USD", "ETH/USD"))
	if ev := handle(t, f, eth); len(ev) != 0 {
		t.Errorf("unsubscribed ETH produced %d events", len(ev))
	}
}

// BTC/USD precision as sent by the instrument channel.
var instrument = []byte(`{"channel":"instrument","type":"snapshot","data":{"assets":[],"pairs":[{"symbol":"BTC/USD","price_precision":1,"qty_precision":8}]}}`)

func bookFrames(t *testing.T) (snapshot []byte, updates [][]byte) {
	t.Helper()
	for _, l := range strings.Split(strings.TrimSpace(string(frame(t, "book_updates.json"))), "\n") {
		updates = append(updates, []byte(l))
	}
	return frame(t, "book_snapshot.json"), updates
}

func books(ev []model.Event) (out []model.Book, resyncs int) {
	for _, e := range ev {
		switch b := e.(type) {
		case model.Book:
			out = append(out, b)
		case model.BookResync:
			resyncs++
		}
	}
	return out, resyncs
}

// Real frames with Kraken's checksums: every one must verify.
func TestBookChecksumsVerifyOnRealFrames(t *testing.T) {
	f := newFeed(t)
	handle(t, f, instrument)
	snap, updates := bookFrames(t)
	ev := handle(t, f, snap)
	for _, u := range updates {
		ev = append(ev, handle(t, f, u)...)
	}
	bs, resyncs := books(ev)
	if resyncs != 0 {
		t.Fatalf("%d checksum mismatches on real frames", resyncs)
	}
	if len(bs) == 0 {
		t.Fatal("no book published")
	}
	b := bs[0]
	if b.Symbol != "BTC-USD" || len(b.Bids) != 50 || len(b.Asks) != 50 || !b.Bids[0].Price.LessThan(b.Asks[0].Price) {
		t.Errorf("book: %s %d bids %d asks, best %s/%s", b.Symbol, len(b.Bids), len(b.Asks), b.Bids[0].Price, b.Asks[0].Price)
	}
}

// A book that diverged from the exchange is detected, not published, and
// resubscribed; updates are ignored until the new snapshot.
func TestBookChecksumMismatchResyncs(t *testing.T) {
	f := newFeed(t)
	handle(t, f, instrument)
	snap, updates := bookFrames(t)
	// Corrupt one top-10 quantity, as if an update had been lost.
	var m map[string]any
	_ = json.Unmarshal(snap, &m)
	m["data"].([]any)[0].(map[string]any)["bids"].([]any)[0].(map[string]any)["qty"] = 123.0
	bad, _ := json.Marshal(m)

	bs, resyncs := books(handle(t, f, bad))
	if resyncs != 1 || len(bs) != 0 {
		t.Fatalf("corrupt snapshot: %d books, %d resyncs", len(bs), resyncs)
	}
	out := f.Outbox()
	if len(out) != 2 || !strings.Contains(string(out[0]), "unsubscribe") || !strings.Contains(string(out[1]), `"method":"subscribe"`) {
		t.Fatalf("outbox = %s", out)
	}
	if bs, _ := books(handle(t, f, updates[0])); len(bs) != 0 {
		t.Error("update applied before the fresh snapshot")
	}
	if bs, resyncs := books(handle(t, f, snap)); len(bs) != 1 || resyncs != 0 {
		t.Errorf("fresh snapshot: %d books, %d resyncs", len(bs), resyncs)
	}
}

// A checksum that keeps failing must not turn into a resubscribe flood.
func TestBookResyncsAreRateLimited(t *testing.T) {
	f := newFeed(t)
	handle(t, f, instrument)
	snap, _ := bookFrames(t)
	var m map[string]any
	_ = json.Unmarshal(snap, &m)
	m["data"].([]any)[0].(map[string]any)["bids"].([]any)[0].(map[string]any)["qty"] = 123.0
	bad, _ := json.Marshal(m)

	t0 := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	sent := 0
	for i := range 100 { // a bad snapshot every 100 ms for 10 s
		if _, err := f.Handle(bad, t0.Add(time.Duration(i)*100*time.Millisecond), nil); err != nil {
			t.Fatal(err)
		}
		sent += len(f.Outbox())
	}
	if sent != 2 {
		t.Errorf("sent %d frames in 10 s, want one unsubscribe+subscribe", sent)
	}
	// After the window the pending resync goes out, on any frame.
	if _, err := f.Handle(frame(t, "heartbeat.json"), t0.Add(31*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if n := len(f.Outbox()); n != 2 {
		t.Errorf("pending resync after the window: %d frames, want 2", n)
	}
}
