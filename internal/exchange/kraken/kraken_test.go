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
	if err := json.Unmarshal(subs[0], &got); err != nil {
		t.Fatal(err)
	}
	if got.Method != "subscribe" || got.Params.Channel != "trade" || !got.Params.Snapshot ||
		strings.Join(got.Params.Symbol, ",") != "BTC/USD,ETH/USD" {
		t.Errorf("subscription = %s", subs[0])
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
		`{"method":"unsubscribe","params":{"channel":"trade","symbol":["ETH/USD"]}}`,
	}
	if len(frames) != 2 || string(frames[0]) != want[0] || string(frames[1]) != want[1] {
		t.Errorf("frames = %s", frames)
	}
	// ETH trades still in flight are dropped.
	eth := []byte(strings.ReplaceAll(string(frame(t, "update.json")), "BTC/USD", "ETH/USD"))
	if ev := handle(t, f, eth); len(ev) != 0 {
		t.Errorf("unsubscribed ETH produced %d events", len(ev))
	}
}
