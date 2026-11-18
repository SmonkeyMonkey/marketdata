package coinbase

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const testURL = "wss://feed.test"

var now = time.Date(2026, 11, 17, 14, 3, 47, 0, time.UTC)

func frame(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// withTradeID rewrites the trade_id in the match fixture.
func withTradeID(t *testing.T, id string) []byte {
	return []byte(strings.Replace(string(frame(t, "match.json")), "1133191511", id, 1))
}

func handle(t *testing.T, f *Feed, b []byte) []model.Event {
	t.Helper()
	events, err := f.Handle(b, now, nil)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	return events
}

func TestMatchIsNormalized(t *testing.T) {
	events := handle(t, New(testURL, []string{"BTC-USD", "ETH-USD"}), frame(t, "match.json"))
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	tr, ok := events[0].(model.Trade)
	if !ok {
		t.Fatalf("got %T, want model.Trade", events[0])
	}

	want := model.Trade{
		Exchange:     "coinbase",
		Symbol:       "BTC-USD",
		TradeID:      "1133191511",
		Price:        decimal.RequireFromString("65007.4"),
		Size:         decimal.RequireFromString("0.00005630"),
		TakerSide:    model.SideSell, // match side "buy" is the maker side
		ExchangeTime: time.Date(2026, 11, 17, 14, 3, 46, 691275000, time.UTC),
		ReceiveTime:  now,
	}
	if tr.Exchange != want.Exchange || tr.Symbol != want.Symbol || tr.TradeID != want.TradeID ||
		!tr.Price.Equal(want.Price) || !tr.Size.Equal(want.Size) || tr.TakerSide != want.TakerSide ||
		!tr.ExchangeTime.Equal(want.ExchangeTime) || !tr.ReceiveTime.Equal(want.ReceiveTime) {
		t.Errorf("trade mismatch\n got: %+v\nwant: %+v", tr, want)
	}
	// Exact decimal, no float rounding.
	if tr.Size.String() != "0.0000563" {
		t.Errorf("size = %s", tr.Size)
	}
	if tr.Snapshot {
		t.Error("live match marked as snapshot")
	}
}

func TestLastMatchIsSnapshot(t *testing.T) {
	ev := handle(t, New(testURL, []string{"BTC-USD", "ETH-USD"}), frame(t, "last_match.json"))
	if len(ev) != 1 || !ev[0].(model.Trade).Snapshot {
		t.Errorf("last_match not marked as snapshot: %+v", ev)
	}
}

func TestDuplicateTradesAreDropped(t *testing.T) {
	f := New(testURL, []string{"BTC-USD", "ETH-USD"})
	handle(t, f, frame(t, "match.json"))
	if got := handle(t, f, frame(t, "match.json")); len(got) != 0 {
		t.Errorf("duplicate produced %d events", len(got))
	}
	if got := handle(t, f, withTradeID(t, "1133191500")); len(got) != 0 {
		t.Errorf("older trade produced %d events", len(got))
	}
}

func TestGapBetweenTrades(t *testing.T) {
	f := New(testURL, []string{"BTC-USD", "ETH-USD"})
	handle(t, f, frame(t, "last_match.json")) // id ...509

	events := handle(t, f, frame(t, "match.json")) // id ...511, so ...510 is missing
	if len(events) != 2 {
		t.Fatalf("got %d events, want gap + trade", len(events))
	}
	gap, ok := events[0].(model.Gap)
	if !ok {
		t.Fatalf("first event is %T, want model.Gap", events[0])
	}
	if gap.AfterID != "1133191509" || gap.BeforeID != "1133191511" || gap.Missing != 1 {
		t.Errorf("unexpected gap %+v", gap)
	}
	if _, ok := events[1].(model.Trade); !ok {
		t.Errorf("second event is %T, want model.Trade", events[1])
	}
}

func TestHeartbeatRevealsGapWithoutTrades(t *testing.T) {
	f := New(testURL, []string{"BTC-USD", "ETH-USD"})
	handle(t, f, frame(t, "match.json")) // last seen ...511

	events := handle(t, f, frame(t, "heartbeat.json")) // last_trade_id ...515
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1 gap", len(events))
	}
	gap := events[0].(model.Gap)
	if gap.Missing != 4 || gap.AfterID != "1133191511" || gap.BeforeID != "1133191516" {
		t.Errorf("unexpected gap %+v", gap)
	}

	// The trades the heartbeat accounted for must not be reported again.
	if got := handle(t, f, withTradeID(t, "1133191515")); len(got) != 0 {
		t.Errorf("trade covered by heartbeat produced %d events", len(got))
	}
	if got := handle(t, f, withTradeID(t, "1133191516")); len(got) != 1 {
		t.Errorf("next trade produced %d events, want 1", len(got))
	}
}

func TestHeartbeatBeforeAnyTradeIsSilent(t *testing.T) {
	if got := handle(t, New(testURL, []string{"BTC-USD", "ETH-USD"}), frame(t, "heartbeat.json")); len(got) != 0 {
		t.Errorf("got %d events, want 0", len(got))
	}
}

func TestServerErrorEndsSession(t *testing.T) {
	_, err := New(testURL, []string{"BTC-USD", "ETH-USD"}).Handle(frame(t, "error_auth.json"), now, nil)
	if err == nil || !strings.Contains(err.Error(), "require authentication") {
		t.Fatalf("err = %v, want server error", err)
	}
}

func TestIgnoredMessages(t *testing.T) {
	if got := handle(t, New(testURL, []string{"BTC-USD", "ETH-USD"}), frame(t, "subscriptions.json")); len(got) != 0 {
		t.Errorf("got %d events, want 0", len(got))
	}
}

func TestMalformedFrame(t *testing.T) {
	if _, err := New(testURL, []string{"BTC-USD", "ETH-USD"}).Handle([]byte(`{"type":`), now, nil); err == nil {
		t.Fatal("want decode error")
	}
}

func TestSetSymbols(t *testing.T) {
	f := New(testURL, []string{"BTC-USD", "ETH-USD"})
	frames, err := f.SetSymbols([]string{"BTC-USD", "SOL-USD"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"channels":["matches","heartbeat"],"product_ids":["SOL-USD"],"type":"subscribe"}`,
		`{"channels":["matches","heartbeat"],"product_ids":["ETH-USD"],"type":"unsubscribe"}`,
	}
	if len(frames) != 2 || string(frames[0]) != want[0] || string(frames[1]) != want[1] {
		t.Errorf("frames = %s", frames)
	}
	if ev := handle(t, f, []byte(strings.Replace(string(frame(t, "match.json")), "BTC-USD", "ETH-USD", 1))); len(ev) != 0 {
		t.Errorf("unsubscribed product produced %d events", len(ev))
	}
}

func TestInvalidProductIsPerSymbol(t *testing.T) {
	_, err := New(testURL, nil).Handle([]byte(`{"type":"error","message":"Failed to subscribe","reason":"NOPE-USD is not a valid product"}`), now, nil)
	var se *exchange.SymbolError
	if !errors.As(err, &se) || se.Symbol != "NOPE-USD" {
		t.Fatalf("err = %v, want SymbolError for NOPE-USD", err)
	}
}

func TestSubscriptions(t *testing.T) {
	subs, err := New(testURL, []string{"BTC-USD", "ETH-USD"}).Subscriptions()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"channels":["matches","heartbeat"],"product_ids":["BTC-USD","ETH-USD"],"type":"subscribe"}`
	if len(subs) != 1 || string(subs[0]) != want {
		t.Errorf("got %s\nwant %s", subs, want)
	}
}
