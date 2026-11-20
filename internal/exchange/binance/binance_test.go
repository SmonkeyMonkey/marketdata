package binance

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const baseURL = "wss://stream.binance.com:9443/stream"

var now = time.Date(2026, 11, 17, 15, 16, 48, 0, time.UTC)

func frame(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newFeed(t testing.TB) *Feed {
	t.Helper()
	f, err := New(baseURL, []string{"BTC-USDT", "ETH-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func trades(t *testing.T, f *Feed, b []byte) []model.Event {
	t.Helper()
	ev, err := f.Handle(b, now, nil)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	return ev
}

func TestURLSelectsTradeStreams(t *testing.T) {
	want := baseURL + "?streams=btcusdt@trade/btcusdt@depth20@100ms/ethusdt@trade/ethusdt@depth20@100ms"
	if got := newFeed(t).URL(); got != want {
		t.Errorf("URL = %s, want %s", got, want)
	}
}

func TestRejectsNonCanonicalSymbols(t *testing.T) {
	for _, s := range []string{"BTCUSDT", "btc-usdt", "BTC-", "-USDT"} {
		if _, err := New(baseURL, []string{s}); err == nil {
			t.Errorf("New(%q) succeeded, want error", s)
		}
	}
}

func TestTradeIsNormalized(t *testing.T) {
	ev := trades(t, newFeed(t), frame(t, "trade_taker_sell.json"))
	if len(ev) != 1 {
		t.Fatalf("got %d events", len(ev))
	}
	tr := ev[0].(model.Trade)
	if tr.Exchange != "binance" || tr.Symbol != "BTC-USDT" || tr.TradeID != "6873309016" {
		t.Errorf("identity: %+v", tr)
	}
	if tr.Price.String() != "64626.54" || tr.Size.String() != "0.00155" {
		t.Errorf("price/size = %s/%s", tr.Price, tr.Size)
	}
	// m=true: buyer is maker, so the taker sold.
	if tr.TakerSide != model.SideSell {
		t.Errorf("taker side = %s, want sell", tr.TakerSide)
	}
	// T (trade time), not E (event time).
	if want := time.UnixMilli(1794928607465).UTC(); !tr.ExchangeTime.Equal(want) {
		t.Errorf("exchange time = %s, want %s", tr.ExchangeTime, want)
	}
}

func TestTakerBuy(t *testing.T) {
	tr := trades(t, newFeed(t), frame(t, "trade_taker_buy.json"))[0].(model.Trade)
	if tr.TakerSide != model.SideBuy {
		t.Errorf("taker side = %s, want buy", tr.TakerSide)
	}
}

func TestSequentialTradesHaveNoGap(t *testing.T) {
	f := newFeed(t)
	trades(t, f, frame(t, "trade_taker_sell.json")) // ...016
	ev := trades(t, f, frame(t, "trade_taker_buy.json"))
	if len(ev) != 1 {
		t.Fatalf("got %d events, want just the trade", len(ev))
	}
}

func TestGapAndDuplicate(t *testing.T) {
	f := newFeed(t)
	trades(t, f, frame(t, "trade_taker_sell.json")) // ...016
	jump := []byte(strings.Replace(string(frame(t, "trade_taker_buy.json")), "6873309017", "6873309020", 1))
	ev := trades(t, f, jump)
	if len(ev) != 2 {
		t.Fatalf("got %d events, want gap + trade", len(ev))
	}
	if g := ev[0].(model.Gap); g.Missing != 3 || g.Symbol != "BTC-USDT" {
		t.Errorf("gap = %+v", g)
	}
	if ev := trades(t, f, frame(t, "trade_taker_sell.json")); len(ev) != 0 {
		t.Errorf("old trade produced %d events", len(ev))
	}
}

// Trades can still be in flight right after an UNSUBSCRIBE: drop them quietly.
func TestUnsubscribedSymbolIsIgnored(t *testing.T) {
	f, _ := New(baseURL, []string{"ETH-USDT"})
	ev, err := f.Handle(frame(t, "trade_taker_sell.json"), now, nil)
	if err != nil || len(ev) != 0 {
		t.Fatalf("got %d events, err %v; want none", len(ev), err)
	}
}

func TestSetSymbolsOnLiveConnection(t *testing.T) {
	f := newFeed(t) // BTC-USDT, ETH-USDT
	frames, err := f.SetSymbols([]string{"BTC-USDT", "SOL-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"id":1,"method":"SUBSCRIBE","params":["solusdt@trade","solusdt@depth20@100ms"]}`,
		`{"id":2,"method":"UNSUBSCRIBE","params":["ethusdt@trade","ethusdt@depth20@100ms"]}`,
	}
	if len(frames) != 2 || string(frames[0]) != want[0] || string(frames[1]) != want[1] {
		t.Errorf("frames = %s", frames)
	}
	// A reconnect starts with the new set.
	if got := f.URL(); got != baseURL+"?streams=btcusdt@trade/btcusdt@depth20@100ms/solusdt@trade/solusdt@depth20@100ms" {
		t.Errorf("URL = %s", got)
	}
	if frames, _ := f.SetSymbols([]string{"BTC-USDT", "SOL-USDT"}); len(frames) != 0 {
		t.Errorf("no-op change produced frames: %s", frames)
	}
}

func TestSubscribeErrorDoesNotEndSession(t *testing.T) {
	_, err := newFeed(t).Handle([]byte(`{"error":{"code":2,"msg":"Invalid request"},"id":3}`), now, nil)
	var se *exchange.SymbolError
	if !errors.As(err, &se) {
		t.Fatalf("err = %v, want *exchange.SymbolError", err)
	}
}

func TestNonTradeFramesAreIgnored(t *testing.T) {
	if ev := trades(t, newFeed(t), []byte(`{"result":null,"id":1}`)); len(ev) != 0 {
		t.Errorf("got %d events", len(ev))
	}
}

func TestDepthSnapshotBecomesBook(t *testing.T) {
	ev := trades(t, newFeed(t), frame(t, "depth20.json"))
	if len(ev) != 1 {
		t.Fatalf("got %d events", len(ev))
	}
	b := ev[0].(model.Book)
	if b.Exchange != "binance" || b.Symbol != "BTC-USDT" || b.Seq != 103182541822 || len(b.Bids) != 5 || len(b.Asks) != 5 {
		t.Fatalf("book = %+v", b)
	}
	if b.Bids[0].Price.String() != "63853.45" || !b.Bids[0].Price.LessThan(b.Asks[0].Price) {
		t.Errorf("best bid/ask = %s/%s", b.Bids[0].Price, b.Asks[0].Price)
	}
}
