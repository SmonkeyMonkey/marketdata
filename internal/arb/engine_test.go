package arb

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

var t0 = time.Unix(1_800_000_000, 0).UTC()

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// levels builds "price:size" levels.
func levels(ls ...string) []model.BookLevel {
	out := make([]model.BookLevel, len(ls))
	for i, l := range ls {
		p, s, _ := strings.Cut(l, ":")
		out[i] = model.BookLevel{Price: d(p), Size: d(s)}
	}
	return out
}

func book(exchange, symbol string, at time.Time, bids, asks []model.BookLevel) model.Book {
	return model.Book{Exchange: exchange, Symbol: symbol, Bids: bids, Asks: asks, ExchangeTime: at, ReceiveTime: at}
}

func engine(fees map[string]string) *Engine {
	f := map[string]decimal.Decimal{}
	for k, v := range fees {
		f[k] = d(v)
	}
	return NewEngine(Config{
		Quotes:        map[string]string{"USDT": "USD", "USDC": "USD"},
		FX:            map[string]string{"USDT": "kraken:USDT-USD"},
		FeesBps:       f,
		AlertBps:      d("10"),
		AlertCooldown: time.Minute,
		MaxBookAge:    5 * time.Second,
	})
}

func routeOf(s model.ArbSnapshot, buy, sell string) (model.ArbRoute, bool) {
	for _, r := range s.Routes {
		if r.BuyExchange == buy && r.SellExchange == sell {
			return r, true
		}
	}
	return model.ArbRoute{}, false
}

func lastSnapshot(t *testing.T, out Output) model.ArbSnapshot {
	t.Helper()
	if len(out.Snapshots) == 0 {
		t.Fatal("no snapshot")
	}
	return out.Snapshots[len(out.Snapshots)-1]
}

func TestAssetGroupsQuotes(t *testing.T) {
	e := engine(nil)
	for sym, want := range map[string]string{"BTC-USDT": "BTC-USD", "BTC-USD": "BTC-USD", "ETH-USDC": "ETH-USD", "ETH-EUR": "ETH-EUR"} {
		if got, _ := e.AssetOf(sym); got != want {
			t.Errorf("AssetOf(%s) = %s, want %s", sym, got, want)
		}
	}
	if _, ok := e.AssetOf("BTCUSD"); ok {
		t.Error("symbol without a quote accepted")
	}
}

func TestRouteNetOfFeesAndDepth(t *testing.T) {
	e := engine(map[string]string{"kraken": "10", "binance": "10"})
	e.Update(book("kraken", "BTC-USD", t0, levels("99:1"), levels("100:1", "100.5:2", "103:5")), t0)
	out := e.Update(book("binance", "BTC-USD", t0, levels("102:1.5", "101:1", "99:10"), levels("103:1")), t0)

	r, ok := routeOf(lastSnapshot(t, out), "kraken", "binance")
	if !ok {
		t.Fatal("route kraken -> binance missing")
	}
	// Top: buy 100 * 1.001 = 100.1, sell 102 * 0.999 = 101.898.
	if r.GrossBps.String() != "200" || r.NetBps.String() != "179.62" {
		t.Errorf("gross %s net %s, want 200 / 179.62", r.GrossBps, r.NetBps)
	}
	// Walk: 1 @100 -> 102, 0.5 @100.5 -> 102, 1 @100.5 -> 101; then 100.5 vs
	// 99 is a loss. Size 2.5.
	if r.Size.String() != "2.5" {
		t.Errorf("size = %s, want 2.5", r.Size)
	}
	// 1*(101.898-100.1) + 0.5*(101.898-100.6005) + 1*(100.899-100.6005)
	want := d("1.798").Add(d("0.5").Mul(d("1.2975"))).Add(d("0.2985"))
	if !r.Profit.Equal(want) {
		t.Errorf("profit = %s, want %s", r.Profit, want)
	}
	if back, _ := routeOf(lastSnapshot(t, out), "binance", "kraken"); back.Profitable() || !back.NetBps.IsNegative() {
		t.Errorf("reverse route must lose: %+v", back)
	}
}

func TestFeesEatTheSpread(t *testing.T) {
	e := engine(map[string]string{"coinbase": "60", "kraken": "40"})
	e.Update(book("coinbase", "BTC-USD", t0, levels("99.9:1"), levels("100:1")), t0)
	out := e.Update(book("kraken", "BTC-USD", t0, levels("100.5:1"), levels("100.6:1")), t0)
	r, _ := routeOf(lastSnapshot(t, out), "coinbase", "kraken")
	if !r.GrossBps.IsPositive() || r.NetBps.IsPositive() || r.Profitable() {
		t.Errorf("50 bps gross minus 100 bps fees must not be an opportunity: %+v", r)
	}
	if len(out.Alerts) > 0 || e.OpenWindows() > 0 {
		t.Error("window or alert without a net edge")
	}
}

func TestQuoteConversion(t *testing.T) {
	e := engine(nil)
	e.Update(book("coinbase", "BTC-USD", t0, levels("100:1"), levels("100.1:1")), t0)
	out := e.Update(book("binance", "BTC-USDT", t0, levels("100:1"), levels("100.1:1")), t0)
	v := lastSnapshot(t, out).Venues[0] // binance sorts first
	if v.Exchange != "binance" || v.RateSource != "peg" || !v.Bid.Equal(d("100")) {
		t.Errorf("without a rate market USDT is pegged: %+v", v)
	}

	// USDT trades at 0.999 USD: binance prices are converted down.
	e.Update(book("kraken", "USDT-USD", t0, levels("0.9989:1000"), levels("0.9991:1000")), t0)
	out = e.Update(book("binance", "BTC-USDT", t0.Add(time.Millisecond), levels("100:1"), levels("100.1:1")), t0.Add(time.Second))
	v = lastSnapshot(t, out).Venues[0]
	if v.RateSource != "kraken:USDT-USD" || !v.Rate.Equal(d("0.999")) || !v.Bid.Equal(d("99.9")) {
		t.Errorf("converted venue = %+v", v)
	}

	// A rate book unchanged for a minute is still used (FXMaxAge), a trading
	// book that old is not.
	e.cfg.FXMaxAge = 5 * time.Minute
	later := t0.Add(time.Minute)
	out = e.Update(book("binance", "BTC-USDT", later, levels("100:1"), levels("100.1:1")), later)
	if v := lastSnapshot(t, out).Venues[0]; v.RateSource != "kraken:USDT-USD" {
		t.Errorf("rate dropped after a quiet minute: %+v", v)
	}
	if n := len(lastSnapshot(t, out).Venues); n != 1 {
		t.Errorf("venues = %d, want only binance (coinbase is a minute old)", n)
	}
}

func TestWindowLifecycleAndAlert(t *testing.T) {
	e := engine(nil)
	e.Update(book("kraken", "BTC-USD", t0, levels("99:1"), levels("100:1")), t0)
	open := e.Update(book("binance", "BTC-USD", t0, levels("100.2:1"), levels("101:1")), t0)
	if e.OpenWindows() != 1 || len(open.Alerts) != 1 {
		t.Fatalf("open windows %d alerts %d, want 1 and 1 (20 bps >= 10)", e.OpenWindows(), len(open.Alerts))
	}
	a := open.Alerts[0]
	if a.Type != model.AlertArbitrage || a.Exchange != "kraken" || !strings.Contains(a.Message, "+20 bps") {
		t.Errorf("alert = %+v", a)
	}

	// Wider, then gone.
	t1, t2 := t0.Add(100*time.Millisecond), t0.Add(300*time.Millisecond)
	wider := e.Update(book("binance", "BTC-USD", t1, levels("100.5:2"), levels("101:1")), t1)
	if len(wider.Alerts) != 0 {
		t.Error("second alert in the same window")
	}
	closed := e.Update(book("binance", "BTC-USD", t2, levels("99.5:1"), levels("101:1")), t2)
	if len(closed.Windows) != 1 || e.OpenWindows() != 0 {
		t.Fatalf("closed %d open %d", len(closed.Windows), e.OpenWindows())
	}
	w := closed.Windows[0]
	if w.Duration() != 300*time.Millisecond || w.PeakNetBps.String() != "50" || w.PeakSize.String() != "1" || w.Updates != 2 {
		t.Errorf("window = %+v (duration %s)", w, w.Duration())
	}
	if !strings.HasPrefix(a.ID, "arb:"+w.ID) {
		t.Errorf("alert id %q does not derive from window id %q", a.ID, w.ID)
	}

	// Reopens within the cooldown: a window, but no new alert.
	t3 := t0.Add(time.Second)
	again := e.Update(book("binance", "BTC-USD", t3, levels("100.5:1"), levels("101:1")), t3)
	if e.OpenWindows() != 1 || len(again.Alerts) != 0 {
		t.Errorf("reopen: open %d alerts %d, want 1 and 0", e.OpenWindows(), len(again.Alerts))
	}
}

func TestStaleBookClosesWindow(t *testing.T) {
	e := engine(nil)
	e.Update(book("kraken", "BTC-USD", t0, levels("99:1"), levels("100:1")), t0)
	e.Update(book("binance", "BTC-USD", t0, levels("100.2:1"), levels("101:1")), t0)
	if e.OpenWindows() != 1 {
		t.Fatal("no window")
	}
	if out := e.Tick(t0.Add(4 * time.Second)); len(out.Windows) != 0 {
		t.Error("closed before MaxBookAge")
	}
	out := e.Tick(t0.Add(6 * time.Second))
	if len(out.Windows) != 1 || e.OpenWindows() != 0 {
		t.Fatalf("a dead feed must close the window: %+v", out.Windows)
	}
	if s := lastSnapshot(t, out); len(s.Venues) != 0 || len(s.Routes) != 0 {
		t.Errorf("stale venues still published: %+v", s)
	}
}

func TestCrossedBookIgnored(t *testing.T) {
	e := engine(nil)
	e.Update(book("kraken", "BTC-USD", t0, levels("101:1"), levels("100:1")), t0) // bid > ask
	out := e.Update(book("binance", "BTC-USD", t0, levels("100.5:1"), levels("100.6:1")), t0)
	if s := lastSnapshot(t, out); len(s.Venues) != 1 || e.OpenWindows() != 0 {
		t.Errorf("crossed book used: %+v", s)
	}
}

func TestSnapshotsAreThrottled(t *testing.T) {
	e := engine(nil)
	e.cfg.PublishEvery = 250 * time.Millisecond
	e.Update(book("kraken", "BTC-USD", t0, levels("99:1"), levels("100:1")), t0)
	if out := e.Update(book("kraken", "BTC-USD", t0.Add(time.Millisecond), levels("99:1"), levels("100:1")), t0.Add(100*time.Millisecond)); len(out.Snapshots) != 0 {
		t.Error("snapshot inside the publish interval")
	}
	if out := e.Tick(t0.Add(300 * time.Millisecond)); len(out.Snapshots) != 1 {
		t.Error("held-back snapshot not published by the tick")
	}
	if out := e.Tick(t0.Add(600 * time.Millisecond)); len(out.Snapshots) != 0 {
		t.Error("snapshot without changes")
	}
}

func TestCloseAll(t *testing.T) {
	e := engine(nil)
	e.Update(book("kraken", "BTC-USD", t0, levels("99:1"), levels("100:1")), t0)
	e.Update(book("binance", "BTC-USD", t0, levels("100.2:1"), levels("101:1")), t0)
	if ws := e.CloseAll(t0.Add(time.Second)); len(ws) != 1 || ws[0].Duration() != time.Second || e.OpenWindows() != 0 {
		t.Errorf("CloseAll = %+v", ws)
	}
}

func TestParseConfig(t *testing.T) {
	if f, err := ParseFees([]string{"binance:10", "kraken:40"}); err != nil || !f["kraken"].Equal(d("40")) {
		t.Errorf("fees = %v %v", f, err)
	}
	for _, bad := range []string{"binance", "binance:-1", "binance:x", ":5"} {
		if _, err := ParseFees([]string{bad}); err == nil {
			t.Errorf("ParseFees(%q) accepted", bad)
		}
	}
	if p, err := ParsePairs([]string{"USDT:kraken:USDT-USD"}); err != nil || p["USDT"] != "kraken:USDT-USD" {
		t.Errorf("pairs = %v %v", p, err)
	}
}

func BenchmarkUpdate(b *testing.B) {
	e := engine(map[string]string{"coinbase": "60", "kraken": "40", "binance": "10"})
	var bids, asks []string
	for i := range 50 {
		bids = append(bids, decimal.NewFromInt(int64(64000-i)).String()+":0.5")
		asks = append(asks, decimal.NewFromInt(int64(64001+i)).String()+":0.5")
	}
	for _, m := range []string{"coinbase:BTC-USD", "kraken:BTC-USD", "binance:BTC-USDT"} {
		ex, sym, _ := strings.Cut(m, ":")
		e.Update(book(ex, sym, t0, levels(bids...), levels(asks...)), t0)
	}
	bk := book("binance", "BTC-USDT", t0, levels(bids...), levels(asks...))
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		bk.ReceiveTime = t0.Add(time.Duration(i) * time.Microsecond)
		e.Update(bk, bk.ReceiveTime)
	}
}
