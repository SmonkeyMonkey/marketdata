package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
	"marketdata/internal/store"
)

type fakeSubs struct {
	mu    sync.Mutex
	items map[string]model.Subscription
}

func newFakeSubs() *fakeSubs {
	return &fakeSubs{items: map[string]model.Subscription{
		"binance:BTC-USDT": {Exchange: "binance", Symbol: "BTC-USDT", Pinned: true},
	}}
}

func (f *fakeSubs) All() []model.Subscription {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []model.Subscription
	for _, s := range f.items {
		out = append(out, s)
	}
	return out
}

func (f *fakeSubs) Get(ex, sym string) (model.Subscription, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.items[ex+":"+sym]
	return s, ok
}

func (f *fakeSubs) Put(_ context.Context, s model.Subscription) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[s.Key()] = s
	return nil
}

func (f *fakeSubs) Delete(_ context.Context, ex, sym string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.items, ex+":"+sym)
	return nil
}

type fakeSettings struct {
	def     decimal.Decimal
	markets map[string]model.MarketSettings
}

func (f *fakeSettings) DefaultWhale() decimal.Decimal { return f.def }
func (f *fakeSettings) Markets() []model.MarketSettings {
	var out []model.MarketSettings
	for _, m := range f.markets {
		out = append(out, m)
	}
	return out
}
func (f *fakeSettings) SetWhale(_ context.Context, ex, sym string, v decimal.Decimal) error {
	if ex == "" {
		f.def = v
		return nil
	}
	if f.markets == nil {
		f.markets = map[string]model.MarketSettings{}
	}
	f.markets[ex+":"+sym] = model.MarketSettings{Exchange: ex, Symbol: sym, WhaleNotional: v}
	return nil
}
func (f *fakeSettings) ResetWhale(_ context.Context, ex, sym string) error {
	delete(f.markets, ex+":"+sym)
	return nil
}

func TestSettings(t *testing.T) {
	s, _ := server(t, &fakeHistory{}, nil)
	if _, body := get(t, s.URL+"/api/settings"); !strings.Contains(body, `"default_whale":"100000"`) || !strings.Contains(body, `"markets":[]`) {
		t.Fatalf("initial = %s", body)
	}
	if c, body := send(t, "PUT", s.URL+"/api/settings/binance/XRP-BTC", `{"whale_notional":"0.5"}`); c != 200 || !strings.Contains(body, `"whale_notional":"0.5"`) {
		t.Errorf("set market -> %d %s", c, body)
	}
	if c, body := send(t, "PUT", s.URL+"/api/settings/default", `{"whale_notional":"250000"}`); c != 200 || !strings.Contains(body, `"default_whale":"250000"`) {
		t.Errorf("set default -> %d %s", c, body)
	}
	for _, bad := range []string{`{"whale_notional":"-1"}`, `{"whale_notional":"lots"}`, `nope`} {
		if c, _ := send(t, "PUT", s.URL+"/api/settings/default", bad); c != 400 {
			t.Errorf("%s -> %d, want 400", bad, c)
		}
	}
	if c, body := send(t, "DELETE", s.URL+"/api/settings/binance/XRP-BTC", ""); c != 200 || strings.Contains(body, "XRP-BTC") {
		t.Errorf("reset -> %d %s", c, body)
	}
}

type fakeCatalog struct{}

var instruments = []exchange.Instrument{
	{Symbol: "BTC-USDT", Base: "BTC", Quote: "USDT"}, {Symbol: "ETH-USDT", Base: "ETH", Quote: "USDT"},
	{Symbol: "SOL-USDT", Base: "SOL", Quote: "USDT"}, {Symbol: "USDT-TRY", Base: "USDT", Quote: "TRY"},
	{Symbol: "XRP-USDT", Base: "XRP", Quote: "USDT"},
}

func (fakeCatalog) Exchanges() []string { return []string{"binance"} }
func (fakeCatalog) Get(context.Context, string) ([]exchange.Instrument, error) {
	return instruments, nil
}
func (fakeCatalog) Has(_ context.Context, _, sym string) (bool, error) {
	for _, in := range instruments {
		if in.Symbol == sym {
			return true, nil
		}
	}
	return false, nil
}

func send(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestSubscribeLifecycle(t *testing.T) {
	s, _ := server(t, &fakeHistory{}, nil)
	add := func(sym string) int {
		code, _ := send(t, "POST", s.URL+"/api/subscriptions", `{"exchange":"binance","symbol":"`+sym+`"}`)
		return code
	}
	if c := add("SOL-USDT"); c != 201 {
		t.Fatalf("add SOL -> %d", c)
	}
	if c := add("SOL-USDT"); c != 200 {
		t.Errorf("re-add -> %d, want 200 (idempotent)", c)
	}
	if c := add("NOPE-USDT"); c != 404 {
		t.Errorf("not traded -> %d, want 404", c)
	}
	if c, _ := send(t, "POST", s.URL+"/api/subscriptions", `{"exchange":"ftx","symbol":"BTC-USD"}`); c != 400 {
		t.Errorf("unknown exchange -> %d", c)
	}
	add("ETH-USDT")
	if c := add("XRP-USDT"); c != 429 {
		t.Errorf("third dynamic subscription -> %d, want 429 (limit 2, pinned excluded)", c)
	}
	if _, body := get(t, s.URL+"/api/markets"); !strings.Contains(body, "SOL-USDT") {
		t.Errorf("markets = %s", body)
	}
	if c, _ := send(t, "DELETE", s.URL+"/api/subscriptions/binance/BTC-USDT", ""); c != 409 {
		t.Errorf("delete pinned -> %d, want 409", c)
	}
	if c, _ := send(t, "DELETE", s.URL+"/api/subscriptions/binance/SOL-USDT", ""); c != 204 {
		t.Errorf("delete -> %d", c)
	}
	if c, _ := send(t, "DELETE", s.URL+"/api/subscriptions/binance/SOL-USDT", ""); c != 404 {
		t.Errorf("delete again -> %d", c)
	}
}

func TestCatalogSearch(t *testing.T) {
	s, _ := server(t, &fakeHistory{}, nil)
	_, body := get(t, s.URL+"/api/catalog?exchange=binance&q=usdt")
	var items []struct {
		Symbol     string `json:"symbol"`
		Subscribed bool   `json:"subscribed"`
	}
	if err := json.Unmarshal([]byte(body), &items); err != nil {
		t.Fatal(body)
	}
	// Prefix matches (USDT-TRY) first, then symbols containing the query.
	if len(items) != 5 || items[0].Symbol != "USDT-TRY" {
		t.Fatalf("items = %+v", items)
	}
	for _, it := range items {
		if it.Symbol == "BTC-USDT" && !it.Subscribed {
			t.Error("BTC-USDT should be marked subscribed")
		}
	}
	if c, _ := get(t, s.URL+"/api/catalog?exchange=nope"); c != 400 {
		t.Errorf("unknown exchange -> %d", c)
	}
}

type fakeHistory struct {
	fail   bool
	gotArg string
}

func (f *fakeHistory) Candles(_ context.Context, ex, sym, base string, bucket, limit int) ([]store.CandleRow, error) {
	if f.fail {
		return nil, errors.New("clickhouse down")
	}
	f.gotArg = strings.Join([]string{ex, sym, base, itoa(bucket), itoa(limit)}, "|")
	return []store.CandleRow{{Start: time.Unix(60, 0).UTC(), Open: decimal.NewFromInt(1), Close: decimal.RequireFromString("1.5")}}, nil
}

func (f *fakeHistory) RecentTrades(context.Context, string, string, int) ([]store.TradeRow, error) {
	return nil, nil
}

func (f *fakeHistory) RecentAlerts(context.Context, int) ([]store.AlertRow, error) { return nil, nil }

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func server(t *testing.T, h History, frontend fstest.MapFS) (*httptest.Server, *httptest.Server) {
	t.Helper()
	// Stand-in for the alerts service: echoes the path it received.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Method + " " + r.URL.Path))
	}))
	t.Cleanup(upstream.Close)
	u, _ := url.Parse(upstream.URL)
	s := httptest.NewServer(Handler(Options{
		History: h, Frontend: frontend, Gateway: u, Alerts: u,
		Subs: newFakeSubs(), Catalog: fakeCatalog{}, MaxSubscriptions: 2, Settings: &fakeSettings{def: decimal.NewFromInt(100_000)},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}))
	t.Cleanup(s.Close)
	return s, upstream
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestCandles(t *testing.T) {
	h := &fakeHistory{}
	s, _ := server(t, h, nil)
	code, body := get(t, s.URL+"/api/candles?exchange=binance&symbol=BTC-USDT&period=1m&limit=99999")
	if code != 200 || !strings.Contains(body, `"close":"1.5"`) {
		t.Fatalf("%d %s", code, body)
	}
	if h.gotArg != "binance|BTC-USDT|1m|60|2000" {
		t.Errorf("args = %s (limit must be capped)", h.gotArg)
	}
	for period, want := range map[string]string{"15s": "1s|15", "4h": "1m|14400", "1d": "1m|86400", "1s": "1s|1"} {
		get(t, s.URL+"/api/candles?exchange=binance&symbol=BTC-USDT&period="+period)
		if !strings.Contains(h.gotArg, "|"+want+"|") {
			t.Errorf("%s -> %s, want base|seconds %s", period, h.gotArg, want)
		}
	}
	if code, _ := get(t, s.URL+"/api/candles?exchange=binance&symbol=BTC-USDT&period=7m"); code != 400 {
		t.Errorf("unknown period -> %d", code)
	}
	if code, _ := get(t, s.URL+"/api/candles?exchange=binance"); code != 400 {
		t.Errorf("missing params -> %d", code)
	}
	h.fail = true
	if code, body := get(t, s.URL+"/api/candles?exchange=a&symbol=b&period=1m"); code != 503 || strings.Contains(body, "clickhouse") {
		t.Errorf("failure -> %d %s (must not leak internals)", code, body)
	}
}

func TestEmptyResultIsArray(t *testing.T) {
	s, _ := server(t, &fakeHistory{}, nil)
	if _, body := get(t, s.URL+"/api/alerts"); strings.TrimSpace(body) != "[]" {
		t.Errorf("body = %q, want []", body)
	}
}

func TestRulesAreProxiedToAlertsService(t *testing.T) {
	s, _ := server(t, &fakeHistory{}, nil)
	if _, body := get(t, s.URL+"/api/rules/abc"); body != "GET /v1/rules/abc" {
		t.Errorf("upstream saw %q", body)
	}
	resp, err := http.Post(s.URL+"/api/rules", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "POST /v1/rules" {
		t.Errorf("upstream saw %q", b)
	}
}

func TestSPA(t *testing.T) {
	files := fstest.MapFS{
		"index.html":      {Data: []byte("<html>app</html>")},
		"assets/app-1.js": {Data: []byte("js")},
	}
	s, _ := server(t, &fakeHistory{}, files)
	if _, body := get(t, s.URL+"/"); body != "<html>app</html>" {
		t.Errorf("/ -> %q", body)
	}
	if _, body := get(t, s.URL+"/some/client/route"); body != "<html>app</html>" {
		t.Errorf("deep link -> %q, want index.html", body)
	}
	resp, _ := http.Get(s.URL + "/assets/app-1.js")
	resp.Body.Close()
	if !strings.Contains(resp.Header.Get("Cache-Control"), "immutable") {
		t.Error("hashed assets should be cached forever")
	}

	if code, _ := get(t, s.URL+"/api/nope"); code != 404 {
		t.Errorf("unknown API route -> %d, want 404 (not index.html)", code)
	}

	s2, _ := server(t, &fakeHistory{}, fstest.MapFS{})
	if code, body := get(t, s2.URL+"/"); code != 404 || !strings.Contains(body, "make frontend") {
		t.Errorf("unbuilt frontend -> %d %q", code, body)
	}
}
