// Package api is the backend for the web frontend: market history from
// ClickHouse, the built frontend itself, and reverse proxies to the gateway
// (websocket) and the alerts service (rules), so the browser talks to a
// single origin.
package api

import (
	"context"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
	"marketdata/internal/store"
)

// Subscriptions is the set of streamed markets (*subs.Table).
type Subscriptions interface {
	All() []model.Subscription
	Get(exchange, symbol string) (model.Subscription, bool)
	Put(ctx context.Context, s model.Subscription) error
	Delete(ctx context.Context, exchange, symbol string) error
}

// Catalog lists what each exchange trades (*catalog.Cache).
type Catalog interface {
	Exchanges() []string
	Get(ctx context.Context, exchange string) ([]exchange.Instrument, error)
	Has(ctx context.Context, exchange, symbol string) (bool, error)
}

// History is the read side the API needs; *store.ClickHouse implements it.
type History interface {
	Candles(ctx context.Context, exchange, symbol, base string, bucket, limit int) ([]store.CandleRow, error)
	RecentTrades(ctx context.Context, exchange, symbol string, limit int) ([]store.TradeRow, error)
	RecentAlerts(ctx context.Context, limit int) ([]store.AlertRow, error)
}

type Options struct {
	History History
	Subs    Subscriptions
	Catalog Catalog
	// MaxSubscriptions caps markets added through the API (pinned ones from
	// ingester config don't count).
	MaxSubscriptions int
	Frontend         fs.FS // built frontend (index.html at the root)
	Gateway          *url.URL
	Alerts           *url.URL
	Log              *slog.Logger
	Observe          func(route string, code int, d time.Duration)
}

func Handler(o Options) http.Handler {
	h := &handler{o: o}
	mux := http.NewServeMux()
	route := func(pattern string, fn http.HandlerFunc) { mux.Handle(pattern, h.instrument(pattern, fn)) }

	route("GET /api/markets", h.markets)
	route("GET /api/catalog", h.catalog)
	route("POST /api/subscriptions", h.subscribe)
	route("DELETE /api/subscriptions/{exchange}/{symbol}", h.unsubscribe)
	route("GET /api/candles", h.candles)
	route("GET /api/trades", h.trades)
	route("GET /api/alerts", h.alerts)

	// /api/rules... -> alerts service /v1/rules...
	rules := httputil.NewSingleHostReverseProxy(o.Alerts)
	rules.Director = rewrite(o.Alerts, "/api/rules", "/v1/rules", rules.Director)
	mux.Handle("/api/rules", h.instrument("/api/rules", rules.ServeHTTP))
	mux.Handle("/api/rules/", h.instrument("/api/rules/", rules.ServeHTTP))

	// Websocket upgrade is supported by ReverseProxy.
	mux.Handle("GET /ws", httputil.NewSingleHostReverseProxy(o.Gateway))

	// Method-less on purpose: "GET /" would conflict with the method-less
	// /api/rules/ proxy pattern in ServeMux's precedence rules.
	mux.Handle("/", spa(o.Frontend))
	return mux
}

type handler struct{ o Options }

// markets are the streamed markets: everything subscribed, pinned or not.
func (h *handler) markets(w http.ResponseWriter, r *http.Request) {
	reply(h, w, h.o.Subs.All(), nil)
}

type catalogItem struct {
	exchange.Instrument
	Subscribed bool `json:"subscribed"`
}

// catalog searches an exchange's instruments: ?exchange=binance&q=sol&limit=50.
// Matches on the symbol, base asset first.
func (h *handler) catalog(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("exchange")
	if !slicesContains(h.o.Catalog.Exchanges(), name) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unknown exchange", "exchanges": h.o.Catalog.Exchanges()})
		return
	}
	list, err := h.o.Catalog.Get(r.Context(), name)
	if err != nil {
		h.o.Log.Error("catalog fetch failed", "exchange", name, "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "exchange catalog unavailable"})
		return
	}
	needle := strings.ToUpper(strings.TrimSpace(q.Get("q")))
	n := limit(q, 50, 500)
	var prefix, contains []catalogItem
	for _, in := range list {
		item := func() catalogItem {
			_, sub := h.o.Subs.Get(name, in.Symbol)
			return catalogItem{Instrument: in, Subscribed: sub}
		}
		switch {
		case needle == "" || strings.HasPrefix(in.Symbol, needle):
			if len(prefix) < n {
				prefix = append(prefix, item())
			}
		case strings.Contains(in.Symbol, needle) && len(contains) < n:
			contains = append(contains, item())
		}
	}
	out := append(prefix, contains...)
	reply(h, w, out[:min(len(out), n)], nil)
}

type subscribeRequest struct {
	Exchange string `json:"exchange"`
	Symbol   string `json:"symbol"`
}

func (h *handler) subscribe(w http.ResponseWriter, r *http.Request) {
	var req subscribeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if !slicesContains(h.o.Catalog.Exchanges(), req.Exchange) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unknown exchange"})
		return
	}
	if existing, ok := h.o.Subs.Get(req.Exchange, req.Symbol); ok {
		writeJSON(w, http.StatusOK, existing) // idempotent
		return
	}
	// Only instruments the exchange actually trades: a typo would otherwise
	// make Binance silently stream nothing.
	ok, err := h.o.Catalog.Has(r.Context(), req.Exchange, req.Symbol)
	switch {
	case err != nil:
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "exchange catalog unavailable"})
		return
	case !ok:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": req.Exchange + " does not trade " + req.Symbol})
		return
	}
	dynamic := 0
	for _, s := range h.o.Subs.All() {
		if !s.Pinned {
			dynamic++
		}
	}
	if dynamic >= h.o.MaxSubscriptions {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "subscription limit reached"})
		return
	}
	s := model.Subscription{Exchange: req.Exchange, Symbol: req.Symbol, CreatedAt: time.Now().UTC()}
	if err := h.o.Subs.Put(r.Context(), s); err != nil {
		h.o.Log.Error("subscribe failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "could not save subscription"})
		return
	}
	writeJSON(w, http.StatusCreated, s)
}

func (h *handler) unsubscribe(w http.ResponseWriter, r *http.Request) {
	ex, sym := r.PathValue("exchange"), r.PathValue("symbol")
	s, ok := h.o.Subs.Get(ex, sym)
	switch {
	case !ok:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not subscribed"})
		return
	case s.Pinned:
		writeJSON(w, http.StatusConflict, map[string]string{"error": "pinned by ingester configuration"})
		return
	}
	if err := h.o.Subs.Delete(r.Context(), ex, sym); err != nil {
		h.o.Log.Error("unsubscribe failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "could not remove subscription"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func slicesContains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func (h *handler) candles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	exchange, symbol, period := q.Get("exchange"), q.Get("symbol"), q.Get("period")
	if exchange == "" || symbol == "" || period == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "exchange, symbol and period are required"})
		return
	}
	p, ok := Periods[period]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unknown period", "periods": PeriodNames})
		return
	}
	out, err := h.o.History.Candles(r.Context(), exchange, symbol, p.Base, p.Seconds, limit(q, 500, 2000))
	reply(h, w, out, err)
}

// Period is a chart interval built from a base candle the aggregator emits.
// Longer intervals are rolled up here (history) and in the client (live)
// rather than by the aggregator: its restart guarantee replays from the
// oldest open candle, which for a 1d candle would be a day of trades.
type Period struct {
	Base    string // "1s" or "1m"
	Seconds int
}

var PeriodNames = []string{"1s", "5s", "15s", "30s", "1m", "5m", "15m", "30m", "1h", "4h", "1d"}

var Periods = func() map[string]Period {
	m := map[string]Period{}
	for _, name := range PeriodNames {
		d, err := time.ParseDuration(strings.Replace(name, "d", "h", 1))
		if err != nil {
			panic(err)
		}
		if strings.HasSuffix(name, "d") {
			d *= 24
		}
		base := "1m"
		if d < time.Minute {
			base = "1s"
		}
		m[name] = Period{Base: base, Seconds: int(d / time.Second)}
	}
	return m
}()

func (h *handler) trades(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("exchange") == "" || q.Get("symbol") == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "exchange and symbol are required"})
		return
	}
	out, err := h.o.History.RecentTrades(r.Context(), q.Get("exchange"), q.Get("symbol"), limit(q, 50, 500))
	reply(h, w, out, err)
}

func (h *handler) alerts(w http.ResponseWriter, r *http.Request) {
	out, err := h.o.History.RecentAlerts(r.Context(), limit(r.URL.Query(), 50, 500))
	reply(h, w, out, err)
}

// reply writes a list; an empty result is [] rather than null. (A nil slice
// stored in an `any` is not == nil, hence the type parameter.)
func reply[T any](h *handler, w http.ResponseWriter, list []T, err error) {
	if err != nil {
		h.o.Log.Error("history query failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "history unavailable"})
		return
	}
	if list == nil {
		list = []T{}
	}
	writeJSON(w, http.StatusOK, list)
}

func limit(q url.Values, def, max int) int {
	n, err := strconv.Atoi(q.Get("limit"))
	if err != nil || n <= 0 {
		return def
	}
	return min(n, max)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// rewrite maps the public path prefix to the upstream one.
func rewrite(target *url.URL, from, to string, next func(*http.Request)) func(*http.Request) {
	return func(r *http.Request) {
		next(r)
		r.URL.Path = to + strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, target.Path), from)
		r.URL.RawPath = ""
		r.Host = target.Host
	}
}

// spa serves the frontend; unknown paths get index.html so client-side
// routes work. Without a build it explains how to make one.
func spa(files fs.FS) http.Handler {
	fileServer := http.FileServerFS(files)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r) // unknown API route: never answer with index.html
			return
		}
		if _, err := fs.Stat(files, "index.html"); err != nil {
			http.Error(w, "frontend is not built: run `make frontend` (or use the Docker image)", http.StatusNotFound)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" {
			if _, err := fs.Stat(files, path); err != nil {
				r = r.Clone(r.Context())
				r.URL.Path = "/"
			}
		}
		if strings.HasPrefix(path, "assets/") {
			// Vite puts a content hash in asset names: cache them forever.
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		fileServer.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

func (h *handler) instrument(route string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next(rec, r)
		if h.o.Observe != nil {
			h.o.Observe(route, rec.code, time.Since(start))
		}
	})
}
