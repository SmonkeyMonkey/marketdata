// Package arb compares the order books of one asset across exchanges and
// finds moments when buying on one and selling on another at the same time
// is profitable after taker fees.
//
// Markets are grouped by asset: base plus a common quote, so binance
// BTC-USDT, coinbase BTC-USD and kraken BTC-USD are all BTC-USD. Prices in
// another quote are converted with that quote's live rate (the mid of e.g.
// kraken USDT-USD) and assumed pegged 1:1 while the rate market is stale.
//
// The engine is pure: it takes books and a clock and returns what to
// publish, so it is tested without Kafka.
package arb

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

type Config struct {
	// Quotes maps a quote currency to the common quote it is compared in
	// (USDT -> USD). A quote not listed is its own common quote.
	Quotes map[string]string
	// FX maps a quote to the market quoting it in its common quote
	// (USDT -> kraken:USDT-USD); its mid is the conversion rate.
	FX map[string]string
	// FeesBps is the taker fee per exchange in basis points (missing: 0).
	FeesBps map[string]decimal.Decimal
	// A route is an opportunity while its net edge is above MinNetBps.
	MinNetBps decimal.Decimal
	// AlertBps fires an alert once per window when the net edge reaches it,
	// at most once per AlertCooldown per route; 0 disables alerts.
	AlertBps      decimal.Decimal
	AlertCooldown time.Duration
	// MaxBookAge: a book not updated for this long (receive time) is left
	// out, so a dead feed cannot produce phantom opportunities.
	MaxBookAge time.Duration
	// PublishEvery limits snapshots per asset; windows are still evaluated
	// on every book.
	PublishEvery time.Duration
}

// Output is what an Update or Tick produced.
type Output struct {
	Snapshots []model.ArbSnapshot
	Windows   []model.ArbWindow // closed windows
	Alerts    []model.Alert
}

func (o *Output) merge(x Output) {
	o.Snapshots = append(o.Snapshots, x.Snapshots...)
	o.Windows = append(o.Windows, x.Windows...)
	o.Alerts = append(o.Alerts, x.Alerts...)
}

type Engine struct {
	cfg    Config
	books  map[string]model.Book // by exchange:symbol
	assets map[string]*asset
	// fxMarkets are the rate markets; their updates re-price other assets.
	fxMarkets map[string]bool
	buyMul    map[string]decimal.Decimal // 1 + fee, per exchange
	sellMul   map[string]decimal.Decimal // 1 - fee
}

type asset struct {
	name    string
	markets map[string]bool
	open    map[string]*window // by route key
	// lastAlert per route, for the cooldown.
	lastAlert   map[string]time.Time
	lastPublish time.Time
	lastVenues  string // venue keys of the last snapshot
	dirty       bool   // evaluated since the last published snapshot
}

type window struct {
	model.ArbWindow
	alerted bool
}

var (
	bpsUnit = decimal.NewFromInt(10_000)
	one     = decimal.NewFromInt(1)
)

func NewEngine(cfg Config) *Engine {
	e := &Engine{
		cfg: cfg, books: map[string]model.Book{}, assets: map[string]*asset{},
		fxMarkets: map[string]bool{}, buyMul: map[string]decimal.Decimal{}, sellMul: map[string]decimal.Decimal{},
	}
	for _, m := range cfg.FX {
		e.fxMarkets[m] = true
	}
	for ex, fee := range cfg.FeesBps {
		f := fee.Div(bpsUnit)
		e.buyMul[ex], e.sellMul[ex] = one.Add(f), one.Sub(f)
	}
	return e
}

// AssetOf maps a market symbol to its asset: BTC-USDT -> BTC-USD.
func (e *Engine) AssetOf(symbol string) (string, bool) {
	base, quote, ok := strings.Cut(symbol, "-")
	if !ok || base == "" || quote == "" {
		return "", false
	}
	return base + "-" + e.common(quote), true
}

func (e *Engine) common(quote string) string {
	if c, ok := e.cfg.Quotes[quote]; ok {
		return c
	}
	return quote
}

// Update takes a book that is newer than the previous one of its market
// (the caller drops replays and copies from redundant ingesters).
func (e *Engine) Update(b model.Book, now time.Time) Output {
	key := b.Exchange + ":" + b.Symbol
	name, ok := e.AssetOf(b.Symbol)
	if !ok {
		return Output{}
	}
	e.books[key] = b
	a := e.assets[name]
	if a == nil {
		a = &asset{name: name, markets: map[string]bool{}, open: map[string]*window{}, lastAlert: map[string]time.Time{}}
		e.assets[name] = a
	}
	a.markets[key] = true
	if e.fxMarkets[key] {
		// Rates changed: other assets are re-priced on the next tick.
		for _, other := range e.assets {
			other.dirty = true
		}
	}
	at := b.ReceiveTime
	if at.IsZero() {
		at = now
	}
	return e.evaluate(a, at, now, false)
}

// Tick re-evaluates every asset: books that went stale drop out (closing
// their windows) and snapshots held back by PublishEvery go out.
func (e *Engine) Tick(now time.Time) Output {
	var out Output
	for _, name := range slices.Sorted(maps.Keys(e.assets)) {
		out.merge(e.evaluate(e.assets[name], now, now, true))
	}
	return out
}

// CloseAll closes every open window (on shutdown), so none is lost.
func (e *Engine) CloseAll(now time.Time) []model.ArbWindow {
	var out []model.ArbWindow
	for _, a := range e.assets {
		for k, w := range a.open {
			w.Closed = now
			out = append(out, w.ArbWindow)
			delete(a.open, k)
		}
	}
	return out
}

// OpenWindows counts routes that are profitable right now.
func (e *Engine) OpenWindows() int {
	n := 0
	for _, a := range e.assets {
		n += len(a.open)
	}
	return n
}

// evaluate compares the fresh venues of an asset. at is the event time of
// what triggered it (receive time of the book, or now on a tick).
func (e *Engine) evaluate(a *asset, at, now time.Time, tick bool) Output {
	var out Output
	books := e.venues(a, now)
	snap := model.ArbSnapshot{Asset: a.name, Time: now, Venues: make([]model.ArbVenue, len(books))}
	for i, vb := range books {
		snap.Venues[i] = vb.ArbVenue
	}
	for i := range books {
		for j := range books {
			if i != j {
				snap.Routes = append(snap.Routes, e.route(books[i], books[j]))
			}
		}
	}
	slices.SortStableFunc(snap.Routes, func(x, y model.ArbRoute) int { return y.NetBps.Cmp(x.NetBps) })

	e.trackWindows(a, snap, at, now, &out)

	venues := venueKeys(snap.Venues)
	if venues != a.lastVenues {
		a.dirty = true // a venue appeared or went stale
	}
	if !tick {
		a.dirty = true
	}
	if a.dirty && now.Sub(a.lastPublish) >= e.cfg.PublishEvery {
		out.Snapshots = append(out.Snapshots, snap)
		a.lastPublish, a.lastVenues, a.dirty = now, venues, false
	}
	return out
}

func venueKeys(vs []model.ArbVenue) string {
	var b strings.Builder
	for _, v := range vs {
		b.WriteString(v.Key())
		b.WriteByte(',')
	}
	return b.String()
}

func (e *Engine) trackWindows(a *asset, snap model.ArbSnapshot, at, now time.Time, out *Output) {
	live := map[string]bool{}
	for _, r := range snap.Routes {
		if !r.Profitable() || !r.NetBps.GreaterThan(e.cfg.MinNetBps) {
			continue
		}
		key := r.Key()
		live[key] = true
		w := a.open[key]
		if w == nil {
			w = &window{ArbWindow: model.ArbWindow{
				ID:    fmt.Sprintf("%s|%s|%d", a.name, key, at.UnixNano()),
				Asset: a.name, BuyExchange: r.BuyExchange, BuySymbol: r.BuySymbol,
				SellExchange: r.SellExchange, SellSymbol: r.SellSymbol, Opened: at,
			}}
			a.open[key] = w
		}
		w.Updates++
		if r.NetBps.GreaterThan(w.PeakNetBps) || w.Updates == 1 {
			w.PeakNetBps = r.NetBps
		}
		if r.GrossBps.GreaterThan(w.PeakGrossBps) || w.Updates == 1 {
			w.PeakGrossBps = r.GrossBps
		}
		if r.Profit.GreaterThan(w.PeakProfit) {
			w.PeakProfit, w.PeakSize = r.Profit, r.Size
		}
		if e.shouldAlert(a, w, r, now) {
			w.alerted = true
			a.lastAlert[key] = now
			out.Alerts = append(out.Alerts, alertFor(a.name, w, r, at))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(a.open)) {
		if !live[key] {
			w := a.open[key]
			w.Closed = at
			out.Windows = append(out.Windows, w.ArbWindow)
			delete(a.open, key)
		}
	}
}

func (e *Engine) shouldAlert(a *asset, w *window, r model.ArbRoute, now time.Time) bool {
	if w.alerted || !e.cfg.AlertBps.IsPositive() || r.NetBps.LessThan(e.cfg.AlertBps) {
		return false
	}
	last, ok := a.lastAlert[r.Key()]
	return !ok || now.Sub(last) >= e.cfg.AlertCooldown
}

func alertFor(asset string, w *window, r model.ArbRoute, at time.Time) model.Alert {
	_, quote, _ := strings.Cut(asset, "-")
	base, _, _ := strings.Cut(asset, "-")
	return model.Alert{
		ID: "arb:" + w.ID, Type: model.AlertArbitrage,
		Exchange: r.BuyExchange, Symbol: r.BuySymbol, Price: r.BuyPrice, Time: at,
		Notional: r.Size.Mul(r.BuyPrice).Round(2), ChangePct: r.NetBps.Div(decimal.NewFromInt(100)).Round(4),
		Message: fmt.Sprintf("%s: buy %s %s at %s, sell %s %s at %s: %s bps after fees, %s %s for %s %s profit",
			asset, r.BuyExchange, r.BuySymbol, r.BuyPrice, r.SellExchange, r.SellSymbol, r.SellPrice,
			signed(r.NetBps), r.Size.String(), base, r.Profit.StringFixed(2), quote),
	}
}

func signed(d decimal.Decimal) string {
	if d.IsPositive() {
		return "+" + d.String()
	}
	return d.String()
}

// venueBook is a fresh venue with the book it came from.
type venueBook struct {
	model.ArbVenue
	book model.Book
}

func (e *Engine) venues(a *asset, now time.Time) []venueBook {
	var out []venueBook
	for _, key := range slices.Sorted(maps.Keys(a.markets)) {
		b, ok := e.books[key]
		if !ok || !e.fresh(b, now) || len(b.Bids) == 0 || len(b.Asks) == 0 {
			continue
		}
		if b.Bids[0].Price.GreaterThanOrEqual(b.Asks[0].Price) {
			continue // crossed: the local book is broken, not an opportunity
		}
		_, quote, _ := strings.Cut(b.Symbol, "-")
		rate, src := e.rate(quote, now)
		out = append(out, venueBook{book: b, ArbVenue: model.ArbVenue{
			Exchange: b.Exchange, Symbol: b.Symbol,
			Bid: b.Bids[0].Price.Mul(rate), Ask: b.Asks[0].Price.Mul(rate),
			BidSize: b.Bids[0].Size, AskSize: b.Asks[0].Size,
			Rate: rate, RateSource: src, FeeBps: e.cfg.FeesBps[b.Exchange], BookTime: b.ExchangeTime,
		}})
	}
	return out
}

func (e *Engine) fresh(b model.Book, now time.Time) bool {
	return !b.ReceiveTime.IsZero() && now.Sub(b.ReceiveTime) <= e.cfg.MaxBookAge
}

// rate converts quote to its common quote: 1 for the common quote itself,
// the mid of the FX market when it is fresh, else 1 (assumed pegged).
func (e *Engine) rate(quote string, now time.Time) (decimal.Decimal, string) {
	if e.common(quote) == quote {
		return one, "1"
	}
	market, ok := e.cfg.FX[quote]
	if !ok {
		return one, "peg"
	}
	b, ok := e.books[market]
	if !ok || !e.fresh(b, now) || len(b.Bids) == 0 || len(b.Asks) == 0 {
		return one, "peg"
	}
	return b.Bids[0].Price.Add(b.Asks[0].Price).Div(decimal.NewFromInt(2)), market
}

// route evaluates buying at buy's asks and selling at sell's bids. The edge
// is quoted at the top of the books; Size and Profit walk both books level
// by level while each next unit is still profitable after fees.
func (e *Engine) route(buy, sell venueBook) model.ArbRoute {
	r := model.ArbRoute{
		BuyExchange: buy.Exchange, BuySymbol: buy.Symbol, SellExchange: sell.Exchange, SellSymbol: sell.Symbol,
		BuyPrice: buy.Ask, SellPrice: sell.Bid,
	}
	bm, sm := e.mul(e.buyMul, buy.Exchange), e.mul(e.sellMul, sell.Exchange)
	r.GrossBps = sell.Bid.Sub(buy.Ask).Div(buy.Ask).Mul(bpsUnit).Round(2)
	buyCost, sellGain := buy.Ask.Mul(bm), sell.Bid.Mul(sm)
	r.NetBps = sellGain.Sub(buyCost).Div(buyCost).Mul(bpsUnit).Round(2)
	if !sellGain.GreaterThan(buyCost) {
		return r
	}

	asks, bids := buy.book.Asks, sell.book.Bids
	i, j := 0, 0
	askLeft, bidLeft := asks[0].Size, bids[0].Size
	for i < len(asks) && j < len(bids) {
		cost := asks[i].Price.Mul(buy.Rate).Mul(bm)
		gain := bids[j].Price.Mul(sell.Rate).Mul(sm)
		if !gain.GreaterThan(cost) {
			break
		}
		q := decimal.Min(askLeft, bidLeft)
		r.Size = r.Size.Add(q)
		r.Profit = r.Profit.Add(q.Mul(gain.Sub(cost)))
		askLeft, bidLeft = askLeft.Sub(q), bidLeft.Sub(q)
		if !askLeft.IsPositive() {
			if i++; i < len(asks) {
				askLeft = asks[i].Size
			}
		}
		if !bidLeft.IsPositive() {
			if j++; j < len(bids) {
				bidLeft = bids[j].Size
			}
		}
	}
	r.Profit = r.Profit.Round(8)
	return r
}

func (e *Engine) mul(m map[string]decimal.Decimal, exchange string) decimal.Decimal {
	if v, ok := m[exchange]; ok {
		return v
	}
	return one
}

// ParseFees reads "binance:10,kraken:40" (basis points).
func ParseFees(list []string) (map[string]decimal.Decimal, error) {
	out := map[string]decimal.Decimal{}
	for _, item := range list {
		name, v, ok := strings.Cut(item, ":")
		d, err := decimal.NewFromString(v)
		if !ok || name == "" || err != nil || d.IsNegative() || d.GreaterThanOrEqual(bpsUnit) {
			return nil, fmt.Errorf("fee %q: want exchange:bps", item)
		}
		out[name] = d
	}
	return out, nil
}

// ParsePairs reads "USDT:USD,USDC:USD" into a map.
func ParsePairs(list []string) (map[string]string, error) {
	out := map[string]string{}
	for _, item := range list {
		k, v, ok := strings.Cut(item, ":")
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("%q: want key:value", item)
		}
		out[k] = v
	}
	return out, nil
}
