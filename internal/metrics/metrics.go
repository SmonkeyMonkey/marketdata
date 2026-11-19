// Package metrics defines the Prometheus metrics of each service.
//
// Labels are limited to low-cardinality values (exchange, symbol, topic,
// partition). Never use trade IDs, prices or error strings as labels.
package metrics

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"marketdata/internal/model"
)

const namespace = "md"

type Ingest struct {
	frames      *prometheus.CounterVec
	bytes       *prometheus.CounterVec
	lastFrame   *prometheus.GaugeVec
	trades      *prometheus.CounterVec
	missing     *prometheus.CounterVec
	feedLatency *prometheus.HistogramVec
	connected   *prometheus.GaugeVec
	disconnects *prometheus.CounterVec
	books       *prometheus.CounterVec
	resyncs     *prometheus.CounterVec
}

func NewIngest(reg prometheus.Registerer) *Ingest {
	f := promauto.With(reg)
	return &Ingest{
		frames: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "frames_total",
			Help: "Websocket frames received.",
		}, []string{"exchange"}),
		bytes: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "bytes_total",
			Help: "Websocket payload bytes received.",
		}, []string{"exchange"}),
		lastFrame: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "last_frame_timestamp_seconds",
			Help: "Unix time of the last received frame; use time() - this to detect a frozen feed.",
		}, []string{"exchange"}),
		trades: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "trades_total",
			Help: "Normalized trades produced.",
		}, []string{"exchange", "symbol"}),
		missing: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "missing_trades_total",
			Help: "Trades detected as missed by trade ID gaps.",
		}, []string{"exchange", "symbol"}),
		feedLatency: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "feed_latency_seconds",
			Help:    "Time from exchange match to frame receipt.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 11), // 5ms .. ~5s
		}, []string{"exchange"}),
		connected: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "connected",
			Help: "1 while the websocket session is up.",
		}, []string{"exchange"}),
		disconnects: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "disconnects_total",
			Help: "Websocket sessions that ended and triggered a reconnect.",
		}, []string{"exchange"}),
		books: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "book_snapshots_total",
			Help: "Order book snapshots published (rate-limited per market).",
		}, []string{"exchange"}),
		resyncs: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "ingest", Name: "book_resyncs_total",
			Help: "Order books rebuilt after diverging from the exchange (checksum mismatch).",
		}, []string{"exchange"}),
	}
}

// Init creates per-exchange series at zero, so rate() and increase() see the
// first event instead of starting from it.
func (m *Ingest) Init(exchange string) {
	m.disconnects.WithLabelValues(exchange)
	m.resyncs.WithLabelValues(exchange)
	m.frames.WithLabelValues(exchange)
	m.connected.WithLabelValues(exchange)
}

func (m *Ingest) Frame(exchange string, size int, at time.Time) {
	m.frames.WithLabelValues(exchange).Inc()
	m.bytes.WithLabelValues(exchange).Add(float64(size))
	m.lastFrame.WithLabelValues(exchange).Set(float64(at.UnixNano()) / 1e9)
}

func (m *Ingest) Events(events []model.Event) {
	for _, e := range events {
		switch ev := e.(type) {
		case model.Trade:
			m.trades.WithLabelValues(ev.Exchange, ev.Symbol).Inc()
			if !ev.Snapshot && !ev.ExchangeTime.IsZero() {
				m.feedLatency.WithLabelValues(ev.Exchange).Observe(ev.ReceiveTime.Sub(ev.ExchangeTime).Seconds())
			}
		case model.Gap:
			m.missing.WithLabelValues(ev.Exchange, ev.Symbol).Add(float64(ev.Missing))
		case model.Book:
			m.books.WithLabelValues(ev.Exchange).Inc()
		case model.BookResync:
			m.resyncs.WithLabelValues(ev.Exchange).Inc()
		}
	}
}

func (m *Ingest) Connected(exchange string, up bool) {
	v := 0.0
	if up {
		v = 1
	}
	m.connected.WithLabelValues(exchange).Set(v)
}

func (m *Ingest) Disconnected(exchange string) { m.disconnects.WithLabelValues(exchange).Inc() }

type Writer struct {
	rows          *prometheus.CounterVec
	batchRows     prometheus.Histogram
	flushDuration prometheus.Histogram
	insertErrors  prometheus.Counter
	decodeErrors  *prometheus.CounterVec
	duplicates    prometheus.Counter
	e2eLatency    prometheus.Histogram
	lag           *prometheus.GaugeVec
}

func NewWriter(reg prometheus.Registerer) *Writer {
	f := promauto.With(reg)
	return &Writer{
		rows: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "writer", Name: "rows_inserted_total",
			Help: "Rows stored, by table.",
		}, []string{"table"}),
		batchRows: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "writer", Name: "batch_rows",
			Help:    "Kafka records per flushed batch.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 9), // 1 .. 65536
		}),
		flushDuration: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "writer", Name: "flush_duration_seconds",
			Help:    "Successful insert + commit time per batch, including retries.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16), // 1ms .. ~33s
		}),
		insertErrors: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "writer", Name: "insert_errors_total",
			Help: "Failed insert attempts (each is retried).",
		}),
		decodeErrors: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "writer", Name: "decode_errors_total",
			Help: "Kafka records skipped because they could not be decoded.",
		}, []string{"topic"}),
		duplicates: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "writer", Name: "duplicates_dropped_total",
			Help: "Trades dropped because another ingester already delivered them.",
		}),
		e2eLatency: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "writer", Name: "pipeline_latency_seconds",
			Help:    "Time from websocket receipt to stored in ClickHouse, per trade.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 10), // 50ms .. ~25s
		}),
		lag: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "writer", Name: "consumer_lag",
			Help: "Records not yet committed by the consumer group, per partition.",
		}, []string{"topic", "partition"}),
	}
}

// Init creates series at zero for known tables and counters.
func (m *Writer) Init(tables ...string) {
	for _, t := range tables {
		m.rows.WithLabelValues(t)
	}
	m.insertErrors.Add(0)
	m.duplicates.Add(0)
}

func (m *Writer) Flushed(rowsByTable map[string]int, records int, d time.Duration) {
	for table, n := range rowsByTable {
		m.rows.WithLabelValues(table).Add(float64(n))
	}
	m.batchRows.Observe(float64(records))
	m.flushDuration.Observe(d.Seconds())
}

func (m *Writer) Stored(trades []model.Trade, at time.Time) {
	for _, t := range trades {
		if !t.ReceiveTime.IsZero() {
			m.e2eLatency.Observe(at.Sub(t.ReceiveTime).Seconds())
		}
	}
}

func (m *Writer) InsertError()             { m.insertErrors.Inc() }
func (m *Writer) Duplicate()               { m.duplicates.Inc() }
func (m *Writer) DecodeError(topic string) { m.decodeErrors.WithLabelValues(topic).Inc() }

type Aggregator struct {
	reg          prometheus.Registerer
	emitted      *prometheus.CounterVec
	produceErrs  prometheus.Counter
	decodeErrors prometheus.Counter
	candleLag    *prometheus.HistogramVec
}

func NewAggregator(reg prometheus.Registerer) *Aggregator {
	f := promauto.With(reg)
	return &Aggregator{
		reg: reg,
		emitted: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "agg", Name: "candles_emitted_total",
			Help: "Closed candles published, by interval.",
		}, []string{"interval"}),
		produceErrs: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "agg", Name: "produce_errors_total",
			Help: "Candles that could not be published.",
		}),
		decodeErrors: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "agg", Name: "decode_errors_total",
			Help: "Trade records skipped because they could not be decoded.",
		}),
		candleLag: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "agg", Name: "close_delay_seconds",
			Help:    "Wall time from a candle's end to its publication.",
			Buckets: prometheus.ExponentialBuckets(0.25, 2, 10), // 250ms .. ~2min
		}, []string{"interval"}),
	}
}

// AggStats is the aggregator's live counters.
type AggStats interface {
	DuplicatesTotal() int64
	LateTotal() int64
	Open() int64
}

// Watch exposes the aggregator's counters; they are read at scrape time.
func (m *Aggregator) Watch(s AggStats) {
	f := promauto.With(m.reg)
	f.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "agg", Name: "duplicates_dropped_total",
		Help: "Trades dropped because another ingester already delivered them.",
	}, func() float64 { return float64(s.DuplicatesTotal()) })
	f.NewCounterFunc(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "agg", Name: "late_trades_total",
		Help: "Trades that arrived after their candle was closed and were dropped.",
	}, func() float64 { return float64(s.LateTotal()) })
	f.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: "agg", Name: "open_candles",
		Help: "Candles currently being built.",
	}, func() float64 { return float64(s.Open()) })
}

func (m *Aggregator) Emitted(c model.Candle) {
	p := model.PeriodName(c.Interval)
	m.emitted.WithLabelValues(p).Inc()
	m.candleLag.WithLabelValues(p).Observe(time.Since(c.Start.Add(c.Interval)).Seconds())
}

func (m *Aggregator) ProduceError() { m.produceErrs.Inc() }
func (m *Aggregator) DecodeError()  { m.decodeErrors.Inc() }

func (m *Writer) Lag(topic string, partition int32, lag int64) {
	m.lag.WithLabelValues(topic, strconv.Itoa(int(partition))).Set(float64(lag))
}

type Gateway struct {
	conns       prometheus.Gauge
	subs        prometheus.Gauge
	accepted    prometheus.Counter
	rejected    prometheus.Counter
	disconnects *prometheus.CounterVec
	published   *prometheus.CounterVec
	delivered   *prometheus.CounterVec
	sentMsgs    prometheus.Counter
	sentBytes   prometheus.Counter
	fanout      prometheus.Histogram
	ingestLag   prometheus.Histogram
	lastRecord  prometheus.Gauge
}

func NewGateway(reg prometheus.Registerer) *Gateway {
	f := promauto.With(reg)
	g := &Gateway{
		conns: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "connections",
			Help: "Open websocket connections.",
		}),
		subs: f.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "subscriptions",
			Help: "Active channel subscriptions across all connections.",
		}),
		accepted: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "connections_accepted_total",
			Help: "Websocket connections accepted.",
		}),
		rejected: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "connections_rejected_total",
			Help: "Connections refused because the limit was reached.",
		}),
		disconnects: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "disconnects_total",
			Help: "Closed connections by reason (client, slow, write_error, ping_timeout, read_error, server).",
		}, []string{"reason"}),
		published: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "published_total",
			Help: "Messages published to the hub, by type.",
		}, []string{"type"}),
		delivered: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "enqueued_total",
			Help: "Messages queued to clients (published x subscribers), by type.",
		}, []string{"type"}),
		sentMsgs: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "sent_messages_total",
			Help: "Messages written to client sockets.",
		}),
		sentBytes: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "sent_bytes_total",
			Help: "Payload bytes written to client sockets.",
		}),
		fanout: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "fanout_seconds",
			Help:    "Time to queue one message to all subscribers of its channel.",
			Buckets: prometheus.ExponentialBuckets(0.000_01, 2, 16), // 10µs .. ~0.3s
		}),
		ingestLag: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "gateway", Name: "ingest_lag_seconds",
			Help:    "Time from ingester receipt of a trade to the gateway publishing it.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. ~8s
		}),
	}
	g.lastRecord = f.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: "gateway", Name: "feed_last_record_timestamp_seconds",
		Help: "Unix time of the last record read from Kafka; time() - this = staleness.",
	})
	for _, r := range []string{"client", "slow", "write_error", "ping_timeout", "read_error", "server"} {
		g.disconnects.WithLabelValues(r)
	}
	return g
}

func (g *Gateway) Connected()                { g.conns.Inc(); g.accepted.Inc() }
func (g *Gateway) FeedRecord(at time.Time)   { g.lastRecord.Set(float64(at.UnixNano()) / 1e9) }
func (g *Gateway) Rejected()                 { g.rejected.Inc() }
func (g *Gateway) Subscriptions(delta int)   { g.subs.Add(float64(delta)) }
func (g *Gateway) IngestLag(d time.Duration) { g.ingestLag.Observe(d.Seconds()) }

func (g *Gateway) Disconnected(reason string) {
	g.conns.Dec()
	g.disconnects.WithLabelValues(reason).Inc()
}

func (g *Gateway) Sent(bytes int) {
	g.sentMsgs.Inc()
	g.sentBytes.Add(float64(bytes))
}

func (g *Gateway) Published(kind string, subscribers int, fanout time.Duration) {
	g.published.WithLabelValues(kind).Inc()
	g.delivered.WithLabelValues(kind).Add(float64(subscribers))
	g.fanout.Observe(fanout.Seconds())
}

type Alerts struct {
	reg   prometheus.Registerer
	fired *prometheus.CounterVec
	eval  prometheus.Histogram
}

func NewAlerts(reg prometheus.Registerer) *Alerts {
	f := promauto.With(reg)
	a := &Alerts{
		reg: reg,
		fired: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "alerts", Name: "fired_total",
			Help: "Alerts published, by type.",
		}, []string{"type"}),
		eval: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "alerts", Name: "evaluation_seconds",
			Help:    "Time to evaluate one trade against all rules and detectors.",
			Buckets: prometheus.ExponentialBuckets(0.000_000_5, 2, 18), // 500ns .. ~65ms
		}),
	}
	for _, t := range []string{"price_rule", "whale_trade", "price_move"} {
		a.fired.WithLabelValues(t)
	}
	return a
}

// WatchRules exposes the number of active rules, read at scrape time.
func (a *Alerts) WatchRules(count func() int) {
	promauto.With(a.reg).NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: "alerts", Name: "rules",
		Help: "Active (not yet fired) price rules.",
	}, func() float64 { return float64(count()) })
}

func (a *Alerts) Fired(kind string)         { a.fired.WithLabelValues(kind).Inc() }
func (a *Alerts) Evaluated(d time.Duration) { a.eval.Observe(d.Seconds()) }

type HTTP struct {
	duration *prometheus.HistogramVec
}

func NewHTTP(reg prometheus.Registerer) *HTTP {
	return &HTTP{duration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "api", Name: "request_duration_seconds",
		Help:    "HTTP request duration by route pattern and status code.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. ~8s
	}, []string{"route", "code"})}
}

func (h *HTTP) Observe(route string, code int, d time.Duration) {
	h.duration.WithLabelValues(route, strconv.Itoa(code)).Observe(d.Seconds())
}

type Arbitrage struct {
	books    prometheus.Counter
	eval     prometheus.Histogram
	windows  *prometheus.CounterVec
	duration prometheus.Histogram
	netBps   *prometheus.GaugeVec
	bestNet  *prometheus.GaugeVec
	rate     *prometheus.GaugeVec
	alerts   prometheus.Counter
}

func NewArbitrage(reg prometheus.Registerer) *Arbitrage {
	f := promauto.With(reg)
	return &Arbitrage{
		books: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "arb", Name: "books_total",
			Help: "Book snapshots evaluated (copies from redundant ingesters excluded).",
		}),
		eval: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "arb", Name: "evaluation_seconds",
			Help:    "Time to compare one book against the other venues of its asset.",
			Buckets: prometheus.ExponentialBuckets(0.000_001, 2, 16), // 1us .. ~33ms
		}),
		windows: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "arb", Name: "windows_total",
			Help: "Closed opportunity windows, by asset.",
		}, []string{"asset"}),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "arb", Name: "window_duration_seconds",
			Help:    "How long opportunities stay open.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12), // 50ms .. ~100s
		}),
		netBps: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "arb", Name: "route_net_bps",
			Help: "Top-of-book edge after taker fees per route (negative: no opportunity).",
		}, []string{"asset", "buy", "sell"}),
		bestNet: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "arb", Name: "best_net_bps",
			Help: "Best route edge after fees per asset.",
		}, []string{"asset"}),
		rate: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "arb", Name: "quote_rate",
			Help: "Conversion rate of a quote to its common quote (1 when pegged).",
		}, []string{"quote", "source"}),
		alerts: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "arb", Name: "alerts_total",
			Help: "Arbitrage alerts published.",
		}),
	}
}

func (a *Arbitrage) Evaluated(d time.Duration) { a.books.Inc(); a.eval.Observe(d.Seconds()) }
func (a *Arbitrage) Alert()                    { a.alerts.Inc() }

func (a *Arbitrage) Window(asset string, d time.Duration) {
	a.windows.WithLabelValues(asset).Inc()
	a.duration.Observe(d.Seconds())
}

// Snapshot records the latest edges of an asset. Routes that disappear
// (stale venue) are removed, so a dead feed does not leave a frozen value.
func (a *Arbitrage) Snapshot(s model.ArbSnapshot) {
	a.netBps.DeletePartialMatch(prometheus.Labels{"asset": s.Asset})
	if len(s.Routes) == 0 {
		a.bestNet.DeleteLabelValues(s.Asset)
	} else {
		a.bestNet.WithLabelValues(s.Asset).Set(s.Routes[0].NetBps.InexactFloat64())
	}
	for _, r := range s.Routes {
		a.netBps.WithLabelValues(s.Asset, r.BuyExchange+":"+r.BuySymbol, r.SellExchange+":"+r.SellSymbol).Set(r.NetBps.InexactFloat64())
	}
	for _, v := range s.Venues {
		if v.RateSource != "1" {
			_, quote, _ := strings.Cut(v.Symbol, "-")
			a.rate.DeletePartialMatch(prometheus.Labels{"quote": quote})
			a.rate.WithLabelValues(quote, v.RateSource).Set(v.Rate.InexactFloat64())
		}
	}
}
