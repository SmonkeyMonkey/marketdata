// Package config loads process configuration from environment variables:
// the same build runs in every environment, only the env differs.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type Log struct {
	Level  slog.Level
	Format string // json | text
}

// NewLogger writes to stderr so stdout stays free for data.
func (l Log) NewLogger() *slog.Logger {
	opts := &slog.HandlerOptions{Level: l.Level}
	if l.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

type Kafka struct {
	Brokers      []string
	TopicTrades  string
	TopicGaps    string
	TopicCandles string
	TopicAlerts  string
	TopicRules   string
	// TopicSubscriptions is the compacted topic of streamed markets.
	TopicSubscriptions string
	// TopicSettings is the compacted topic of per-market alert settings.
	TopicSettings string
	// TopicBooks carries order book snapshots (short retention).
	TopicBooks string
	// TopicArb carries cross-exchange comparisons (short retention);
	// TopicArbWindows the closed arbitrage opportunities.
	TopicArb        string
	TopicArbWindows string
}

type Aggregator struct {
	Log            Log
	MetricsAddr    string
	Kafka          Kafka
	Group          string
	Intervals      []time.Duration
	Lateness       time.Duration
	Idle           time.Duration
	CommitInterval time.Duration
	DedupWindow    int
	TailRecords    int64
}

func LoadAggregator() (Aggregator, error) {
	e := &env{}
	c := Aggregator{
		Log:            e.log(),
		MetricsAddr:    e.str("AGGREGATOR_METRICS_ADDR", ":9103"),
		Kafka:          e.kafka(),
		Group:          e.str("AGG_CONSUMER_GROUP", "candles-aggregator"),
		Intervals:      e.intervals("AGG_INTERVALS", []time.Duration{time.Second, time.Minute}),
		Lateness:       e.duration("AGG_LATENESS", 2*time.Second),
		Idle:           e.duration("AGG_IDLE", 5*time.Second),
		CommitInterval: e.duration("AGG_COMMIT_INTERVAL", time.Second),
		DedupWindow:    int(e.int64("AGG_DEDUP_WINDOW", 20_000)),
		TailRecords:    e.int64("AGG_TAIL_RECORDS", 5_000),
	}
	return c, e.err()
}

// intervals parses a list like "1s,1m,5m". Each must be a whole number of
// seconds that divides a day evenly, so buckets align to UTC midnight.
func (e *env) intervals(key string, def []time.Duration) []time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	var out []time.Duration
	for _, s := range e.list(key, nil) {
		d, err := time.ParseDuration(s)
		if err != nil || d < time.Second || d%time.Second != 0 || (24*time.Hour)%d != 0 {
			e.fail("%s: %q must be whole seconds and divide 24h", key, s)
			continue
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		e.fail("%s must not be empty", key)
	}
	return out
}

type ClickHouse struct {
	Addr     string
	Database string
	User     string
	Password string
}

type Ingester struct {
	Log Log
	// InstanceID tells redundant ingesters apart in logs and gap events.
	InstanceID  string
	MetricsAddr string
	Publisher   string // stdout | kafka
	Kafka       Kafka
	Exchanges   []Exchange

	DialTimeout time.Duration
	ReadLimit   int64
	BackoffMin  time.Duration
	BackoffMax  time.Duration
	StableAfter time.Duration
}

type Writer struct {
	Log         Log
	MetricsAddr string
	Kafka       Kafka
	Group       string
	ClickHouse  ClickHouse

	BatchSize     int
	FlushInterval time.Duration
	InsertTimeout time.Duration
	DedupWindow   int
	RetryMin      time.Duration
	RetryMax      time.Duration
}

func LoadIngester() (Ingester, error) {
	e := &env{}
	c := Ingester{
		Log:         e.log(),
		InstanceID:  e.str("INGESTER_ID", hostname()),
		MetricsAddr: e.str("INGESTER_METRICS_ADDR", ":9101"),
		Publisher:   e.oneOf("PUBLISHER", "stdout", "stdout", "kafka"),
		DialTimeout: e.duration("WS_DIAL_TIMEOUT", 10*time.Second),
		ReadLimit:   e.int64("WS_READ_LIMIT_BYTES", 32<<20),
		BackoffMin:  e.duration("RECONNECT_BACKOFF_MIN", 500*time.Millisecond),
		BackoffMax:  e.duration("RECONNECT_BACKOFF_MAX", 30*time.Second),
		StableAfter: e.duration("RECONNECT_STABLE_AFTER", time.Minute),
	}
	if c.Publisher == "kafka" {
		c.Kafka = e.kafka()
	}
	if c.BackoffMin > c.BackoffMax {
		e.fail("RECONNECT_BACKOFF_MIN must be <= RECONNECT_BACKOFF_MAX")
	}
	c.Exchanges = e.exchanges()
	return c, e.err()
}

func LoadWriter() (Writer, error) {
	e := &env{}
	c := Writer{
		Log:           e.log(),
		MetricsAddr:   e.str("WRITER_METRICS_ADDR", ":9102"),
		Kafka:         e.kafka(),
		Group:         e.str("WRITER_CONSUMER_GROUP", "clickhouse-writer"),
		ClickHouse:    e.clickhouse(),
		BatchSize:     int(e.int64("WRITER_BATCH_SIZE", 10_000)),
		FlushInterval: e.duration("WRITER_FLUSH_INTERVAL", time.Second),
		InsertTimeout: e.duration("WRITER_INSERT_TIMEOUT", 10*time.Second),
		DedupWindow:   int(e.int64("WRITER_DEDUP_WINDOW", 20_000)),
		RetryMin:      e.duration("WRITER_RETRY_MIN", 500*time.Millisecond),
		RetryMax:      e.duration("WRITER_RETRY_MAX", 30*time.Second),
	}
	if c.RetryMin > c.RetryMax {
		e.fail("WRITER_RETRY_MIN must be <= WRITER_RETRY_MAX")
	}
	return c, e.err()
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "ingester"
	}
	return h
}

// env collects all parse errors so a misconfigured deploy reports every
// problem at once instead of one per restart.
type env struct{ errs []error }

func (e *env) fail(format string, args ...any) { e.errs = append(e.errs, fmt.Errorf(format, args...)) }
func (e *env) err() error                      { return errors.Join(e.errs...) }

func (e *env) log() Log {
	return Log{
		Level:  e.level("LOG_LEVEL", slog.LevelInfo),
		Format: e.oneOf("LOG_FORMAT", "json", "json", "text"),
	}
}

// Exchange configures one feed. Symbols are canonical BASE-QUOTE; adapters
// translate them to the exchange's own notation.
type Exchange struct {
	Name    string
	URL     string
	Symbols []string
	// ReadTimeout: no frame for this long means the feed is dead.
	ReadTimeout time.Duration
}

// exchangeDefaults lists the supported exchanges. Coinbase and Kraken send
// heartbeats every second; Binance sends nothing between trades, so it gets
// a longer timeout.
var exchangeDefaults = map[string]Exchange{
	"coinbase": {URL: "wss://ws-feed.exchange.coinbase.com", Symbols: []string{"BTC-USD", "ETH-USD"}, ReadTimeout: 15 * time.Second},
	"binance":  {URL: "wss://stream.binance.com:9443/stream", Symbols: []string{"BTC-USDT", "ETH-USDT"}, ReadTimeout: 30 * time.Second},
	"kraken":   {URL: "wss://ws.kraken.com/v2", Symbols: []string{"BTC-USD", "ETH-USD"}, ReadTimeout: 15 * time.Second},
	// Bybit and OKX answer our 20 s pings, so a quiet feed still gets a
	// frame within the timeout.
	"bybit": {URL: "wss://stream.bybit.com/v5/public/spot", Symbols: []string{"BTC-USDT", "ETH-USDT"}, ReadTimeout: 30 * time.Second},
	"okx":   {URL: "wss://ws.okx.com:443/ws/v5/public", Symbols: []string{"BTC-USDT", "ETH-USDT"}, ReadTimeout: 30 * time.Second},
}

// exchanges reads EXCHANGES and, for each name, <NAME>_WS_URL,
// <NAME>_SYMBOLS and <NAME>_READ_TIMEOUT.
func (e *env) exchanges() []Exchange {
	names := e.list("EXCHANGES", []string{"coinbase", "binance", "kraken", "bybit", "okx"})
	if len(names) == 0 {
		e.fail("EXCHANGES must not be empty")
	}
	seen := make(map[string]bool, len(names))
	var out []Exchange
	for _, name := range names {
		def, ok := exchangeDefaults[name]
		if !ok {
			e.fail("EXCHANGES: unknown exchange %q", name)
			continue
		}
		if seen[name] {
			e.fail("EXCHANGES: %q listed twice", name)
			continue
		}
		seen[name] = true
		p := strings.ToUpper(name) + "_"
		x := Exchange{
			Name:        name,
			URL:         e.str(p+"WS_URL", def.URL),
			Symbols:     e.list(p+"SYMBOLS", def.Symbols),
			ReadTimeout: e.duration(p+"READ_TIMEOUT", def.ReadTimeout),
		}
		if len(x.Symbols) == 0 {
			e.fail("%sSYMBOLS must not be empty", p)
		}
		out = append(out, x)
	}
	return out
}

func (e *env) kafka() Kafka {
	k := Kafka{
		Brokers:            e.list("KAFKA_BROKERS", []string{"localhost:9092"}),
		TopicTrades:        e.str("KAFKA_TOPIC_TRADES", "md.trades"),
		TopicGaps:          e.str("KAFKA_TOPIC_GAPS", "md.gaps"),
		TopicCandles:       e.str("KAFKA_TOPIC_CANDLES", "md.candles"),
		TopicAlerts:        e.str("KAFKA_TOPIC_ALERTS", "md.alerts"),
		TopicRules:         e.str("KAFKA_TOPIC_ALERT_RULES", "md.alert-rules"),
		TopicSubscriptions: e.str("KAFKA_TOPIC_SUBSCRIPTIONS", "md.subscriptions"),
		TopicSettings:      e.str("KAFKA_TOPIC_SETTINGS", "md.settings"),
		TopicBooks:         e.str("KAFKA_TOPIC_BOOKS", "md.books"),
		TopicArb:           e.str("KAFKA_TOPIC_ARB", "md.arb"),
		TopicArbWindows:    e.str("KAFKA_TOPIC_ARB_WINDOWS", "md.arb-windows"),
	}
	if len(k.Brokers) == 0 {
		e.fail("KAFKA_BROKERS must not be empty")
	}
	return k
}

func (e *env) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (e *env) oneOf(key, def string, allowed ...string) string {
	v := e.str(key, def)
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	e.fail("%s: %q is not one of %v", key, v, allowed)
	return def
}

func (e *env) list(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (e *env) duration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		e.fail("%s: invalid positive duration %q", key, v)
		return def
	}
	return d
}

func (e *env) int64(key string, def int64) int64 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		e.fail("%s: invalid positive integer %q", key, v)
		return def
	}
	return n
}

func (e *env) level(key string, def slog.Level) slog.Level {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		e.fail("%s: %v", key, err)
		return def
	}
	return l
}

type Gateway struct {
	Log              Log
	Addr             string
	MetricsAddr      string
	Kafka            Kafka
	MaxConns         int64
	MaxSubscriptions int
	SendQueue        int
	WriteTimeout     time.Duration
	PingInterval     time.Duration
	DedupWindow      int
}

func LoadGateway() (Gateway, error) {
	e := &env{}
	c := Gateway{
		Log:              e.log(),
		Addr:             e.str("GATEWAY_ADDR", ":8090"),
		MetricsAddr:      e.str("GATEWAY_METRICS_ADDR", ":9104"),
		Kafka:            e.kafka(),
		MaxConns:         e.int64("GATEWAY_MAX_CONNS", 100_000),
		MaxSubscriptions: int(e.int64("GATEWAY_MAX_SUBSCRIPTIONS", 200)),
		SendQueue:        int(e.int64("GATEWAY_SEND_QUEUE", 1024)),
		WriteTimeout:     e.duration("GATEWAY_WRITE_TIMEOUT", 5*time.Second),
		PingInterval:     e.duration("GATEWAY_PING_INTERVAL", 30*time.Second),
		DedupWindow:      int(e.int64("GATEWAY_DEDUP_WINDOW", 20_000)),
	}
	return c, e.err()
}

type Alerts struct {
	Log            Log
	Addr           string
	MetricsAddr    string
	Kafka          Kafka
	Group          string
	MovePct        decimal.Decimal
	MoveWindow     time.Duration
	MoveCooldown   time.Duration
	MaxRules       int
	DedupWindow    int
	CommitInterval time.Duration
}

func LoadAlerts() (Alerts, error) {
	e := &env{}
	c := Alerts{
		Log:            e.log(),
		Addr:           e.str("ALERTS_ADDR", ":8091"),
		MetricsAddr:    e.str("ALERTS_METRICS_ADDR", ":9105"),
		Kafka:          e.kafka(),
		Group:          e.str("ALERTS_CONSUMER_GROUP", "alerts"),
		MovePct:        e.decimal("ALERT_MOVE_PCT", "0.5"),
		MoveWindow:     e.duration("ALERT_MOVE_WINDOW", time.Minute),
		MoveCooldown:   e.duration("ALERT_MOVE_COOLDOWN", 5*time.Minute),
		MaxRules:       int(e.int64("ALERT_MAX_RULES", 1_000_000)),
		DedupWindow:    int(e.int64("ALERT_DEDUP_WINDOW", 20_000)),
		CommitInterval: e.duration("ALERT_COMMIT_INTERVAL", time.Second),
	}
	return c, e.err()
}

// decimal reads a non-negative decimal; 0 disables the feature it configures.
func (e *env) decimal(key, def string) decimal.Decimal {
	v := e.str(key, def)
	d, err := decimal.NewFromString(v)
	if err != nil || d.IsNegative() {
		e.fail("%s: invalid non-negative decimal %q", key, v)
		return decimal.RequireFromString(def)
	}
	return d
}

func (e *env) clickhouse() ClickHouse {
	return ClickHouse{
		Addr:     e.str("CLICKHOUSE_ADDR", "localhost:9000"),
		Database: e.str("CLICKHOUSE_DB", "marketdata"),
		User:     e.str("CLICKHOUSE_USER", "marketdata"),
		Password: e.str("CLICKHOUSE_PASSWORD", "marketdata"),
	}
}

type API struct {
	Log         Log
	Addr        string
	MetricsAddr string
	ClickHouse  ClickHouse
	Kafka       Kafka
	// MaxSubscriptions caps markets added from the UI.
	MaxSubscriptions int
	CatalogTTL       time.Duration
	GatewayURL       *url.URL
	AlertsURL        *url.URL
}

func LoadAPI() (API, error) {
	e := &env{}
	c := API{
		Log:              e.log(),
		Addr:             e.str("API_ADDR", ":8088"),
		MetricsAddr:      e.str("API_METRICS_ADDR", ":9106"),
		ClickHouse:       e.clickhouse(),
		Kafka:            e.kafka(),
		MaxSubscriptions: int(e.int64("API_MAX_SUBSCRIPTIONS", 40)),
		CatalogTTL:       e.duration("API_CATALOG_TTL", time.Hour),
		GatewayURL:       e.url("API_GATEWAY_URL", "http://localhost:8090"),
		AlertsURL:        e.url("API_ALERTS_URL", "http://localhost:8091"),
	}
	return c, e.err()
}

func (e *env) url(key, def string) *url.URL {
	v := e.str(key, def)
	u, err := url.Parse(v)
	if err != nil || u.Scheme == "" || u.Host == "" {
		e.fail("%s: invalid URL %q", key, v)
		u, _ = url.Parse(def)
	}
	return u
}

type Arbitrage struct {
	Log         Log
	MetricsAddr string
	Kafka       Kafka
	// Quotes: quote -> common quote it is compared in (USDT:USD).
	Quotes []string
	// FX: quote -> market whose mid converts it (USDT:kraken:USDT-USD).
	FX []string
	// TakerFees: exchange:bps.
	TakerFees     []string
	MinNetBps     decimal.Decimal
	AlertBps      decimal.Decimal
	AlertCooldown time.Duration
	MaxBookAge    time.Duration
	FXMaxAge      time.Duration
	PublishEvery  time.Duration
	Tick          time.Duration
}

func LoadArbitrage() (Arbitrage, error) {
	e := &env{}
	c := Arbitrage{
		Log:         e.log(),
		MetricsAddr: e.str("ARB_METRICS_ADDR", ":9107"),
		Kafka:       e.kafka(),
		Quotes:      e.list("ARB_QUOTES", []string{"USDT:USD", "USDC:USD"}),
		FX:          e.list("ARB_FX", []string{"USDT:kraken:USDT-USD", "USDC:kraken:USDC-USD"}),
		// Lowest-tier taker fees; check the current fee schedules.
		TakerFees:     e.list("ARB_TAKER_FEES_BPS", []string{"coinbase:60", "binance:10", "kraken:40", "bybit:10", "okx:10"}),
		MinNetBps:     e.decimal("ARB_MIN_NET_BPS", "0"),
		AlertBps:      e.decimal("ARB_ALERT_BPS", "10"),
		AlertCooldown: e.duration("ARB_ALERT_COOLDOWN", time.Minute),
		MaxBookAge:    e.duration("ARB_MAX_BOOK_AGE", 5*time.Second),
		// Rate books (USDT-USD) can stay unchanged for minutes, and books are
		// only published on change.
		FXMaxAge:     e.duration("ARB_FX_MAX_AGE", 5*time.Minute),
		PublishEvery: e.duration("ARB_PUBLISH_EVERY", 250*time.Millisecond),
		Tick:         e.duration("ARB_TICK", 500*time.Millisecond),
	}
	return c, e.err()
}
