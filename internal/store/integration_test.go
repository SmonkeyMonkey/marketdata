//go:build integration

// Integration tests against a real ClickHouse: the SQL of every insert and
// query, on the schema the migrations create. Run with `make
// test-integration` (CLICKHOUSE_ADDR, default localhost:9000). Each run
// applies the migrations to its own throwaway database and drops it, so a
// live marketdata database on the same server is never touched.
package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"

	"marketdata/internal/config"
	"marketdata/internal/model"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	lineComment = regexp.MustCompile(`--[^\n]*`)
	dbName      = regexp.MustCompile(`\bmarketdata\b`)
)

// statements splits a migration into statements, retargeted to db.
// Comments are dropped first: they may contain semicolons.
func statements(sql, db string) []string {
	sql = dbName.ReplaceAllString(lineComment.ReplaceAllString(sql, ""), db)
	var out []string
	for _, s := range strings.Split(sql, ";") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// open applies every migration to a new database and returns a store on it.
func open(t *testing.T) *ClickHouse {
	t.Helper()
	ctx := context.Background()
	cfg := config.ClickHouse{
		Addr: env("CLICKHOUSE_ADDR", "localhost:9000"), User: env("CLICKHOUSE_USER", "marketdata"),
		Password: env("CLICKHOUSE_PASSWORD", "marketdata"),
	}
	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{cfg.Addr}, Auth: clickhouse.Auth{Username: cfg.User, Password: cfg.Password}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("ClickHouse at %s: %v", cfg.Addr, err)
	}
	cfg.Database = fmt.Sprintf("it_%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+cfg.Database) })

	files, _ := filepath.Glob("../../migrations/clickhouse/*.sql")
	slices.Sort(files)
	if len(files) == 0 {
		t.Fatal("no migrations found")
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range statements(string(b), cfg.Database) {
			if err := admin.Exec(ctx, stmt); err != nil {
				t.Fatalf("%s: %v\n%s", filepath.Base(f), err, stmt)
			}
		}
	}

	s, err := OpenClickHouse(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func d(v string) decimal.Decimal { return decimal.RequireFromString(v) }

func TestIntegrationTradesDedupAndRecent(t *testing.T) {
	s, ctx := open(t), context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	tr := func(id string, at time.Duration, price string) model.Trade {
		return model.Trade{Exchange: "binance", Symbol: "BTC-USDT", TradeID: id, Price: d(price), Size: d("0.00000001"),
			TakerSide: model.SideSell, ExchangeTime: now.Add(at), ReceiveTime: now.Add(at + 40*time.Millisecond)}
	}
	batch := []model.Trade{tr("1", -3*time.Second, "64000.123456789012345678"), tr("2", -2*time.Second, "64001"), tr("3", -time.Second, "64002")}
	if err := s.InsertTrades(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// The same trades again (a replay after a crash): FINAL hides them.
	if err := s.InsertTrades(ctx, batch[1:]); err != nil {
		t.Fatal(err)
	}
	got, err := s.RecentTrades(ctx, "binance", "BTC-USDT", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != "3" || got[2].ID != "1" {
		t.Fatalf("recent = %+v, want 3 trades newest first", got)
	}
	if !got[2].Price.Equal(d("64000.123456789012345678")) || !got[2].Size.Equal(d("0.00000001")) || got[2].Side != "sell" {
		t.Errorf("decimals or side not exact: %+v", got[2])
	}
}

func TestIntegrationCandlesRollUp(t *testing.T) {
	s, ctx := open(t), context.Background()
	// Five 1m candles of the current 5m bucket's predecessor, so the bucket is complete.
	bucket := time.Now().UTC().Truncate(5 * time.Minute).Add(-5 * time.Minute)
	var cs []model.Candle
	for i := range 5 {
		p := decimal.NewFromInt(int64(100 + i))
		cs = append(cs, model.Candle{Exchange: "kraken", Symbol: "ETH-USD", Interval: time.Minute, Start: bucket.Add(time.Duration(i) * time.Minute),
			Open: p, High: p.Add(d("1")), Low: p.Sub(d("1")), Close: p.Add(d("0.5")),
			Volume: d("2"), QuoteVolume: p.Mul(d("2")), BuyVolume: d("1.5"), SellVolume: d("0.5"), Trades: 10,
			FirstTradeID: fmt.Sprint(i * 10), LastTradeID: fmt.Sprint(i*10 + 9)})
	}
	if err := s.InsertCandles(ctx, cs); err != nil {
		t.Fatal(err)
	}
	got, err := s.Candles(ctx, "kraken", "ETH-USD", "1m", 300, 3)
	if err != nil {
		t.Fatal(err)
	}
	var bar *CandleRow
	for i := range got {
		if got[i].Start.Equal(bucket) {
			bar = &got[i]
		}
	}
	if bar == nil {
		t.Fatalf("no 5m bar at %s in %+v", bucket, got)
	}
	// open of the first, close of the last, extremes, sums; VWAP = 1020/10.
	if !bar.Open.Equal(d("100")) || !bar.Close.Equal(d("104.5")) || !bar.High.Equal(d("105")) || !bar.Low.Equal(d("99")) ||
		!bar.Volume.Equal(d("10")) || bar.Trades != 50 || !bar.VWAP.Equal(d("102")) || !bar.LastBase.Equal(bucket.Add(4*time.Minute)) {
		t.Errorf("5m bar = %+v", *bar)
	}
	markets, err := s.Markets(ctx)
	if err != nil || !slices.Contains(markets, Market{Exchange: "kraken", Symbol: "ETH-USD"}) {
		t.Errorf("markets = %+v %v", markets, err)
	}
}

func TestIntegrationAlertsAndArbitrage(t *testing.T) {
	s, ctx := open(t), context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	a := model.Alert{ID: "whale:binance:BTC-USDT:42", Type: model.AlertWhaleTrade, Exchange: "binance", Symbol: "BTC-USDT",
		Message: "whale", Price: d("64000"), Time: now, TradeID: "42", Notional: d("1280000"), ChangePct: decimal.Zero}
	if err := s.InsertAlerts(ctx, []model.Alert{a, a}); err != nil {
		t.Fatal(err)
	}
	alerts, err := s.RecentAlerts(ctx, 10)
	if err != nil || len(alerts) != 1 || alerts[0].Type != "whale_trade" || !alerts[0].Notional.Equal(d("1280000")) {
		t.Errorf("alerts = %+v %v", alerts, err)
	}

	w := model.ArbWindow{ID: "w1", Asset: "BTC-USD", BuyExchange: "kraken", BuySymbol: "BTC-USD", SellExchange: "binance", SellSymbol: "BTC-USDT",
		Opened: now.Add(-time.Minute), Closed: now.Add(-time.Minute + 420*time.Millisecond),
		PeakNetBps: d("3.21"), PeakGrossBps: d("53.2"), PeakSize: d("0.123456789"), PeakProfit: d("8.19"), Updates: 3}
	if err := s.InsertArbWindows(ctx, []model.ArbWindow{w, w}); err != nil {
		t.Fatal(err)
	}
	windows, err := s.ArbWindows(ctx, "BTC-USD", 10)
	if err != nil || len(windows) != 1 || windows[0].DurationMs != 420 || windows[0].Buy != "kraken:BTC-USD" || !windows[0].PeakSize.Equal(d("0.123456789")) {
		t.Errorf("windows = %+v %v", windows, err)
	}
	stats, err := s.ArbStats(ctx, "BTC-USD", now.Add(-time.Hour))
	if err != nil || len(stats) != 1 || stats[0].Windows != 1 || stats[0].MaxNetBps != 3.21 || !stats[0].SumPeakProfit.Equal(d("8.19")) {
		t.Errorf("stats = %+v %v", stats, err)
	}
}

func TestIntegrationGapsAndBookStats(t *testing.T) {
	s, ctx := open(t), context.Background()
	now := time.Now().UTC()
	if err := s.InsertGaps(ctx, []model.Gap{{Exchange: "coinbase", Symbol: "BTC-USD", AfterID: "10", BeforeID: "14", Missing: 3, DetectedAt: now, Source: "ingester-a"}}); err != nil {
		t.Fatal(err)
	}
	st, ok := model.Book{Bids: []model.BookLevel{{Price: d("99"), Size: d("3")}}, Asks: []model.BookLevel{{Price: d("101"), Size: d("1")}}}.Stats(10)
	if !ok {
		t.Fatal("stats")
	}
	if err := s.InsertBookSamples(ctx, []model.BookSample{{Exchange: "okx", Symbol: "BTC-USDT", Time: now.Truncate(time.Second), Stats: st}}); err != nil {
		t.Fatal(err)
	}
}
