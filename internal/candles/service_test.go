package candles

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/model"
	"marketdata/internal/publish"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// stream returns trades for two symbols over 20s of exchange time, followed
// by one far-future trade per symbol that closes every earlier candle.
func stream(t0 time.Time) []model.Trade {
	var out []model.Trade
	for i := range 200 {
		for _, sym := range []string{"BTC-USD", "ETH-USD"} {
			out = append(out, model.Trade{
				Exchange: "coinbase", Symbol: sym, TradeID: strconv.Itoa(1000 + i),
				Price:     decimal.NewFromInt(int64(100 + i%17)),
				Size:      decimal.RequireFromString("0.01").Mul(decimal.NewFromInt(int64(1 + i%5))),
				TakerSide: model.Side(1 + i%2), ExchangeTime: t0.Add(time.Duration(i) * 100 * time.Millisecond),
			})
		}
	}
	for _, sym := range []string{"BTC-USD", "ETH-USD"} {
		out = append(out, model.Trade{
			Exchange: "coinbase", Symbol: sym, TradeID: "999999", Price: decimal.NewFromInt(1),
			Size: decimal.NewFromInt(1), TakerSide: model.SideBuy, ExchangeTime: t0.Add(10 * time.Minute),
		})
	}
	return out
}

// publishTwice sends every trade twice, like two redundant ingesters.
func publishTwice(t *testing.T, k config.Kafka, trades []model.Trade) {
	t.Helper()
	pub, err := publish.NewKafka(k, discard())
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range trades {
		if err := pub.Publish(context.Background(), []model.Event{tr, tr}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func startService(t *testing.T, k config.Kafka, group string) (stop func()) {
	t.Helper()
	s, err := NewService(ServiceOptions{
		Kafka: k, Group: group, CommitInterval: 20 * time.Millisecond, TailRecords: 10_000,
		Aggregator: Config{
			Intervals: []time.Duration{time.Second, time.Minute},
			Lateness:  2 * time.Second, Idle: time.Hour, DedupWindow: 10_000,
		},
	}, discard())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
		s.Close()
	}
}

type candleKey struct {
	key   string
	iv    time.Duration
	start int64
}

// readCandles reads the whole candles topic once it stops growing.
func readCandles(t *testing.T, brokers []string, topic string, want int) (map[candleKey]model.Candle, int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	got := map[candleKey]model.Candle{}
	total := 0
	deadline := time.Now().Add(10 * time.Second)
	for total < want && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			c, err := codec.DecodeCandle(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			total++
			got[candleKey{c.Exchange + ":" + c.Symbol, c.Interval, c.Start.UnixNano()}] = c
		})
		cancel()
	}
	// Give any unexpected extra emissions a moment to show up.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	cl.PollFetches(ctx).EachRecord(func(*kgo.Record) { total++ })
	cancel()
	return got, total
}

func sameCandle(a, b model.Candle) bool {
	return a.Open.Equal(b.Open) && a.High.Equal(b.High) && a.Low.Equal(b.Low) && a.Close.Equal(b.Close) &&
		a.Volume.Equal(b.Volume) && a.QuoteVolume.Equal(b.QuoteVolume) && a.BuyVolume.Equal(b.BuyVolume) &&
		a.SellVolume.Equal(b.SellVolume) && a.Trades == b.Trades &&
		a.FirstTradeID == b.FirstTradeID && a.LastTradeID == b.LastTradeID
}

func TestRestartProducesSameCandlesExactlyOnce(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1),
		kfake.SeedTopics(3, "md.trades", "md.gaps", "candles.ref", "candles.restart", "trades.restart"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	t0 := time.Now().Add(-time.Hour).Truncate(time.Minute)
	trades := stream(t0)
	const want = 2 * (20 + 1) // per symbol: 20 one-second candles + 1 one-minute candle

	// Reference: one uninterrupted run.
	ref := config.Kafka{Brokers: brokers, TopicTrades: "md.trades", TopicGaps: "md.gaps", TopicCandles: "candles.ref"}
	publishTwice(t, ref, trades)
	stop := startService(t, ref, "ref")
	refCandles, refTotal := readCandles(t, brokers, ref.TopicCandles, want)
	stop()
	if len(refCandles) != want || refTotal != want {
		t.Fatalf("reference: %d distinct / %d emitted candles, want %d", len(refCandles), refTotal, want)
	}

	// Same stream, but the service is stopped half way and started again.
	rs := config.Kafka{Brokers: brokers, TopicTrades: "trades.restart", TopicGaps: "md.gaps", TopicCandles: "candles.restart"}
	half := len(trades) / 2
	publishTwice(t, rs, trades[:half])
	stop = startService(t, rs, "restart")
	readCandles(t, brokers, rs.TopicCandles, 10) // let it emit a few candles
	stop()
	publishTwice(t, rs, trades[half:])
	stop = startService(t, rs, "restart")
	got, total := readCandles(t, brokers, rs.TopicCandles, want)
	stop()

	if total != want {
		t.Errorf("restart run emitted %d candles, want exactly %d (no duplicates, no partial re-emits)", total, want)
	}
	for k, rc := range refCandles {
		c, ok := got[k]
		switch {
		case !ok:
			t.Errorf("missing candle %v", k)
		case !sameCandle(c, rc):
			t.Errorf("candle %v differs after restart\n got: %s\nwant: %s", k, fmtCandle(c), fmtCandle(rc))
		}
	}
}

func fmtCandle(c model.Candle) string {
	return fmt.Sprintf("O=%s H=%s L=%s C=%s V=%s n=%d ids=%s..%s", c.Open, c.High, c.Low, c.Close, c.Volume, c.Trades, c.FirstTradeID, c.LastTradeID)
}
