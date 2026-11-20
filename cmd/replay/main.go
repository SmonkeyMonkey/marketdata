// Command replay feeds stored trades back through the pipeline, for
// benchmarks and for checking that processing is deterministic.
//
//	replay seed  -topic bench.trades [-limit N] [-from T] [-to T] [-speed X]
//	    reads trades from ClickHouse in exchange-time order and produces them
//	    to a topic: as fast as possible (speed 0) or X times real time.
//	replay watch -topic bench.trades -groups bench-writer,bench-aggregator
//	    reports how fast consumer groups drain the topic, until they catch up.
//
// It never writes to the live topics (md.*): replayed trades would reach
// live alerts and the UI.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/model"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: replay seed|watch [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "seed":
		err = seed(ctx, os.Args[2:])
	case "watch":
		err = watch(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "replay:", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func brokers() []string { return strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",") }

func liveTopic(t string) error {
	if strings.HasPrefix(t, "md.") {
		return fmt.Errorf("refusing to replay into live topic %s", t)
	}
	return nil
}

func seed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	topic := fs.String("topic", "bench.trades", "destination topic (never md.*)")
	limit := fs.Int("limit", 0, "max trades (0: all in range)")
	from := fs.String("from", "", "start of exchange time range, RFC 3339 (default: all)")
	to := fs.String("to", "", "end of exchange time range, RFC 3339 (default: now)")
	speed := fs.Float64("speed", 0, "0: as fast as possible; X: X times real time")
	_ = fs.Parse(args)
	if err := liveTopic(*topic); err != nil {
		return err
	}
	start, end := time.Unix(0, 0).UTC(), time.Now().UTC()
	var err error
	if *from != "" {
		if start, err = time.Parse(time.RFC3339, *from); err != nil {
			return fmt.Errorf("-from: %w", err)
		}
	}
	if *to != "" {
		if end, err = time.Parse(time.RFC3339, *to); err != nil {
			return fmt.Errorf("-to: %w", err)
		}
	}

	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{env("CLICKHOUSE_ADDR", "localhost:9000")},
		Auth:        clickhouse.Auth{Database: env("CLICKHOUSE_DB", "marketdata"), Username: env("CLICKHOUSE_USER", "marketdata"), Password: env("CLICKHOUSE_PASSWORD", "marketdata")},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		ReadTimeout: 10 * time.Minute,
	})
	if err != nil {
		return err
	}
	defer ch.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()...),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.MaxBufferedRecords(200_000))
	if err != nil {
		return err
	}
	defer cl.Close()

	// Read in exchange-time windows: one ORDER BY over the whole history
	// (with FINAL) needs memory that grows with it, and took ClickHouse down
	// on a small Docker VM. Each window is sorted on its own.
	if start.Unix() == 0 {
		if err := ch.QueryRow(ctx, "SELECT min(exchange_time) FROM trades").Scan(&start); err != nil {
			return fmt.Errorf("first trade: %w", err)
		}
	}
	const window = 10 * time.Minute
	q := `SELECT exchange, symbol, trade_id, price, size, toString(taker_side), exchange_time, receive_time
		FROM trades FINAL
		WHERE exchange_time >= ? AND exchange_time < ?
		ORDER BY exchange_time, exchange, symbol, toUInt64OrZero(trade_id)`

	var (
		n       int64
		failed  atomic.Int64
		first   time.Time // exchange time of the first trade, for pacing
		began   = time.Now()
		lastLog = began
	)
	produce := func(t model.Trade) error {
		if *speed > 0 {
			if first.IsZero() {
				first = t.ExchangeTime
			}
			due := began.Add(time.Duration(float64(t.ExchangeTime.Sub(first)) / *speed))
			if d := time.Until(due); d > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(d):
				}
			}
		}
		v, err := codec.EncodeTrade(t)
		if err != nil {
			return err
		}
		cl.Produce(ctx, &kgo.Record{Topic: *topic, Key: []byte(t.Exchange + ":" + t.Symbol), Value: v}, func(_ *kgo.Record, err error) {
			if err != nil {
				failed.Add(1)
			}
		})
		n++
		if now := time.Now(); now.Sub(lastLog) >= 2*time.Second {
			fmt.Printf("seeded %d trades (%.0f/s), exchange time %s\n", n, float64(n)/now.Sub(began).Seconds(), t.ExchangeTime.Format(time.DateTime))
			lastLog = now
		}
		return nil
	}
	for lo := start; lo.Before(end) && (*limit == 0 || n < int64(*limit)); lo = lo.Add(window) {
		hi := lo.Add(window)
		if hi.After(end) {
			hi = end
		}
		rows, err := ch.Query(ctx, q, lo, hi)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}
		for rows.Next() && (*limit == 0 || n < int64(*limit)) {
			var (
				t    model.Trade
				side string
			)
			if err := rows.Scan(&t.Exchange, &t.Symbol, &t.TradeID, &t.Price, &t.Size, &side, &t.ExchangeTime, &t.ReceiveTime); err != nil {
				rows.Close()
				return fmt.Errorf("scan: %w", err)
			}
			_ = t.TakerSide.UnmarshalText([]byte(side))
			if err := produce(t); err != nil {
				rows.Close()
				return err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	if err := cl.Flush(ctx); err != nil {
		return err
	}
	d := time.Since(began)
	fmt.Printf("seeded %d trades into %s in %s (%.0f trades/s), %d failed\n", n, *topic, d.Round(time.Millisecond), float64(n)/d.Seconds(), failed.Load())
	if failed.Load() > 0 {
		return errors.New("some records were not delivered")
	}
	return nil
}

// watch measures how fast the benchmark consumers process the topic. Each
// consumer's own counter (from its /metrics) is sampled every 100 ms;
// throughput is counted from its last sample at zero to the sample that
// reached the topic size, so group join and startup are excluded. It then
// waits until every group has committed everything (the aggregator commits
// a candle's trades only once the candle is closed), so results can be
// checked.
func watch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	topic := fs.String("topic", "bench.trades", "topic the consumers read")
	var counters counterFlags
	fs.Var(&counters, "counter", "name=group=url=metric line prefix; repeatable")
	timeout := fs.Duration("timeout", 30*time.Minute, "give up after this long")
	_ = fs.Parse(args)
	if len(counters) == 0 {
		return errors.New("no -counter")
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	ends, err := adm.ListEndOffsets(ctx, *topic)
	if err != nil {
		return err
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	fmt.Printf("%d trades in %s\n", total, *topic)

	began, lastLog := time.Now(), time.Time{}
	for {
		done := true
		for _, c := range counters {
			if c.end.IsZero() {
				v, err := scrape(ctx, c.url, c.metric)
				if err != nil {
					v = 0 // not up yet
				}
				now := time.Now()
				if v == 0 {
					c.start = now
				}
				if v >= float64(total) {
					c.end = now
				}
				c.last = v
			}
			done = done && !c.end.IsZero()
		}
		if time.Since(lastLog) >= time.Second || done {
			var line []string
			for _, c := range counters {
				line = append(line, fmt.Sprintf("%s %.0f", c.name, c.last))
			}
			fmt.Printf("%6.1fs  %s\n", time.Since(began).Seconds(), strings.Join(line, "  "))
			lastLog = time.Now()
		}
		if done {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("consumers did not finish: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	fmt.Println()
	for _, c := range counters {
		d := c.end.Sub(c.start)
		fmt.Printf("%-18s %9d trades in %6.2fs = %9.0f trades/s\n", c.name, total, d.Seconds(), float64(total)/d.Seconds())
	}

	// Wait for the commits, so the results are complete before checking.
	for _, c := range counters {
		for {
			committed, err := committedSum(ctx, adm, c.group, *topic)
			if err != nil {
				return err
			}
			if committed >= total {
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("%s did not commit everything: %w", c.group, ctx.Err())
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	fmt.Println("all consumer groups committed")
	return nil
}

type counter struct {
	name, group, url, metric string
	start, end               time.Time
	last                     float64
}

type counterFlags []*counter

func (c *counterFlags) String() string { return fmt.Sprint(len(*c)) }
func (c *counterFlags) Set(v string) error {
	p := strings.SplitN(v, "=", 4)
	if len(p) != 4 {
		return fmt.Errorf("want name=group=url=metric, got %q", v)
	}
	*c = append(*c, &counter{name: p[0], group: p[1], url: p[2], metric: p[3]})
	return nil
}

// scrape returns the value of the first metric line starting with prefix.
func scrape(ctx context.Context, url, prefix string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		if line := sc.Text(); strings.HasPrefix(line, prefix+" ") {
			return strconv.ParseFloat(strings.TrimSpace(line[len(prefix):]), 64)
		}
	}
	return 0, fmt.Errorf("%s: no %s", url, prefix)
}

func committedSum(ctx context.Context, adm *kadm.Client, group, topic string) (int64, error) {
	offs, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		var ge *kadm.ShardErrors
		if errors.As(err, &ge) {
			return 0, nil // group does not exist yet
		}
		return 0, err
	}
	var sum int64
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.Err == nil && o.At > 0 {
			sum += o.At
		}
	})
	return sum, nil
}
