// Command ingester connects to exchange websocket feeds, normalizes the
// messages and publishes them. Config comes from env, logs go to stderr,
// and SIGINT/SIGTERM trigger a graceful shutdown.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"marketdata/internal/backoff"
	"marketdata/internal/config"
	"marketdata/internal/exchange"
	"marketdata/internal/exchange/binance"
	"marketdata/internal/exchange/bybit"
	"marketdata/internal/exchange/coinbase"
	"marketdata/internal/exchange/kraken"
	"marketdata/internal/exchange/okx"
	"marketdata/internal/ingest"
	"marketdata/internal/metrics"
	"marketdata/internal/ops"
	"marketdata/internal/publish"
	"marketdata/internal/subs"
)

func main() {
	run := run
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		run = healthcheck
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ingester:", err)
		os.Exit(1)
	}
}

// healthcheck exits non-zero unless every feed is live. It is the container
// health check (the image has no shell or curl).
func healthcheck() error {
	cfg, err := config.LoadIngester()
	if err != nil {
		return err
	}
	return ops.Probe(cfg.MetricsAddr, "/readyz")
}

func run() error {
	cfg, err := config.LoadIngester()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := cfg.Log.NewLogger().With("instance", cfg.InstanceID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := ops.NewRegistry()
	pub, closePub, err := newPublisher(cfg, reg, log)
	if err != nil {
		return err
	}
	defer closePub()

	m := metrics.NewIngest(reg)
	var runners []*ingest.Runner
	for _, x := range cfg.Exchanges {
		feed, err := newFeed(x)
		if err != nil {
			return err
		}
		runners = append(runners, &ingest.Runner{
			Feed:        feed,
			Publisher:   pub,
			Log:         log,
			Backoff:     backoff.Exponential{Min: cfg.BackoffMin, Max: cfg.BackoffMax},
			ReadTimeout: x.ReadTimeout,
			DialTimeout: cfg.DialTimeout,
			ReadLimit:   cfg.ReadLimit,
			StableAfter: cfg.StableAfter,
			Metrics:     m,
			Source:      cfg.InstanceID,
		})
		log.Info("feed configured", "exchange", x.Name, "symbols", x.Symbols)
	}

	ready := func() error {
		var down []string
		for _, r := range runners {
			if !r.Live() {
				down = append(down, r.Feed.Name())
			}
		}
		if len(down) > 0 {
			return fmt.Errorf("feeds not live: %v", down)
		}
		return nil
	}
	if err := ops.Serve(ctx, cfg.MetricsAddr, reg, ready, log); err != nil {
		return err
	}

	if cfg.Publisher == "kafka" {
		stopSubs, err := followSubscriptions(ctx, cfg, runners, log)
		if err != nil {
			return err
		}
		defer stopSubs()
	}

	log.Info("ingester starting", "publisher", cfg.Publisher, "feeds", len(runners))
	var wg sync.WaitGroup
	for _, r := range runners {
		wg.Go(func() { _ = r.Run(ctx) })
	}
	wg.Wait()
	return nil
}

// followSubscriptions makes the subscriptions topic drive what each feed
// streams. Symbols from the ingester config are written there as pinned
// (idempotent; every replica does it), so the topic is the single list of
// streamed markets and the UI can add more at runtime.
func followSubscriptions(ctx context.Context, cfg config.Ingester, runners []*ingest.Runner, log *slog.Logger) (func(), error) {
	table, err := subs.Open(cfg.Kafka.Brokers, cfg.Kafka.TopicSubscriptions, log)
	if err != nil {
		return nil, err
	}
	if err := table.Bootstrap(ctx); err != nil {
		table.Close()
		return nil, fmt.Errorf("load subscriptions: %w", err)
	}
	for _, x := range cfg.Exchanges {
		if err := table.Pin(ctx, x.Name, x.Symbols...); err != nil {
			table.Close()
			return nil, err
		}
	}

	var mu sync.Mutex
	applied := map[string][]string{}
	apply := func() {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range runners {
			name := r.Feed.Name()
			want := table.Symbols(name)
			if len(want) == 0 || slices.Equal(want, applied[name]) {
				continue
			}
			added, removed := exchange.Diff(applied[name], want)
			if err := r.SetSymbols(ctx, want); err != nil {
				log.Error("apply subscriptions failed", "exchange", name, "err", err)
				continue
			}
			applied[name] = want
			log.Info("subscriptions changed", "exchange", name, "added", added, "removed", removed, "total", len(want))
		}
	}
	apply()
	go table.Follow(ctx, apply)
	return table.Close, nil
}

func newFeed(x config.Exchange) (exchange.Feed, error) {
	switch x.Name {
	case coinbase.Name:
		return coinbase.New(x.URL, x.Symbols), nil
	case binance.Name:
		return binance.New(x.URL, x.Symbols)
	case kraken.Name:
		return kraken.New(x.URL, x.Symbols)
	case bybit.Name:
		return bybit.New(x.URL, x.Symbols)
	case okx.Name:
		return okx.New(x.URL, x.Symbols)
	default:
		return nil, fmt.Errorf("no feed implementation for %q", x.Name)
	}
}

func newPublisher(cfg config.Ingester, reg prometheus.Registerer, log *slog.Logger) (ingest.Publisher, func(), error) {
	switch cfg.Publisher {
	case "kafka":
		k, err := publish.NewKafka(cfg.Kafka, log)
		if err != nil {
			return nil, nil, err
		}
		f := promauto.With(reg)
		f.NewCounterFunc(prometheus.CounterOpts{
			Name: "md_kafka_produce_failed_total", Help: "Records that could not be delivered to Kafka.",
		}, func() float64 { return float64(k.Failed()) })
		f.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "md_kafka_produce_buffered_records", Help: "Records buffered in the producer, not yet acked.",
		}, func() float64 { return float64(k.Buffered()) })

		return k, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := k.Close(ctx); err != nil {
				log.Error("kafka flush on shutdown failed", "err", err)
			}
			if n := k.Failed(); n > 0 {
				log.Error("records not delivered", "count", n)
			}
		}, nil
	default:
		return publish.NewJSONLines(os.Stdout), func() {}, nil
	}
}
