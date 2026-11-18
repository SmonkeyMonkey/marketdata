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
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"marketdata/internal/backoff"
	"marketdata/internal/config"
	"marketdata/internal/exchange"
	"marketdata/internal/exchange/binance"
	"marketdata/internal/exchange/coinbase"
	"marketdata/internal/exchange/kraken"
	"marketdata/internal/ingest"
	"marketdata/internal/metrics"
	"marketdata/internal/ops"
	"marketdata/internal/publish"
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

	log.Info("ingester starting", "publisher", cfg.Publisher, "feeds", len(runners))
	var wg sync.WaitGroup
	for _, r := range runners {
		wg.Go(func() { _ = r.Run(ctx) })
	}
	wg.Wait()
	return nil
}

func newFeed(x config.Exchange) (exchange.Feed, error) {
	switch x.Name {
	case coinbase.Name:
		return coinbase.New(x.URL, x.Symbols), nil
	case binance.Name:
		return binance.New(x.URL, x.Symbols)
	case kraken.Name:
		return kraken.New(x.URL, x.Symbols)
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
