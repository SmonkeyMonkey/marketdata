// Command writer consumes normalized events from Kafka and stores them in
// ClickHouse in batches.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"marketdata/internal/backoff"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
	"marketdata/internal/ops"
	"marketdata/internal/store"
	"marketdata/internal/writer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "writer:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadWriter()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := cfg.Log.NewLogger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := ops.NewRegistry()
	if err := ops.Serve(ctx, cfg.MetricsAddr, reg, nil, log); err != nil {
		return err
	}

	ch, err := store.OpenClickHouse(ctx, cfg.ClickHouse)
	if err != nil {
		return err
	}
	defer ch.Close()

	w, err := writer.New(ch, writer.Options{
		Kafka:         cfg.Kafka,
		Group:         cfg.Group,
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
		InsertTimeout: cfg.InsertTimeout,
		Retry:         backoff.Exponential{Min: cfg.RetryMin, Max: cfg.RetryMax},
		Metrics:       metrics.NewWriter(reg),
		DedupWindow:   cfg.DedupWindow,
	}, log)
	if err != nil {
		return err
	}
	defer w.Close()

	go w.WatchLag(ctx, 5*time.Second)

	log.Info("writer starting", "group", cfg.Group, "batch_size", cfg.BatchSize, "flush_interval", cfg.FlushInterval)
	err = w.Run(ctx)
	log.Info("writer stopped", "err", err)
	return err
}
