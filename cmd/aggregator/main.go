// Command aggregator builds OHLCV candles from the trade stream and
// publishes them to Kafka.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"marketdata/internal/candles"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
	"marketdata/internal/model"
	"marketdata/internal/ops"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "aggregator:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAggregator()
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

	svc, err := candles.NewService(candles.ServiceOptions{
		Kafka:          cfg.Kafka,
		Group:          cfg.Group,
		CommitInterval: cfg.CommitInterval,
		TailRecords:    cfg.TailRecords,
		Metrics:        metrics.NewAggregator(reg),
		Aggregator: candles.Config{
			Intervals:   cfg.Intervals,
			Lateness:    cfg.Lateness,
			Idle:        cfg.Idle,
			DedupWindow: cfg.DedupWindow,
		},
	}, log)
	if err != nil {
		return err
	}
	defer svc.Close()

	periods := make([]string, len(cfg.Intervals))
	for i, iv := range cfg.Intervals {
		periods[i] = model.PeriodName(iv)
	}
	log.Info("aggregator starting", "intervals", periods, "lateness", cfg.Lateness, "group", cfg.Group)
	return svc.Run(ctx)
}
