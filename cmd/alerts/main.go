// Command alerts evaluates the trade stream against user price rules and
// built-in detectors (whale trades, sharp moves) and serves the rules API.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"marketdata/internal/alerts"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
	"marketdata/internal/ops"
	"marketdata/internal/settings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "alerts:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAlerts()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := cfg.Log.NewLogger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := ops.NewRegistry()
	// Whale thresholds (default and per market) are edited from the UI.
	st, err := settings.Open(cfg.Kafka.Brokers, cfg.Kafka.TopicSettings, log)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Bootstrap(ctx); err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	go st.Follow(ctx, nil)

	svc, err := alerts.NewService(alerts.Options{
		Settings:       st,
		Kafka:          cfg.Kafka,
		Group:          cfg.Group,
		MovePct:        cfg.MovePct,
		MoveWindow:     cfg.MoveWindow,
		MoveCooldown:   cfg.MoveCooldown,
		MaxRules:       cfg.MaxRules,
		DedupWindow:    cfg.DedupWindow,
		CommitInterval: cfg.CommitInterval,
		Metrics:        metrics.NewAlerts(reg),
	}, log)
	if err != nil {
		return err
	}
	defer svc.Close()

	// Not ready (and not evaluating trades) until every rule is loaded.
	var loaded atomic.Bool
	ready := func() error {
		if !loaded.Load() {
			return errors.New("loading rules")
		}
		return nil
	}
	if err := ops.Serve(ctx, cfg.MetricsAddr, reg, ready, log); err != nil {
		return err
	}
	if err := svc.Bootstrap(ctx); err != nil {
		return fmt.Errorf("load rules: %w", err)
	}
	loaded.Store(true)
	go svc.FollowRules(ctx)

	api := &http.Server{Addr: cfg.Addr, Handler: alerts.NewAPI(svc).Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := api.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("api server failed", "err", err)
			stop()
		}
	}()
	log.Info("alerts running", "api", cfg.Addr, "default_whale", st.DefaultWhale(), "move_pct", cfg.MovePct, "move_window", cfg.MoveWindow)

	err = svc.Run(ctx)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = api.Shutdown(shutdownCtx)
	return err
}
