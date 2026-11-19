// Command arbitrage compares order books of the same asset across exchanges
// and publishes the spreads, opportunity windows and alerts.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"marketdata/internal/arb"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
	"marketdata/internal/ops"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "arbitrage:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadArbitrage()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := cfg.Log.NewLogger()
	engCfg, err := engineConfig(cfg)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := ops.NewRegistry()
	svc, err := arb.NewService(arb.NewEngine(engCfg), cfg.Kafka, cfg.Tick, metrics.NewArbitrage(reg), log)
	if err != nil {
		return err
	}
	defer svc.Close()
	ready := func() error { return svc.Fresh(30 * time.Second) }
	if err := ops.Serve(ctx, cfg.MetricsAddr, reg, ready, log); err != nil {
		return err
	}
	log.Info("arbitrage running", "fees_bps", cfg.TakerFees, "quotes", cfg.Quotes, "fx", cfg.FX,
		"min_net_bps", cfg.MinNetBps, "alert_bps", cfg.AlertBps)
	return svc.Run(ctx)
}

func engineConfig(c config.Arbitrage) (arb.Config, error) {
	quotes, err := arb.ParsePairs(c.Quotes)
	if err != nil {
		return arb.Config{}, fmt.Errorf("ARB_QUOTES: %w", err)
	}
	fx, err := arb.ParsePairs(c.FX)
	if err != nil {
		return arb.Config{}, fmt.Errorf("ARB_FX: %w", err)
	}
	// The rate market must quote the currency in its common quote, so its
	// mid is "common per quote" (kraken:USDT-USD for USDT -> USD).
	for quote, market := range fx {
		if want := quote + "-" + quotes[quote]; quotes[quote] == "" || !strings.HasSuffix(market, ":"+want) {
			return arb.Config{}, fmt.Errorf("ARB_FX: %s:%s must be an exchange:%s market and %s must be in ARB_QUOTES", quote, market, want, quote)
		}
	}
	fees, err := arb.ParseFees(c.TakerFees)
	if err != nil {
		return arb.Config{}, fmt.Errorf("ARB_TAKER_FEES_BPS: %w", err)
	}
	return arb.Config{
		Quotes: quotes, FX: fx, FeesBps: fees,
		MinNetBps: c.MinNetBps, AlertBps: c.AlertBps, AlertCooldown: c.AlertCooldown,
		MaxBookAge: c.MaxBookAge, PublishEvery: c.PublishEvery,
	}, nil
}
