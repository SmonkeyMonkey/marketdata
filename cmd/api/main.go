// Command api serves the web frontend, market history from ClickHouse and
// proxies the gateway websocket and the alerts rules API under one origin.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"marketdata/frontend"
	"marketdata/internal/api"
	"marketdata/internal/catalog"
	"marketdata/internal/config"
	"marketdata/internal/exchange"
	"marketdata/internal/exchange/binance"
	"marketdata/internal/exchange/coinbase"
	"marketdata/internal/exchange/kraken"
	"marketdata/internal/metrics"
	"marketdata/internal/ops"
	"marketdata/internal/settings"
	"marketdata/internal/store"
	"marketdata/internal/subs"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI()
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

	table, err := subs.Open(cfg.Kafka.Brokers, cfg.Kafka.TopicSubscriptions, log)
	if err != nil {
		return err
	}
	defer table.Close()
	if err := table.Bootstrap(ctx); err != nil {
		return fmt.Errorf("load subscriptions: %w", err)
	}
	go table.Follow(ctx, nil)

	httpc := &http.Client{Timeout: 30 * time.Second}
	fetcher := func(fetch func(context.Context, *http.Client, string) ([]exchange.Instrument, error), url string) catalog.Fetcher {
		return func(ctx context.Context) ([]exchange.Instrument, error) { return fetch(ctx, httpc, url) }
	}
	cat := catalog.New(cfg.CatalogTTL, map[string]catalog.Fetcher{
		coinbase.Name: fetcher(coinbase.Catalog, coinbase.CatalogURL),
		binance.Name:  fetcher(binance.Catalog, binance.CatalogURL),
		kraken.Name:   fetcher(kraken.Catalog, kraken.CatalogURL),
	})

	st, err := settings.Open(cfg.Kafka.Brokers, cfg.Kafka.TopicSettings, log)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Bootstrap(ctx); err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	go st.Follow(ctx, nil)

	m := metrics.NewHTTP(reg)
	srv := &http.Server{
		Addr: cfg.Addr,
		Handler: api.Handler(api.Options{
			History: ch, Frontend: frontend.Files(),
			Subs: table, Catalog: cat, MaxSubscriptions: cfg.MaxSubscriptions, Settings: st,
			Gateway: cfg.GatewayURL, Alerts: cfg.AlertsURL,
			Log: log, Observe: m.Observe,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("api listening", "addr", cfg.Addr, "gateway", cfg.GatewayURL.String(), "alerts", cfg.AlertsURL.String())

	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
