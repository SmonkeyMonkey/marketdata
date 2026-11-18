// Command gateway streams live trades and candles to websocket clients.
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

	"marketdata/internal/config"
	"marketdata/internal/gateway"
	"marketdata/internal/metrics"
	"marketdata/internal/ops"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadGateway()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := cfg.Log.NewLogger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := ops.NewRegistry()
	m := metrics.NewGateway(reg)

	hub := gateway.NewHub()
	feed, err := gateway.NewFeed(hub, cfg.Kafka, cfg.DedupWindow, m, log)
	if err != nil {
		return err
	}
	defer feed.Close()
	// Not ready while Kafka is silent: clients would get no data at all.
	ready := func() error { return feed.Fresh(30 * time.Second) }
	if err := ops.Serve(ctx, cfg.MetricsAddr, reg, ready, log); err != nil {
		return err
	}
	go func() {
		if err := feed.Run(ctx); err != nil {
			log.Error("feed stopped", "err", err)
			stop()
		}
	}()

	ws := gateway.NewServer(hub, gateway.Options{
		MaxConns:         cfg.MaxConns,
		MaxSubscriptions: cfg.MaxSubscriptions,
		SendQueue:        cfg.SendQueue,
		WriteTimeout:     cfg.WriteTimeout,
		PingInterval:     cfg.PingInterval,
		Metrics:          m,
	}, log)

	mux := http.NewServeMux()
	mux.Handle("GET /ws", ws)
	srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("gateway listening", "addr", cfg.Addr, "max_conns", cfg.MaxConns)

	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	// Stop accepting, then close every client with "going away" so they
	// reconnect to another instance.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	ws.Shutdown(shutdownCtx)
	log.Info("gateway stopped")
	return nil
}
