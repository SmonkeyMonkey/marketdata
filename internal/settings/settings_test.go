package settings

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestThresholdPrecedence(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "md.settings"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d := decimal.RequireFromString

	ui, _ := Open(c.ListenAddrs(), "md.settings", log)
	defer ui.Close()
	if !ui.Whale("binance", "BTC-USDT").Equal(d("100000")) {
		t.Fatal("initial default not used")
	}
	_ = ui.SetWhale(ctx, "", "", d("50000"))             // default from the UI
	_ = ui.SetWhale(ctx, "binance", "XRP-BTC", d("0.5")) // quoted in BTC
	if got := ui.Whale("binance", "BTC-USDT"); !got.Equal(d("50000")) {
		t.Errorf("default = %s", got)
	}
	if got := ui.Whale("binance", "XRP-BTC"); !got.Equal(d("0.5")) {
		t.Errorf("override = %s", got)
	}

	// Another process (the alerts service) sees the same after bootstrap.
	alerts, _ := Open(c.ListenAddrs(), "md.settings", log)
	defer alerts.Close()
	if err := alerts.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if !alerts.Whale("binance", "XRP-BTC").Equal(d("0.5")) || !alerts.DefaultWhale().Equal(d("50000")) {
		t.Error("settings not replicated")
	}
	if len(alerts.Markets()) != 1 {
		t.Errorf("markets = %+v (default must not be listed)", alerts.Markets())
	}

	_ = ui.ResetWhale(ctx, "binance", "XRP-BTC")
	if got := ui.Whale("binance", "XRP-BTC"); !got.Equal(d("50000")) {
		t.Errorf("after reset = %s, want default", got)
	}
}
