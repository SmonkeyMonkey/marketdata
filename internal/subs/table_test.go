package subs

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"marketdata/internal/model"
)

func TestTableReplicatesThroughKafka(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "md.subscriptions"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	writer, _ := Open(c.ListenAddrs(), "md.subscriptions", log)
	defer writer.Close()
	_ = writer.Put(ctx, model.Subscription{Exchange: "binance", Symbol: "BTC-USDT", Pinned: true})
	_ = writer.Put(ctx, model.Subscription{Exchange: "binance", Symbol: "SOL-USDT"})
	_ = writer.Put(ctx, model.Subscription{Exchange: "kraken", Symbol: "ETH-USD"})
	_ = writer.Delete(ctx, "kraken", "ETH-USD")

	// A second process sees the same state after bootstrapping...
	reader, _ := Open(c.ListenAddrs(), "md.subscriptions", log)
	defer reader.Close()
	if err := reader.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	if got := reader.Symbols("binance"); !slices.Equal(got, []string{"BTC-USDT", "SOL-USDT"}) {
		t.Fatalf("binance = %v", got)
	}
	if len(reader.Symbols("kraken")) != 0 {
		t.Error("deleted subscription survived")
	}
	if s, _ := reader.Get("binance", "BTC-USDT"); !s.Pinned {
		t.Error("pinned flag lost")
	}

	// ...and follows later changes.
	changed := make(chan struct{}, 4)
	go reader.Follow(ctx, func() { changed <- struct{}{} })
	_ = writer.Put(ctx, model.Subscription{Exchange: "kraken", Symbol: "DOGE-USD"})
	select {
	case <-changed:
	case <-ctx.Done():
		t.Fatal("change not delivered")
	}
	if got := reader.Symbols("kraken"); !slices.Equal(got, []string{"DOGE-USD"}) {
		t.Errorf("kraken = %v", got)
	}
}
