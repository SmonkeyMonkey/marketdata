// Package subs keeps the set of streamed markets in a compacted Kafka topic:
// one record per exchange:symbol, a tombstone to remove it. Every reader
// (ingesters, api) rebuilds the same set from the topic and follows changes.
package subs

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/kvtable"
	"marketdata/internal/model"
)

type Table struct {
	*kvtable.Table[model.Subscription]
}

func Open(brokers []string, topic string, log *slog.Logger, opts ...kgo.Opt) (*Table, error) {
	t, err := kvtable.Open(brokers, topic, kvtable.Codec[model.Subscription]{
		Encode: codec.EncodeSubscription,
		Decode: codec.DecodeSubscription,
		Key:    model.Subscription.Key,
		Equal:  func(a, b model.Subscription) bool { return a.Pinned == b.Pinned },
	}, log, opts...)
	if err != nil {
		return nil, err
	}
	return &Table{t}, nil
}

// All returns the subscriptions sorted by exchange and symbol.
func (t *Table) All() []model.Subscription {
	out := t.Table.All()
	slices.SortFunc(out, func(a, b model.Subscription) int {
		return cmp.Or(cmp.Compare(a.Exchange, b.Exchange), cmp.Compare(a.Symbol, b.Symbol))
	})
	return out
}

// Symbols returns the sorted symbols of one exchange.
func (t *Table) Symbols(exchange string) []string {
	var out []string
	for _, s := range t.All() {
		if s.Exchange == exchange {
			out = append(out, s.Symbol)
		}
	}
	return out
}

// Pin makes sure each market is subscribed and pinned, so it cannot be
// removed from the UI. Idempotent: replicas and restarts write nothing new.
func (t *Table) Pin(ctx context.Context, exchange string, symbols ...string) error {
	for _, sym := range symbols {
		if s, ok := t.Get(exchange, sym); ok && s.Pinned {
			continue
		}
		if err := t.Put(ctx, model.Subscription{Exchange: exchange, Symbol: sym, Pinned: true, CreatedAt: time.Now().UTC()}); err != nil {
			return fmt.Errorf("pin %s %s: %w", exchange, sym, err)
		}
	}
	return nil
}

func (t *Table) Get(exchange, symbol string) (model.Subscription, bool) {
	return t.Table.Get(exchange + ":" + symbol)
}

func (t *Table) Delete(ctx context.Context, exchange, symbol string) error {
	return t.Table.Delete(ctx, exchange+":"+symbol)
}
