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
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/model"
)

type Table struct {
	topic string
	cl    *kgo.Client
	log   *slog.Logger

	mu    sync.RWMutex
	items map[string]model.Subscription
}

func Open(brokers []string, topic string, log *slog.Logger, opts ...kgo.Opt) (*Table, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Table{topic: topic, cl: cl, log: log, items: map[string]model.Subscription{}}, nil
}

func (t *Table) Close() { t.cl.Close() }

// Bootstrap reads the topic up to its current end.
func (t *Table) Bootstrap(ctx context.Context) error {
	ends, err := kadm.NewClient(t.cl).ListEndOffsets(ctx, t.topic)
	if err != nil {
		return fmt.Errorf("list %s end offsets: %w", t.topic, err)
	}
	until := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			until[o.Partition] = o.Offset
		}
	})
	for len(until) > 0 {
		fetches := t.cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		fetches.EachRecord(func(r *kgo.Record) {
			t.apply(r)
			if r.Offset+1 >= until[r.Partition] {
				delete(until, r.Partition)
			}
		})
	}
	return nil
}

// Follow applies changes until ctx ends, calling onChange after each batch
// that changed something.
func (t *Table) Follow(ctx context.Context, onChange func()) {
	for ctx.Err() == nil {
		fetches := t.cl.PollFetches(ctx)
		changed := false
		fetches.EachRecord(func(r *kgo.Record) { changed = t.apply(r) || changed })
		if changed && onChange != nil {
			onChange()
		}
	}
}

func (t *Table) apply(r *kgo.Record) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := string(r.Key)
	if r.Value == nil {
		_, had := t.items[key]
		delete(t.items, key)
		return had
	}
	s, err := codec.DecodeSubscription(r.Value)
	if err != nil {
		t.log.Error("skip bad subscription", "key", key, "err", err)
		return false
	}
	prev, had := t.items[key]
	t.items[key] = s
	return !had || prev.Pinned != s.Pinned
}

func (t *Table) All() []model.Subscription {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]model.Subscription, 0, len(t.items))
	for _, s := range t.items {
		out = append(out, s)
	}
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

func (t *Table) Get(exchange, symbol string) (model.Subscription, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.items[exchange+":"+symbol]
	return s, ok
}

// Put persists s (synchronously) and applies it locally for read-your-writes.
func (t *Table) Put(ctx context.Context, s model.Subscription) error {
	b, err := codec.EncodeSubscription(s)
	if err != nil {
		return err
	}
	return t.write(ctx, &kgo.Record{Topic: t.topic, Key: []byte(s.Key()), Value: b})
}

func (t *Table) Delete(ctx context.Context, exchange, symbol string) error {
	return t.write(ctx, &kgo.Record{Topic: t.topic, Key: []byte(exchange + ":" + symbol)})
}

func (t *Table) write(ctx context.Context, r *kgo.Record) error {
	if err := t.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		return fmt.Errorf("write %s: %w", t.topic, err)
	}
	t.apply(r)
	return nil
}
