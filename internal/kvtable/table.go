// Package kvtable keeps a key/value table in a compacted Kafka topic: one
// record per key, a tombstone to delete it. Every reader rebuilds the same
// table from the topic and follows changes; writes go through the topic.
package kvtable

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Codec describes how values are stored.
type Codec[V any] struct {
	Encode func(V) ([]byte, error)
	Decode func([]byte) (V, error)
	Key    func(V) string
	// Equal tells whether an update changed anything (nil: always changed).
	Equal func(a, b V) bool
}

type Table[V any] struct {
	topic string
	codec Codec[V]
	cl    *kgo.Client
	log   *slog.Logger

	mu    sync.RWMutex
	items map[string]V
}

func Open[V any](brokers []string, topic string, codec Codec[V], log *slog.Logger, opts ...kgo.Opt) (*Table[V], error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Table[V]{topic: topic, codec: codec, cl: cl, log: log, items: map[string]V{}}, nil
}

func (t *Table[V]) Close() { t.cl.Close() }

// Bootstrap reads the topic up to its current end.
func (t *Table[V]) Bootstrap(ctx context.Context) error {
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
func (t *Table[V]) Follow(ctx context.Context, onChange func()) {
	for ctx.Err() == nil {
		fetches := t.cl.PollFetches(ctx)
		changed := false
		fetches.EachRecord(func(r *kgo.Record) { changed = t.apply(r) || changed })
		if changed && onChange != nil {
			onChange()
		}
	}
}

func (t *Table[V]) apply(r *kgo.Record) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := string(r.Key)
	if r.Value == nil {
		_, had := t.items[key]
		delete(t.items, key)
		return had
	}
	v, err := t.codec.Decode(r.Value)
	if err != nil {
		t.log.Error("skip bad record", "topic", t.topic, "key", key, "err", err)
		return false
	}
	prev, had := t.items[key]
	t.items[key] = v
	return !had || t.codec.Equal == nil || !t.codec.Equal(prev, v)
}

// All returns every value (unordered).
func (t *Table[V]) All() []V {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]V, 0, len(t.items))
	for _, v := range t.items {
		out = append(out, v)
	}
	return out
}

func (t *Table[V]) Get(key string) (V, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	v, ok := t.items[key]
	return v, ok
}

// Put persists v (synchronously) and applies it locally for read-your-writes.
func (t *Table[V]) Put(ctx context.Context, v V) error {
	b, err := t.codec.Encode(v)
	if err != nil {
		return err
	}
	return t.write(ctx, &kgo.Record{Topic: t.topic, Key: []byte(t.codec.Key(v)), Value: b})
}

func (t *Table[V]) Delete(ctx context.Context, key string) error {
	return t.write(ctx, &kgo.Record{Topic: t.topic, Key: []byte(key)})
}

func (t *Table[V]) write(ctx context.Context, r *kgo.Record) error {
	if err := t.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		return fmt.Errorf("write %s: %w", t.topic, err)
	}
	t.apply(r)
	return nil
}
