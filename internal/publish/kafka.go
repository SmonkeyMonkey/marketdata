package publish

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/model"
)

// Kafka produces events asynchronously. Records are keyed by
// exchange:symbol, so all events of one instrument land in one partition and
// keep their order.
type Kafka struct {
	client *kgo.Client
	topics config.Kafka
	log    *slog.Logger
	failed atomic.Int64
}

func NewKafka(cfg config.Kafka, log *slog.Logger, opts ...kgo.Opt) (*Kafka, error) {
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(5 * time.Millisecond), // small latency cost, much better batching
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Kafka{client: cl, topics: cfg, log: log}, nil
}

// Publish enqueues events and returns without waiting for acks. If the
// producer buffer is full it blocks, which slows the websocket reader down
// instead of dropping data silently.
func (k *Kafka) Publish(ctx context.Context, events []model.Event) error {
	for _, e := range events {
		rec, err := k.record(e)
		if err != nil {
			return err
		}
		if rec == nil {
			continue // not published (e.g. book resync notices)
		}
		// Records outlive the caller's ctx: a reconnect or shutdown must not
		// cancel data that is already buffered. Close flushes them.
		k.client.Produce(context.Background(), rec, k.onDelivered)
	}
	return nil
}

func (k *Kafka) record(e model.Event) (*kgo.Record, error) {
	var (
		topic, key string
		value      []byte
		err        error
	)
	switch ev := e.(type) {
	case model.Trade:
		topic, key = k.topics.TopicTrades, ev.Exchange+":"+ev.Symbol
		value, err = codec.EncodeTrade(ev)
	case model.Gap:
		topic, key = k.topics.TopicGaps, ev.Exchange+":"+ev.Symbol
		value, err = codec.EncodeGap(ev)
	case model.Book:
		topic, key = k.topics.TopicBooks, ev.Exchange+":"+ev.Symbol
		value, err = codec.EncodeBook(ev)
	case model.BookResync:
		return nil, nil
	default:
		return nil, fmt.Errorf("kafka: unsupported event %T", e)
	}
	if err != nil {
		return nil, err
	}
	return &kgo.Record{Topic: topic, Key: []byte(key), Value: value}, nil
}

func (k *Kafka) onDelivered(r *kgo.Record, err error) {
	if err != nil {
		k.failed.Add(1)
		k.log.Error("kafka produce failed", "topic", r.Topic, "key", string(r.Key), "err", err)
	}
}

// Failed reports how many records could not be delivered.
func (k *Kafka) Failed() int64 { return k.failed.Load() }

// Buffered reports records waiting to be sent; a growing value means Kafka
// is slower than the feed.
func (k *Kafka) Buffered() int64 { return k.client.BufferedProduceRecords() }

// Close waits for buffered records to be delivered (bounded by ctx).
func (k *Kafka) Close(ctx context.Context) error {
	err := k.client.Flush(ctx)
	k.client.Close()
	return err
}
