// Package writer moves events from Kafka into storage in large batches with
// at-least-once delivery: offsets are committed only after a successful
// insert. Duplicates after a crash are removed by the storage engine
// (ReplacingMergeTree keyed by trade identity).
package writer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/backoff"
	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/dedup"
	"marketdata/internal/metrics"
	"marketdata/internal/model"
)

type Store interface {
	InsertTrades(ctx context.Context, trades []model.Trade) error
	InsertGaps(ctx context.Context, gaps []model.Gap) error
	InsertCandles(ctx context.Context, candles []model.Candle) error
	InsertAlerts(ctx context.Context, alerts []model.Alert) error
	InsertBookSamples(ctx context.Context, samples []model.BookSample) error
}

type Options struct {
	Kafka         config.Kafka
	Group         string
	BatchSize     int
	FlushInterval time.Duration
	Retry         backoff.Policy
	// InsertTimeout bounds one insert attempt so a dead connection fails fast
	// and gets retried instead of hanging.
	InsertTimeout time.Duration
	// ShutdownTimeout bounds the final flush after ctx is cancelled.
	ShutdownTimeout time.Duration
	// Metrics is optional; when nil, metrics go to a private registry.
	Metrics *metrics.Writer
	// DedupWindow is how many recent trade IDs per exchange:symbol are
	// remembered to drop copies sent by redundant ingesters.
	DedupWindow int
}

type Writer struct {
	client *kgo.Client
	store  Store
	opts   Options
	log    *slog.Logger

	// mu guards the pending batch: the poll loop and the partition-revoke
	// callback (called from a client goroutine) both flush it.
	mu      sync.Mutex
	records []*kgo.Record
	trades  []model.Trade
	gaps    []model.Gap
	candles []model.Candle
	alerts  []model.Alert
	// books keeps the latest snapshot stats per market and second: ~10
	// snapshots a second per market are sampled down to one row.
	books     map[string]model.BookSample
	lastFlush time.Time

	// stored holds IDs that are already in storage; batchIDs holds IDs in
	// the pending batch. IDs move to stored only after a successful insert:
	// if a batch is dropped (partitions lost) and re-read later, its trades
	// must not be mistaken for duplicates.
	stored   *dedup.Window
	batchIDs map[string]struct{}
}

func New(store Store, opts Options, log *slog.Logger, kopts ...kgo.Opt) (*Writer, error) {
	if opts.ShutdownTimeout == 0 {
		opts.ShutdownTimeout = 10 * time.Second
	}
	if opts.InsertTimeout == 0 {
		opts.InsertTimeout = 30 * time.Second
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NewWriter(prometheus.NewRegistry())
	}
	opts.Metrics.Init("trades", "gaps", "candles", "alerts", "book_stats")
	if opts.DedupWindow == 0 {
		opts.DedupWindow = 20_000
	}
	w := &Writer{
		store: store, opts: opts, log: log, lastFlush: time.Now(),
		stored:   dedup.NewWindow(opts.DedupWindow),
		books:    map[string]model.BookSample{},
		batchIDs: make(map[string]struct{}),
	}
	kopts = append([]kgo.Opt{
		kgo.SeedBrokers(opts.Kafka.Brokers...),
		kgo.ConsumerGroup(opts.Group),
		kgo.ConsumeTopics(consumeTopics(opts.Kafka)...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(w.onRevoked),
		kgo.OnPartitionsLost(w.onLost),
	}, kopts...)
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	w.client = cl
	return w, nil
}

// Run consumes until ctx is cancelled, then flushes what it holds.
func (w *Writer) Run(ctx context.Context) error {
	for {
		pollCtx, cancel := context.WithTimeout(ctx, w.opts.FlushInterval)
		fetches := w.client.PollRecords(pollCtx, w.opts.BatchSize)
		cancel()

		if ctx.Err() != nil {
			return w.finalFlush()
		}
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				w.log.Error("kafka fetch error", "topic", topic, "partition", p, "err", err)
			}
		})

		w.mu.Lock()
		fetches.EachRecord(w.add)
		var err error
		if len(w.records) >= w.opts.BatchSize || time.Since(w.lastFlush) >= w.opts.FlushInterval {
			err = w.flushLocked(ctx)
		}
		w.mu.Unlock()
		w.client.AllowRebalance()

		if err != nil {
			if ctx.Err() != nil {
				return w.finalFlush()
			}
			return err
		}
	}
}

// Close leaves the consumer group. Call after Run returns.
func (w *Writer) Close() {
	w.client.AllowRebalance()
	w.client.Close()
}

// add decodes a record into the pending batch. Undecodable records are
// logged and skipped (still committed) so one bad message cannot block the
// partition forever.
func (w *Writer) add(r *kgo.Record) {
	w.records = append(w.records, r)
	switch r.Topic {
	case w.opts.Kafka.TopicTrades:
		t, err := codec.DecodeTrade(r.Value)
		if err != nil {
			w.skip(r, err)
			return
		}
		key := t.Exchange + ":" + t.Symbol
		batchKey := key + ":" + t.TradeID
		if _, inBatch := w.batchIDs[batchKey]; inBatch || w.stored.Seen(key, t.TradeID) {
			w.opts.Metrics.Duplicate()
			return // the record is still committed with the batch
		}
		w.batchIDs[batchKey] = struct{}{}
		w.trades = append(w.trades, t)
	case w.opts.Kafka.TopicGaps:
		g, err := codec.DecodeGap(r.Value)
		if err != nil {
			w.skip(r, err)
			return
		}
		w.gaps = append(w.gaps, g)
	case w.opts.Kafka.TopicCandles:
		c, err := codec.DecodeCandle(r.Value)
		if err != nil {
			w.skip(r, err)
			return
		}
		w.candles = append(w.candles, c)
	case w.opts.Kafka.TopicAlerts:
		a, err := codec.DecodeAlert(r.Value)
		if err != nil {
			w.skip(r, err)
			return
		}
		w.alerts = append(w.alerts, a)
	case w.opts.Kafka.TopicBooks:
		b, err := codec.DecodeBook(r.Value)
		if err != nil {
			w.skip(r, err)
			return
		}
		stats, ok := b.Stats(10)
		if !ok {
			return
		}
		sec := b.ExchangeTime.Truncate(time.Second)
		w.books[b.Exchange+"|"+b.Symbol+"|"+sec.String()] = model.BookSample{Exchange: b.Exchange, Symbol: b.Symbol, Time: sec, Stats: stats}
	}
}

func consumeTopics(k config.Kafka) []string {
	topics := []string{k.TopicTrades, k.TopicGaps}
	for _, t := range []string{k.TopicCandles, k.TopicAlerts, k.TopicBooks} {
		if t != "" {
			topics = append(topics, t)
		}
	}
	return topics
}

func (w *Writer) skip(r *kgo.Record, err error) {
	w.opts.Metrics.DecodeError(r.Topic)
	w.log.Error("skip bad record", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "err", err)
}

// flushLocked inserts the batch, retrying until it succeeds or ctx ends,
// then commits offsets. While storage is down the writer stops consuming and
// Kafka absorbs the backlog.
func (w *Writer) flushLocked(ctx context.Context) error {
	started := time.Now()
	w.lastFlush = started
	if len(w.records) == 0 {
		return nil
	}
	for attempt := 0; ; attempt++ {
		err := w.insert(ctx)
		if err == nil {
			break
		}
		w.opts.Metrics.InsertError()
		if ctx.Err() != nil {
			return fmt.Errorf("flush aborted: %w", err)
		}
		delay := w.opts.Retry.Delay(attempt)
		w.log.Warn("insert failed, retrying", "err", err, "attempt", attempt+1, "delay", delay, "rows", len(w.records))
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("flush aborted: %w", err)
		case <-t.C:
		}
	}

	// The batch is stored, so its commit must not be skipped because ctx was
	// cancelled a moment ago (shutdown): that would replay the batch on the
	// next start. Commit with its own short deadline instead.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := w.client.CommitRecords(commitCtx, w.records...); err != nil {
		// Data is stored; a failed commit only means these records may be
		// read again, and duplicates are collapsed by the table engine.
		w.log.Warn("offset commit failed", "err", err)
	}
	for _, t := range w.trades {
		w.stored.Add(t.Exchange+":"+t.Symbol, t.TradeID)
	}
	now := time.Now()
	w.opts.Metrics.Stored(w.trades, now)
	w.opts.Metrics.Flushed(map[string]int{"trades": len(w.trades), "gaps": len(w.gaps), "candles": len(w.candles), "alerts": len(w.alerts), "book_stats": len(w.books)}, len(w.records), now.Sub(started))
	w.log.Debug("flushed", "trades", len(w.trades), "gaps", len(w.gaps))
	w.reset()
	return nil
}

// WatchLag periodically reports the consumer group lag (log end offset minus
// committed offset) until ctx ends. It runs independently of the poll loop,
// so lag keeps growing on the dashboard even while inserts are failing.
func (w *Writer) WatchLag(ctx context.Context, every time.Duration) {
	adm := kadm.NewClient(w.client)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		lags, err := adm.Lag(ctx, w.opts.Group)
		if err == nil {
			err = lags.Error()
		}
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("lag check failed", "err", err)
			}
		} else {
			for _, gl := range lags {
				for topic, parts := range gl.Lag {
					for p, l := range parts {
						w.opts.Metrics.Lag(topic, p, l.Lag)
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *Writer) insert(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, w.opts.InsertTimeout)
	defer cancel()
	if err := w.store.InsertTrades(ctx, w.trades); err != nil {
		return err
	}
	if err := w.store.InsertGaps(ctx, w.gaps); err != nil {
		return err
	}
	if err := w.store.InsertCandles(ctx, w.candles); err != nil {
		return err
	}
	if err := w.store.InsertAlerts(ctx, w.alerts); err != nil {
		return err
	}
	samples := make([]model.BookSample, 0, len(w.books))
	for _, s := range w.books {
		samples = append(samples, s)
	}
	return w.store.InsertBookSamples(ctx, samples)
}

func (w *Writer) reset() {
	clear(w.records)
	w.records = w.records[:0]
	w.trades = w.trades[:0]
	w.gaps = w.gaps[:0]
	w.candles = w.candles[:0]
	w.alerts = w.alerts[:0]
	clear(w.books)
	clear(w.batchIDs)
}

func (w *Writer) finalFlush() error {
	ctx, cancel := context.WithTimeout(context.Background(), w.opts.ShutdownTimeout)
	defer cancel()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked(ctx)
}

// onRevoked flushes before partitions move to another consumer, so the new
// owner starts after what we already stored.
func (w *Writer) onRevoked(context.Context, *kgo.Client, map[string][]int32) {
	if err := w.finalFlush(); err != nil {
		w.log.Error("flush on revoke failed", "err", err)
	}
}

// onLost drops the batch: we no longer own the partitions, the new owner
// will re-read these records from the last committed offset.
func (w *Writer) onLost(context.Context, *kgo.Client, map[string][]int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reset()
}
