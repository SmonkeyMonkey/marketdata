package arb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	bookorder "marketdata/internal/book"
	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/fetcherr"
	"marketdata/internal/metrics"
	"marketdata/internal/model"
)

// Service feeds the engine from the books topic and publishes snapshots,
// closed windows and alerts.
//
// Comparing an asset needs the books of every exchange, and books are keyed
// by market, so one instance reads all partitions (no consumer group). It
// starts at the end: a book older than MaxBookAge is useless anyway, so
// there is nothing to resume after a restart. A window open during a
// restart is lost; its ID comes from the book that opened it, so a second
// replica would publish the same windows under the same IDs.
type Service struct {
	cl      *kgo.Client
	eng     *Engine
	topics  config.Kafka
	books   *bookorder.Latest
	tick    time.Duration
	metrics *metrics.Arbitrage
	log     *slog.Logger

	lastRecord atomic.Int64 // unix nanos
}

func NewService(eng *Engine, k config.Kafka, tick time.Duration, m *metrics.Arbitrage, log *slog.Logger, opts ...kgo.Opt) (*Service, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(k.Brokers...),
		kgo.ConsumeTopics(k.TopicBooks),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.ProducerLinger(5 * time.Millisecond),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Service{cl: cl, eng: eng, topics: k, books: bookorder.NewLatest(), tick: tick, metrics: m, log: log}, nil
}

// Fresh fails when no book arrived for maxAge: the service would only be
// reporting stale venues.
func (s *Service) Fresh(maxAge time.Duration) error {
	last := s.lastRecord.Load()
	if last == 0 {
		return errors.New("no books from kafka yet")
	}
	if age := time.Since(time.Unix(0, last)); age > maxAge {
		return fmt.Errorf("no books for %s", age.Round(time.Second))
	}
	return nil
}

// Run evaluates books until ctx ends, then closes open windows and flushes.
func (s *Service) Run(ctx context.Context) error {
	lastTick := time.Now()
	fetchErrs := fetcherr.New(s.log, time.Minute)
	for {
		pollCtx, cancel := context.WithTimeout(ctx, s.tick)
		fetches := s.cl.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil {
			return s.shutdown()
		}
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		if err := fetchErrs.Check(fetches); err != nil {
			return err
		}
		fetches.EachRecord(s.handle)
		if now := time.Now(); now.Sub(lastTick) >= s.tick {
			s.publish(s.eng.Tick(now))
			lastTick = now
		}
	}
}

func (s *Service) handle(r *kgo.Record) {
	s.lastRecord.Store(time.Now().UnixNano())
	b, err := codec.DecodeBook(r.Value)
	if err != nil {
		s.log.Error("skip bad book", "offset", r.Offset, "err", err)
		return
	}
	if !s.books.Accept(b) {
		return // the other ingester's copy, or an older snapshot
	}
	start := time.Now()
	out := s.eng.Update(b, start)
	s.metrics.Evaluated(time.Since(start))
	s.publish(out)
}

func (s *Service) publish(out Output) {
	for _, snap := range out.Snapshots {
		s.metrics.Snapshot(snap)
		s.produce(s.topics.TopicArb, snap.Asset, func() ([]byte, error) { return codec.EncodeArbSnapshot(snap) })
	}
	s.windows(out.Windows)
	for _, a := range out.Alerts {
		s.metrics.Alert()
		s.log.Info("arbitrage", "id", a.ID, "message", a.Message)
		s.produce(s.topics.TopicAlerts, a.Exchange+":"+a.Symbol, func() ([]byte, error) { return codec.EncodeAlert(a) })
	}
}

func (s *Service) windows(ws []model.ArbWindow) {
	for _, w := range ws {
		s.metrics.Window(w.Asset, w.Duration())
		s.log.Debug("window closed", "id", w.ID, "duration", w.Duration(), "peak_net_bps", w.PeakNetBps)
		s.produce(s.topics.TopicArbWindows, w.Asset, func() ([]byte, error) { return codec.EncodeArbWindow(w) })
	}
}

func (s *Service) produce(topic, key string, encode func() ([]byte, error)) {
	v, err := encode()
	if err != nil {
		s.log.Error("encode", "topic", topic, "err", err)
		return
	}
	s.cl.Produce(context.Background(), &kgo.Record{Topic: topic, Key: []byte(key), Value: v}, func(r *kgo.Record, err error) {
		if err != nil {
			s.log.Error("produce failed", "topic", r.Topic, "err", err)
		}
	})
}

func (s *Service) shutdown() error {
	s.windows(s.eng.CloseAll(time.Now()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.cl.Flush(ctx)
}

func (s *Service) Close() { s.cl.Close() }
