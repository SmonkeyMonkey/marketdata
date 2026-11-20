package candles

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/fetcherr"
	"marketdata/internal/metrics"
	"marketdata/internal/model"
)

type ServiceOptions struct {
	Kafka          config.Kafka
	Group          string
	Aggregator     Config
	CommitInterval time.Duration
	// TailRecords is how many of the latest records per partition of the
	// candles topic are read on (re)assignment to learn what was emitted.
	TailRecords int64
	Metrics     *metrics.Aggregator
}

// Service consumes trades, aggregates candles and publishes them.
//
// Delivery: candles are produced and flushed before input offsets are
// committed, and the committed offset never passes the first trade of an
// open candle. After a crash the open candles are rebuilt from Kafka and
// already published candles are recognised from the tail of the output topic,
// so every candle is published complete, normally exactly once.
type Service struct {
	opts ServiceOptions
	log  *slog.Logger
	cl   *kgo.Client

	// mu serialises the poll loop and rebalance callbacks.
	mu   sync.Mutex
	agg  *Aggregator
	last map[int32]int64 // last processed offset per partition
}

func NewService(opts ServiceOptions, log *slog.Logger, kopts ...kgo.Opt) (*Service, error) {
	if opts.Metrics == nil {
		opts.Metrics = metrics.NewAggregator(prometheus.NewRegistry())
	}
	s := &Service{opts: opts, log: log, agg: New(opts.Aggregator), last: make(map[int32]int64)}
	opts.Metrics.Watch(&s.agg.Stats)
	kopts = append([]kgo.Opt{
		kgo.SeedBrokers(opts.Kafka.Brokers...),
		kgo.ConsumerGroup(opts.Group),
		kgo.ConsumeTopics(opts.Kafka.TopicTrades),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(s.onAssigned),
		kgo.OnPartitionsRevoked(s.onRevoked),
		kgo.OnPartitionsLost(s.onLost),
		kgo.ProducerLinger(5 * time.Millisecond),
	}, kopts...)
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	s.cl = cl
	return s, nil
}

func (s *Service) Run(ctx context.Context) error {
	fetchErrs := fetcherr.New(s.log, time.Minute)
	lastCommit := time.Now()
	for {
		pollCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		fetches := s.cl.PollRecords(pollCtx, 10_000)
		cancel()
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		if err := fetchErrs.Check(fetches); err != nil {
			return err
		}

		s.mu.Lock()
		now := time.Now()
		s.opts.Metrics.Trades(fetches.NumRecords())
		fetches.EachRecord(func(r *kgo.Record) {
			t, err := codec.DecodeTrade(r.Value)
			if err != nil {
				s.opts.Metrics.DecodeError()
				s.log.Error("skip bad record", "partition", r.Partition, "offset", r.Offset, "err", err)
			} else {
				s.agg.Add(t, r.Partition, r.Offset, now, s.emit)
			}
			s.last[r.Partition] = r.Offset
		})
		s.agg.Tick(now, s.emit)

		var err error
		if ctx.Err() != nil || time.Since(lastCommit) >= s.opts.CommitInterval {
			cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = s.commitLocked(cctx)
			ccancel()
			lastCommit = time.Now()
		}
		s.mu.Unlock()
		s.cl.AllowRebalance()

		if ctx.Err() != nil {
			return err
		}
		if err != nil {
			s.log.Warn("commit failed", "err", err)
		}
	}
}

// Close leaves the group (committing via the revoke callback).
func (s *Service) Close() {
	s.cl.AllowRebalance()
	s.cl.Close()
}

func (s *Service) emit(c model.Candle) {
	value, err := codec.EncodeCandle(c)
	if err != nil {
		s.log.Error("encode candle", "err", err)
		return
	}
	s.opts.Metrics.Emitted(c)
	s.cl.Produce(context.Background(), &kgo.Record{
		Topic: s.opts.Kafka.TopicCandles,
		Key:   []byte(c.Exchange + ":" + c.Symbol),
		Value: value,
	}, func(r *kgo.Record, err error) {
		if err != nil {
			s.opts.Metrics.ProduceError()
			s.log.Error("candle produce failed", "key", string(r.Key), "err", err)
		}
	})
}

// commitLocked flushes produced candles, then commits for each partition
// either the next offset or the first offset still needed by an open candle.
func (s *Service) commitLocked(ctx context.Context) error {
	if err := s.cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush candles: %w", err)
	}
	if len(s.last) == 0 {
		return nil
	}
	safe := s.agg.SafeOffsets()
	offsets := make(map[int32]kgo.EpochOffset, len(s.last))
	for p, last := range s.last {
		off := last + 1
		if so, ok := safe[p]; ok && so < off {
			off = so
		}
		offsets[p] = kgo.EpochOffset{Epoch: -1, Offset: off}
	}
	var commitErr error
	s.cl.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{s.opts.Kafka.TopicTrades: offsets},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				commitErr = err
				return
			}
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if e := kerr.ErrorForCode(p.ErrorCode); e != nil && commitErr == nil {
						commitErr = e
					}
				}
			}
		})
	return commitErr
}

func (s *Service) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := LoadBoundaries(ctx, s.opts.Kafka.Brokers, s.opts.Kafka.TopicCandles, s.opts.TailRecords)
	if err != nil {
		// Without boundaries, replayed trades of already published candles
		// could produce partial duplicates. Failing loudly beats bad data.
		s.log.Error("load emitted candle boundaries failed", "err", err)
		panic(fmt.Sprintf("candles: cannot load boundaries: %v", err))
	}
	s.agg.Restore(b)
	s.log.Info("partitions assigned", "partitions", assigned[s.opts.Kafka.TopicTrades], "known_symbols", len(b))
}

func (s *Service) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.commitLocked(cctx); err != nil {
		s.log.Warn("commit on revoke failed", "err", err)
	}
	s.dropLocked(revoked[s.opts.Kafka.TopicTrades])
}

func (s *Service) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropLocked(lost[s.opts.Kafka.TopicTrades])
}

func (s *Service) dropLocked(partitions []int32) {
	set := make(map[int32]bool, len(partitions))
	for _, p := range partitions {
		set[p] = true
		delete(s.last, p)
	}
	s.agg.Drop(set)
}

// LoadBoundaries reads the newest records of every partition of the candles
// topic and returns, per symbol and interval, the newest candle start.
func LoadBoundaries(ctx context.Context, brokers []string, topic string, perPartition int64) (Boundaries, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}

	from := make(map[int32]kgo.Offset)
	until := make(map[int32]int64)
	var listErr error
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			listErr = o.Err
			return
		}
		start := o.Offset - perPartition
		if so, ok := starts.Lookup(topic, o.Partition); ok && so.Offset > start {
			start = so.Offset
		}
		if o.Offset > start {
			from[o.Partition] = kgo.NewOffset().At(start)
			until[o.Partition] = o.Offset
		}
	})
	if listErr != nil {
		return nil, listErr
	}

	b := Boundaries{}
	if len(from) == 0 {
		return b, nil
	}
	rc, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: from}))
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for len(until) > 0 {
		fetches := rc.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("reading %s tail: %w", topic, err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if c, err := codec.DecodeCandle(r.Value); err == nil {
				b.Observe(c)
			}
			if r.Offset+1 >= until[r.Partition] {
				delete(until, r.Partition)
			}
		})
	}
	return b, nil
}
