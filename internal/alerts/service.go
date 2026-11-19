package alerts

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/dedup"
	"marketdata/internal/fetcherr"
	"marketdata/internal/metrics"
	"marketdata/internal/model"
)

// WhaleSettings gives per-market whale thresholds (*settings.Store).
type WhaleSettings interface {
	Whale(exchange, symbol string) decimal.Decimal
}

type Options struct {
	Kafka config.Kafka
	Group string
	// Whale is one threshold for every market, used only when Settings is
	// nil (tests).
	Whale          decimal.Decimal
	Settings       WhaleSettings
	MovePct        decimal.Decimal
	MoveWindow     time.Duration
	MoveCooldown   time.Duration
	MaxRules       int
	DedupWindow    int
	CommitInterval time.Duration
	Metrics        *metrics.Alerts
}

// Service evaluates trades against rules and detectors and publishes alerts.
//
// Rules are event-sourced: the compacted rules topic is the source of truth
// and the in-memory index is rebuilt from it on start. A fired rule is
// removed by writing a tombstone, and trade offsets are committed only after
// alerts and tombstones are flushed, so after a crash a rule either fires
// again with the same alert ID (deduplicable) or not at all.
type Service struct {
	opts Options
	log  *slog.Logger

	trades *kgo.Client // consumer group on trades; also the producer
	rules  *kgo.Client // reads the rules topic from the start, no group

	mu    sync.Mutex
	idx   *Index
	whale Whale
	move  *Move
	seen  *dedup.Window
}

func NewService(opts Options, log *slog.Logger, kopts ...kgo.Opt) (*Service, error) {
	if opts.Metrics == nil {
		opts.Metrics = metrics.NewAlerts(prometheus.NewRegistry())
	}
	s := &Service{
		opts: opts, log: log, idx: NewIndex(),
		whale: StaticWhale(opts.Whale),
		move:  &Move{Pct: opts.MovePct, Window: opts.MoveWindow, Cooldown: opts.MoveCooldown},
		seen:  dedup.NewWindow(opts.DedupWindow),
	}
	if opts.Settings != nil {
		s.whale = Whale{Threshold: opts.Settings.Whale}
	}
	opts.Metrics.WatchRules(func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.idx.Len()
	})
	var err error
	s.trades, err = kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(opts.Kafka.Brokers...),
		kgo.ConsumerGroup(opts.Group),
		kgo.ConsumeTopics(opts.Kafka.TopicTrades),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), // a new group alerts on live data only
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(s.onRevoked),
		kgo.ProducerLinger(2 * time.Millisecond),
	}, kopts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka trades client: %w", err)
	}
	s.rules, err = kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(opts.Kafka.Brokers...),
		kgo.ConsumeTopics(opts.Kafka.TopicRules),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, kopts...)...)
	if err != nil {
		s.trades.Close()
		return nil, fmt.Errorf("kafka rules client: %w", err)
	}
	return s, nil
}

func (s *Service) Close() {
	s.trades.AllowRebalance()
	s.trades.Close()
	s.rules.Close()
}

// Bootstrap loads every rule from the rules topic. Trades must not be
// evaluated before it returns, or rules would be missed.
func (s *Service) Bootstrap(ctx context.Context) error {
	adm := kadm.NewClient(s.rules)
	ends, err := adm.ListEndOffsets(ctx, s.opts.Kafka.TopicRules)
	if err != nil {
		return fmt.Errorf("list rules end offsets: %w", err)
	}
	until := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			until[o.Partition] = o.Offset
		}
	})
	for len(until) > 0 {
		fetches := s.rules.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		fetches.EachRecord(func(r *kgo.Record) {
			s.applyRule(r)
			if r.Offset+1 >= until[r.Partition] {
				delete(until, r.Partition)
			}
		})
	}
	s.mu.Lock()
	n := s.idx.Len()
	s.mu.Unlock()
	s.log.Info("rules loaded", "rules", n)
	return nil
}

// FollowRules applies rule changes until ctx ends. Run after Bootstrap.
func (s *Service) FollowRules(ctx context.Context) error {
	fetchErrs := fetcherr.New(s.log, time.Minute)
	for ctx.Err() == nil {
		fetches := s.rules.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		if err := fetchErrs.Check(fetches); err != nil {
			return fmt.Errorf("rules: %w", err)
		}
		fetches.EachRecord(s.applyRule)
	}
	return nil
}

func (s *Service) applyRule(r *kgo.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Value == nil { // tombstone: deleted or fired
		s.idx.Remove(string(r.Key))
		return
	}
	rule, err := codec.DecodeRule(r.Value)
	if err != nil {
		s.log.Error("skip bad rule", "offset", r.Offset, "err", err)
		return
	}
	if err := s.idx.Put(rule); err != nil {
		s.log.Error("skip invalid rule", "id", rule.ID, "err", err)
	}
}

// Run evaluates trades until ctx ends.
func (s *Service) Run(ctx context.Context) error {
	lastCommit := time.Now()
	fetchErrs := fetcherr.New(s.log, time.Minute)
	for {
		pollCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		fetches := s.trades.PollRecords(pollCtx, 10_000)
		cancel()
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		if err := fetchErrs.Check(fetches); err != nil {
			return fmt.Errorf("trades: %w", err)
		}
		fetches.EachRecord(s.evaluate)

		var err error
		if ctx.Err() != nil || time.Since(lastCommit) >= s.opts.CommitInterval {
			cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = s.commit(cctx)
			ccancel()
			lastCommit = time.Now()
		}
		s.trades.AllowRebalance()
		if ctx.Err() != nil {
			return err
		}
		if err != nil {
			s.log.Warn("commit failed", "err", err)
		}
	}
}

func (s *Service) evaluate(r *kgo.Record) {
	t, err := codec.DecodeTrade(r.Value)
	if err != nil {
		s.log.Error("skip bad trade", "offset", r.Offset, "err", err)
		return
	}
	start := time.Now()
	var alerts []model.Alert
	var firedRules []string

	s.mu.Lock()
	k := t.Exchange + ":" + t.Symbol
	if s.seen.Seen(k, t.TradeID) {
		s.mu.Unlock()
		return // copy from the second ingester
	}
	s.seen.Add(k, t.TradeID)
	s.idx.Match(t.Exchange, t.Symbol, t.Price, func(rule model.AlertRule) {
		firedRules = append(firedRules, rule.ID)
		alerts = append(alerts, ruleAlert(rule, t))
	})
	if !t.Snapshot { // replayed history must not look like a live whale or move
		if a, ok := s.whale.Check(t); ok {
			alerts = append(alerts, a)
		}
		if a, ok := s.move.Check(t); ok {
			alerts = append(alerts, a)
		}
	}
	s.mu.Unlock()
	s.opts.Metrics.Evaluated(time.Since(start))

	for _, a := range alerts {
		s.publish(a)
	}
	for _, id := range firedRules {
		s.produce(&kgo.Record{Topic: s.opts.Kafka.TopicRules, Key: []byte(id)}) // tombstone
	}
}

func ruleAlert(r model.AlertRule, t model.Trade) model.Alert {
	msg := fmt.Sprintf("%s %s is %s %s: traded at %s", t.Exchange, t.Symbol, r.Direction, r.Price, t.Price)
	if r.Note != "" {
		msg += " (" + r.Note + ")"
	}
	return model.Alert{
		ID: "rule:" + r.ID, Type: model.AlertPriceRule, Exchange: t.Exchange, Symbol: t.Symbol,
		Price: t.Price, Time: t.ExchangeTime, RuleID: r.ID, TradeID: t.TradeID, Message: msg,
	}
}

func (s *Service) publish(a model.Alert) {
	b, err := codec.EncodeAlert(a)
	if err != nil {
		s.log.Error("encode alert", "err", err)
		return
	}
	s.opts.Metrics.Fired(a.Type.String())
	s.log.Info("alert", "type", a.Type.String(), "id", a.ID, "message", a.Message)
	s.produce(&kgo.Record{Topic: s.opts.Kafka.TopicAlerts, Key: []byte(a.Exchange + ":" + a.Symbol), Value: b})
}

func (s *Service) produce(r *kgo.Record) {
	s.trades.Produce(context.Background(), r, func(r *kgo.Record, err error) {
		if err != nil {
			s.log.Error("produce failed", "topic", r.Topic, "err", err)
		}
	})
}

// commit flushes alerts and tombstones, then commits what was evaluated.
func (s *Service) commit(ctx context.Context) error {
	if err := s.trades.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return s.trades.CommitUncommittedOffsets(ctx)
}

func (s *Service) onRevoked(ctx context.Context, _ *kgo.Client, _ map[string][]int32) {
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.commit(cctx); err != nil {
		s.log.Warn("commit on revoke failed", "err", err)
	}
}

// --- rule management used by the HTTP API

var ErrTooManyRules = errors.New("rule limit reached")

// CreateRule persists the rule (synchronously, so a 201 means durable) and
// applies it locally right away for read-your-writes.
func (s *Service) CreateRule(ctx context.Context, r model.AlertRule) error {
	s.mu.Lock()
	full := s.idx.Len() >= s.opts.MaxRules
	s.mu.Unlock()
	if full {
		return ErrTooManyRules
	}
	b, err := codec.EncodeRule(r)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: s.opts.Kafka.TopicRules, Key: []byte(r.ID), Value: b}
	if err := s.trades.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("persist rule: %w", err)
	}
	s.applyRule(rec)
	return nil
}

// DeleteRule reports false if the rule does not exist (or already fired).
func (s *Service) DeleteRule(ctx context.Context, id string) (bool, error) {
	if _, ok := s.GetRule(id); !ok {
		return false, nil
	}
	rec := &kgo.Record{Topic: s.opts.Kafka.TopicRules, Key: []byte(id)}
	if err := s.trades.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return false, fmt.Errorf("delete rule: %w", err)
	}
	s.applyRule(rec)
	return true, nil
}

func (s *Service) GetRule(id string) (model.AlertRule, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idx.Get(id)
}

func (s *Service) ListRules(exchange, symbol string, limit int) []model.AlertRule {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idx.List(exchange, symbol, limit)
}
