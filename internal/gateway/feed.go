package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/book"
	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/dedup"
	"marketdata/internal/fetcherr"
	"marketdata/internal/metrics"
)

// Feed reads trades, candles, books, alerts and arbitrage snapshots from
// Kafka and publishes them to the hub.
//
// Every gateway instance must see every message (its clients may subscribe
// to anything), so this is a plain consumer of all partitions, not a member
// of a consumer group. It starts at the end: clients get live data only.
type Feed struct {
	cl      *kgo.Client
	hub     *Hub
	topics  config.Kafka
	seen    *dedup.Window
	metrics *metrics.Gateway
	log     *slog.Logger

	lastRecord atomic.Int64 // unix nanos of the last record read from Kafka

	// books orders book snapshots per market: both ingesters publish the
	// same books, and a client must never see an older one after a newer.
	books *book.Latest
}

// Fresh reports an error when nothing was read from Kafka for maxAge: the
// gateway would be serving silence, so it should be taken out of rotation.
func (f *Feed) Fresh(maxAge time.Duration) error {
	last := f.lastRecord.Load()
	if last == 0 {
		return errors.New("no data from kafka yet")
	}
	if age := time.Since(time.Unix(0, last)); age > maxAge {
		return fmt.Errorf("no data from kafka for %s", age.Round(time.Second))
	}
	return nil
}

func NewFeed(hub *Hub, k config.Kafka, dedupWindow int, m *metrics.Gateway, log *slog.Logger, opts ...kgo.Opt) (*Feed, error) {
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(k.Brokers...),
		kgo.ConsumeTopics(k.TopicTrades, k.TopicCandles, k.TopicAlerts, k.TopicBooks, k.TopicArb),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Feed{cl: cl, hub: hub, topics: k, seen: dedup.NewWindow(dedupWindow), metrics: m, log: log, books: book.NewLatest()}, nil
}

func (f *Feed) Close() { f.cl.Close() }

func (f *Feed) Run(ctx context.Context) error {
	fetchErrs := fetcherr.New(f.log, time.Minute)
	for {
		fetches := f.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		if err := fetchErrs.Check(fetches); err != nil {
			return err
		}
		fetches.EachRecord(f.handle)
	}
}

func (f *Feed) handle(r *kgo.Record) {
	now := time.Now()
	f.lastRecord.Store(now.UnixNano())
	f.metrics.FeedRecord(now)
	var (
		channel string
		msg     []byte
		err     error
		kind    string
	)
	switch r.Topic {
	case f.topics.TopicTrades:
		t, derr := codec.DecodeTrade(r.Value)
		if derr != nil {
			err = derr
			break
		}
		key := t.Exchange + ":" + t.Symbol
		if f.seen.Seen(key, t.TradeID) {
			return // copy from the second ingester
		}
		f.seen.Add(key, t.TradeID)
		kind, channel = "trade", TradesChannel(t.Exchange, t.Symbol)
		msg, err = encodeTrade(t, now)
		if !t.Snapshot && !t.ReceiveTime.IsZero() {
			f.metrics.IngestLag(now.Sub(t.ReceiveTime))
		}
	case f.topics.TopicCandles:
		c, derr := codec.DecodeCandle(r.Value)
		if derr != nil {
			err = derr
			break
		}
		kind, channel = "candle", CandlesChannel(c)
		msg, err = encodeCandle(c, now)
	case f.topics.TopicBooks:
		b, derr := codec.DecodeBook(r.Value)
		if derr != nil {
			err = derr
			break
		}
		if !f.books.Accept(b) {
			return // the other ingester's copy, or an older snapshot
		}
		kind, channel = "book", BookChannel(b.Exchange, b.Symbol)
		msg, err = encodeBook(b, now)
	case f.topics.TopicArb:
		s, derr := codec.DecodeArbSnapshot(r.Value)
		if derr != nil {
			err = derr
			break
		}
		kind, channel = "arb", ArbChannel(s.Asset)
		msg, err = encodeArb(s, now)
	case f.topics.TopicAlerts:
		a, derr := codec.DecodeAlert(r.Value)
		if derr != nil {
			err = derr
			break
		}
		// An alert goes to its symbol's channel and, for user rules, to the
		// rule's own channel. Each channel gets its own encoding.
		if a.RuleID != "" {
			if b, err := encodeAlert(a, RuleChannel(a.RuleID), now); err == nil {
				f.metrics.Published("alert", f.hub.Publish(RuleChannel(a.RuleID), b), 0)
			}
		}
		kind, channel = "alert", AlertsChannel(a.Exchange, a.Symbol)
		msg, err = encodeAlert(a, channel, now)
	default:
		return
	}
	if err != nil {
		f.log.Error("skip bad record", "topic", r.Topic, "offset", r.Offset, "err", err)
		return
	}

	start := time.Now()
	n := f.hub.Publish(channel, msg)
	f.metrics.Published(kind, n, time.Since(start))
}
