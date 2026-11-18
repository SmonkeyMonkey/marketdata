package writer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/backoff"
	"marketdata/internal/config"
	"marketdata/internal/model"
	"marketdata/internal/publish"
)

const group = "test-writer"

var topics = config.Kafka{TopicTrades: "md.trades", TopicGaps: "md.gaps"}

type fakeStore struct {
	mu       sync.Mutex
	trades   []model.Trade
	gaps     []model.Gap
	failNext int // number of InsertTrades calls to fail
	calls    int
}

func (s *fakeStore) InsertTrades(_ context.Context, t []model.Trade) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failNext > 0 {
		s.failNext--
		return errors.New("clickhouse unavailable")
	}
	s.trades = append(s.trades, t...)
	return nil
}

func (s *fakeStore) InsertGaps(_ context.Context, g []model.Gap) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gaps = append(s.gaps, g...)
	return nil
}

func (s *fakeStore) InsertCandles(context.Context, []model.Candle) error { return nil }
func (s *fakeStore) InsertAlerts(context.Context, []model.Alert) error   { return nil }

func (s *fakeStore) counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.trades), len(s.gaps), s.calls
}

func setup(t *testing.T) (brokers []string) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topics.TopicTrades, topics.TopicGaps))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

// produce publishes n trades and one gap through the real ingester publisher.
func produce(t *testing.T, cfg config.Kafka, n int) {
	t.Helper()
	pub, err := publish.NewKafka(cfg, discard())
	if err != nil {
		t.Fatal(err)
	}
	events := make([]model.Event, 0, n+1)
	for i := range n {
		events = append(events, model.Trade{
			Exchange: "coinbase", Symbol: "BTC-USD", TradeID: strconv.Itoa(i),
			Price: decimal.RequireFromString("65000.1"), Size: decimal.RequireFromString("0.01"),
			TakerSide: model.SideBuy, ExchangeTime: time.Now().UTC(), ReceiveTime: time.Now().UTC(),
		})
	}
	events = append(events, model.Gap{Exchange: "coinbase", Symbol: "BTC-USD", AfterID: "1", BeforeID: "5", Missing: 3})
	if err := pub.Publish(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if pub.Failed() != 0 {
		t.Fatalf("%d records failed", pub.Failed())
	}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func start(t *testing.T, cfg config.Kafka, store Store) (stop func() error) {
	t.Helper()
	w, err := New(store, Options{
		Kafka: cfg, Group: group, BatchSize: 100, FlushInterval: 50 * time.Millisecond,
		Retry: backoff.Constant(10 * time.Millisecond),
	}, discard())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	return func() error {
		cancel()
		err := <-done
		w.Close()
		return err
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func committed(t *testing.T, brokers []string) int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	offs, err := kadm.NewClient(cl).FetchOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	offs.Each(func(o kadm.OffsetResponse) { total += o.At })
	return total
}

func TestWritesAllEventsAndCommits(t *testing.T) {
	brokers := setup(t)
	cfg := topics
	cfg.Brokers = brokers
	produce(t, cfg, 250)

	store := &fakeStore{}
	stop := start(t, cfg, store)
	eventually(t, func() bool { tr, g, _ := store.counts(); return tr == 250 && g == 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := committed(t, brokers); got != 251 {
		t.Errorf("committed offsets sum = %d, want 251", got)
	}
}

func TestRetriesFailedInsertWithoutLosingData(t *testing.T) {
	brokers := setup(t)
	cfg := topics
	cfg.Brokers = brokers
	produce(t, cfg, 50)

	store := &fakeStore{failNext: 3}
	stop := start(t, cfg, store)
	eventually(t, func() bool { tr, _, _ := store.counts(); return tr == 50 })
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, _, calls := store.counts(); calls < 4 {
		t.Errorf("InsertTrades calls = %d, want >= 4 (3 failures + success)", calls)
	}
}

// Two ingesters publish the same trades; each must be stored exactly once.
func TestDropsCopiesFromRedundantIngesters(t *testing.T) {
	brokers := setup(t)
	cfg := topics
	cfg.Brokers = brokers
	produce(t, cfg, 120) // ingester A
	produce(t, cfg, 120) // ingester B, same trade IDs

	store := &fakeStore{}
	stop := start(t, cfg, store)
	eventually(t, func() bool { _, g, _ := store.counts(); return g == 2 })
	time.Sleep(200 * time.Millisecond)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if tr, _, _ := store.counts(); tr != 120 {
		t.Errorf("stored %d trades, want 120", tr)
	}
	if got := committed(t, brokers); got != 242 {
		t.Errorf("committed %d records, want all 242 (duplicates are committed too)", got)
	}
}

// A restarted writer must continue from committed offsets, not re-read everything.
func TestResumesFromCommittedOffsets(t *testing.T) {
	brokers := setup(t)
	cfg := topics
	cfg.Brokers = brokers
	produce(t, cfg, 30)

	first := &fakeStore{}
	stop := start(t, cfg, first)
	eventually(t, func() bool { tr, _, _ := first.counts(); return tr == 30 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	produce(t, cfg, 20)
	second := &fakeStore{}
	stop = start(t, cfg, second)
	eventually(t, func() bool { tr, _, _ := second.counts(); return tr == 20 })
	time.Sleep(200 * time.Millisecond) // give it a chance to over-read
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if tr, _, _ := second.counts(); tr != 20 {
		t.Errorf("second run stored %d trades, want 20", tr)
	}
}
