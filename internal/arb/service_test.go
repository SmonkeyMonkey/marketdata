package arb

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
	"marketdata/internal/model"
)

// End to end over a fake Kafka: books from two "ingesters" in, a snapshot,
// an alert and (after shutdown) the closed window out, each exactly once.
func TestServiceOverKafka(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "md.books", "md.arb", "md.arb-windows", "md.alerts"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	k := config.Kafka{Brokers: c.ListenAddrs(), TopicBooks: "md.books", TopicArb: "md.arb", TopicArbWindows: "md.arb-windows", TopicAlerts: "md.alerts"}

	eng := engine(nil)
	eng.cfg.MaxBookAge = time.Minute
	svc, err := NewService(eng, k, 20*time.Millisecond, metrics.NewArbitrage(prometheus.NewRegistry()),
		slog.New(slog.NewTextHandler(io.Discard, nil)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	prod, err := kgo.NewClient(kgo.SeedBrokers(k.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	now := time.Now()
	for _, b := range []model.Book{
		book("kraken", "BTC-USD", now, levels("99:1"), levels("100:1")),
		book("binance", "BTC-USD", now, levels("100.2:1"), levels("101:1")),
	} {
		v, _ := codec.EncodeBook(b)
		for range 2 { // both ingesters publish every book
			if err := prod.ProduceSync(context.Background(), &kgo.Record{Topic: k.TopicBooks, Key: []byte(b.Exchange + ":" + b.Symbol), Value: v}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	// The engine belongs to Run's goroutine: wait for its output instead.
	deadline := time.Now().Add(5 * time.Second)
	for len(consume(t, k.Brokers, 100*time.Millisecond, k.TopicAlerts)[k.TopicAlerts]) == 0 && time.Now().Before(deadline) {
	}
	if svc.Fresh(time.Minute) != nil {
		t.Error("not fresh after reading books")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	got := consume(t, k.Brokers, time.Second, k.TopicArb, k.TopicArbWindows, k.TopicAlerts)
	if len(got[k.TopicArbWindows]) != 1 || len(got[k.TopicAlerts]) != 1 || len(got[k.TopicArb]) == 0 {
		t.Fatalf("windows %d alerts %d snapshots %d, want 1 / 1 / >0",
			len(got[k.TopicArbWindows]), len(got[k.TopicAlerts]), len(got[k.TopicArb]))
	}
	w, err := codec.DecodeArbWindow(got[k.TopicArbWindows][0])
	if err != nil || w.BuyExchange != "kraken" || w.SellExchange != "binance" || w.PeakNetBps.String() != "20" {
		t.Errorf("window = %+v %v", w, err)
	}
	a, _ := codec.DecodeAlert(got[k.TopicAlerts][0])
	if a.Type != model.AlertArbitrage || a.ID != "arb:"+w.ID {
		t.Errorf("alert = %+v", a)
	}
}

func consume(t *testing.T, brokers []string, wait time.Duration, topics ...string) map[string][][]byte {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	out := map[string][][]byte{}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	for ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { out[r.Topic] = append(out[r.Topic], r.Value) })
	}
	return out
}
