package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/config"
	"marketdata/internal/model"
	"marketdata/internal/publish"
)

type harness struct {
	t     *testing.T
	kafka config.Kafka
	svc   *Service
	api   *httptest.Server
	stop  func()
}

func newCluster(t *testing.T) config.Kafka {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1),
		kfake.SeedTopics(3, "md.trades", "md.gaps", "md.alerts"), kfake.SeedTopics(1, "md.alert-rules"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return config.Kafka{Brokers: c.ListenAddrs(), TopicTrades: "md.trades", TopicGaps: "md.gaps",
		TopicAlerts: "md.alerts", TopicRules: "md.alert-rules"}
}

func start(t *testing.T, k config.Kafka) *harness {
	t.Helper()
	svc, err := NewService(Options{
		Kafka: k, Group: "alerts", Whale: decimal.NewFromInt(100_000),
		MovePct: decimal.NewFromInt(50), MoveWindow: time.Minute, MaxRules: 100,
		DedupWindow: 1000, CommitInterval: 20 * time.Millisecond,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := svc.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	go svc.FollowRules(ctx)
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	api := httptest.NewServer(NewAPI(svc).Handler())
	h := &harness{t: t, kafka: k, svc: svc, api: api}
	h.stop = func() {
		api.Close()
		cancel()
		<-done
		svc.Close()
	}
	return h
}

func (h *harness) createRule(dir, price string) ruleResponse {
	h.t.Helper()
	body, _ := json.Marshal(createRequest{Exchange: "binance", Symbol: "BTC-USDT", Direction: dir, Price: price, Note: "test"})
	resp, err := http.Post(h.api.URL+"/v1/rules", "application/json", bytes.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		h.t.Fatalf("create: %d %s", resp.StatusCode, b)
	}
	var r ruleResponse
	_ = json.NewDecoder(resp.Body).Decode(&r)
	return r
}

func (h *harness) status(method, path string) int {
	h.t.Helper()
	req, _ := http.NewRequest(method, h.api.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func sendTrades(t *testing.T, k config.Kafka, trades ...model.Trade) {
	t.Helper()
	pub, err := publish.NewKafka(k, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range trades {
		_ = pub.Publish(context.Background(), []model.Event{tr, tr}) // two ingesters
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func trade(id string, price, size string) model.Trade {
	return model.Trade{Exchange: "binance", Symbol: "BTC-USDT", TradeID: id, TakerSide: model.SideBuy,
		Price: decimal.RequireFromString(price), Size: decimal.RequireFromString(size),
		ExchangeTime: time.Now().UTC(), ReceiveTime: time.Now().UTC()}
}

// readAlerts returns all alerts published so far (waits up to d for at least want).
func readAlerts(t *testing.T, k config.Kafka, want int, d time.Duration) []model.Alert {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(k.Brokers...), kgo.ConsumeTopics(k.TopicAlerts),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []model.Alert
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && (len(out) < want || want == 0) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			a, err := codec.DecodeAlert(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, a)
		})
		cancel()
	}
	return out
}

func TestRuleFiresOnceAndSurvivesRestart(t *testing.T) {
	k := newCluster(t)
	h := start(t, k)
	above := h.createRule("above", "70000")
	below := h.createRule("below", "60000")
	if above.Channel != "alerts:rule:"+above.ID {
		t.Errorf("channel = %s", above.Channel)
	}
	if h.status("GET", "/v1/rules/"+above.ID) != 200 {
		t.Fatal("rule not readable right after create")
	}

	sendTrades(t, k, trade("1", "65000", "0.1"), trade("2", "70000.5", "0.1"), trade("3", "71000", "0.1"))
	alerts := readAlerts(t, k, 1, 3*time.Second)
	var ruleAlerts []model.Alert
	for _, a := range alerts {
		if a.Type == model.AlertPriceRule {
			ruleAlerts = append(ruleAlerts, a)
		}
	}
	if len(ruleAlerts) != 1 || ruleAlerts[0].RuleID != above.ID || ruleAlerts[0].TradeID != "2" {
		t.Fatalf("rule alerts = %+v, want exactly one for the above rule at trade 2", ruleAlerts)
	}
	waitStatus(t, h, "/v1/rules/"+above.ID, 404) // fired rules are gone
	if h.status("GET", "/v1/rules/"+below.ID) != 200 {
		t.Error("untouched rule disappeared")
	}
	h.stop()

	// A new instance rebuilds rules from the compacted topic: the fired rule
	// was tombstoned, the other one is still there, and nothing fires twice.
	h2 := start(t, k)
	defer h2.stop()
	if h2.status("GET", "/v1/rules/"+above.ID) != 404 || h2.status("GET", "/v1/rules/"+below.ID) != 200 {
		t.Fatal("rules not restored correctly after restart")
	}
	sendTrades(t, k, trade("4", "72000", "0.1"))
	time.Sleep(300 * time.Millisecond)
	if n := countType(readAlerts(t, k, 0, 300*time.Millisecond), model.AlertPriceRule); n != 1 {
		t.Errorf("price rule alerts after restart = %d, want still 1", n)
	}

	if h2.status("DELETE", "/v1/rules/"+below.ID) != 204 || h2.status("DELETE", "/v1/rules/"+below.ID) != 404 {
		t.Error("delete should succeed once")
	}
}

func TestWhaleAlertAndValidation(t *testing.T) {
	k := newCluster(t)
	h := start(t, k)
	defer h.stop()
	sendTrades(t, k, trade("w1", "65000", "2")) // 130k notional, sent twice
	readAlerts(t, k, 1, 3*time.Second)
	time.Sleep(300 * time.Millisecond) // room for a wrong second alert to show up
	if n := countType(readAlerts(t, k, 0, 300*time.Millisecond), model.AlertWhaleTrade); n != 1 {
		t.Errorf("whale alerts = %d, want 1 (duplicate trade must not re-alert)", n)
	}

	for _, body := range []string{
		`{"exchange":"binance","symbol":"btcusdt","direction":"above","price":"1"}`,
		`{"exchange":"binance","symbol":"BTC-USDT","direction":"sideways","price":"1"}`,
		`{"exchange":"binance","symbol":"BTC-USDT","direction":"above","price":"-5"}`,
		`not json`,
	} {
		resp, err := http.Post(h.api.URL+"/v1/rules", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s -> %d, want 400", body, resp.StatusCode)
		}
	}
}

func countType(alerts []model.Alert, typ model.AlertType) int {
	n := 0
	for _, a := range alerts {
		if a.Type == typ {
			n++
		}
	}
	return n
}

func waitStatus(t *testing.T, h *harness, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for h.status("GET", path) != want {
		if time.Now().After(deadline) {
			t.Fatalf("GET %s never returned %d", path, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
