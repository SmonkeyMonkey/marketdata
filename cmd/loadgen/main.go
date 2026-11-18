// Command loadgen opens many websocket connections to the gateway, subscribes
// them to channels and reports throughput and fan-out latency (gateway send
// time "ts" -> received here; both clocks are the same host).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

const maxLatencyMs = 10_000

type stats struct {
	open, dialed, dialFailed, dropped atomic.Int64
	msgs, bytes                       atomic.Int64
	latency                           [maxLatencyMs + 1]atomic.Int64 // ms buckets, last one = overflow

	mu         sync.Mutex
	closeCodes map[string]int
}

func main() {
	url := flag.String("url", "ws://localhost:8090/ws", "gateway websocket URL")
	conns := flag.Int("conns", 1000, "connections to open")
	rate := flag.Int("rate", 500, "new connections per second")
	channels := flag.String("channels",
		"candles:binance:BTC-USDT:1s,candles:binance:ETH-USDT:1s,candles:coinbase:BTC-USD:1s,candles:coinbase:ETH-USD:1s,candles:kraken:BTC-USD:1s,candles:kraken:ETH-USD:1s",
		"comma-separated channels every connection subscribes to")
	tradeChannels := flag.String("trade-channels",
		"trades:binance:BTC-USDT,trades:binance:ETH-USDT,trades:coinbase:BTC-USD,trades:coinbase:ETH-USD,trades:kraken:BTC-USD,trades:kraken:ETH-USD",
		"trade channels; a share of connections (see -trades) subscribes to one at random")
	tradesShare := flag.Float64("trades", 0.1, "share of connections that also subscribe to one trade channel (0..1)")
	duration := flag.Duration("duration", 2*time.Minute, "how long to hold connections after ramp-up")
	report := flag.Duration("report", 5*time.Second, "report interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := &stats{closeCodes: map[string]int{}}
	base := strings.Split(*channels, ",")
	trades := strings.Split(*tradeChannels, ",")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	go reporter(runCtx, st, *report)

	start := time.Now()
	tick := time.NewTicker(time.Second / time.Duration(max(*rate, 1)))
	for i := 0; i < *conns && runCtx.Err() == nil; i++ {
		<-tick.C
		subs := base
		if rand.Float64() < *tradesShare {
			subs = append(append([]string{}, base...), trades[rand.IntN(len(trades))])
		}
		wg.Go(func() { client(runCtx, *url, subs, st) })
	}
	tick.Stop()
	fmt.Printf("ramp-up done: %d connections requested in %s\n", *conns, time.Since(start).Round(time.Millisecond))

	select {
	case <-runCtx.Done():
	case <-time.After(*duration):
	}
	cancel()
	wg.Wait()
	fmt.Println("final:")
	printWindow(st, snapshot(st), time.Since(start))
}

func client(ctx context.Context, url string, channels []string, st *stats) {
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	c, _, err := websocket.Dial(dctx, url, &websocket.DialOptions{CompressionMode: websocket.CompressionDisabled})
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			st.dialFailed.Add(1)
		}
		return
	}
	c.SetReadLimit(1 << 20)
	st.dialed.Add(1)
	st.open.Add(1)
	defer st.open.Add(-1)

	sub, _ := json.Marshal(map[string]any{"op": "subscribe", "channels": channels})
	if err := c.Write(ctx, websocket.MessageText, sub); err != nil {
		c.CloseNow()
		return
	}
	// The first message per channel is the gateway's cached snapshot, whose
	// ts is when it was originally sent; leave those out of latency.
	measureFrom := time.Now().Add(time.Second).UnixMilli()
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.Close(websocket.StatusNormalClosure, "")
				return
			}
			st.dropped.Add(1)
			st.mu.Lock()
			st.closeCodes[websocket.CloseStatus(err).String()]++
			st.mu.Unlock()
			return
		}
		now := time.Now().UnixMilli()
		st.msgs.Add(1)
		st.bytes.Add(int64(len(data)))
		if ts, ok := extractTS(data); ok && now >= measureFrom {
			ms := min(max(now-ts, 0), maxLatencyMs)
			st.latency[ms].Add(1)
		}
	}
}

var tsKey = []byte(`"ts":`)

// extractTS finds "ts":<digits> without decoding the whole message.
func extractTS(b []byte) (int64, bool) {
	i := bytes.Index(b, tsKey)
	if i < 0 {
		return 0, false
	}
	b = b[i+len(tsKey):]
	j := 0
	for j < len(b) && b[j] >= '0' && b[j] <= '9' {
		j++
	}
	v, err := strconv.ParseInt(string(b[:j]), 10, 64)
	return v, err == nil
}

type window struct {
	msgs, bytes int64
	hist        [maxLatencyMs + 1]int64
}

// snapshot reads and resets the per-window counters.
func snapshot(st *stats) window {
	w := window{msgs: st.msgs.Swap(0), bytes: st.bytes.Swap(0)}
	for i := range st.latency {
		w.hist[i] = st.latency[i].Swap(0)
	}
	return w
}

func reporter(ctx context.Context, st *stats, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			printWindow(st, snapshot(st), now.Sub(last))
			last = now
		}
	}
}

func printWindow(st *stats, w window, d time.Duration) {
	secs := d.Seconds()
	st.mu.Lock()
	codes := fmt.Sprint(st.closeCodes)
	st.mu.Unlock()
	fmt.Printf("open=%d dialed=%d dial_failed=%d dropped=%d %s | %.0f msg/s %.2f MB/s | latency p50=%s p99=%s p999=%s max=%s\n",
		st.open.Load(), st.dialed.Load(), st.dialFailed.Load(), st.dropped.Load(), codes,
		float64(w.msgs)/secs, float64(w.bytes)/secs/1e6,
		pct(w.hist, 0.50), pct(w.hist, 0.99), pct(w.hist, 0.999), pct(w.hist, 1))
}

func pct(h [maxLatencyMs + 1]int64, q float64) string {
	var total int64
	for _, n := range h {
		total += n
	}
	if total == 0 {
		return "-"
	}
	target := int64(float64(total)*q + 0.5)
	if target < 1 {
		target = 1
	}
	var acc int64
	for ms, n := range h {
		acc += n
		if acc >= target {
			if ms == maxLatencyMs {
				return ">10s"
			}
			return fmt.Sprintf("%dms", ms)
		}
	}
	return "-"
}
