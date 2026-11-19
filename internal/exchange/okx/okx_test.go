package okx

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const url = "wss://ws.okx.com:443/ws/v5/public"

var t0 = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

func newFeed(t *testing.T, symbols ...string) *Feed {
	t.Helper()
	f, err := New(url, symbols)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func session(t *testing.T) [][]byte {
	t.Helper()
	fh, err := os.Open(filepath.Join("testdata", "session.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	var out [][]byte
	sc := bufio.NewScanner(fh)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		out = append(out, append([]byte(nil), sc.Bytes()...))
	}
	return out
}

func replay(t *testing.T, f *Feed, frames [][]byte) []model.Event {
	t.Helper()
	var all []model.Event
	for i, fr := range frames {
		ev, err := f.Handle(fr, t0.Add(time.Duration(i)*50*time.Millisecond), nil)
		if err != nil {
			t.Fatalf("frame %d (%.80s): %v", i, fr, err)
		}
		all = append(all, ev...)
	}
	return all
}

// The captured session has aggregated trades (count 2): their IDs jump by
// the count, which must not be reported as missed trades.
func TestRealSessionHasNoGapsOrResyncs(t *testing.T) {
	f := newFeed(t, "BTC-USDT")
	var trades, books int
	var last model.Book
	for _, e := range replay(t, f, session(t)) {
		switch ev := e.(type) {
		case model.Trade:
			trades++
			if ev.Symbol != "BTC-USDT" || ev.TakerSide == model.SideUnknown || !ev.Size.IsPositive() || ev.ExchangeTime.Year() < 2026 {
				t.Errorf("trade = %+v", ev)
			}
		case model.Book:
			books++
			last = ev
		default:
			t.Errorf("unexpected %T %+v (gap or resync on a clean session)", e, e)
		}
	}
	if trades == 0 || books == 0 {
		t.Fatalf("trades %d books %d", trades, books)
	}
	if !last.Bids[0].Price.LessThan(last.Asks[0].Price) || last.Seq == 0 || len(last.Asks) == 0 {
		t.Errorf("last book: best %s/%s seq %d", last.Bids[0].Price, last.Asks[0].Price, last.Seq)
	}
}

func TestAggregatedTradeGap(t *testing.T) {
	f := newFeed(t, "BTC-USDT")
	tr := func(id, count string) []byte {
		return []byte(`{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"` + id +
			`","px":"1","sz":"1","side":"buy","ts":"1795100029796","count":"` + count + `"}]}`)
	}
	ev := replay(t, f, [][]byte{tr("100", "1"), tr("103", "3"), tr("103", "3"), tr("110", "2")})
	var kinds []string
	for _, e := range ev {
		kinds = append(kinds, e.Kind())
	}
	// 101..103 follows 100; the repeat is dropped; 109..110 after 103 misses 104..108.
	if strings.Join(kinds, ",") != "trade,trade,gap,trade" {
		t.Fatalf("events = %v", kinds)
	}
	if g := ev[2].(model.Gap); g.Missing != 5 {
		t.Errorf("gap = %+v, want 5 missing", g)
	}
}

func TestErrorsAndPong(t *testing.T) {
	f := newFeed(t, "BTC-USDT")
	if ev, err := f.Handle([]byte("pong"), t0, nil); err != nil || len(ev) != 0 {
		t.Errorf("pong: %v %v", ev, err)
	}
	b, _ := os.ReadFile(filepath.Join("testdata", "subscribe_error.json"))
	_, err := f.Handle(b, t0, nil)
	var se *exchange.SymbolError
	if !errors.As(err, &se) || se.Symbol != "FOOBAR-USDT" {
		t.Errorf("err = %v, want SymbolError for FOOBAR-USDT", err)
	}
	if _, err := f.Handle([]byte(`{"event":"notice","msg":"port retired","code":"64010"}`), t0, nil); err != nil {
		t.Errorf("notice ended the session: %v", err)
	}
}

func bookFrame(action string, seq, prev int64, bid string) []byte {
	return []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"` + action + `","data":[{"asks":[["70000","1","0","1"]],"bids":[["` +
		bid + `","1","0","1"]],"ts":"1795100055909","checksum":0,"seqId":` + itoa(seq) + `,"prevSeqId":` + itoa(prev) + `}]}`)
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestSequenceBreakResyncs(t *testing.T) {
	f := newFeed(t, "BTC-USDT")
	replay(t, f, [][]byte{bookFrame("snapshot", 10, -1, "69000"), bookFrame("update", 11, 10, "69001"), bookFrame("update", 11, 11, "69001")})
	if len(f.Outbox()) != 0 {
		t.Fatal("resync on a contiguous sequence (incl. a repeat without changes)")
	}
	ev, _ := f.Handle(bookFrame("update", 15, 13, "69002"), t0.Add(time.Second), nil)
	if len(ev) != 1 || ev[0].Kind() != "book_resync" {
		t.Fatalf("events = %+v", ev)
	}
	if out := f.Outbox(); len(out) != 2 || !strings.Contains(string(out[0]), "unsubscribe") || !strings.Contains(string(out[1]), `"books"`) {
		t.Errorf("outbox = %q", out)
	}
}

func TestSetSymbols(t *testing.T) {
	f := newFeed(t, "BTC-USDT")
	frames, err := f.SetSymbols([]string{"BTC-USDT", "SOL-USDC"})
	if err != nil || len(frames) != 1 || !strings.Contains(string(frames[0]), `"instId":"SOL-USDC"`) {
		t.Fatalf("frames = %q %v", frames, err)
	}
	frames, _ = f.SetSymbols([]string{"SOL-USDC"})
	if len(frames) != 1 || !strings.Contains(string(frames[0]), `"unsubscribe"`) {
		t.Errorf("frames = %q", frames)
	}
	ev, _ := f.Handle([]byte(`{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"1","sz":"1","side":"buy","ts":"1","count":"1"}]}`), t0, nil)
	if len(ev) != 0 {
		t.Errorf("trade of an unsubscribed symbol: %+v", ev)
	}
}
