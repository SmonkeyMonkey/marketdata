package book

import (
	"testing"
	"time"

	"marketdata/internal/model"
)

// Both ingesters publish the same books; a consumer must never accept an
// older snapshot after a newer one.
func TestLatestOnlyMovesForward(t *testing.T) {
	l := NewLatest()
	t0 := time.Unix(1000, 0)
	kraken := func(at time.Duration) model.Book {
		return model.Book{Exchange: "kraken", Symbol: "BTC-USD", ExchangeTime: t0.Add(at)}
	}
	binance := func(seq int64, at time.Duration) model.Book {
		return model.Book{Exchange: "binance", Symbol: "BTC-USDT", Seq: seq, ExchangeTime: t0.Add(at)}
	}
	for i, c := range []struct {
		b    model.Book
		want bool
	}{
		{kraken(0), true},
		{kraken(0), false},                 // the other ingester's copy
		{kraken(-time.Millisecond), false}, // older
		{kraken(time.Millisecond), true},
		{binance(10, 0), true},
		{binance(9, time.Second), false}, // older update id wins over a later time
		{binance(11, -time.Second), true},
		{binance(3, 10*time.Second), true}, // sequence restarted by the exchange
		{binance(2, 11*time.Second), false},
	} {
		if got := l.Accept(c.b); got != c.want {
			t.Errorf("case %d: Accept = %v, want %v", i, got, c.want)
		}
	}
}
