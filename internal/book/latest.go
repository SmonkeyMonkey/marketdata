package book

import (
	"time"

	"marketdata/internal/model"
)

// Latest tells whether a book snapshot is newer than the last one accepted
// for its market. Both ingesters publish the same books, so a consumer sees
// most snapshots twice and may get an older one after a newer one.
type Latest struct {
	marks map[string]mark
}

type mark struct {
	seq int64
	at  time.Time
}

func NewLatest() *Latest { return &Latest{marks: map[string]mark{}} }

// seqRestart: a snapshot this much newer by exchange time is accepted even
// with a lower update id, because the exchange restarted its sequence
// (OKX and Bybit do after maintenance). Copies of the same snapshot from
// the two ingesters are milliseconds apart.
const seqRestart = 5 * time.Second

// Accept records b and reports true when it is newer: by exchange update id
// when both snapshots have one, else by exchange time.
func (l *Latest) Accept(b model.Book) bool {
	key := b.Exchange + ":" + b.Symbol
	prev, ok := l.marks[key]
	newer := !ok
	if ok {
		if b.Seq > 0 && prev.seq > 0 {
			newer = b.Seq > prev.seq || b.ExchangeTime.Sub(prev.at) > seqRestart
		} else {
			newer = b.ExchangeTime.After(prev.at)
		}
	}
	if newer {
		l.marks[key] = mark{seq: b.Seq, at: b.ExchangeTime}
	}
	return newer
}
