package book

import (
	"time"

	"marketdata/internal/model"
)

const (
	// PublishEvery limits snapshots per market: books change hundreds of
	// times a second, clients and storage need ~10 frames a second.
	PublishEvery = 100 * time.Millisecond
	// PublishDepth is the number of levels per side in a published snapshot.
	PublishDepth = 50
)

// Tracker keeps the books of one exchange and decides when to publish a
// snapshot: right away if the last one is older than PublishEvery,
// otherwise on a later frame (any frame of the connection, heartbeats
// included, flushes pending books). Not safe for concurrent use.
type Tracker struct {
	exchange string
	books    map[string]*entry
}

type entry struct {
	book     *Book
	ready    bool // a snapshot has been applied
	dirty    bool
	lastPub  time.Time
	exTime   time.Time
	seq      int64
	received time.Time
}

func NewTracker(exchange string) *Tracker {
	return &Tracker{exchange: exchange, books: map[string]*entry{}}
}

// Book returns the book of symbol, creating it.
func (t *Tracker) Book(symbol string) *Book { return t.entry(symbol).book }

// Snapshot marks the book as rebuilt from a full snapshot.
func (t *Tracker) Snapshot(symbol string) { t.entry(symbol).ready = true }

// Ready reports whether a snapshot has been applied since the last reset.
func (t *Tracker) Ready(symbol string) bool { return t.entry(symbol).ready }

// Reset clears a book until the next snapshot (e.g. after a checksum error).
func (t *Tracker) Reset(symbol string) {
	e := t.entry(symbol)
	e.book.Reset()
	e.ready, e.dirty = false, false
}

// Forget drops a market (unsubscribed).
func (t *Tracker) Forget(symbol string) { delete(t.books, symbol) }

// Changed records an applied update and publishes if due.
func (t *Tracker) Changed(symbol string, exTime, received time.Time, seq int64, dst []model.Event) []model.Event {
	e := t.entry(symbol)
	e.dirty, e.exTime, e.received, e.seq = true, exTime, received, seq
	return t.publish(symbol, e, received, dst)
}

// Flush publishes books whose last change is waiting for the rate limit.
func (t *Tracker) Flush(now time.Time, dst []model.Event) []model.Event {
	for symbol, e := range t.books {
		dst = t.publish(symbol, e, now, dst)
	}
	return dst
}

func (t *Tracker) publish(symbol string, e *entry, now time.Time, dst []model.Event) []model.Event {
	if !e.dirty || !e.ready || now.Sub(e.lastPub) < PublishEvery || e.book.Crossed() {
		return dst
	}
	bids, asks := e.book.Top(PublishDepth)
	e.dirty, e.lastPub = false, now
	return append(dst, model.Book{
		Exchange: t.exchange, Symbol: symbol, Bids: bids, Asks: asks,
		ExchangeTime: e.exTime, ReceiveTime: e.received, Seq: e.seq,
	})
}

func (t *Tracker) entry(symbol string) *entry {
	e, ok := t.books[symbol]
	if !ok {
		e = &entry{book: New()}
		t.books[symbol] = e
	}
	return e
}
