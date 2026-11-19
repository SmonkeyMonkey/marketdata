// Package book maintains L2 order books (aggregated size per price level)
// from exchange snapshots and incremental updates.
package book

import (
	"slices"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

// priceScale turns prices into int64 keys (10 decimals covers every tick
// size we see, e.g. 0.00000001 BTC).
const priceScale = 10

func key(p decimal.Decimal) int64 { return p.Shift(priceScale).IntPart() }

// Book is one market's L2 book. Not safe for concurrent use.
type Book struct {
	bids side
	asks side
}

func New() *Book {
	return &Book{bids: newSide(true), asks: newSide(false)}
}

// side keeps price keys sorted so that the BEST price is LAST: almost all
// updates hit the top of the book, so inserts and deletes there move only a
// few elements instead of shifting the whole slice (Coinbase's BTC book has
// ~38k levels).
type side struct {
	bids   bool
	sizes  map[int64]model.BookLevel
	sorted []int64 // worst ... best
}

func newSide(bids bool) side { return side{bids: bids, sizes: map[int64]model.BookLevel{}} }

// cmp orders keys worst-to-best: bids ascending (best = highest last),
// asks descending (best = lowest last).
func (s *side) cmp(a, b int64) int {
	switch {
	case a == b:
		return 0
	case (a < b) == s.bids:
		return -1
	default:
		return 1
	}
}

func (s *side) set(price, size decimal.Decimal) {
	k := key(price)
	i, found := slices.BinarySearchFunc(s.sorted, k, s.cmp)
	if size.IsZero() {
		if found {
			s.sorted = slices.Delete(s.sorted, i, i+1)
			delete(s.sizes, k)
		}
		return
	}
	if !found {
		s.sorted = slices.Insert(s.sorted, i, k)
	}
	s.sizes[k] = model.BookLevel{Price: price, Size: size}
}

func (s *side) top(n int) []model.BookLevel {
	n = min(n, len(s.sorted))
	out := make([]model.BookLevel, n)
	for i := range n {
		out[i] = s.sizes[s.sorted[len(s.sorted)-1-i]]
	}
	return out
}

// truncate keeps the best depth levels.
func (s *side) truncate(depth int) {
	if extra := len(s.sorted) - depth; extra > 0 {
		for _, k := range s.sorted[:extra] {
			delete(s.sizes, k)
		}
		s.sorted = slices.Delete(s.sorted, 0, extra)
	}
}

func (s *side) reset() {
	clear(s.sizes)
	s.sorted = s.sorted[:0]
}

// SetBid sets a bid level; size 0 removes it.
func (b *Book) SetBid(price, size decimal.Decimal) { b.bids.set(price, size) }

// SetAsk sets an ask level; size 0 removes it.
func (b *Book) SetAsk(price, size decimal.Decimal) { b.asks.set(price, size) }

// Top returns up to n best levels per side, best first.
func (b *Book) Top(n int) (bids, asks []model.BookLevel) { return b.bids.top(n), b.asks.top(n) }

// Truncate keeps the best depth levels per side (exchanges such as Kraken
// require it: levels pushed out of the subscribed depth get no updates).
func (b *Book) Truncate(depth int) {
	b.bids.truncate(depth)
	b.asks.truncate(depth)
}

func (b *Book) Reset() {
	b.bids.reset()
	b.asks.reset()
}

// Len returns the number of levels per side.
func (b *Book) Len() (bids, asks int) { return len(b.bids.sorted), len(b.asks.sorted) }

// Crossed reports a best bid at or above the best ask: a broken book.
func (b *Book) Crossed() bool {
	if len(b.bids.sorted) == 0 || len(b.asks.sorted) == 0 {
		return false
	}
	return b.bids.sorted[len(b.bids.sorted)-1] >= b.asks.sorted[len(b.asks.sorted)-1]
}
