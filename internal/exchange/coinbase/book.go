package coinbase

import (
	"fmt"
	"slices"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

// bookSnapshot replaces a product's book (the full book: ~38k levels for
// BTC-USD, about 1 MB).
func (f *Feed) bookSnapshot(m message, received time.Time, dst []model.Event) []model.Event {
	if !slices.Contains(f.products, m.ProductID) {
		return dst
	}
	f.books.Reset(m.ProductID)
	b := f.books.Book(m.ProductID)
	for _, l := range m.Bids {
		b.SetBid(l[0], l[1])
	}
	for _, l := range m.Asks {
		b.SetAsk(l[0], l[1])
	}
	f.books.Snapshot(m.ProductID)
	return f.books.Changed(m.ProductID, m.Time, received, 0, dst)
}

// bookUpdate applies changes ["buy"|"sell", price, new size]; size 0
// removes the level.
func (f *Feed) bookUpdate(m message, received time.Time, dst []model.Event) ([]model.Event, error) {
	if !slices.Contains(f.products, m.ProductID) || !f.books.Ready(m.ProductID) {
		return dst, nil
	}
	b := f.books.Book(m.ProductID)
	for _, c := range m.Changes {
		price, err := decimal.NewFromString(c[1])
		if err != nil {
			return dst, fmt.Errorf("coinbase: l2update price %q: %w", c[1], err)
		}
		size, err := decimal.NewFromString(c[2])
		if err != nil {
			return dst, fmt.Errorf("coinbase: l2update size %q: %w", c[2], err)
		}
		switch c[0] {
		case "buy":
			b.SetBid(price, size)
		case "sell":
			b.SetAsk(price, size)
		default:
			return dst, fmt.Errorf("coinbase: l2update side %q", c[0])
		}
	}
	return f.books.Changed(m.ProductID, m.Time, received, 0, dst), nil
}
