package kraken

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"slices"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

// resyncEvery limits book resubscriptions per pair: if checksums kept
// failing (a bug on our side), resubscribing on every update would flood
// Kraken and could get the connection rate-limited or banned.
const resyncEvery = 30 * time.Second

// bookDepth is the subscribed depth. Kraken only sends updates within it,
// so the local book must be truncated to it after every update.
const bookDepth = 100

type precision struct{ price, qty int32 }

type bookMsg struct {
	Symbol    string      `json:"symbol"`
	Bids      []bookLevel `json:"bids"`
	Asks      []bookLevel `json:"asks"`
	Checksum  uint32      `json:"checksum"`
	Timestamp time.Time   `json:"timestamp"`
}

type bookLevel struct {
	Price decimal.Decimal `json:"price"`
	Qty   decimal.Decimal `json:"qty"`
}

func (f *Feed) handleInstrument(m message) error {
	var data struct {
		Pairs []struct {
			Symbol         string `json:"symbol"`
			PricePrecision int32  `json:"price_precision"`
			QtyPrecision   int32  `json:"qty_precision"`
		} `json:"pairs"`
	}
	if err := json.Unmarshal(m.Data, &data); err != nil {
		return fmt.Errorf("kraken: decode instruments: %w", err)
	}
	for _, p := range data.Pairs {
		f.precision[p.Symbol] = precision{price: p.PricePrecision, qty: p.QtyPrecision}
	}
	return nil
}

// handleBook applies a snapshot or update and verifies Kraken's CRC32 of the
// top 10 levels. On a mismatch the local book has diverged (a missed or
// misapplied update): it is reset and resubscribed for a fresh snapshot.
func (f *Feed) handleBook(m message, received time.Time, dst []model.Event) ([]model.Event, error) {
	var books []bookMsg
	if err := json.Unmarshal(m.Data, &books); err != nil {
		return dst, fmt.Errorf("kraken: decode book: %w", err)
	}
	for _, d := range books {
		if !slices.Contains(f.symbols, d.Symbol) {
			continue // in flight when we unsubscribed
		}
		symbol := canonical(d.Symbol)
		if m.Type == "snapshot" {
			f.books.Reset(symbol)
		} else if !f.books.Ready(symbol) {
			continue // waiting for the snapshot after a resync
		}
		b := f.books.Book(symbol)
		for _, l := range d.Bids {
			b.SetBid(l.Price, l.Qty)
		}
		for _, l := range d.Asks {
			b.SetAsk(l.Price, l.Qty)
		}
		b.Truncate(bookDepth)
		if m.Type == "snapshot" {
			f.books.Snapshot(symbol)
		}

		if p, ok := f.precision[d.Symbol]; ok && f.checksum(symbol, p) != d.Checksum {
			f.books.Reset(symbol)
			f.requestResync(d.Symbol, received)
			dst = append(dst, model.BookResync{Exchange: Name, Symbol: symbol, Reason: "checksum mismatch"})
			continue
		}
		dst = f.books.Changed(symbol, d.Timestamp, received, 0, dst)
	}
	return dst, nil
}

// checksum is Kraken's book CRC32: top 10 asks (best first) then top 10
// bids, each as price then quantity formatted to the pair's precision with
// the decimal point and leading zeros removed.
func (f *Feed) checksum(symbol string, p precision) uint32 {
	bids, asks := f.books.Book(symbol).Top(10)
	var sb strings.Builder
	for _, l := range asks {
		sb.WriteString(checksumNumber(l.Price, p.price))
		sb.WriteString(checksumNumber(l.Size, p.qty))
	}
	for _, l := range bids {
		sb.WriteString(checksumNumber(l.Price, p.price))
		sb.WriteString(checksumNumber(l.Size, p.qty))
	}
	return crc32.ChecksumIEEE([]byte(sb.String()))
}

func checksumNumber(d decimal.Decimal, places int32) string {
	return strings.TrimLeft(strings.Replace(d.StringFixed(places), ".", "", 1), "0")
}

// requestResync resubscribes the book now, or once resyncEvery has passed
// since the last resubscription of that pair.
func (f *Feed) requestResync(native string, now time.Time) {
	if f.resyncs.Request(native, now) {
		f.resubscribe(native)
	}
}

func (f *Feed) retryResyncs(now time.Time) {
	for _, native := range f.resyncs.Due(now) {
		f.resubscribe(native)
	}
}

func (f *Feed) resubscribe(native string) {
	for _, method := range []string{"unsubscribe", "subscribe"} {
		b, _ := json.Marshal(map[string]any{"method": method, "params": map[string]any{
			"channel": "book", "symbol": []string{native}, "depth": bookDepth,
		}})
		f.outbox = append(f.outbox, b)
	}
}
