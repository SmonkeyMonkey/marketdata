// Package coinbase adapts the Coinbase Exchange websocket feed
// (wss://ws-feed.exchange.coinbase.com) to the normalized model.
//
// Only public channels are used: "matches" (trades) and "heartbeat".
// level2/level3/full require authentication on this endpoint.
package coinbase

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/book"
	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const Name = "coinbase"

// Feed implements exchange.Feed. Coinbase product IDs (BTC-USD) are already
// in canonical form.
type Feed struct {
	// mu guards products and ids: Handle runs on the reader goroutine while
	// SetSymbols is called by the subscription controller.
	mu       sync.Mutex
	url      string
	products []string
	// Trade IDs are sequential per product; kept across reconnects so the
	// last_match after a reconnect reveals what was missed.
	ids   *exchange.TradeIDs
	books *book.Tracker
}

var (
	_ exchange.Feed    = (*Feed)(nil)
	_ exchange.Dynamic = (*Feed)(nil)
)

func New(url string, products []string) *Feed {
	return &Feed{url: url, products: slices.Clone(products), ids: exchange.NewTradeIDs(Name), books: book.NewTracker(Name)}
}

func (f *Feed) Name() string { return Name }
func (f *Feed) URL() string  { return f.url }

func (f *Feed) Subscriptions() ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.products) == 0 {
		return nil, nil
	}
	msg, err := request("subscribe", f.products)
	if err != nil {
		return nil, err
	}
	return [][]byte{msg}, nil
}

func (f *Feed) SetSymbols(symbols []string) ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	added, removed := exchange.Diff(f.products, symbols)
	f.products = slices.Clone(symbols)
	var frames [][]byte
	if len(added) > 0 {
		m, err := request("subscribe", added)
		if err != nil {
			return nil, err
		}
		frames = append(frames, m)
	}
	if len(removed) > 0 {
		m, err := request("unsubscribe", removed)
		if err != nil {
			return nil, err
		}
		frames = append(frames, m)
		for _, p := range removed {
			f.ids.Forget(p)
			f.books.Forget(p)
		}
	}
	return frames, nil
}

func request(typ string, products []string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"type":        typ,
		"product_ids": products,
		// level2_batch is the public L2 book (level2/level3/full need auth):
		// a full snapshot, then changes batched every ~50 ms.
		"channels": []string{"matches", "heartbeat", "level2_batch"},
	})
}

// message is a union of the fields we need from all handled message types.
type message struct {
	Type        string          `json:"type"`
	ProductID   string          `json:"product_id"`
	TradeID     int64           `json:"trade_id"`
	Side        string          `json:"side"`
	Price       decimal.Decimal `json:"price"`
	Size        decimal.Decimal `json:"size"`
	Time        time.Time       `json:"time"`
	LastTradeID int64           `json:"last_trade_id"`
	Message     string          `json:"message"`
	Reason      string          `json:"reason"`
	// level2_batch
	Bids    [][2]decimal.Decimal `json:"bids"`
	Asks    [][2]decimal.Decimal `json:"asks"`
	Changes [][3]string          `json:"changes"`
}

func (f *Feed) Handle(frame []byte, received time.Time, dst []model.Event) ([]model.Event, error) {
	var m message
	if err := json.Unmarshal(frame, &m); err != nil {
		return dst, fmt.Errorf("coinbase: decode frame: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	dst, err := f.handle(m, received, dst)
	// Any frame (heartbeats arrive every second) publishes books whose last
	// change waited for the rate limit.
	return f.books.Flush(received, dst), err
}

func (f *Feed) handle(m message, received time.Time, dst []model.Event) ([]model.Event, error) {
	switch m.Type {
	case "snapshot":
		return f.bookSnapshot(m, received, dst), nil
	case "l2update":
		return f.bookUpdate(m, received, dst)
	case "match", "last_match":
		if !slices.Contains(f.products, m.ProductID) {
			return dst, nil // in flight when we unsubscribed
		}
		// last_match is sent once after subscribing: the latest trade before
		// our session. After a reconnect it reveals trades we missed.
		var dup bool
		dst, dup = f.ids.Observe(m.ProductID, m.TradeID, received, dst)
		if dup {
			return dst, nil
		}
		side, err := parseSide(m.Side)
		if err != nil {
			return dst, err
		}
		return append(dst, model.Trade{
			Exchange: Name,
			Symbol:   m.ProductID,
			TradeID:  strconv.FormatInt(m.TradeID, 10),
			Price:    m.Price,
			Size:     m.Size,
			// Coinbase reports the MAKER order side; the taker is the opposite.
			TakerSide:    side.Opposite(),
			ExchangeTime: m.Time,
			ReceiveTime:  received,
			Snapshot:     m.Type == "last_match",
		}), nil

	case "heartbeat":
		// Heartbeats carry the last trade ID, so gaps are visible even when
		// no new trades arrive.
		return f.ids.AdvanceTo(m.ProductID, m.LastTradeID, received, dst), nil

	case "error":
		// e.g. "XYZ-USD is not a valid product": only that product fails.
		if p, ok := strings.CutSuffix(m.Reason, " is not a valid product"); ok {
			return dst, &exchange.SymbolError{Symbol: p, Reason: m.Reason}
		}
		return dst, fmt.Errorf("coinbase: server error: %s: %s", m.Message, m.Reason)

	default: // subscriptions, ticker, etc.
		return dst, nil
	}
}

func parseSide(s string) (model.Side, error) {
	switch s {
	case "buy":
		return model.SideBuy, nil
	case "sell":
		return model.SideSell, nil
	default:
		return model.SideUnknown, fmt.Errorf("coinbase: unknown side %q", s)
	}
}
