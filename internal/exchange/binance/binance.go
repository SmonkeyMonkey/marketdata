// Package binance adapts the Binance spot combined-stream websocket
// (wss://stream.binance.com:9443/stream) to the normalized model.
//
// Streams are selected in the URL (?streams=btcusdt@trade/...), so nothing is
// sent after connecting. Binance has no application-level heartbeat and stays
// silent for unknown symbols, so the read timeout is what detects both a
// frozen feed and a typo in the symbol list.
package binance

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/exchange"
	"marketdata/internal/model"
)

const Name = "binance"

type Feed struct {
	url       string
	canonical map[string]string // BTCUSDT -> BTC-USDT
	ids       *exchange.TradeIDs
}

var _ exchange.Feed = (*Feed)(nil)

// New takes canonical symbols (BTC-USDT) and a base URL ending in /stream.
func New(baseURL string, symbols []string) (*Feed, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("binance: no symbols")
	}
	canonical := make(map[string]string, len(symbols))
	streams := make([]string, 0, len(symbols))
	for _, s := range symbols {
		base, quote, err := exchange.SplitSymbol(s)
		if err != nil {
			return nil, fmt.Errorf("binance: %w", err)
		}
		native := base + quote
		canonical[native] = s
		streams = append(streams, strings.ToLower(native)+"@trade")
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("binance: url: %w", err)
	}
	// Stream names are [a-z0-9@_], safe in a query without escaping; Binance
	// documents them unescaped.
	u.RawQuery = "streams=" + strings.Join(streams, "/")
	return &Feed{url: u.String(), canonical: canonical, ids: exchange.NewTradeIDs(Name)}, nil
}

func (f *Feed) Name() string                     { return Name }
func (f *Feed) URL() string                      { return f.url }
func (f *Feed) Subscriptions() ([][]byte, error) { return nil, nil }

type envelope struct {
	Stream string `json:"stream"`
	Data   trade  `json:"data"`
}

// trade mirrors the @trade payload. Binance uses keys that differ only in
// case (e/E, t/T, m/M); encoding/json matches keys case-insensitively, so each
// one must be declared explicitly or the wrong value lands in a field.
type trade struct {
	EventType  string          `json:"e"`
	EventTime  int64           `json:"E"`
	Symbol     string          `json:"s"`
	ID         int64           `json:"t"`
	Price      decimal.Decimal `json:"p"`
	Qty        decimal.Decimal `json:"q"`
	TradeTime  int64           `json:"T"`
	BuyerMaker bool            `json:"m"`
	Ignore     bool            `json:"M"`
}

func (f *Feed) Handle(frame []byte, received time.Time, dst []model.Event) ([]model.Event, error) {
	var env envelope
	if err := json.Unmarshal(frame, &env); err != nil {
		return dst, fmt.Errorf("binance: decode frame: %w", err)
	}
	if env.Data.EventType != "trade" {
		return dst, nil
	}
	t := env.Data
	symbol, ok := f.canonical[t.Symbol]
	if !ok {
		return dst, fmt.Errorf("binance: trade for unsubscribed symbol %q", t.Symbol)
	}

	var dup bool
	dst, dup = f.ids.Observe(symbol, t.ID, received, dst)
	if dup {
		return dst, nil
	}

	// m=true: the buyer was the maker, so the aggressor sold.
	side := model.SideBuy
	if t.BuyerMaker {
		side = model.SideSell
	}
	return append(dst, model.Trade{
		Exchange:     Name,
		Symbol:       symbol,
		TradeID:      strconv.FormatInt(t.ID, 10),
		Price:        t.Price,
		Size:         t.Qty,
		TakerSide:    side,
		ExchangeTime: time.UnixMilli(t.TradeTime).UTC(),
		ReceiveTime:  received,
	}), nil
}
