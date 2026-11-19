package model

import (
	"time"

	"github.com/shopspring/decimal"
)

// ArbVenue is one market's top of book in the asset's common quote.
type ArbVenue struct {
	Exchange string
	Symbol   string
	Bid      decimal.Decimal
	Ask      decimal.Decimal
	BidSize  decimal.Decimal
	AskSize  decimal.Decimal
	// Rate converted the native quote to the common quote (1 for the common
	// quote itself); RateSource says where it came from: "1", a market key
	// such as kraken:USDT-USD, or "peg" when that market is not fresh.
	Rate       decimal.Decimal
	RateSource string
	FeeBps     decimal.Decimal
	BookTime   time.Time
}

func (v ArbVenue) Key() string { return v.Exchange + ":" + v.Symbol }

// ArbRoute is buying on one market and selling on another at the same time.
type ArbRoute struct {
	BuyExchange, BuySymbol   string
	SellExchange, SellSymbol string
	// Best ask of the buy market and best bid of the sell market.
	BuyPrice, SellPrice decimal.Decimal
	// Top-of-book spread in basis points before and after both taker fees.
	GrossBps, NetBps decimal.Decimal
	// Size (base units) tradeable while profitable after fees, walking both
	// books; Profit over it, in the common quote.
	Size, Profit decimal.Decimal
}

func (r ArbRoute) Key() string {
	return r.BuyExchange + ":" + r.BuySymbol + ">" + r.SellExchange + ":" + r.SellSymbol
}

// Profitable reports a positive edge after fees.
func (r ArbRoute) Profitable() bool { return r.Size.IsPositive() }

// ArbSnapshot compares every market of one asset. Asset is BASE-QUOTE with
// the common quote, e.g. BTC-USD for BTC-USD, BTC-USDT and BTC-USDC markets.
type ArbSnapshot struct {
	Asset  string
	Time   time.Time
	Venues []ArbVenue
	Routes []ArbRoute // best net first
}

// ArbWindow is an opportunity from the first evaluation where a route was
// profitable after fees until the first one where it was not.
type ArbWindow struct {
	ID                       string
	Asset                    string
	BuyExchange, BuySymbol   string
	SellExchange, SellSymbol string
	Opened, Closed           time.Time
	PeakNetBps, PeakGrossBps decimal.Decimal
	PeakSize, PeakProfit     decimal.Decimal
	Updates                  int64
}

func (w ArbWindow) Duration() time.Duration { return w.Closed.Sub(w.Opened) }
