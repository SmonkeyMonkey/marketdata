package store

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/shopspring/decimal"
)

// Read-side queries for the HTTP API. Tables are ReplacingMergeTree, so reads
// use FINAL (or DISTINCT) to hide duplicates not merged yet; every query is
// bounded by its primary-key prefix and a LIMIT.

type Market struct {
	Exchange string `ch:"exchange" json:"exchange"`
	Symbol   string `ch:"symbol" json:"symbol"`
}

type CandleRow struct {
	Start  time.Time       `ch:"bucket" json:"start"`
	Open   decimal.Decimal `ch:"open" json:"open"`
	High   decimal.Decimal `ch:"high" json:"high"`
	Low    decimal.Decimal `ch:"low" json:"low"`
	Close  decimal.Decimal `ch:"close" json:"close"`
	Volume decimal.Decimal `ch:"volume" json:"volume"`
	VWAP   decimal.Decimal `ch:"-" json:"vwap"`
	// QuoteVolume is read to compute VWAP in Go: dividing two Decimal(38,18)
	// in ClickHouse scales the dividend by 10^18 and silently overflows
	// Decimal128 for realistic volumes (it returned a negative VWAP).
	QuoteVolume decimal.Decimal `ch:"quote_volume" json:"-"`
	BuyVolume   decimal.Decimal `ch:"buy_volume" json:"buy_volume"`
	SellVolume  decimal.Decimal `ch:"sell_volume" json:"sell_volume"`
	Trades      uint64          `ch:"trades" json:"trades"`
	// LastBase is the start of the newest base candle rolled into this bar,
	// so a client can continue the bar live without counting it twice.
	LastBase time.Time `ch:"last_base" json:"last_base"`
}

type TradeRow struct {
	ID    string          `ch:"trade_id" json:"id"`
	Price decimal.Decimal `ch:"price" json:"price"`
	Size  decimal.Decimal `ch:"size" json:"size"`
	Side  string          `ch:"taker_side" json:"side"`
	Time  time.Time       `ch:"exchange_time" json:"time"`
}

type AlertRow struct {
	ID        string          `ch:"id" json:"id"`
	Type      string          `ch:"type" json:"type"`
	Exchange  string          `ch:"exchange" json:"exchange"`
	Symbol    string          `ch:"symbol" json:"symbol"`
	Message   string          `ch:"message" json:"message"`
	Price     decimal.Decimal `ch:"price" json:"price"`
	Time      time.Time       `ch:"time" json:"time"`
	RuleID    string          `ch:"rule_id" json:"rule_id,omitempty"`
	Notional  decimal.Decimal `ch:"notional" json:"notional"`
	ChangePct decimal.Decimal `ch:"change_pct" json:"change_pct"`
}

// Markets lists exchange/symbol pairs that produced candles in the last day.
func (c *ClickHouse) Markets(ctx context.Context) ([]Market, error) {
	var out []Market
	err := c.conn.Select(ctx, &out, `
		SELECT DISTINCT exchange, symbol FROM candles
		WHERE start > now() - INTERVAL 1 DAY
		ORDER BY exchange, symbol`)
	if err != nil {
		return nil, fmt.Errorf("markets: %w", err)
	}
	return out, nil
}

// Candles returns the latest limit bars of bucket seconds, oldest first,
// rolled up from the stored base candles (base "1s" or "1m"). bucket must be
// a multiple of the base; buckets align to the Unix epoch, so 1h starts on
// the hour and 1d at 00:00 UTC.
func (c *ClickHouse) Candles(ctx context.Context, exchange, symbol, base string, bucket, limit int) ([]CandleRow, error) {
	var out []CandleRow
	err := c.conn.Select(ctx, &out, `
		SELECT toStartOfInterval(b.start, toIntervalSecond(?)) AS bucket,
		       argMin(open, b.start) AS open, max(high) AS high, min(low) AS low, argMax(close, b.start) AS close,
		       sum(volume) AS volume, sum(quote_volume) AS quote_volume,
		       sum(buy_volume) AS buy_volume, sum(sell_volume) AS sell_volume, sum(trades) AS trades,
		       max(b.start) AS last_base
		FROM candles AS b FINAL
		WHERE exchange = ? AND symbol = ? AND period = ?
		  AND b.start >= toStartOfInterval(now64(3), toIntervalSecond(?)) - toIntervalSecond(?)
		GROUP BY bucket
		ORDER BY bucket DESC
		LIMIT ?`, bucket, exchange, symbol, base, bucket, (limit-1)*bucket, limit)
	if err != nil {
		return nil, fmt.Errorf("candles: %w", err)
	}
	slices.Reverse(out)
	for i := range out {
		out[i].VWAP = VWAP(out[i].QuoteVolume, out[i].Volume)
	}
	return out, nil
}

// RecentTrades returns the latest trades of the last hour, newest first.
func (c *ClickHouse) RecentTrades(ctx context.Context, exchange, symbol string, limit int) ([]TradeRow, error) {
	var out []TradeRow
	err := c.conn.Select(ctx, &out, `
		SELECT trade_id, price, size, toString(taker_side) AS taker_side, exchange_time
		FROM trades FINAL
		WHERE exchange = ? AND symbol = ? AND exchange_time > now() - INTERVAL 1 HOUR
		ORDER BY exchange_time DESC, trade_id DESC
		LIMIT ?`, exchange, symbol, limit)
	if err != nil {
		return nil, fmt.Errorf("recent trades: %w", err)
	}
	return out, nil
}

// RecentAlerts returns the latest alerts, newest first.
func (c *ClickHouse) RecentAlerts(ctx context.Context, limit int) ([]AlertRow, error) {
	var out []AlertRow
	err := c.conn.Select(ctx, &out, `
		SELECT id, type, exchange, symbol, message, price, time, rule_id, notional, change_pct
		FROM alerts FINAL
		WHERE time > now() - INTERVAL 1 DAY
		ORDER BY time DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("recent alerts: %w", err)
	}
	return out, nil
}

// VWAP is quote volume / base volume, rounded to 8 decimals (0 without volume).
func VWAP(quoteVolume, volume decimal.Decimal) decimal.Decimal {
	if volume.IsZero() {
		return decimal.Zero
	}
	return quoteVolume.DivRound(volume, 8)
}
