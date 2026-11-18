// Package store persists normalized events.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"marketdata/internal/config"
	"marketdata/internal/model"
)

type ClickHouse struct {
	conn driver.Conn
}

func OpenClickHouse(ctx context.Context, cfg config.ClickHouse) (*ClickHouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return &ClickHouse{conn: conn}, nil
}

func (c *ClickHouse) Close() error { return c.conn.Close() }

// InsertTrades writes all trades in one native-protocol batch. ClickHouse
// handles few large inserts well and many small ones badly.
func (c *ClickHouse) InsertTrades(ctx context.Context, trades []model.Trade) error {
	if len(trades) == 0 {
		return nil
	}
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO trades
		(exchange, symbol, trade_id, price, size, taker_side, exchange_time, receive_time)`)
	if err != nil {
		return fmt.Errorf("prepare trades batch: %w", err)
	}
	defer b.Abort()
	for _, t := range trades {
		if err := b.Append(t.Exchange, t.Symbol, t.TradeID, t.Price, t.Size,
			t.TakerSide.String(), t.ExchangeTime, t.ReceiveTime); err != nil {
			return fmt.Errorf("append trade %s/%s/%s: %w", t.Exchange, t.Symbol, t.TradeID, err)
		}
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("send trades batch: %w", err)
	}
	return nil
}

func (c *ClickHouse) InsertCandles(ctx context.Context, candles []model.Candle) error {
	if len(candles) == 0 {
		return nil
	}
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO candles
		(exchange, symbol, period, start, open, high, low, close,
		 volume, quote_volume, buy_volume, sell_volume, trades, first_trade_id, last_trade_id)`)
	if err != nil {
		return fmt.Errorf("prepare candles batch: %w", err)
	}
	defer b.Abort()
	for _, k := range candles {
		if err := b.Append(k.Exchange, k.Symbol, model.PeriodName(k.Interval), k.Start,
			k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume, k.BuyVolume, k.SellVolume,
			uint64(k.Trades), k.FirstTradeID, k.LastTradeID); err != nil {
			return fmt.Errorf("append candle: %w", err)
		}
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("send candles batch: %w", err)
	}
	return nil
}

func (c *ClickHouse) InsertAlerts(ctx context.Context, alerts []model.Alert) error {
	if len(alerts) == 0 {
		return nil
	}
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO alerts
		(id, type, exchange, symbol, message, price, time, rule_id, trade_id, notional, change_pct)`)
	if err != nil {
		return fmt.Errorf("prepare alerts batch: %w", err)
	}
	defer b.Abort()
	for _, a := range alerts {
		if err := b.Append(a.ID, a.Type.String(), a.Exchange, a.Symbol, a.Message, a.Price, a.Time,
			a.RuleID, a.TradeID, a.Notional, a.ChangePct); err != nil {
			return fmt.Errorf("append alert: %w", err)
		}
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("send alerts batch: %w", err)
	}
	return nil
}

func (c *ClickHouse) InsertGaps(ctx context.Context, gaps []model.Gap) error {
	if len(gaps) == 0 {
		return nil
	}
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO gaps
		(exchange, symbol, after_id, before_id, missing, detected_at, source)`)
	if err != nil {
		return fmt.Errorf("prepare gaps batch: %w", err)
	}
	defer b.Abort()
	for _, g := range gaps {
		if err := b.Append(g.Exchange, g.Symbol, g.AfterID, g.BeforeID, g.Missing, g.DetectedAt, g.Source); err != nil {
			return fmt.Errorf("append gap: %w", err)
		}
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("send gaps batch: %w", err)
	}
	return nil
}
