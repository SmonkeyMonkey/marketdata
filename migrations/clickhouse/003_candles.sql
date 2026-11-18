-- Candles published by the aggregator. A candle may be written more than
-- once (at-least-once delivery); the newest copy wins on merge.
CREATE TABLE IF NOT EXISTS marketdata.candles
(
    exchange       LowCardinality(String),
    symbol         LowCardinality(String),
    period         LowCardinality(String), -- 1s, 1m, ...
    start          DateTime64(3, 'UTC'),
    open           Decimal(38, 18),
    high           Decimal(38, 18),
    low            Decimal(38, 18),
    close          Decimal(38, 18),
    volume         Decimal(38, 18),
    quote_volume   Decimal(38, 18),
    buy_volume     Decimal(38, 18),
    sell_volume    Decimal(38, 18),
    trades         UInt64,
    first_trade_id String,
    last_trade_id  String,
    inserted_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(start)
ORDER BY (exchange, symbol, period, start);
