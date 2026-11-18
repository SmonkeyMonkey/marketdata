CREATE DATABASE IF NOT EXISTS marketdata;

-- ReplacingMergeTree collapses rows with the same ORDER BY key during merges,
-- which removes duplicates produced by at-least-once delivery.
-- Query with FINAL (or GROUP BY the key) when exact counts matter.
CREATE TABLE IF NOT EXISTS marketdata.trades
(
    exchange      LowCardinality(String),
    symbol        LowCardinality(String),
    trade_id      String,
    price         Decimal(38, 18),
    size          Decimal(38, 18),
    taker_side    Enum8('unknown' = 0, 'buy' = 1, 'sell' = 2),
    exchange_time DateTime64(6, 'UTC'),
    receive_time  DateTime64(6, 'UTC'),
    inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(exchange_time)
ORDER BY (exchange, symbol, exchange_time, trade_id);

CREATE TABLE IF NOT EXISTS marketdata.gaps
(
    exchange    LowCardinality(String),
    symbol      LowCardinality(String),
    after_id    String,
    before_id   String,
    missing     Int64,
    detected_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (exchange, symbol, after_id, before_id);
