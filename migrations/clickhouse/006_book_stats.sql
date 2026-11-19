-- Order book stats, one row per market and second (latest snapshot of the
-- second). Sizes are the sum over the top 10 levels per side.
CREATE TABLE IF NOT EXISTS marketdata.book_stats
(
    exchange    LowCardinality(String),
    symbol      LowCardinality(String),
    ts          DateTime('UTC'),
    best_bid    Decimal(38, 18),
    best_ask    Decimal(38, 18),
    mid         Decimal(38, 18),
    spread      Decimal(38, 18),
    spread_bps  Float64,
    bid_size10  Decimal(38, 18),
    ask_size10  Decimal(38, 18),
    imbalance   Float64, -- (bid - ask) / (bid + ask) over the top 10 levels
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(ts)
ORDER BY (exchange, symbol, ts)
TTL ts + INTERVAL 30 DAY
SETTINGS ttl_only_drop_parts = 1;
