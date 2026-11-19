-- Closed cross-exchange arbitrage opportunities: a route (buy on one market,
-- sell on another) that stayed profitable after taker fees from opened to
-- closed. Window IDs are deterministic, so replays collapse on merge.
CREATE TABLE IF NOT EXISTS marketdata.arb_windows
(
    id             String,
    asset          LowCardinality(String), -- BASE-common quote, e.g. BTC-USD
    buy_exchange   LowCardinality(String),
    buy_symbol     LowCardinality(String),
    sell_exchange  LowCardinality(String),
    sell_symbol    LowCardinality(String),
    opened         DateTime64(3, 'UTC'),
    closed         DateTime64(3, 'UTC'),
    duration_ms    UInt64,
    peak_net_bps   Float64,
    peak_gross_bps Float64,
    peak_size      Decimal(38, 18), -- base units
    peak_profit    Decimal(38, 18), -- common quote
    updates        UInt64,
    inserted_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(opened)
ORDER BY (asset, opened, id)
TTL toDateTime(opened) + INTERVAL 90 DAY
SETTINGS ttl_only_drop_parts = 1;
