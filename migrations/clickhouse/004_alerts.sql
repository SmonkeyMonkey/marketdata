-- Alerts history. Alert IDs are deterministic, so a replayed alert collapses
-- into the original on merge.
CREATE TABLE IF NOT EXISTS marketdata.alerts
(
    id          String,
    type        LowCardinality(String), -- price_rule, whale_trade, price_move, arbitrage
    exchange    LowCardinality(String),
    symbol      LowCardinality(String),
    message     String,
    price       Decimal(38, 18),
    time        DateTime64(6, 'UTC'),
    rule_id     String,
    trade_id    String,
    notional    Decimal(38, 18),
    change_pct  Decimal(38, 18),
    inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(time)
ORDER BY (exchange, symbol, type, id);
