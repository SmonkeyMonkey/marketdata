-- trade_id_holes only works where trade IDs are sequential per market.
-- Not for:
--   bybit: IDs come from counters shared by the markets of one matching
--          shard, so every market skips values (checked live);
--   okx:   one trade covers `count` IDs (one per price level of a match)
--          and storage keeps only the last one. The ingester still detects
--          OKX gaps, because it sees the count.
CREATE OR REPLACE VIEW marketdata.trade_id_holes AS
SELECT exchange, symbol, prev_id AS after_id, id AS before_id, id - prev_id - 1 AS missing,
       prev_time AS after_time, exchange_time AS before_time
FROM
(
    SELECT exchange, symbol, id, exchange_time,
           lagInFrame(id) OVER w AS prev_id,
           lagInFrame(exchange_time) OVER w AS prev_time
    FROM
    (
        SELECT DISTINCT exchange, symbol, toUInt64OrZero(trade_id) AS id, exchange_time
        FROM marketdata.trades
        WHERE exchange_time > now() - INTERVAL 1 DAY
          AND exchange NOT IN ('bybit', 'okx')
    )
    WHERE id > 0
    WINDOW w AS (PARTITION BY exchange, symbol ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
)
WHERE prev_id > 0 AND id > prev_id + 1;
