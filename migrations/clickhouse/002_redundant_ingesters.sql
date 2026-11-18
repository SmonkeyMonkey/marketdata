-- Which ingester instance reported the gap. With redundant ingesters a gap in
-- one instance is normally covered by the other; trade_id_holes shows what
-- is really missing from storage.
ALTER TABLE marketdata.gaps ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT '';

-- Trade ID holes actually present in storage over the last day, for
-- exchanges with sequential numeric trade IDs. This is the ground truth for
-- data loss, independent of what any single ingester observed.
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
    )
    WHERE id > 0
    WINDOW w AS (PARTITION BY exchange, symbol ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
)
WHERE prev_id > 0 AND id > prev_id + 1;
