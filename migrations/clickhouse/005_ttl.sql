-- Data retention. Kafka keeps its own copy for 7 days (trades) / 30 days
-- (candles); this bounds what ClickHouse stores.
--
-- Migrations run on every `docker compose up`, so changing TTL must not
-- rewrite existing data each time: with this off, a TTL change applies to new
-- parts and during merges. To apply it to old data at once, run
-- `ALTER TABLE <t> MATERIALIZE TTL` by hand.
SET materialize_ttl_after_modify = 0;

-- Raw trades: 30 days. Expired data is dropped as whole parts (cheap: no
-- part is rewritten). Partitions are monthly, so a part goes once all its
-- rows are older than 30 days: trades are kept 30 to ~60 days.
ALTER TABLE marketdata.trades
    MODIFY TTL toDateTime(exchange_time) + INTERVAL 30 DAY;
ALTER TABLE marketdata.trades
    MODIFY SETTING ttl_only_drop_parts = 1;

-- 1s candles: 30 days; 1m candles: 1 year (charts of 5m..1d roll up from 1m).
ALTER TABLE marketdata.candles
    MODIFY TTL toDateTime(start) + INTERVAL 30 DAY DELETE WHERE period = '1s',
               toDateTime(start) + INTERVAL 1 YEAR DELETE WHERE period = '1m';

ALTER TABLE marketdata.alerts
    MODIFY TTL toDateTime(time) + INTERVAL 90 DAY;

ALTER TABLE marketdata.gaps
    MODIFY TTL toDateTime(detected_at) + INTERVAL 90 DAY;
