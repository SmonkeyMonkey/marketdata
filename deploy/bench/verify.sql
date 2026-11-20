-- After a pipeline benchmark: did every replayed trade land exactly once,
-- and do candles rebuilt from stored trades equal the live ones?
SELECT 'trades stored' AS check, (SELECT count() FROM bench.trades FINAL) AS bench,
       (SELECT uniqExact(exchange, symbol, trade_id) FROM bench.trades) AS distinct_ids
FORMAT PrettyCompact;

-- Closed 1m candles of minutes that both pipelines saw completely (not the
-- first or last minute of a market in the replayed range).
WITH
  live AS (SELECT exchange, symbol, start, open, high, low, close, volume, trades FROM marketdata.candles FINAL WHERE period = '1m'),
  rep  AS (SELECT exchange, symbol, start, open, high, low, close, volume, trades FROM bench.candles FINAL WHERE period = '1m'),
  edges AS (SELECT exchange, symbol, min(start) AS lo, max(start) AS hi FROM rep GROUP BY exchange, symbol)
SELECT count() AS minutes_compared,
       countIf(l.open = r.open AND l.high = r.high AND l.low = r.low AND l.close = r.close AND l.volume = r.volume AND l.trades = r.trades) AS identical,
       countIf(r.trades > l.trades) AS replay_has_more_trades,
       countIf(r.trades < l.trades) AS replay_has_fewer_trades,
       round(identical / minutes_compared * 100, 3) AS identical_pct
FROM rep AS r
INNER JOIN live AS l USING (exchange, symbol, start)
INNER JOIN edges AS e USING (exchange, symbol)
WHERE r.start > e.lo AND r.start < e.hi
FORMAT PrettyCompact;
