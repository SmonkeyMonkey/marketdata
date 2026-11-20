-- Pipeline benchmark storage: same tables as marketdata, in their own
-- database, emptied before every run. Live data is never touched.
CREATE DATABASE IF NOT EXISTS bench;
CREATE TABLE IF NOT EXISTS bench.trades AS marketdata.trades;
CREATE TABLE IF NOT EXISTS bench.gaps AS marketdata.gaps;
CREATE TABLE IF NOT EXISTS bench.candles AS marketdata.candles;
CREATE TABLE IF NOT EXISTS bench.alerts AS marketdata.alerts;
CREATE TABLE IF NOT EXISTS bench.book_stats AS marketdata.book_stats;
CREATE TABLE IF NOT EXISTS bench.arb_windows AS marketdata.arb_windows;
TRUNCATE TABLE bench.trades;
TRUNCATE TABLE bench.gaps;
TRUNCATE TABLE bench.candles;
TRUNCATE TABLE bench.alerts;
TRUNCATE TABLE bench.book_stats;
TRUNCATE TABLE bench.arb_windows;
