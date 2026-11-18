# marketdata

Real-time crypto market data platform in Go: ingests public exchange websocket
feeds, normalizes them, and (next stages) streams them through Kafka into
ClickHouse, real-time analytics, alerts and a websocket gateway.

## Status

- [x] Stage 0 — skeleton: config from env, Docker, compose (Kafka 4 KRaft, kafka-ui, ClickHouse), CI
- [x] Stage 1 — Coinbase ingester: trades + heartbeats, reconnect with jittered backoff,
      stale-feed detection, trade-ID gap detection, dedup, graceful shutdown
- [x] Stage 2 — Kafka (protobuf, keyed by exchange:symbol) → batch writer → ClickHouse,
      at-least-once with commit-after-insert, retries with backoff, dedup via ReplacingMergeTree.
      Verified: SIGKILL of the writer and a ClickHouse outage lose no trades and leave no duplicates.
- [x] Observability: Prometheus metrics (`/metrics`, `/healthz` on every service) and a
      provisioned Grafana dashboard — throughput, feed and end-to-end latency histograms,
      consumer lag, flush timings, errors, runtime. Metrics are PromQL-compatible, so
      VictoriaMetrics can replace Prometheus by swapping one compose service.
- [x] Stage 3a — Binance and Kraken next to Coinbase: one runner per exchange, canonical
      BASE-QUOTE symbols, shared trade-ID gap tracker, snapshot trades excluded from latency.
- [x] Stage 5a — streaming OHLCV candles (1s, 1m; configurable): event-time buckets closed by a
      per-symbol watermark + allowed lateness, idle close for quiet symbols, VWAP via quote volume,
      taker buy/sell volume. Restart-safe: input offsets are committed only up to the first trade of
      the oldest open candle, and the tail of `md.candles` tells a restarted aggregator what was
      already published — a test proves a mid-stream restart yields the identical candle set with no
      duplicates (and fails if either mechanism is removed). Verified live: 235/235 one-minute candles
      match candles recomputed from raw trades exactly; 1 of 11.6k one-second candles missed because
      Kraken delivered its trades 2.5s late (> `AGG_LATENESS=2s`).
- [x] Stage 6 — websocket gateway: `trades:<exchange>:<symbol>` and
      `candles:<exchange>:<symbol>:<period>` channels, snapshot of the last message on subscribe,
      copy-on-write subscriber lists (publishing never waits on subscribe/unsubscribe), one JSON
      encoding per message shared by all clients, bounded per-client queue with slow-client
      disconnect, ping-based liveness, graceful "going away" shutdown, readiness tied to Kafka
      freshness, pprof on the ops port. Every instance reads all partitions (no consumer group), so
      gateways scale horizontally without shared state.
- [x] Stage 7 — alerts: one-shot user price rules (REST API), whale trades (notional threshold) and
      sharp moves (X% within a window, monotonic min/max deques, O(1) per trade, cooldown).
      Rules are event-sourced in a compacted Kafka topic (create = record, delete/fire = tombstone)
      and rebuilt on start — no extra database. Trade offsets are committed only after alerts and
      tombstones are flushed, and alert IDs are deterministic, so a crash never loses a fired rule
      and never fires it twice under a different ID (tested across a restart). Alerts go to Kafka,
      the gateway (`alerts:<exchange>:<symbol>`, `alerts:rule:<id>`) and ClickHouse. Live check:
      a rule 3 USDT below the market fired 3.3 s after creation and arrived on both channels.
- [ ] Stage 3b — Bybit, OKX, Coinbase Advanced

### Rule index benchmark (one trade checked against n rules of its symbol, Apple M-series core)

Per symbol, "above" rules live in a min-heap and "below" rules in a max-heap; a trade only touches
the rules it actually crossed.

| Rules | Heap index | Linear scan | Speed-up |
|---|---|---|---|
| 1,000 | 48 ns | 1.7 µs | 36× |
| 100,000 | 49 ns | 189 µs | 3,800× |
| 1,000,000 | 58 ns | 2.6 ms | ~46,000× |

With a linear scan, a million rules would cap evaluation at ~380 trades/s per core — below the live
feed rate. `go test -bench . ./internal/alerts/` reproduces it.

### Gateway load test (single Docker Desktop VM: 8 vCPU, 3.8 GiB for the whole stack + load generator)

`make load LOAD_CONNS=15000` — every connection subscribes to six 1s-candle channels, 10% also to a
live trade channel. Latency = gateway send → client receive.

| Connections | Messages/s (peak) | Latency p50 / p99 (steady) | Slow-client drops | Gateway CPU | Gateway RSS |
|---|---|---|---|---|---|
| 10,000 | ~60k | 35 ms / 80–150 ms | 0 | 1.5 cores | 444 MiB |
| 15,000 | ~139k | 60–90 ms / 170–400 ms | 0 | 2.3 cores | 726 MiB |

Profiling-driven fixes along the way (pprof heap + goroutine counts at 10k connections):

| Change | Effect |
|---|---|
| Removed a per-client watcher goroutine (server keeps a client registry instead) | goroutines 30k → 20k |
| Replaced the 256-slot buffered channel with a queue that allocates only for the real backlog | RSS 619 → 444 MiB (−28%) |
| With memory now proportional to backlog, raised the queue limit 256 → 1024 | slow-client drops during trade bursts 1% → 0 |

The load generator runs in the same VM and competes for CPU, so latency numbers are an upper bound.
Remaining per-connection cost is mostly net/http's hijacked bufio reader/writer (~7.5 KB) and two
goroutine stacks; going much further means an epoll-based server instead of goroutine-per-connection.
- [x] Zero-loss deploys: two redundant ingesters, writer-side dedup (bounded window of recent
      trade IDs, marked only after a successful insert) backed by ReplacingMergeTree, readiness
      probe, `make deploy-ingesters` rolling restart, `make audit` for ground-truth holes.
      Verified under live traffic: two rolling deploys + SIGKILL of one replica for 30s →
      0 missing of ~26.5k trades, 0 duplicates stored (21.6k copies dropped by the writer).
      The same restarts with a single ingester lost 100–1400 trades each.

## Quick start

```sh
cp .env.example .env
make test
make run | jq .          # normalized events on stdout, logs on stderr
make up                  # full stack in docker
make deploy-ingesters    # rolling restart, one replica at a time
make audit               # trade ID holes actually present in storage (24h)
make verify-candles      # streamed 1m candles vs. recomputed from raw trades
make load LOAD_CONNS=10000   # websocket load test against the gateway

# live data in a browser console or with any websocket client:
#   ws://localhost:8090/ws
#   {"op":"subscribe","channels":["candles:binance:BTC-USDT:1s","trades:coinbase:BTC-USD"]}

# price alert: fires once, delivered on the returned channel (alerts:rule:<id>)
curl -X POST localhost:8091/v1/rules -d '{"exchange":"binance","symbol":"BTC-USDT","direction":"above","price":"70000"}'
```

| UI | URL |
|---|---|
| Grafana (dashboard "Market Data Pipeline") | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Kafka UI | http://localhost:8080 |
| ClickHouse HTTP | http://localhost:8123 |

## Layout

```
cmd/ingester            entrypoint
internal/exchange       Feed interface implemented by every exchange
internal/exchange/*     per-exchange adapters (+ testdata with real frames)
internal/ingest         connection lifecycle: dial, subscribe, read, reconnect
internal/model          normalized events (Trade, Gap)
internal/publish        event sinks (stdout now, Kafka next)
internal/backoff        reconnect delay policies
internal/config         env configuration
```

## Notes

- Prices and sizes are decimals end to end, never floats.
- Coinbase `match.side` is the **maker** side; the normalized `taker_side` is the opposite.
- Gap detection relies on Coinbase trade IDs being sequential per product;
  heartbeats (`last_trade_id`) expose gaps even when no trades arrive.
