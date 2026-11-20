# Load .env if present so local runs use the same config as docker compose.
ifneq (,$(wildcard .env))
include .env
export
endif

INGESTERS := ingester-a ingester-b
# Build version, shown as md_build_info{version} on every service.
export VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
CH := docker compose exec -T clickhouse clickhouse-client -u $${CLICKHOUSE_USER:-marketdata} --password $${CLICKHOUSE_PASSWORD:-marketdata} -d marketdata

.PHONY: bench bench-pipeline build test test-integration vuln lint proto dashboard run-ingester run-writer up down logs deploy-ingesters audit verify-candles load frontend frontend-dev frontend-check

build:
	go build -o bin/ ./cmd/...

# Unit tests (no external services). gotestsum prints a readable summary,
# writes a JUnit report and reruns a failed test once: a test that passes on
# the second try is still reported as flaky, not hidden.
test: bin/gotestsum
	./bin/gotestsum --format $(or $(GOTESTSUM_FORMAT),pkgname) --junitfile $(or $(JUNIT),/dev/null) \
		--rerun-fails=1 --packages=./... -- -race -count=1

# Store SQL against a real ClickHouse (make up, or the CI service container).
# Uses a throwaway database; the live one is never touched.
test-integration: bin/gotestsum
	./bin/gotestsum --format $(or $(GOTESTSUM_FORMAT),testname) --junitfile $(or $(JUNIT),/dev/null) \
		--packages=./internal/store/... -- -race -count=1 -tags integration -run Integration

# Known vulnerabilities in the code paths we actually call (stdlib and deps).
vuln: bin/govulncheck
	./bin/govulncheck ./...

lint: bin/buf
	gofmt -l . | (! grep .)
	go vet ./...
	go vet -tags integration ./internal/store/...
	./bin/buf lint

bin/buf: tools/go.mod
	cd tools && go build -o ../bin/buf github.com/bufbuild/buf/cmd/buf

bin/gotestsum: tools/go.mod
	cd tools && go build -o ../bin/gotestsum gotest.tools/gotestsum

bin/govulncheck: tools/go.mod
	cd tools && go build -o ../bin/govulncheck golang.org/x/vuln/cmd/govulncheck

proto: bin/buf
	./bin/buf lint
	./bin/buf generate

# Web UI. `make frontend` builds it into frontend/dist (embedded by the api
# service); `make frontend-dev` runs Vite with hot reload against the stack.
frontend:
	cd frontend && npm ci && npm run build

frontend-dev:
	cd frontend && npm run dev

frontend-check:
	cd frontend && npm run typecheck && npm test

dashboard:
	node deploy/grafana/dashboards.mjs deploy/grafana/dashboards/pipeline.json

run-ingester: build
	./bin/ingester

run-writer: build
	./bin/writer

# All images share one Go build stage (see Dockerfile), so building them in
# parallel compiles once.
up:
	docker compose build -q
	docker compose up -d --remove-orphans

down:
	docker compose down

logs:
	docker compose logs -f $(INGESTERS) writer aggregator gateway

# Rolling restart: replace one ingester, wait until all its feeds are live
# (health check), then the next. The other one keeps streaming meanwhile.
deploy-ingesters:
	docker compose build $(firstword $(INGESTERS))
	@for s in $(INGESTERS); do \
		echo "==> $$s"; \
		docker compose up -d --no-deps --force-recreate --wait --wait-timeout 120 $$s || exit 1; \
	done

# Trade ID holes actually present in storage (last 24h).
audit:
	@$(CH) --query "SELECT exchange, symbol, count() AS holes, sum(missing) AS missing_trades FROM trade_id_holes GROUP BY exchange, symbol ORDER BY exchange, symbol FORMAT PrettyCompact"

# Micro-benchmarks of the hot paths (parsing, codec, candles, dedup, fan-out,
# order books, rule index, arbitrage).
bench:
	go test -run '^$$' -bench . -benchmem ./internal/... | grep -E '^(Benchmark|ok )'

# Pipeline benchmark: replay stored trades into bench.trades, then measure how
# fast a writer (-> ClickHouse database bench) and an aggregator drain the
# backlog, and check the result. BENCH_TRADES=0 replays everything.
BENCH_TRADES ?= 1000000
BENCH_TOPICS := trades:12 candles:12 gaps:1 alerts:1 books:1 arb-windows:1
KT := docker compose exec -T kafka /opt/kafka/bin
bench-pipeline:
	docker compose --profile bench rm -sf bench-writer bench-aggregator
	docker compose --profile bench build -q writer aggregator replay
	-@for g in bench-writer bench-aggregator; do $(KT)/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group $$g >/dev/null 2>&1; done
	@for tp in $(BENCH_TOPICS); do \
		t=bench.$${tp%%:*}; p=$${tp##*:}; \
		$(KT)/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic $$t; \
		until $(KT)/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $$t --partitions $$p --replication-factor 1 >/dev/null 2>&1 \
			&& [ "$$($(KT)/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $$t | awk -F: '{s+=$$3} END {print s+0}')" = 0 ]; do sleep 1; done; \
	done
	$(CH) --multiquery < deploy/bench/schema.sql
	docker compose --profile bench run --rm replay seed -limit $(BENCH_TRADES)
	docker compose --profile bench up -d bench-writer bench-aggregator
	docker compose --profile bench run --rm replay watch -topic bench.trades \
		-counter 'writer=bench-writer=http://bench-writer:9102/metrics=md_writer_rows_inserted_total{table="trades"}' \
		-counter 'aggregator=bench-aggregator=http://bench-aggregator:9103/metrics=md_agg_trades_total'
	docker compose --profile bench stop bench-writer bench-aggregator
	$(CH) --multiquery < deploy/bench/verify.sql

# Streamed 1m candles vs. candles recomputed from raw trades (closed minutes
# of the last hour). Every row should match exactly.
verify-candles:
	@$(CH) --query "\
	WITH s AS (SELECT exchange, symbol, start, open, high, low, close, volume, buy_volume, sell_volume, trades \
	           FROM candles FINAL WHERE period = '1m' AND start >= now() - INTERVAL 1 HOUR AND start < now() - INTERVAL 2 MINUTE), \
	     r AS (SELECT exchange, symbol, toStartOfMinute(exchange_time) AS start, \
	                  argMin(price, toUInt64(trade_id)) AS open, max(price) AS high, min(price) AS low, \
	                  argMax(price, toUInt64(trade_id)) AS close, sum(size) AS volume, \
	                  sumIf(size, taker_side = 'buy') AS buy_volume, sumIf(size, taker_side = 'sell') AS sell_volume, count() AS trades \
	           FROM trades FINAL WHERE exchange_time >= now() - INTERVAL 1 HOUR GROUP BY exchange, symbol, start) \
	SELECT s.exchange, s.symbol, count() AS candles, \
	       countIf(s.open = r.open AND s.high = r.high AND s.low = r.low AND s.close = r.close \
	               AND s.volume = r.volume AND s.buy_volume = r.buy_volume AND s.sell_volume = r.sell_volume AND s.trades = r.trades) AS exact, \
	       countIf(s.trades != r.trades) AS trade_count_diff \
	FROM s INNER JOIN r USING (exchange, symbol, start) \
	GROUP BY s.exchange, s.symbol ORDER BY s.exchange, s.symbol FORMAT PrettyCompact"

# Websocket load test against the gateway. Each loadgen replica is its own
# source IP (~28k connections max per replica to one gateway port).
LOAD_CONNS ?= 10000
LOAD_REPLICAS ?= 1
LOAD_RATE ?= 1000
LOAD_TRADES_SHARE ?= 0.1
LOAD_DURATION ?= 2m
load:
	LOAD_CONNS=$(LOAD_CONNS) LOAD_RATE=$(LOAD_RATE) LOAD_TRADES_SHARE=$(LOAD_TRADES_SHARE) LOAD_DURATION=$(LOAD_DURATION) \
		docker compose --profile load up --build --no-deps --scale loadgen=$(LOAD_REPLICAS) loadgen; \
		docker compose --profile load rm -sf loadgen # don't leave it for "docker compose start" to revive
