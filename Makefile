# Load .env if present so local runs use the same config as docker compose.
ifneq (,$(wildcard .env))
include .env
export
endif

INGESTERS := ingester-a ingester-b
CH := docker compose exec -T clickhouse clickhouse-client -u $${CLICKHOUSE_USER:-marketdata} --password $${CLICKHOUSE_PASSWORD:-marketdata} -d marketdata

.PHONY: build test lint proto dashboard run-ingester run-writer up down logs deploy-ingesters audit verify-candles load frontend frontend-dev frontend-check

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

lint: bin/buf
	gofmt -l . | (! grep .)
	go vet ./...
	./bin/buf lint

bin/buf: tools/go.mod
	cd tools && go build -o ../bin/buf github.com/bufbuild/buf/cmd/buf

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

# Services built from this repo (ingester-b shares ingester-a's image).
BUILT := ingester-a writer aggregator alerts arbitrage gateway api

# Images are built one at a time: in parallel, seven Go builds plus Kafka and
# ClickHouse don't fit a 4 GB Docker VM and the OOM killer takes the brokers.
up:
	@for s in $(BUILT); do docker compose build -q $$s || exit 1; done
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
