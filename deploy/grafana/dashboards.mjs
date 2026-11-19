// Generates dashboards/pipeline.json. Run: make dashboard
import { writeFileSync } from 'node:fs';

const ds = { type: 'prometheus', uid: 'prometheus' };
let id = 1;
const panels = [];
let y = 0;

const target = (expr, legend, refId) => ({ datasource: ds, expr, legendFormat: legend ?? '', refId, range: true });

function row(title) {
  panels.push({ type: 'row', id: id++, title, collapsed: false, gridPos: { h: 1, w: 24, x: 0, y }, panels: [] });
  y += 1;
}

function stat(x, w, title, expr, { unit = 'short', steps, mappings, decimals, legend = '' } = {}) {
  panels.push({
    type: 'stat', id: id++, title, datasource: ds,
    gridPos: { h: 4, w, x, y },
    targets: [target(expr, legend, 'A')],
    options: { reduceOptions: { calcs: ['lastNotNull'], fields: '', values: false }, colorMode: 'background', graphMode: 'area', textMode: 'value' },
    fieldConfig: {
      defaults: {
        unit, decimals, mappings: mappings ?? [],
        thresholds: { mode: 'absolute', steps: steps ?? [{ color: 'green', value: null }] },
      },
      overrides: [],
    },
  });
}

function ts(x, w, title, targets, { unit = 'short', stack = false } = {}) {
  panels.push({
    type: 'timeseries', id: id++, title, datasource: ds,
    gridPos: { h: 8, w, x, y },
    targets: targets.map(([expr, legend], i) => target(expr, legend, String.fromCharCode(65 + i))),
    options: { legend: { displayMode: 'table', placement: 'bottom', calcs: ['mean', 'max', 'lastNotNull'] }, tooltip: { mode: 'multi', sort: 'desc' } },
    fieldConfig: {
      defaults: {
        unit,
        custom: { drawStyle: 'line', lineWidth: 1, fillOpacity: 10, showPoints: 'never', stacking: { mode: stack ? 'normal' : 'none' } },
      },
      overrides: [],
    },
  });
}

const q = (p, metric, by = '') =>
  `histogram_quantile(${p}, sum by (le${by ? ', ' + by : ''}) (rate(${metric}_bucket[$__rate_interval])))`;

// --- overview
row('Overview');
// Ingesters are redundant: counts from them are per replica, so totals of
// unique trades come from the writer (after dedup).
stat(0, 4, 'Unique trades / s (stored)', 'sum(rate(md_writer_rows_inserted_total{table="trades"}[1m]))', { decimals: 1 });
stat(4, 4, 'Live replicas per exchange', 'sum by (exchange) (md_ingest_connected)', { legend: '{{exchange}}',
  decimals: 0,
  steps: [{ color: 'red', value: null }, { color: 'yellow', value: 1 }, { color: 'green', value: 2 }],
});
stat(8, 4, 'Since last frame', 'max(time() - md_ingest_last_frame_timestamp_seconds)', {
  unit: 's', decimals: 1,
  steps: [{ color: 'green', value: null }, { color: 'yellow', value: 5 }, { color: 'red', value: 15 }],
});
stat(12, 4, 'Consumer lag', 'sum(md_writer_consumer_lag)', {
  steps: [{ color: 'green', value: null }, { color: 'yellow', value: 10000 }, { color: 'red', value: 100000 }],
});
stat(16, 4, 'Pipeline p99 (recv → stored)', q(0.99, 'md_writer_pipeline_latency_seconds').replace('$__rate_interval', '5m'), {
  unit: 's', decimals: 2,
  steps: [{ color: 'green', value: null }, { color: 'yellow', value: 3 }, { color: 'red', value: 10 }],
});
// A replica missing trades is normally covered by the other one; real loss
// is checked in storage (`make audit`).
stat(20, 4, 'Missed by a replica (1h)', 'sum(increase(md_ingest_missing_trades_total[1h])) or vector(0)', {
  decimals: 0, steps: [{ color: 'green', value: null }, { color: 'yellow', value: 1 }],
});
y += 4;

// --- ingest
row('Ingest');
ts(0, 12, 'Trades / s by symbol', [['max by (exchange, symbol) (rate(md_ingest_trades_total[$__rate_interval]))', '{{exchange}} {{symbol}}']], { unit: 'ops', stack: true });
ts(12, 12, 'Feed latency (exchange match → received)', [
  [q(0.5, 'md_ingest_feed_latency_seconds', 'exchange'), 'p50 {{exchange}}'],
  [q(0.99, 'md_ingest_feed_latency_seconds', 'exchange'), 'p99 {{exchange}}'],
], { unit: 's' });
y += 8;
ts(0, 8, 'Websocket throughput', [['sum by (exchange) (rate(md_ingest_bytes_total[$__rate_interval]))', '{{exchange}}']], { unit: 'Bps' });
ts(8, 8, 'Disconnects & missed trades per replica', [
  ['sum by (instance, exchange) (increase(md_ingest_disconnects_total[5m]))', 'disconnects {{instance}} {{exchange}}'],
  ['sum by (instance, exchange, symbol) (increase(md_ingest_missing_trades_total[5m]))', 'missed {{instance}} {{exchange}} {{symbol}}'],
]);
ts(16, 8, 'Kafka producer', [
  ['md_kafka_produce_buffered_records', 'buffered records'],
  ['increase(md_kafka_produce_failed_total[5m])', 'failed (5m)'],
]);
y += 8;

// --- storage
row('Kafka → ClickHouse');
ts(0, 12, 'Pipeline latency (received → stored)', [
  [q(0.5, 'md_writer_pipeline_latency_seconds'), 'p50'],
  [q(0.95, 'md_writer_pipeline_latency_seconds'), 'p95'],
  [q(0.99, 'md_writer_pipeline_latency_seconds'), 'p99'],
], { unit: 's' });
ts(12, 12, 'Consumer lag by topic', [['sum by (topic) (md_writer_consumer_lag)', '{{topic}}']]);
y += 8;
ts(0, 8, 'Rows inserted / s', [['sum by (table) (rate(md_writer_rows_inserted_total[$__rate_interval]))', '{{table}}']], { unit: 'ops' });
ts(8, 8, 'Flush duration', [
  [q(0.5, 'md_writer_flush_duration_seconds'), 'p50'],
  [q(0.99, 'md_writer_flush_duration_seconds'), 'p99'],
], { unit: 's' });
ts(16, 8, 'Batch size, duplicates & errors', [
  ['rate(md_writer_batch_rows_sum[$__rate_interval]) / rate(md_writer_batch_rows_count[$__rate_interval])', 'avg rows / batch'],
  ['rate(md_writer_duplicates_dropped_total[$__rate_interval])', 'duplicates dropped / s'],
  ['increase(md_writer_insert_errors_total[5m])', 'insert errors (5m)'],
  ['sum(increase(md_writer_decode_errors_total[5m])) or vector(0)', 'decode errors (5m)'],
]);
y += 8;

// --- candles
row('Candles (aggregator)');
ts(0, 8, 'Candles published / s', [['sum by (interval) (rate(md_agg_candles_emitted_total[$__rate_interval]))', '{{interval}}']], { unit: 'ops' });
ts(8, 8, 'Close delay (candle end → published)', [
  [q(0.5, 'md_agg_close_delay_seconds', 'interval'), 'p50 {{interval}}'],
  [q(0.99, 'md_agg_close_delay_seconds', 'interval'), 'p99 {{interval}}'],
], { unit: 's' });
ts(16, 8, 'Open candles, late & duplicate trades', [
  ['md_agg_open_candles', 'open candles'],
  ['rate(md_agg_late_trades_total[$__rate_interval])', 'late trades / s'],
  ['rate(md_agg_duplicates_dropped_total[$__rate_interval])', 'duplicates / s'],
]);
y += 8;

// --- gateway
row('Websocket gateway');
ts(0, 8, 'Connections & subscriptions', [
  ['sum(md_gateway_connections)', 'connections'],
  ['sum(md_gateway_subscriptions)', 'subscriptions'],
]);
ts(8, 8, 'Messages sent / s', [
  ['sum(rate(md_gateway_sent_messages_total[$__rate_interval]))', 'messages / s'],
], { unit: 'ops' });
ts(16, 8, 'Bytes sent / s', [['sum(rate(md_gateway_sent_bytes_total[$__rate_interval]))', 'bytes / s']], { unit: 'Bps' });
y += 8;
ts(0, 8, 'Fan-out time per message (queue to all subscribers)', [
  [q(0.5, 'md_gateway_fanout_seconds'), 'p50'],
  [q(0.99, 'md_gateway_fanout_seconds'), 'p99'],
], { unit: 's' });
ts(8, 8, 'Ingest → gateway lag (trades)', [
  [q(0.5, 'md_gateway_ingest_lag_seconds'), 'p50'],
  [q(0.99, 'md_gateway_ingest_lag_seconds'), 'p99'],
], { unit: 's' });
ts(16, 8, 'Disconnects by reason', [
  ['sum by (reason) (rate(md_gateway_disconnects_total[$__rate_interval]))', '{{reason}}'],
  ['rate(md_gateway_connections_rejected_total[$__rate_interval])', 'rejected (limit)'],
], { unit: 'ops' });
y += 8;

// --- order books
row('Order books');
ts(0, 8, 'Book snapshots published / s', [['sum by (exchange) (max by (exchange, instance) (rate(md_ingest_book_snapshots_total[$__rate_interval])))', '{{exchange}}']], { unit: 'ops' });
ts(8, 8, 'Book resyncs (checksum mismatches)', [['sum by (exchange) (increase(md_ingest_book_resyncs_total[5m]))', '{{exchange}}']]);
ts(16, 8, 'Book frames sent to clients / s', [['sum(rate(md_gateway_published_total{type="book"}[$__rate_interval]))', 'published']], { unit: 'ops' });
y += 8;

// --- arbitrage
row('Arbitrage');
ts(0, 8, 'Best edge after fees per asset (bps; < 0: no opportunity)', [['md_arb_best_net_bps', '{{asset}}']]);
ts(8, 8, 'Opportunities closed / min', [['sum by (asset) (increase(md_arb_windows_total[1m]))', '{{asset}}']]);
ts(16, 8, 'Opportunity duration', [
  [q(0.5, 'md_arb_window_duration_seconds'), 'p50'],
  [q(0.99, 'md_arb_window_duration_seconds'), 'p99'],
], { unit: 's' });
y += 8;
ts(0, 8, 'Edge after fees per route (bps)', [['md_arb_route_net_bps{asset="BTC-USD"}', '{{buy}} → {{sell}}']]);
ts(8, 8, 'Quote rates (USDT, USDC in USD)', [['md_arb_quote_rate', '{{quote}} ({{source}})']]);
ts(16, 8, 'Evaluation time per book', [
  [q(0.5, 'md_arb_evaluation_seconds'), 'p50'],
  [q(0.99, 'md_arb_evaluation_seconds'), 'p99'],
], { unit: 's' });
y += 8;

// --- alerts
row('Alerts');
ts(0, 8, 'Alerts fired / min', [['sum by (type) (increase(md_alerts_fired_total[1m]))', '{{type}}']]);
ts(8, 8, 'Active price rules', [['md_alerts_rules', 'rules']]);
ts(16, 8, 'Evaluation time per trade (all rules + detectors)', [
  [q(0.5, 'md_alerts_evaluation_seconds'), 'p50'],
  [q(0.99, 'md_alerts_evaluation_seconds'), 'p99'],
], { unit: 's' });
y += 8;

// --- runtime
row('Runtime');
ts(0, 8, 'CPU', [['rate(process_cpu_seconds_total{job=~"ingester|writer|aggregator|gateway|alerts|arbitrage"}[$__rate_interval])', '{{instance}}']], { unit: 'percentunit' });
ts(8, 8, 'Heap in use', [['go_memstats_heap_inuse_bytes{job=~"ingester|writer|aggregator|gateway|alerts|arbitrage"}', '{{instance}}']], { unit: 'bytes' });
ts(16, 8, 'Goroutines', [['go_goroutines{job=~"ingester|writer|aggregator|gateway|alerts|arbitrage"}', '{{instance}}']]);
y += 8;

const dashboard = {
  uid: 'marketdata-pipeline',
  title: 'Market Data Pipeline',
  tags: ['marketdata'],
  timezone: 'browser',
  schemaVersion: 39,
  version: 1,
  refresh: '5s',
  time: { from: 'now-30m', to: 'now' },
  panels,
};

writeFileSync(process.argv[2], JSON.stringify(dashboard, null, 2) + '\n');
console.log(`wrote ${panels.length} panels`);
