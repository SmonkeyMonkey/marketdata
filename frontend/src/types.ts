// Wire types of the gateway websocket and the api service. They mirror
// internal/gateway/protocol.go and internal/store/query.go; decimals are
// strings so no precision is lost in transit.

export type Decimal = string
export type Side = 'buy' | 'sell' | 'unknown'
// Chart intervals. Only 1s and 1m candles are produced by the aggregator
// ("base" periods); longer ones are rolled up from them.
export const PERIODS = ['1s', '5s', '15s', '30s', '1m', '5m', '15m', '30m', '1h', '4h', '1d'] as const
export type Period = (typeof PERIODS)[number]
export type BasePeriod = '1s' | '1m'

export interface Market {
  exchange: string
  symbol: string
  pinned?: boolean // streamed by ingester config; cannot be removed from the UI
}

export interface Instrument {
  symbol: string
  base: string
  quote: string
  subscribed: boolean
}

export const marketKey = (m: Market) => `${m.exchange}:${m.symbol}`

export interface TradeMsg {
  exchange: string
  symbol: string
  id: string
  price: Decimal
  size: Decimal
  side: Side // taker side
  time: string // exchange time, RFC 3339
  ingest_time: string
}

export interface CandleMsg {
  exchange: string
  symbol: string
  period: BasePeriod
  start: string
  open: Decimal
  high: Decimal
  low: Decimal
  close: Decimal
  volume: Decimal
  vwap: Decimal
  buy_volume: Decimal
  sell_volume: Decimal
  trades: number
}

export type AlertType = 'price_rule' | 'whale_trade' | 'price_move' | 'arbitrage'

export interface AlertMsg {
  id: string
  type: AlertType
  exchange: string
  symbol: string
  message: string
  price: Decimal
  time: string
  rule_id?: string
  notional?: Decimal
  change_pct?: Decimal
}

export interface BookMsg {
  exchange: string
  symbol: string
  bids: [Decimal, Decimal][] // [price, size], best first
  asks: [Decimal, Decimal][]
  time: string
  mid: Decimal
  spread: Decimal
  spread_bps: Decimal
  imbalance: Decimal // -1..1 over the top 10 levels; > 0: more bid size
}

// Cross-exchange comparison of one asset (arb:<asset>). Prices are in the
// asset's common quote: USDT and USDC markets are converted to USD.
export interface ArbVenue {
  exchange: string
  symbol: string
  bid: Decimal
  ask: Decimal
  bid_size: Decimal
  ask_size: Decimal
  rate: Decimal // native quote -> common quote
  rate_source: string // "1", a market such as kraken:USDT-USD, or "peg"
  fee_bps: Decimal // taker
  time: string
}

export interface ArbRoute {
  buy: string // exchange:symbol, bought at its best ask
  sell: string // exchange:symbol, sold at its best bid
  buy_price: Decimal
  sell_price: Decimal
  gross_bps: Decimal // before fees
  net_bps: Decimal // after both taker fees; > 0 is an opportunity
  size: Decimal // base units profitable after fees, walking both books
  profit: Decimal // common quote, over size
}

export interface ArbMsg {
  asset: string
  time: string
  venues: ArbVenue[]
  routes: ArbRoute[] // best net first
}

interface Envelope<T extends string, D> {
  type: T
  channel: string
  ts: number // gateway send time, unix ms
  data: D
}

export type ServerMessage =
  | Envelope<'trade', TradeMsg>
  | Envelope<'candle', CandleMsg>
  | Envelope<'alert', AlertMsg>
  | Envelope<'book', BookMsg>
  | Envelope<'arb', ArbMsg>
  | { type: 'subscribed' | 'unsubscribed'; ts: number; channels: string[] }
  | { type: 'error'; ts: number; error: string }
  | { type: 'pong'; ts: number }

// REST rows (GET /api/...)
export type CandleRow = Omit<CandleMsg, 'exchange' | 'symbol' | 'period'> & {
  last_base: string // start of the newest base candle rolled into this bar
}

export interface TradeRow {
  id: string
  price: Decimal
  size: Decimal
  side: Side
  time: string
}

export type AlertRow = AlertMsg

export interface ArbWindowRow {
  id: string
  asset: string
  buy: string
  sell: string
  opened: string
  closed: string
  duration_ms: number
  peak_net_bps: number
  peak_gross_bps: number
  peak_size: Decimal
  peak_profit: Decimal
}

export interface ArbRouteStats {
  buy: string
  sell: string
  windows: number
  total_ms: number
  median_ms: number
  max_net_bps: number
  sum_peak_profit: Decimal
}

export interface Rule {
  id: string
  exchange: string
  symbol: string
  direction: 'above' | 'below'
  price: Decimal
  created_at: string
  note?: string
  channel: string
}

// Alert settings (GET /api/settings). Whale thresholds are in each market's
// quote currency; "0" disables whale alerts.
export interface Settings {
  default_whale: Decimal
  markets: { exchange: string; symbol: string; whale_notional: Decimal }[]
}

export const quoteOf = (symbol: string) => symbol.slice(symbol.lastIndexOf('-') + 1)

// Quotes compared as another one; mirrors ARB_QUOTES of the arbitrage service.
const COMMON_QUOTE: Record<string, string> = { USDT: 'USD', USDC: 'USD' }

/** Asset a market is compared under across exchanges: BTC-USDT -> BTC-USD. */
export const assetOf = (symbol: string) => {
  const i = symbol.lastIndexOf('-')
  const quote = symbol.slice(i + 1)
  return `${symbol.slice(0, i)}-${COMMON_QUOTE[quote] ?? quote}`
}

export const whaleFor = (s: Settings | null, m: Market): Decimal | null =>
  s ? (s.markets.find((x) => x.exchange === m.exchange && x.symbol === m.symbol)?.whale_notional ?? s.default_whale) : null

export const channels = {
  trades: (m: Market) => `trades:${m.exchange}:${m.symbol}`,
  candles: (m: Market, p: BasePeriod) => `candles:${m.exchange}:${m.symbol}:${p}`,
  alerts: (m: Market) => `alerts:${m.exchange}:${m.symbol}`,
  book: (m: Market) => `book:${m.exchange}:${m.symbol}`,
  arb: (asset: string) => `arb:${asset}`,
}
