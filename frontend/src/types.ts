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

export type AlertType = 'price_rule' | 'whale_trade' | 'price_move'

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

export const whaleFor = (s: Settings | null, m: Market): Decimal | null =>
  s ? (s.markets.find((x) => x.exchange === m.exchange && x.symbol === m.symbol)?.whale_notional ?? s.default_whale) : null

export const channels = {
  trades: (m: Market) => `trades:${m.exchange}:${m.symbol}`,
  candles: (m: Market, p: BasePeriod) => `candles:${m.exchange}:${m.symbol}:${p}`,
  alerts: (m: Market) => `alerts:${m.exchange}:${m.symbol}`,
}
