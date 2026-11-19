import type { AlertRow, ArbRouteStats, ArbWindowRow, CandleRow, Instrument, Market, Period, Rule, Settings, TradeRow } from '../types'

async function json<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { error?: string }
    throw new Error(body.error ?? `${res.status} ${res.statusText}`)
  }
  return (await res.json()) as T
}

const q = (params: Record<string, string | number>) =>
  new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)])).toString()

export const api = {
  markets: () => fetch('/api/markets').then(json<Market[]>),
  settings: () => fetch('/api/settings').then(json<Settings>),
  /** Sets the whale threshold of a market, or the default when m is null. */
  setWhale: (m: Market | null, value: string) =>
    fetch(m ? `/api/settings/${m.exchange}/${m.symbol}` : '/api/settings/default', {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ whale_notional: value }),
    }).then(json<Settings>),
  resetWhale: (m: Market) => fetch(`/api/settings/${m.exchange}/${m.symbol}`, { method: 'DELETE' }).then(json<Settings>),
  catalog: (exchange: string, q: string, limit = 20) =>
    fetch(`/api/catalog?${new URLSearchParams({ exchange, q, limit: String(limit) })}`).then(json<Instrument[]>),
  subscribe: (m: Market) =>
    fetch('/api/subscriptions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ exchange: m.exchange, symbol: m.symbol }),
    }).then(json<Market>),
  unsubscribe: async (m: Market) => {
    const res = await fetch(`/api/subscriptions/${m.exchange}/${m.symbol}`, { method: 'DELETE' })
    if (!res.ok && res.status !== 404) await json(res)
  },
  candles: (m: Market, period: Period, limit: number) =>
    fetch(`/api/candles?${q({ exchange: m.exchange, symbol: m.symbol, period, limit })}`).then(json<CandleRow[]>),
  trades: (m: Market, limit = 50) =>
    fetch(`/api/trades?${q({ exchange: m.exchange, symbol: m.symbol, limit })}`).then(json<TradeRow[]>),
  arbWindows: (asset: string, limit = 20) => fetch(`/api/arb/windows?${q({ asset, limit })}`).then(json<ArbWindowRow[]>),
  arbStats: (asset: string, window = '24h') => fetch(`/api/arb/stats?${q({ asset, window })}`).then(json<ArbRouteStats[]>),
  alerts: (limit = 30) => fetch(`/api/alerts?${q({ limit })}`).then(json<AlertRow[]>),
  createRule: (r: { exchange: string; symbol: string; direction: 'above' | 'below'; price: string; note?: string }) =>
    fetch('/api/rules', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(r) }).then(json<Rule>),
  getRule: async (id: string): Promise<Rule | null> => {
    const res = await fetch(`/api/rules/${id}`)
    return res.status === 404 ? null : json<Rule>(res)
  },
  deleteRule: async (id: string) => {
    const res = await fetch(`/api/rules/${id}`, { method: 'DELETE' })
    if (!res.ok && res.status !== 404) throw new Error(`${res.status}`)
  },
}
