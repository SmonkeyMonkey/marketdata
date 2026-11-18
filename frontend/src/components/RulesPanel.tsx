import { useEffect, useState, type FormEvent } from 'react'
import { api } from '../lib/api'
import { fmtPrice } from '../lib/format'
import { useChannels } from '../lib/hooks'
import type { Market, Rule } from '../types'

type Tracked = Rule & { status: 'armed' | 'fired' | 'gone'; firedMessage?: string }

// Rule IDs this browser created; remembered across reloads for convenience.
const STORE = 'marketdata.rules'
const loadIDs = (): string[] => {
  try {
    return JSON.parse(localStorage.getItem(STORE) ?? '[]') as string[]
  } catch {
    return []
  }
}
const saveIDs = (ids: string[]) => {
  try {
    localStorage.setItem(STORE, JSON.stringify(ids.slice(0, 50)))
  } catch {
    /* storage unavailable: rules still work for this session */
  }
}

export function RulesPanel({ market, lastPrice }: { market: Market; lastPrice: string | null }) {
  const [rules, setRules] = useState<Tracked[]>([])
  const [direction, setDirection] = useState<'above' | 'below'>('above')
  const [price, setPrice] = useState('')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    Promise.all(loadIDs().map((id) => api.getRule(id).catch(() => null))).then((found) =>
      setRules(found.filter((r): r is Rule => r !== null).map((r) => ({ ...r, status: 'armed' }))),
    )
  }, [])

  useEffect(() => saveIDs(rules.filter((r) => r.status === 'armed').map((r) => r.id)), [rules])

  useChannels(rules.filter((r) => r.status === 'armed').map((r) => r.channel), (m) => {
    if (m.type !== 'alert' || !m.data.rule_id) return
    const { rule_id, message } = m.data
    setRules((rs) => rs.map((r) => (r.id === rule_id ? { ...r, status: 'fired', firedMessage: message } : r)))
  })

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setError(null)
    try {
      const rule = await api.createRule({ exchange: market.exchange, symbol: market.symbol, direction, price: price || lastPrice || '' })
      setRules((rs) => [{ ...rule, status: 'armed' }, ...rs])
      setPrice('')
    } catch (err) {
      setError((err as Error).message)
    }
  }

  const remove = async (id: string) => {
    await api.deleteRule(id).catch(() => {})
    setRules((rs) => rs.map((r) => (r.id === id ? { ...r, status: 'gone' } : r)))
  }

  return (
    <section className="panel rules">
      <h2>Price alerts <span className="muted">{market.exchange} {market.symbol}</span></h2>
      <form onSubmit={submit}>
        <select value={direction} onChange={(e) => setDirection(e.target.value as 'above' | 'below')}>
          <option value="above">above</option>
          <option value="below">below</option>
        </select>
        <input inputMode="decimal" placeholder={lastPrice ?? 'price'}
          value={price} onChange={(e) => setPrice(e.target.value)} />
        <button type="submit">Create</button>
      </form>
      {error && <p className="error">{error}</p>}
      <ul>
        {rules.map((r) => (
          <li key={r.id} className={r.status}>
            <span className={`badge ${r.status}`}>{r.status}</span>
            {r.exchange} {r.symbol} {r.direction} {fmtPrice(r.price)}
            {r.status === 'armed' && <button className="link" onClick={() => remove(r.id)}>cancel</button>}
            {r.firedMessage && <div className="muted">{r.firedMessage}</div>}
          </li>
        ))}
      </ul>
    </section>
  )
}
