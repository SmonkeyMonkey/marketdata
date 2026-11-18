import { useEffect, useState } from 'react'
import { api } from '../lib/api'
import type { Instrument, Market } from '../types'

const EXCHANGES = ['binance', 'coinbase', 'kraken'] as const

/** Searches an exchange's instruments and subscribes the ingesters to one. */
export function AddMarket({ onAdded, onClose }: { onAdded: (m: Market) => void; onClose: () => void }) {
  const [exchange, setExchange] = useState<string>('binance')
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<Instrument[]>([])
  const [busy, setBusy] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    const t = setTimeout(() => {
      api.catalog(exchange, query).then(
        (r) => !cancelled && (setResults(r), setError(null)),
        (e: Error) => !cancelled && setError(e.message),
      )
    }, 250) // debounce typing
    return () => {
      cancelled = true
      clearTimeout(t)
    }
  }, [exchange, query])

  const add = async (symbol: string) => {
    setBusy(symbol)
    setError(null)
    try {
      onAdded(await api.subscribe({ exchange, symbol }))
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="add-market">
      <div className="row">
        <select value={exchange} onChange={(e) => setExchange(e.target.value)}>
          {EXCHANGES.map((x) => <option key={x}>{x}</option>)}
        </select>
        <input autoFocus placeholder="search, e.g. SOL" value={query} onChange={(e) => setQuery(e.target.value)} />
        <button onClick={onClose} aria-label="close">×</button>
      </div>
      {error && <p className="error">{error}</p>}
      <ul>
        {results.map((r) => (
          <li key={r.symbol}>
            {r.symbol}
            {r.subscribed
              ? <span className="muted"> streaming</span>
              : <button className="link" disabled={busy !== null} onClick={() => add(r.symbol)}>{busy === r.symbol ? 'adding…' : '+ add'}</button>}
          </li>
        ))}
      </ul>
    </div>
  )
}
