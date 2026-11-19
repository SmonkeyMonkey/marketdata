import { useEffect, useRef, useState } from 'react'
import { api } from '../lib/api'
import { fmtPrice, fmtSize, fmtTime, pricePrecision } from '../lib/format'
import { useChannels } from '../lib/hooks'
import { assetOf, channels, quoteOf, type ArbMsg, type ArbRouteStats, type ArbVenue, type ArbWindowRow, type Market } from '../types'

type Tab = 'live' | 'history'

/**
 * Cross-exchange comparison of the selected market's asset: every venue's
 * best bid/ask in one quote, and the edge of buying on one venue and selling
 * on another after both taker fees.
 */
export function ArbPanel({ market }: { market: Market }) {
  const asset = assetOf(market.symbol)
  const [snap, setSnap] = useState<ArbMsg | null>(null)
  const [tab, setTab] = useState<Tab>('live')
  const pending = useRef<ArbMsg | null>(null)

  useEffect(() => {
    setSnap(null)
    pending.current = null
  }, [asset])

  // Snapshots arrive ~4 times a second; render at most twice.
  useChannels([channels.arb(asset)], (m) => {
    if (m.type === 'arb') pending.current = m.data
  })
  useEffect(() => {
    const t = setInterval(() => {
      if (pending.current) {
        setSnap(pending.current)
        pending.current = null
      }
    }, 500)
    return () => clearInterval(t)
  }, [])

  return (
    <section className="panel arb">
      <h2>
        Arbitrage <span className="muted">{asset}</span>
        <span className="toggle">
          {(['live', 'history'] as const).map((t) => (
            <button key={t} className={t === tab ? 'active' : ''} onClick={() => setTab(t)}>{t}</button>
          ))}
        </span>
      </h2>
      {tab === 'live' ? <Live asset={asset} snap={snap} /> : <History asset={asset} />}
    </section>
  )
}

const common = (asset: string) => quoteOf(asset)

/** "binance" or, when the market is quoted in another currency, "binance·USDT". */
const label = (key: string, asset: string) => {
  const [exchange, symbol] = key.split(':')
  const q = quoteOf(symbol ?? '')
  return q === common(asset) ? exchange : `${exchange}·${q}`
}

const bps = (v: string | number) => {
  const n = Number(v)
  return `${n > 0 ? '+' : ''}${n.toFixed(1)}`
}

function Live({ asset, snap }: { asset: string; snap: ArbMsg | null }) {
  if (!snap) return <p className="muted">Waiting for books…</p>
  const only = snap.venues[0]
  if (!only || snap.venues.length < 2) {
    return (
      <p className="muted">
        {only ? `Only ${label(`${only.exchange}:${only.symbol}`, asset)} streams` : 'No fresh books for'} {asset}.
        Add the same asset on another exchange (Markets → add) to compare.
      </p>
    )
  }
  const keys = snap.venues.map((v) => `${v.exchange}:${v.symbol}`)
  const native = snap.venues.filter((v) => v.rate_source === '1')
  const precision = pricePrecision((native.length ? native : snap.venues).flatMap((v) => [v.bid, v.ask]))
  const bestBid = Math.max(...snap.venues.map((v) => Number(v.bid)))
  const bestAsk = Math.min(...snap.venues.map((v) => Number(v.ask)))
  const route = new Map(snap.routes.map((r) => [`${r.buy}>${r.sell}`, r]))
  const best = snap.routes[0]

  return (
    <div className="arb-live">
      <table className="arb-venues">
        <thead>
          <tr><th>venue</th><th className="num">bid</th><th className="num">ask</th><th className="num">fee</th></tr>
        </thead>
        <tbody>
          {snap.venues.map((v) => (
            <tr key={`${v.exchange}:${v.symbol}`} title={rateNote(v)}>
              <td>{label(`${v.exchange}:${v.symbol}`, asset)}{v.rate_source === 'peg' && <span className="muted"> ≈</span>}</td>
              <td className={`num ${Number(v.bid) === bestBid ? 'up' : ''}`}>{fmtPrice(v.bid, precision)}</td>
              <td className={`num ${Number(v.ask) === bestAsk ? 'down' : ''}`}>{fmtPrice(v.ask, precision)}</td>
              <td className="num muted">{Number(v.fee_bps)} bp</td>
            </tr>
          ))}
        </tbody>
      </table>

      <table className="arb-matrix">
        <thead>
          <tr>
            <th className="muted">buy ↓ sell →</th>
            {keys.map((k) => <th key={k} className="num">{label(k, asset)}</th>)}
          </tr>
        </thead>
        <tbody>
          {keys.map((b) => (
            <tr key={b}>
              <th>{label(b, asset)}</th>
              {keys.map((s) => {
                const r = route.get(`${b}>${s}`)
                if (!r) return <td key={s} className="num muted">·</td>
                const net = Number(r.net_bps)
                return (
                  <td key={s} className={`num ${net > 0 ? 'arb-open' : ''}`}
                    title={`gross ${bps(r.gross_bps)} bps, after fees ${bps(r.net_bps)} bps` +
                      (net > 0 ? `\n${fmtSize(r.size)} ${asset.split('-')[0]} → ${Number(r.profit).toFixed(2)} ${common(asset)}` : '')}>
                    {bps(r.net_bps)}
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {best && (
        <p className="arb-best">
          best: buy <b>{label(best.buy, asset)}</b> → sell <b>{label(best.sell, asset)}</b>{' '}
          <span className="muted">gross</span> {bps(best.gross_bps)}{' '}
          <span className="muted">after fees</span>{' '}
          <span className={Number(best.net_bps) > 0 ? 'up' : 'down'}>{bps(best.net_bps)} bps</span>
        </p>
      )}
    </div>
  )
}

function rateNote(v: ArbVenue): string | undefined {
  if (v.rate_source === '1') return undefined
  const q = quoteOf(v.symbol)
  return v.rate_source === 'peg'
    ? `${q} assumed 1:1 (no fresh rate market)`
    : `converted from ${q} at ${v.rate} (${v.rate_source} mid)`
}

const fmtMs = (ms: number) => (ms < 1000 ? `${Math.round(ms)}ms` : ms < 60_000 ? `${(ms / 1000).toFixed(1)}s` : `${(ms / 60_000).toFixed(1)}m`)

function History({ asset }: { asset: string }) {
  const [stats, setStats] = useState<ArbRouteStats[] | null>(null)
  const [windows, setWindows] = useState<ArbWindowRow[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let live = true
    const load = () => {
      Promise.all([api.arbStats(asset), api.arbWindows(asset, 20)]).then(([s, w]) => {
        if (!live) return
        setStats(s)
        setWindows(w)
        setError(null)
      }, (e: Error) => live && setError(e.message))
    }
    setStats(null)
    load()
    const t = setInterval(load, 15_000)
    return () => {
      live = false
      clearInterval(t)
    }
  }, [asset])

  if (error) return <p className="error">{error}</p>
  if (!stats) return <p className="muted">Loading…</p>
  if (stats.length === 0) return <p className="muted">No opportunity after fees in the last 24h.</p>
  return (
    <>
      <table>
        <thead>
          <tr><th>last 24h</th><th className="num">n</th><th className="num">median</th><th className="num">max bp</th><th className="num">Σ profit</th></tr>
        </thead>
        <tbody>
          {stats.map((s) => (
            <tr key={`${s.buy}>${s.sell}`}>
              <td>{label(s.buy, asset)} → {label(s.sell, asset)}</td>
              <td className="num">{s.windows}</td>
              <td className="num">{fmtMs(s.median_ms)}</td>
              <td className="num up">{bps(s.max_net_bps)}</td>
              <td className="num">{Number(s.sum_peak_profit).toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <ul className="arb-windows">
        {windows.map((w) => (
          <li key={w.id}>
            <span className="muted">{fmtTime(w.opened)}</span> {label(w.buy, asset)} → {label(w.sell, asset)}{' '}
            <span className="up">{bps(w.peak_net_bps)}</span> <span className="muted">for {fmtMs(w.duration_ms)},</span>{' '}
            {fmtSize(w.peak_size)} → {Number(w.peak_profit).toFixed(2)}
          </li>
        ))}
      </ul>
    </>
  )
}
