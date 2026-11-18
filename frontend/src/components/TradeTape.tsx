import { useEffect, useMemo, useState } from 'react'
import { api } from '../lib/api'
import { fmtPrice, fmtSize, fmtTime, pricePrecision } from '../lib/format'
import { useChannels } from '../lib/hooks'
import { addTrade, allTrades, emptyTape, latestSeq, loadTape, type TapeBuffer } from '../lib/tapeBuffer'
import { channels, type Market, type TradeRow } from '../types'

const ROWS = 60
// Trades kept per side (see tapeBuffer).
const KEEP = 300

// Which taker side to show; colours follow exchange convention
// (green = buyer was the taker, red = seller).
// split: buys in the top half, sells in the bottom half.
type Filter = 'all' | 'split' | 'buy' | 'sell'
const FILTER_KEY = 'marketdata.tapeSide'
const loadFilter = (): Filter => {
  try {
    const v = localStorage.getItem(FILTER_KEY)
    return v === 'split' || v === 'buy' || v === 'sell' ? v : 'all'
  } catch {
    return 'all'
  }
}

/** whale: the market's whale threshold (same as the alerts service); 0 or null: none. */
export function TradeTape({ market, whale }: { market: Market; whale: number | null }) {
  const [tape, setTape] = useState<TapeBuffer>(emptyTape)
  const [filter, setFilter] = useState<Filter>(loadFilter)
  // While the pointer is over the list it shows a frozen snapshot, so rows
  // can be read on fast markets; live trades keep arriving underneath.
  const [frozen, setFrozen] = useState<TapeBuffer | null>(null)

  useEffect(() => {
    try {
      localStorage.setItem(FILTER_KEY, filter)
    } catch {
      /* preference just won't persist */
    }
  }, [filter])

  useEffect(() => {
    let cancelled = false
    setTape(emptyTape())
    setFrozen(null)
    // History: twice the per-side size, so both sides usually start filled.
    api.trades(market, 2 * KEEP).then((rows) => !cancelled && setTape(loadTape(rows, KEEP)), () => {})
    return () => {
      cancelled = true
    }
  }, [market.exchange, market.symbol])

  useChannels([channels.trades(market)], (m) => {
    if (m.type !== 'trade') return
    const { id, price, size, side, time } = m.data
    setTape((b) => addTrade(b, { id, price, size, side, time }, KEEP))
  })

  const shown = frozen ?? tape
  const buys = useMemo(() => shown.buys.slice(0, ROWS), [shown])
  const sells = useMemo(() => shown.sells.slice(0, ROWS), [shown])
  const all = useMemo(() => allTrades(shown, ROWS), [shown])
  const rows = filter === 'buy' ? buys : filter === 'sell' ? sells : all
  // One precision for the whole column ("0.00268" and "0.002679" would not
  // line up: decimals drop trailing zeros).
  const precision = pricePrecision(all.map((t) => t.price))

  // Trades that arrived since the list was frozen.
  const newSincePause = frozen ? latestSeq(tape) - latestSeq(frozen) : 0

  const table = (list: TradeRow[], label?: string) => (
    <div className="tape-part">
      {label && <h3 className={label}>{label === 'buy' ? 'Buys' : 'Sells'} <span className="muted">{list.length}</span></h3>}
      <table>
        <thead><tr><th>Price</th><th>Size</th><th>Time</th></tr></thead>
        <tbody>
          {list.map((t) => (
            <tr key={t.id} className={`${t.side} ${whale && Number(t.price) * Number(t.size) >= whale ? 'whale' : ''}`}>
              <td>{fmtPrice(t.price, precision)}</td>
              <td>{fmtSize(t.size)}</td>
              <td className="muted">{fmtTime(t.time)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )

  return (
    <section className="panel tape">
      <h2>Trades <span className="muted">{market.exchange} {market.symbol}</span></h2>
      <div className="tape-toolbar">
        <span className="toggle">
          {(['all', 'split', 'buy', 'sell'] as Filter[]).map((f) => (
            <button key={f} className={`${f} ${f === filter ? 'active' : ''}`} onClick={() => setFilter(f)}>
              {f}
            </button>
          ))}
        </span>
        <span className="paused">{frozen ? `paused · ${newSincePause} new` : ''}</span>
      </div>
      <div className="tape-body" onMouseEnter={() => setFrozen(tape)} onMouseLeave={() => setFrozen(null)}>
        {filter === 'split' ? (
          <div className="tape-split">
            {table(buys, 'buy')}
            {table(sells, 'sell')}
          </div>
        ) : (
          table(rows)
        )}
      </div>
    </section>
  )
}
