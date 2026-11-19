import { useEffect, useRef, useState } from 'react'
import { fmtPrice, fmtSize, pricePrecision } from '../lib/format'
import { useChannels } from '../lib/hooks'
import { channels, type BookMsg, type Market } from '../types'

const ROWS = 16
// Books arrive ~10 times a second; redrawing that often makes rows and bars
// flicker. Exchange UIs refresh a few times a second; so do we.
const RENDER_EVERY = 250

interface Row {
  price: string
  size: number
  total: number // cumulative from the best price
}

const rows = (levels: [string, string][]): Row[] => {
  let total = 0
  return levels.slice(0, ROWS).map(([price, size]) => {
    total += Number(size)
    return { price, size: Number(size), total }
  })
}

/**
 * L2 order book: asks above (best at the bottom, next to the spread), bids
 * below (best on top). Bars show cumulative size from the best price.
 */
export function OrderBook({ market }: { market: Market }) {
  const [book, setBook] = useState<BookMsg | null>(null)
  const latest = useRef<BookMsg | null>(null)
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null)
  // Bar scale: grows at once, shrinks slowly, so bars don't all jump when
  // the deepest level changes.
  const scale = useRef(0)

  useEffect(() => {
    setBook(null)
    latest.current = null
    scale.current = 0
    return () => {
      if (timer.current) clearTimeout(timer.current)
      timer.current = null
    }
  }, [market.exchange, market.symbol])

  useChannels([channels.book(market)], (m) => {
    if (m.type !== 'book') return
    latest.current = m.data
    timer.current ??= setTimeout(() => {
      timer.current = null
      setBook(latest.current)
    }, RENDER_EVERY)
  })

  if (!book) {
    return (
      <section className="panel book">
        <h2>Order book</h2>
        <p className="muted">waiting for the book…</p>
      </section>
    )
  }

  const asks = rows(book.asks)
  const bids = rows(book.bids)
  const deepest = Math.max(asks.at(-1)?.total ?? 0, bids.at(-1)?.total ?? 0)
  scale.current = Math.max(deepest, scale.current * 0.97)
  const max = scale.current || 1
  const precision = pricePrecision([...book.bids, ...book.asks].slice(0, 2 * ROWS).map((l) => l[0]))
  const imbalance = Number(book.imbalance)

  // Keyed by position, not price: rows update in place instead of DOM rows
  // being moved around as levels shift. Rows are divs, not table rows: the
  // depth bar is positioned inside its row, and not every browser lets a
  // <tr> contain absolutely positioned content (the bars would cover the page).
  const line = (r: Row, side: 'bid' | 'ask', i: number) => (
    <div key={`${side}${i}`} className={`book-row ${side}`}>
      <span className="depth" style={{ width: `${(100 * r.total) / max}%` }} />
      <span className="num px">{fmtPrice(r.price, precision)}</span>
      <span className="num">{fmtSize(r.size)}</span>
      <span className="num muted">{fmtSize(r.total)}</span>
    </div>
  )

  return (
    <section className="panel book">
      <h2>Order book</h2>
      <div className="book-row head muted"><span className="num">Price</span><span className="num">Size</span><span className="num">Total</span></div>
      {[...asks].reverse().map((r, i) => line(r, 'ask', i))}
      <div className="book-mid">
        <span className="num">{fmtPrice(book.mid, precision + 1)}</span>
        <span className="muted">spread {fmtPrice(book.spread, precision)} · {book.spread_bps} bps</span>
      </div>
      {bids.map((r, i) => line(r, 'bid', i))}
      <div className="imbalance" title="Size imbalance over the top 10 levels">
        <span className="bid-share" style={{ width: `${50 + 50 * imbalance}%` }} />
        <label>bids {Math.round(50 + 50 * imbalance)}% · asks {Math.round(50 - 50 * imbalance)}%</label>
      </div>
    </section>
  )
}
