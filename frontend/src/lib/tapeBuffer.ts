import type { TradeRow } from '../types'

/**
 * Recent trades kept per taker side. A single big market order fills
 * hundreds of resting orders in one burst; with one shared buffer those
 * fills would push every trade of the other side out (the sells list fell
 * to 0 during a buy burst). Each side keeps its own `keep` newest trades.
 */
export interface TapeBuffer {
  buys: Row[]
  sells: Row[]
  seq: number
}

// seq orders trades by arrival (exchange times tie within a burst).
type Row = TradeRow & { seq: number }

export const emptyTape = (): TapeBuffer => ({ buys: [], sells: [], seq: 0 })

const sideOf = (t: TradeRow): 'buys' | 'sells' => (t.side === 'sell' ? 'sells' : 'buys')

/** History from the API, newest first. */
export function loadTape(rows: TradeRow[], keep: number): TapeBuffer {
  const b = emptyTape()
  rows.forEach((t, i) => {
    const side = b[sideOf(t)]
    if (side.length < keep) side.push({ ...t, seq: -i })
  })
  return b
}

/** Adds a live trade (newest); returns the same buffer if it is a duplicate. */
export function addTrade(b: TapeBuffer, t: TradeRow, keep: number): TapeBuffer {
  const key = sideOf(t)
  if (b[key].some((x) => x.id === t.id)) return b
  const seq = b.seq + 1
  return { ...b, seq, [key]: [{ ...t, seq }, ...b[key]].slice(0, keep) }
}

/** Both sides merged, newest first. */
export function allTrades(b: TapeBuffer, limit: number): TradeRow[] {
  const out: TradeRow[] = []
  let i = 0
  let j = 0
  while (out.length < limit && (i < b.buys.length || j < b.sells.length)) {
    const buy = b.buys[i]
    const sell = b.sells[j]
    if (sell === undefined || (buy !== undefined && buy.seq > sell.seq)) {
      out.push(buy!)
      i++
    } else {
      out.push(sell)
      j++
    }
  }
  return out
}

/** Arrival index of the newest trade, to count what came after a snapshot. */
export const latestSeq = (b: TapeBuffer) => b.seq
