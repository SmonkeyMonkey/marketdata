import type { BasePeriod, CandleMsg, CandleRow, Period, TradeMsg } from '../types'

// A chart bar; time is the bucket start in unix seconds.
export interface Bar {
  time: number
  open: number
  high: number
  low: number
  close: number
  volume: number
  buyVolume: number
}

const UNIT: Record<string, number> = { s: 1, m: 60, h: 3600, d: 86400 }
export const periodSeconds = (p: Period) => Number(p.slice(0, -1)) * UNIT[p.slice(-1)]!
export const baseOf = (p: Period): BasePeriod => (periodSeconds(p) < 60 ? '1s' : '1m')

const sec = (iso: string) => Math.floor(Date.parse(iso) / 1000)
const floorTo = (t: number, step: number) => Math.floor(t / step) * step

export function barFromCandle(c: Pick<CandleRow, 'start' | 'open' | 'high' | 'low' | 'close' | 'volume' | 'buy_volume'>): Bar {
  return {
    time: sec(c.start),
    open: Number(c.open),
    high: Number(c.high),
    low: Number(c.low),
    close: Number(c.close),
    volume: Number(c.volume),
    buyVolume: Number(c.buy_volume),
  }
}

/** Joins two consecutive parts of one bar (a before b). */
export function combine(a: Bar | undefined, b: Bar | undefined): Bar | undefined {
  if (!a) return b
  if (!b) return a
  return {
    time: a.time,
    open: a.open,
    high: Math.max(a.high, b.high),
    low: Math.min(a.low, b.low),
    close: b.close,
    volume: a.volume + b.volume,
    buyVolume: a.buyVolume + b.buyVolume,
  }
}

function addTrade(b: Bar | undefined, time: number, t: TradeMsg): Bar {
  const p = Number(t.price)
  const v = Number(t.size)
  const buy = t.side === 'buy' ? v : 0
  if (!b) return { time, open: p, high: p, low: p, close: p, volume: v, buyVolume: buy }
  return { ...b, high: Math.max(b.high, p), low: Math.min(b.low, p), close: p, volume: b.volume + v, buyVolume: b.buyVolume + buy }
}

interface Bucket {
  closed?: Bar // rollup of the base candles received so far
  lastBase: number // start of the newest base candle in `closed` (-1: none)
  pending: Map<number, Bar> // live trades per base interval not closed yet
}

/**
 * Builds bars of any period live from two inputs: closed base candles (1s or
 * 1m, authoritative) and trades (immediate). A bar is the rollup of its
 * closed base candles plus the trades of base intervals that have not closed
 * yet; when a base candle arrives it replaces the trades of its interval, so
 * nothing is counted twice and nothing between history and live is lost.
 */
export class BarBuilder {
  readonly step: number
  readonly baseStep: number
  private buckets = new Map<number, Bucket>()

  constructor(readonly period: Period) {
    this.step = periodSeconds(period)
    this.baseStep = periodSeconds(baseOf(period))
  }

  /** History from the api (oldest first); the newest two bars stay open for updates. */
  load(rows: CandleRow[]): Bar[] {
    this.buckets.clear()
    for (const r of rows.slice(-2)) {
      this.buckets.set(sec(r.start), { closed: barFromCandle(r), lastBase: sec(r.last_base), pending: new Map() })
    }
    return rows.map(barFromCandle)
  }

  /** Returns the updated bar, or null for a trade already covered by a closed candle. */
  trade(t: TradeMsg): Bar | null {
    const time = sec(t.time)
    const base = floorTo(time, this.baseStep)
    const b = this.bucket(floorTo(time, this.step))
    if (base <= b.lastBase) return null
    b.pending.set(base, addTrade(b.pending.get(base), floorTo(time, this.step), t))
    return this.bar(floorTo(time, this.step))
  }

  candle(c: CandleMsg): Bar | null {
    const base = sec(c.start)
    const start = floorTo(base, this.step)
    const b = this.bucket(start)
    if (base <= b.lastBase) return null // already rolled up
    b.closed = combine(b.closed, { ...barFromCandle(c), time: start })
    b.lastBase = base
    for (const k of b.pending.keys()) if (k <= base) b.pending.delete(k)
    return this.bar(start)
  }

  private bucket(start: number): Bucket {
    let b = this.buckets.get(start)
    if (!b) {
      b = { lastBase: -1, pending: new Map() }
      this.buckets.set(start, b)
      // Keep the current and the previous bucket (its last base candle can
      // close after the next bucket started).
      const keys = [...this.buckets.keys()].sort((x, y) => y - x)
      for (const k of keys.slice(2)) this.buckets.delete(k)
    }
    return b
  }

  private bar(start: number): Bar {
    const b = this.buckets.get(start)!
    let bar = b.closed
    for (const k of [...b.pending.keys()].sort((x, y) => x - y)) bar = combine(bar, b.pending.get(k))
    return { ...bar!, time: start }
  }
}

/** Inserts or replaces the bar with the same time, keeping time order. */
export function upsertBar(bars: Bar[], bar: Bar): Bar[] {
  const i = bars.findLastIndex((b) => b.time <= bar.time)
  if (i >= 0 && bars[i]!.time === bar.time) {
    const next = bars.slice()
    next[i] = bar
    return next
  }
  return [...bars.slice(0, i + 1), bar, ...bars.slice(i + 1)]
}
