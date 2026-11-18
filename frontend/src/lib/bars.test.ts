import { describe, expect, it } from 'vitest'
import type { CandleMsg, CandleRow, TradeMsg } from '../types'
import { BarBuilder, baseOf, periodSeconds, upsertBar, type Bar } from './bars'

const trade = (time: string, price: string, size = '1', side: 'buy' | 'sell' = 'buy'): TradeMsg => ({
  exchange: 'x', symbol: 'Y', id: time, price, size, side, time, ingest_time: time,
})
const candle = (start: string, o: number, h: number, l: number, c: number, v: number): CandleMsg => ({
  exchange: 'x', symbol: 'Y', period: '1m', start, open: String(o), high: String(h), low: String(l), close: String(c),
  volume: String(v), vwap: '0', buy_volume: '0', sell_volume: '0', trades: 1,
})
const t0 = Date.parse('2026-11-18T12:00:00Z') / 1000

describe('periods', () => {
  it('maps names to seconds and base candles', () => {
    expect(periodSeconds('15s')).toBe(15)
    expect(periodSeconds('4h')).toBe(14400)
    expect(periodSeconds('1d')).toBe(86400)
    expect([baseOf('30s'), baseOf('1m'), baseOf('1d')]).toEqual(['1s', '1m', '1m'])
  })
})

describe('BarBuilder (5m from 1m candles + trades)', () => {
  it('builds the bar from trades, then base candles replace their trades', () => {
    const b = new BarBuilder('5m')
    b.trade(trade('2026-11-18T12:00:10Z', '100'))
    b.trade(trade('2026-11-18T12:00:20Z', '110'))
    let bar = b.trade(trade('2026-11-18T12:01:05Z', '90', '2'))!
    expect(bar).toMatchObject({ time: t0, open: 100, high: 110, low: 90, close: 90, volume: 4 })

    // The 12:00 candle closes: it replaces the two 12:00 trades (no double count).
    bar = b.candle(candle('2026-11-18T12:00:00Z', 100, 112, 99, 110, 2))!
    expect(bar).toMatchObject({ open: 100, high: 112, low: 90, close: 90, volume: 4 })

    // A late trade of the closed minute is ignored.
    expect(b.trade(trade('2026-11-18T12:00:59Z', '500'))).toBeNull()
  })

  it('continues a bar loaded from history without counting it twice', () => {
    const b = new BarBuilder('5m')
    const row: CandleRow = { start: '2026-11-18T12:00:00Z', open: '100', high: '105', low: '95', close: '101', volume: '10',
      vwap: '0', buy_volume: '0', sell_volume: '0', trades: 3, last_base: '2026-11-18T12:01:00Z' }
    b.load([row])
    expect(b.candle(candle('2026-11-18T12:01:00Z', 1, 1, 1, 1, 99))).toBeNull() // already in history
    const bar = b.trade(trade('2026-11-18T12:02:30Z', '120', '1'))!
    expect(bar).toMatchObject({ time: t0, open: 100, high: 120, low: 95, close: 120, volume: 11 })
  })

  it('updates the previous bar when its last candle closes after the next bar began', () => {
    const b = new BarBuilder('5m')
    b.trade(trade('2026-11-18T12:04:50Z', '100'))
    b.trade(trade('2026-11-18T12:05:01Z', '101')) // next bucket
    const prev = b.candle(candle('2026-11-18T12:04:00Z', 99, 100, 98, 100, 3))!
    expect(prev.time).toBe(t0)
    expect(prev.volume).toBe(3)
  })

  it('1s and 1m are their own base: a closed candle is the bar', () => {
    const b = new BarBuilder('1m')
    b.trade(trade('2026-11-18T12:00:10Z', '100'))
    expect(b.candle(candle('2026-11-18T12:00:00Z', 1, 2, 0.5, 1.5, 7))).toMatchObject({ open: 1, close: 1.5, volume: 7 })
  })
})

describe('upsertBar', () => {
  const bar = (time: number, close = 1): Bar => ({ time, open: 1, high: 1, low: 1, close, volume: 1, buyVolume: 0 })

  it('replaces a bar with the same time', () => {
    expect(upsertBar([bar(1), bar(2, 5)], bar(2, 7)).map((b) => b.close)).toEqual([1, 7])
  })

  it('keeps time order for appends and out-of-order inserts', () => {
    expect(upsertBar([bar(1), bar(3)], bar(2)).map((b) => b.time)).toEqual([1, 2, 3])
    expect(upsertBar([], bar(4)).map((b) => b.time)).toEqual([4])
  })
})
