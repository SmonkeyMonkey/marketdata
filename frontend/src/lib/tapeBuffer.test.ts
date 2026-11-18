import { describe, expect, it } from 'vitest'
import type { TradeRow } from '../types'
import { addTrade, allTrades, emptyTape, loadTape } from './tapeBuffer'

const t = (id: number, side: 'buy' | 'sell'): TradeRow => ({ id: String(id), price: '1', size: '1', side, time: '2026-11-18T12:00:00Z' })

describe('tape buffer', () => {
  it('a burst of one side does not evict the other side', () => {
    let b = emptyTape()
    for (let i = 0; i < 20; i++) b = addTrade(b, t(i, 'sell'), 300)
    for (let i = 100; i < 600; i++) b = addTrade(b, t(i, 'buy'), 300) // big market buy: 500 fills
    expect(b.sells).toHaveLength(20)
    expect(b.buys).toHaveLength(300)
  })

  it('merges both sides newest first by arrival', () => {
    let b = emptyTape()
    b = addTrade(b, t(1, 'buy'), 10)
    b = addTrade(b, t(2, 'sell'), 10)
    b = addTrade(b, t(3, 'buy'), 10)
    expect(allTrades(b, 10).map((x) => x.id)).toEqual(['3', '2', '1'])
    expect(allTrades(b, 2).map((x) => x.id)).toEqual(['3', '2'])
  })

  it('history goes below live trades and duplicates are ignored', () => {
    let b = loadTape([t(10, 'sell'), t(9, 'buy')], 10) // API: newest first
    b = addTrade(b, t(11, 'buy'), 10)
    expect(addTrade(b, t(11, 'buy'), 10)).toBe(b)
    expect(allTrades(b, 10).map((x) => x.id)).toEqual(['11', '10', '9'])
  })
})
