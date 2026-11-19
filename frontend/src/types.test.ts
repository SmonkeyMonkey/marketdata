import { describe, expect, it } from 'vitest'
import { assetOf } from './types'

describe('assetOf', () => {
  it('compares stablecoin quotes as USD', () => {
    expect(assetOf('BTC-USDT')).toBe('BTC-USD')
    expect(assetOf('ETH-USDC')).toBe('ETH-USD')
    expect(assetOf('BTC-USD')).toBe('BTC-USD')
  })
  it('keeps other quotes', () => {
    expect(assetOf('ETH-BTC')).toBe('ETH-BTC')
    expect(assetOf('BTC-EUR')).toBe('BTC-EUR')
  })
})
