import { describe, expect, it } from 'vitest'
import { fmtPrice, fmtTime, pricePrecision } from './format'

it('formats a full clock time, not just the fraction', () => {
  expect(fmtTime('2026-11-18T12:21:30.5Z')).toMatch(/^\d{2}:\d{2}:30\.5$/)
})

describe('prices', () => {
  it('takes precision from the exchange tick size seen in the data', () => {
    expect(pricePrecision(['64018.89', '64000.1'])).toBe(2)
    expect(pricePrecision(['0.002683', '0.00268'])).toBe(6) // PUMP-USDC
    expect(pricePrecision(['0.00003348'])).toBe(8) // XRP-BTC
    expect(pricePrecision(['74'])).toBe(2)
  })

  it('never rounds small prices to zero', () => {
    expect(fmtPrice('0.002683')).toBe('0.002683')
    expect(fmtPrice(0.002683)).toBe('0.0026830')
    expect(fmtPrice('64018.89')).toBe('64,018.89')
    expect(fmtPrice(64018.891)).toBe('64,018.89')
    expect(fmtPrice('0.00268', 6)).toBe('0.002680')
  })
})
