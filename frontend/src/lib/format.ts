/** Number of digits after the decimal point in a decimal string ("0.00268" -> 5). */
export const fractionDigits = (v: string) => {
  const i = v.indexOf('.')
  return i < 0 ? 0 : v.length - i - 1
}

/**
 * Display precision for a market: exchanges send prices at their tick size,
 * so the most fraction digits seen is the right precision (BTC 2, PUMP 6,
 * XRP-BTC 8). At least 2, at most 10.
 */
export const pricePrecision = (prices: string[]) =>
  Math.min(10, Math.max(2, ...prices.map(fractionDigits)))

/** Formats a price; strings keep their own precision, numbers get 5 significant digits. */
export const fmtPrice = (v: string | number, precision?: number) => {
  const n = Number(v)
  const digits =
    precision ??
    (typeof v === 'string'
      ? Math.max(2, Math.min(10, fractionDigits(v)))
      : n === 0 || Math.abs(n) >= 100 ? 2 : Math.min(10, Math.max(2, 4 - Math.floor(Math.log10(Math.abs(n))))))
  return n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

// Up to 8 decimals: Kraken sizes such as 0.00000004 must not show as 0.
export const fmtSize = (v: string | number) => Number(v).toLocaleString('en-US', { maximumFractionDigits: 8 })

// HH:MM:SS.s in local time. Every field must be listed: passing only
// fractionalSecondDigits makes the browser print just the fraction.
const timeFmt = new Intl.DateTimeFormat('en-GB', {
  hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 1, hour12: false,
})
export const fmtTime = (iso: string) => timeFmt.format(new Date(iso))

export const fmtCompact = (v: number) => v.toLocaleString('en-US', { notation: 'compact', maximumFractionDigits: 1 })
