import {
  CandlestickSeries, ColorType, createChart, HistogramSeries, TickMarkType,
  type IChartApi, type ISeriesApi, type Time, type UTCTimestamp,
} from 'lightweight-charts'
import { useEffect, useRef, useState } from 'react'
import { api } from '../lib/api'
import { BarBuilder, baseOf, periodSeconds, upsertBar, type Bar } from '../lib/bars'
import { pricePrecision } from '../lib/format'
import { useChannels } from '../lib/hooks'
import { channels, type Market, type Period } from '../types'

const UP = '#26a69a'
const DOWN = '#ef5350'
// Bars of history to load: ~10 min of 1s, ~12 h of 1m, capped at 2000.
const historyBars = (p: Period) => Math.min(2000, Math.max(300, Math.round(43200 / periodSeconds(p))))

// The chart library renders timestamps in UTC; show the viewer's local time
// instead, like the trade tape and exchange websites do.
const local = (t: Time) => new Date((t as number) * 1000)
const hm: Intl.DateTimeFormatOptions = { hour: '2-digit', minute: '2-digit', hour12: false }
const tickLabel = (t: Time, type: TickMarkType) => {
  const d = local(t)
  switch (type) {
    case TickMarkType.Year:
      return String(d.getFullYear())
    case TickMarkType.Month:
      return d.toLocaleDateString(undefined, { month: 'short' })
    case TickMarkType.DayOfMonth:
      return d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' })
    case TickMarkType.TimeWithSeconds:
      return d.toLocaleTimeString(undefined, { ...hm, second: '2-digit' })
    default:
      return d.toLocaleTimeString(undefined, hm)
  }
}
const crosshairLabel = (t: Time) =>
  local(t).toLocaleString(undefined, { ...hm, second: '2-digit', day: 'numeric', month: 'short' })

const toCandle = (b: Bar) => ({ time: b.time as UTCTimestamp, open: b.open, high: b.high, low: b.low, close: b.close })
const toVolume = (b: Bar) => ({
  time: b.time as UTCTimestamp,
  value: b.volume,
  color: b.close >= b.open ? `${UP}80` : `${DOWN}80`,
})

/**
 * Candles: history from the api, then live. Every trade updates the forming
 * bar immediately; the aggregator's closed candle (a few seconds later)
 * replaces it with the authoritative values.
 */
export function PriceChart({ market, period }: { market: Market; period: Period }) {
  const box = useRef<HTMLDivElement>(null)
  const chart = useRef<IChartApi>(null)
  const candles = useRef<ISeriesApi<'Candlestick'>>(null)
  const volume = useRef<ISeriesApi<'Histogram'>>(null)
  const bars = useRef<Bar[]>([])
  const builder = useRef(new BarBuilder(period))
  // Price axis precision follows the market (2 for BTC, 6 for PUMP...);
  // the chart default of 2 decimals flattens small prices to 0.00.
  const precision = useRef(2)
  const setPrecision = (prices: string[]) => {
    const p = Math.max(precision.current, pricePrecision(prices))
    if (p === precision.current) return
    precision.current = p
    candles.current?.applyOptions({ priceFormat: { type: 'price', precision: p, minMove: 10 ** -p } })
  }
  const [ready, setReady] = useState(false)
  const [empty, setEmpty] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const c = createChart(box.current!, {
      autoSize: true,
      layout: { background: { type: ColorType.Solid, color: 'transparent' }, textColor: '#9aa4b2' },
      grid: { vertLines: { color: '#1f2630' }, horzLines: { color: '#1f2630' } },
      timeScale: { timeVisible: true, secondsVisible: true, borderColor: '#2a3340', tickMarkFormatter: tickLabel },
      localization: { timeFormatter: crosshairLabel },
      rightPriceScale: { borderColor: '#2a3340' },
      crosshair: { mode: 0 },
    })
    candles.current = c.addSeries(CandlestickSeries, {
      upColor: UP, downColor: DOWN, wickUpColor: UP, wickDownColor: DOWN, borderVisible: false,
    })
    volume.current = c.addSeries(HistogramSeries, {
      priceScaleId: 'vol', priceFormat: { type: 'volume' },
      lastValueVisible: false, priceLineVisible: false, // keep the price axis for prices
    })
    c.priceScale('vol').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } })
    chart.current = c
    return () => c.remove()
  }, [])

  // History for the selected market and period.
  useEffect(() => {
    let cancelled = false
    setReady(false)
    setError(null)
    bars.current = []
    builder.current = new BarBuilder(period)
    precision.current = 0
    setPrecision([])
    candles.current?.setData([])
    volume.current?.setData([])
    chart.current?.applyOptions({ timeScale: { secondsVisible: periodSeconds(period) < 60 } })
    api.candles(market, period, historyBars(period)).then(
      (rows) => {
        if (cancelled) return
        setPrecision(rows.flatMap((r) => [r.open, r.high, r.low, r.close]))
        bars.current = builder.current.load(rows)
        setEmpty(rows.length === 0)
        candles.current?.setData(bars.current.map(toCandle))
        volume.current?.setData(bars.current.map(toVolume))
        chart.current?.timeScale().scrollToRealTime()
        setReady(true)
      },
      (e: Error) => !cancelled && setError(e.message),
    )
    return () => {
      cancelled = true
    }
  }, [market.exchange, market.symbol, period])

  const put = (bar: Bar) => {
    const last = bars.current.at(-1)
    bars.current = upsertBar(bars.current, bar)
    if (!last || bar.time >= last.time) {
      candles.current?.update(toCandle(bar))
      volume.current?.update(toVolume(bar))
    } else {
      // A closed candle for an older bucket: redraw (cheap, ~1/s).
      candles.current?.setData(bars.current.map(toCandle))
      volume.current?.setData(bars.current.map(toVolume))
    }
  }

  // Live: trades move the current bar at once; closed base candles (1s for
  // second periods, 1m otherwise) make it exact. See BarBuilder.
  useChannels(ready ? [channels.trades(market), channels.candles(market, baseOf(period))] : [], (m) => {
    if (m.type === 'trade') {
      setPrecision([m.data.price])
      const bar = builder.current.trade(m.data)
      if (bar) put(bar)
      if (empty) setEmpty(false)
    } else if (m.type === 'candle') {
      const bar = builder.current.candle(m.data)
      if (bar) put(bar)
    }
  })

  return (
    <div className="chart">
      <div ref={box} className="chart-canvas" />
      {error && <div className="overlay">history unavailable: {error}</div>}
      {!error && ready && empty && <div className="overlay muted">waiting for the first trades of {market.exchange} {market.symbol}…</div>}
    </div>
  )
}
