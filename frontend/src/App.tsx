import { useCallback, useEffect, useState } from 'react'
import { AlertsPanel } from './components/AlertsPanel'
import { MarketsBoard } from './components/MarketsBoard'
import { PriceChart } from './components/PriceChart'
import { RulesPanel } from './components/RulesPanel'
import { TradeTape } from './components/TradeTape'
import { SettingsDialog } from './components/SettingsDialog'
import { WhaleSettings } from './components/WhaleSettings'
import { api } from './lib/api'
import { fmtPrice } from './lib/format'
import { useLastPrice, useSocketState } from './lib/hooks'
import { marketKey, PERIODS, whaleFor, type Market, type Period, type Settings } from './types'

export function App() {
  const [markets, setMarkets] = useState<Market[]>([])
  const [market, setMarket] = useState<Market | null>(null)
  const [period, setPeriod] = useState<Period>(() => {
    const p = new URLSearchParams(location.search).get('p')
    return (PERIODS as readonly string[]).includes(p ?? '') ? (p as Period) : '1m'
  })
  const [error, setError] = useState<string | null>(null)
  const [settings, setSettings] = useState<Settings | null>(null)
  const [settingsOpen, setSettingsOpen] = useState(false)
  const state = useSocketState()
  const { price: lastPrice, precision } = useLastPrice(market)

  // The market list is shared state (another tab or user may add markets),
  // so it is refreshed after our own changes and every 10s.
  const loadMarkets = useCallback(() => {
    api.markets().then((ms) => {
      setMarkets(ms)
      setError(null)
      setMarket((cur) => {
        const want = cur ? marketKey(cur) : new URLSearchParams(location.search).get('m')
        return ms.find((m) => marketKey(m) === want)
          ?? ms.find((m) => m.exchange === 'binance' && m.symbol === 'BTC-USDT') ?? ms[0] ?? null
      })
    }, (e: Error) => setError(e.message))
    api.settings().then(setSettings, () => {})
  }, [])

  // The selected market lives in the URL (?m=binance:PUMP-USDC), so a chart
  // can be linked to and survives a reload.
  useEffect(() => {
    if (!market) return
    const url = new URL(location.href)
    url.searchParams.set('m', marketKey(market))
    url.searchParams.set('p', period)
    history.replaceState(null, '', url)
  }, [market, period])

  useEffect(() => {
    loadMarkets()
    const t = setInterval(loadMarkets, 10_000)
    return () => clearInterval(t)
  }, [loadMarkets])

  return (
    <div className="app">
      <header>
        <h1>Market Data</h1>
        {market && (
          <div className="title">
            <strong>{market.exchange} {market.symbol}</strong>
            <span className="price">{lastPrice ? fmtPrice(lastPrice, precision) : '—'}</span>
          </div>
        )}
        <div className="periods">
          {PERIODS.map((p) => (
            <button key={p} className={p === period ? 'active' : ''} onClick={() => setPeriod(p)}>{p}</button>
          ))}
        </div>
        <span className={`status ${state}`}>{state === 'open' ? 'live' : state}</span>
        <button onClick={() => setSettingsOpen(true)}>Settings</button>
      </header>
      {error && <p className="error">Cannot load markets: {error}</p>}
      {settingsOpen && <SettingsDialog settings={settings} onSaved={setSettings} onClose={() => setSettingsOpen(false)} />}
      {market && (
        <main>
          <PriceChart market={market} period={period} />
          <TradeTape market={market} whale={Number(whaleFor(settings, market) ?? 0) || null} />
          <MarketsBoard markets={markets} selected={market} onSelect={setMarket} onChanged={loadMarkets} />
          <AlertsPanel markets={markets} />
          <section className="panel rules">
            <RulesPanel market={market} lastPrice={lastPrice} />
            <WhaleSettings market={market} settings={settings} onSaved={setSettings} onOpenSettings={() => setSettingsOpen(true)} />
          </section>
        </main>
      )}
    </div>
  )
}
