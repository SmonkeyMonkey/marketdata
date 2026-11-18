import { useEffect, useState, type FormEvent } from 'react'
import { api } from '../lib/api'
import { quoteOf, whaleFor, type Market, type Settings } from '../types'

/**
 * The selected market's whale threshold: trade notional (price × size) in
 * its quote currency, so XRP-BTC is set in BTC. Used by the alerts service
 * and the trade tape highlight. The default lives in the Settings dialog.
 */
export function WhaleSettings({ market, settings, onSaved, onOpenSettings }: {
  market: Market
  settings: Settings | null
  onSaved: (s: Settings) => void
  onOpenSettings: () => void
}) {
  const own = settings?.markets.find((x) => x.exchange === market.exchange && x.symbol === market.symbol)
  const current = whaleFor(settings, market) ?? ''
  const [value, setValue] = useState(current)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => setValue(current), [current, market.exchange, market.symbol])

  const save = async (e: FormEvent) => {
    e.preventDefault()
    setError(null)
    try {
      // Saving the default's value means "follow the default": storing it as
      // an override would silently freeze it when the default changes later.
      const followsDefault = settings && Number(value) === Number(settings.default_whale)
      if (followsDefault) {
        if (own) onSaved(await api.resetWhale(market))
        return
      }
      onSaved(await api.setWhale(market, value.trim()))
    } catch (err) {
      setError((err as Error).message)
    }
  }

  const reset = async () => {
    setError(null)
    try {
      onSaved(await api.resetWhale(market))
    } catch (err) {
      setError((err as Error).message)
    }
  }

  const quote = quoteOf(market.symbol)
  return (
    <div className="whale-settings">
      <h3>Whale trades</h3>
      <form onSubmit={save}>
        <label>{market.symbol} ≥</label>
        <input inputMode="decimal" value={value} onChange={(e) => setValue(e.target.value)} />
        <span className="muted unit">{quote}</span>
        <button type="submit">Save</button>
      </form>
      <p className="muted hint">
        {own
          ? <>own threshold · <button className="link" onClick={reset}>use default ({settings?.default_whale})</button></>
          : <>default for all markets · <button className="link" onClick={onOpenSettings}>change default</button></>}
        {current === '0' && ' · whale alerts off'}
      </p>
      {error && <p className="error">{error}</p>}
    </div>
  )
}
