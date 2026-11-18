import { useEffect, useState, type FormEvent } from 'react'
import { api } from '../lib/api'
import { quoteOf, type Settings } from '../types'

/**
 * Global settings: the default whale threshold and every market that has
 * its own. Thresholds are trade notional (price × size) in each market's
 * quote currency.
 */
export function SettingsDialog({ settings, onSaved, onClose }: {
  settings: Settings | null
  onSaved: (s: Settings) => void
  onClose: () => void
}) {
  const [def, setDef] = useState(settings?.default_whale ?? '')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const run = async (p: Promise<Settings>) => {
    setError(null)
    try {
      onSaved(await p)
    } catch (err) {
      setError((err as Error).message)
    }
  }

  const saveDefault = (e: FormEvent) => {
    e.preventDefault()
    void run(api.setWhale(null, def.trim()))
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" role="dialog" aria-label="Settings" onClick={(e) => e.stopPropagation()}>
        <header>
          <h2>Settings</h2>
          <button className="link" onClick={onClose} aria-label="close">×</button>
        </header>

        <h3>Whale trades</h3>
        <p className="muted">
          A trade is a whale when price × size reaches the threshold, in the market's quote currency
          (USDT for BTC-USDT, BTC for XRP-BTC). It triggers an alert and is highlighted in the trade tape.
          0 turns whale alerts off.
        </p>

        <form onSubmit={saveDefault}>
          <label>Default for all markets ≥</label>
          <input inputMode="decimal" value={def} onChange={(e) => setDef(e.target.value)} />
          <span className="muted">quote</span>
          <button type="submit">Save</button>
        </form>

        <h4>Markets with their own threshold</h4>
        {settings?.markets.length ? (
          <table>
            <tbody>
              {settings.markets.map((m) => (
                <tr key={`${m.exchange}:${m.symbol}`}>
                  <td>{m.exchange}</td>
                  <td>{m.symbol}</td>
                  <td className="num">≥ {Number(m.whale_notional).toLocaleString('en-US', { maximumFractionDigits: 10 })} {quoteOf(m.symbol)}</td>
                  <td className="actions">
                    <button className="link" onClick={() => void run(api.resetWhale(m))}>use default</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="muted">None — every market uses the default. Set one in the market's panel.</p>
        )}
        {error && <p className="error">{error}</p>}
      </div>
    </div>
  )
}
