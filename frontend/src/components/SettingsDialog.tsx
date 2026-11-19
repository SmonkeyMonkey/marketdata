import { useEffect, useState, type FormEvent } from 'react'
import { api } from '../lib/api'
import { quoteOf, type Market, type Settings } from '../types'
import { RulesPanel } from './RulesPanel'
import { WhaleSettings } from './WhaleSettings'

type Tab = 'rules' | 'whales'

/**
 * Alert settings: price alerts of the selected market, and whale thresholds
 * (the selected market's, the default, and every market with its own).
 *
 * Stays mounted while closed, so armed price alerts keep listening for the
 * moment they fire.
 */
export function SettingsDialog({ open, market, lastPrice, settings, onSaved, onClose }: {
  open: boolean
  market: Market
  lastPrice: string | null
  settings: Settings | null
  onSaved: (s: Settings) => void
  onClose: () => void
}) {
  const [tab, setTab] = useState<Tab>('rules')

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose])

  return (
    <div className="modal-backdrop" hidden={!open} onClick={onClose}>
      <div className="modal" role="dialog" aria-label="Settings" onClick={(e) => e.stopPropagation()}>
        <header>
          <div className="tabs">
            <button className={tab === 'rules' ? 'active' : ''} onClick={() => setTab('rules')}>Price alerts</button>
            <button className={tab === 'whales' ? 'active' : ''} onClick={() => setTab('whales')}>Whale trades</button>
          </div>
          <button className="link" onClick={onClose} aria-label="close">×</button>
        </header>
        <div hidden={tab !== 'rules'}>
          <RulesPanel market={market} lastPrice={lastPrice} />
        </div>
        {tab === 'whales' && <Whales market={market} settings={settings} onSaved={onSaved} />}
      </div>
    </div>
  )
}

function Whales({ market, settings, onSaved }: { market: Market; settings: Settings | null; onSaved: (s: Settings) => void }) {
  const [def, setDef] = useState(settings?.default_whale ?? '')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => setDef(settings?.default_whale ?? ''), [settings?.default_whale])

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
    <>
      <p className="muted">
        A trade is a whale when price × size reaches the threshold, in the market's quote currency
        (USDT for BTC-USDT, BTC for XRP-BTC). It triggers an alert and is highlighted in the trade tape.
        0 turns whale alerts off.
      </p>

      <WhaleSettings market={market} settings={settings} onSaved={onSaved} />

      <h4>Default for all markets</h4>
      <form onSubmit={saveDefault}>
        <label>≥</label>
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
        <p className="muted">None — every market uses the default.</p>
      )}
      {error && <p className="error">{error}</p>}
    </>
  )
}
