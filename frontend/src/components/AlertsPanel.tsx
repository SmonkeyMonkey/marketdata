import { useEffect, useState } from 'react'
import { api } from '../lib/api'
import { fmtTime } from '../lib/format'
import { useChannels } from '../lib/hooks'
import { channels, type AlertRow, type Market } from '../types'

const LABEL = { price_rule: 'rule', whale_trade: 'whale', price_move: 'move', arbitrage: 'arb' } as const

export function AlertsPanel({ markets }: { markets: Market[] }) {
  const [alerts, setAlerts] = useState<AlertRow[]>([])

  useEffect(() => {
    api.alerts(30).then(setAlerts, () => {})
  }, [])

  useChannels(markets.map(channels.alerts), (m) => {
    if (m.type !== 'alert') return
    const a = m.data
    setAlerts((prev) => (prev.some((x) => x.id === a.id) ? prev : [a, ...prev].slice(0, 50)))
  })

  return (
    <section className="panel alerts">
      <h2>Alerts</h2>
      {alerts.length === 0 && <p className="muted">No alerts yet.</p>}
      <ul>
        {alerts.map((a) => (
          <li key={a.id}>
            <span className={`badge ${a.type}`}>{LABEL[a.type]}</span>
            <span className="muted">{fmtTime(a.time)}</span> {a.message}
          </li>
        ))}
      </ul>
    </section>
  )
}
