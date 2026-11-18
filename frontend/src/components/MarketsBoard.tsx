import { useState } from 'react'
import { api } from '../lib/api'
import { fmtPrice } from '../lib/format'
import { useChannels } from '../lib/hooks'
import { channels, marketKey, type Market } from '../types'
import { AddMarket } from './AddMarket'

interface Tick {
  price: string // as sent, so its precision is kept
  prev: number
}

/** Last price of every streamed market; click to select, add or remove markets. */
export function MarketsBoard({ markets, selected, onSelect, onChanged }: {
  markets: Market[]
  selected: Market | null
  onSelect: (m: Market) => void
  onChanged: () => void
}) {
  const [ticks, setTicks] = useState<Record<string, Tick>>({})
  const [adding, setAdding] = useState(false)

  useChannels(markets.map((m) => channels.candles(m, '1s')), (m) => {
    if (m.type !== 'candle') return
    const k = marketKey(m.data)
    const price = m.data.close
    setTicks((t) => ({ ...t, [k]: { price, prev: Number(t[k]?.price ?? price) } }))
  })

  const remove = async (m: Market) => {
    await api.unsubscribe(m).catch((e: Error) => alert(e.message))
    onChanged()
  }

  return (
    <section className="panel markets">
      <h2>
        Markets <span className="muted">{markets.length}</span>
        {!adding && <button className="link" onClick={() => setAdding(true)}>+ add</button>}
      </h2>
      {adding && (
        <AddMarket
          onClose={() => setAdding(false)}
          onAdded={(m) => {
            onChanged()
            onSelect(m)
          }}
        />
      )}
      <table>
        <tbody>
          {markets.map((m) => {
            const k = marketKey(m)
            const t = ticks[k]
            const now = Number(t?.price)
            const dir = t ? (now > t.prev ? 'up' : now < t.prev ? 'down' : '') : ''
            return (
              <tr key={k} className={`clickable ${selected && marketKey(selected) === k ? 'selected' : ''}`} onClick={() => onSelect(m)}>
                <td>{m.exchange}</td>
                <td>{m.symbol}</td>
                <td className={`num ${dir}`}>{t ? fmtPrice(t.price) : <span className="muted">waiting…</span>}</td>
                <td className="actions">
                  {!m.pinned && (
                    <button className="link" title="stop streaming" onClick={(e) => { e.stopPropagation(); void remove(m) }}>×</button>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </section>
  )
}
