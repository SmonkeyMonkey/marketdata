import { createContext, useContext, useEffect, useLayoutEffect, useRef, useState } from 'react'
import { channels, type Market, type ServerMessage } from '../types'
import { pricePrecision } from './format'
import type { MarketSocket, SocketState } from './socket'

export const SocketContext = createContext<MarketSocket | null>(null)

export function useSocket(): MarketSocket {
  const s = useContext(SocketContext)
  if (!s) throw new Error('useSocket outside SocketContext')
  return s
}

/** Subscribes to channels for the component's lifetime; the latest handler is always used. */
export function useChannels(chans: string[], handler: (msg: ServerMessage) => void) {
  const socket = useSocket()
  const ref = useRef(handler)
  useLayoutEffect(() => {
    ref.current = handler
  })
  const key = chans.join('|')
  useEffect(() => {
    if (!key) return
    const offs = key.split('|').map((ch) => socket.subscribe(ch, (m) => ref.current(m)))
    return () => offs.forEach((off) => off())
  }, [socket, key])
}

export function useSocketState(): SocketState {
  const socket = useSocket()
  const [state, setState] = useState<SocketState>(socket.state)
  useEffect(() => socket.onState(setState), [socket])
  return state
}

/**
 * Latest traded price of a market and the precision seen so far (decimals
 * drop trailing zeros, so "0.00266" alone would look less precise than
 * "0.002662" a moment earlier).
 */
export function useLastPrice(market: Market | null): { price: string | null; precision: number } {
  const [state, setState] = useState<{ price: string | null; precision: number }>({ price: null, precision: 2 })
  useEffect(() => setState({ price: null, precision: 2 }), [market?.exchange, market?.symbol])
  useChannels(market ? [channels.trades(market)] : [], (m) => {
    if (m.type === 'trade') {
      const price = m.data.price
      setState((s) => ({ price, precision: Math.max(s.precision, pricePrecision([price])) }))
    }
  })
  return state
}
