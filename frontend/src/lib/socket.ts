import type { ServerMessage } from '../types'

export type SocketState = 'connecting' | 'open' | 'reconnecting'
type Handler = (msg: ServerMessage) => void

/**
 * One websocket to the gateway shared by the whole app. Components subscribe
 * to channels with a handler; the first handler of a channel sends
 * "subscribe", the last one to leave sends "unsubscribe". After a disconnect
 * it reconnects with exponential backoff and resubscribes everything.
 */
export class MarketSocket {
  private ws?: WebSocket
  private handlers = new Map<string, Set<Handler>>()
  private stateListeners = new Set<(s: SocketState) => void>()
  private attempt = 0
  private timer?: ReturnType<typeof setTimeout>
  private stopped = false
  state: SocketState = 'connecting'

  constructor(private readonly url: string) {
    this.connect()
  }

  subscribe(channel: string, handler: Handler): () => void {
    let set = this.handlers.get(channel)
    if (!set) {
      set = new Set()
      this.handlers.set(channel, set)
      this.send({ op: 'subscribe', channels: [channel] })
    }
    set.add(handler)
    return () => {
      const s = this.handlers.get(channel)
      if (!s) return
      s.delete(handler)
      if (s.size === 0) {
        this.handlers.delete(channel)
        this.send({ op: 'unsubscribe', channels: [channel] })
      }
    }
  }

  onState(fn: (s: SocketState) => void): () => void {
    this.stateListeners.add(fn)
    fn(this.state)
    return () => this.stateListeners.delete(fn)
  }

  close() {
    this.stopped = true
    clearTimeout(this.timer)
    this.ws?.close()
  }

  private connect() {
    const ws = new WebSocket(this.url)
    this.ws = ws
    ws.onopen = () => {
      this.attempt = 0
      this.setState('open')
      const all = [...this.handlers.keys()]
      // The gateway caps subscriptions per request size; send in chunks.
      for (let i = 0; i < all.length; i += 20) this.send({ op: 'subscribe', channels: all.slice(i, i + 20) })
    }
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data as string) as ServerMessage
      if ('channel' in msg) this.handlers.get(msg.channel)?.forEach((h) => h(msg))
      else if (msg.type === 'error') console.warn('gateway:', msg.error)
    }
    ws.onclose = () => {
      if (this.stopped) return
      this.setState('reconnecting')
      const delay = Math.min(30_000, 500 * 2 ** this.attempt++) * (0.5 + Math.random() / 2)
      this.timer = setTimeout(() => this.connect(), delay)
    }
  }

  private send(v: unknown) {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(v))
    // Otherwise onopen resubscribes everything in handlers.
  }

  private setState(s: SocketState) {
    this.state = s
    this.stateListeners.forEach((fn) => fn(s))
  }
}

export const socketURL = () => `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`
