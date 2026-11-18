import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { App } from './App'
import { SocketContext } from './lib/hooks'
import { MarketSocket, socketURL } from './lib/socket'
import './styles.css'

const socket = new MarketSocket(socketURL())

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <SocketContext value={socket}>
      <App />
    </SocketContext>
  </StrictMode>,
)
