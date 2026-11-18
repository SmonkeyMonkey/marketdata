import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

// In development the api service (make up) serves /api and proxies /ws;
// Vite only adds hot reload on top.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://localhost:8088',
      '/ws': { target: 'ws://localhost:8088', ws: true },
    },
  },
  test: { environment: 'node' },
})
