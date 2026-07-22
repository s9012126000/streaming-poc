import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// 3000 是 Grafana、8080 是 Trino，所以 dev server 用 5173（Vite 預設）。
// /api 走 proxy 到 gateway，順便省掉一整套 CORS 設定。
// 在 compose 內跑時 GATEWAY_URL=http://web-gateway:8000；在本機跑就走 localhost。
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.GATEWAY_URL || 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
