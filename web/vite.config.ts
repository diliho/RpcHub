import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  // 与 FastAPI 挂载点一致：绝对 base，深层路由刷新时资源解析稳定
  base: '/app/',
  server: {
    // 开发模式代理 /api 到管控面，生产为同源请求，无需 CORS
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
})
