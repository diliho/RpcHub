import axios from 'axios'

// 同源请求（控制台由 FastAPI 同端口托管）：免 CORS/预检，连接自动复用；
// 开发模式经 vite proxy 转发。如需独立部署前端，用 VITE_API_BASE 覆盖。
const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE || '',
  timeout: 10000,
})

export default api
