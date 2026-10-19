import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  { path: '/', name: 'Dashboard', component: () => import('../views/Dashboard.vue'), meta: { title: '集群总览' } },
  { path: '/services', name: 'Services', component: () => import('../views/Services.vue'), meta: { title: '服务管理' } },
  { path: '/governance', name: 'Governance', component: () => import('../views/Governance.vue'), meta: { title: '流量治理' } },
  { path: '/monitor', name: 'Monitor', component: () => import('../views/Monitor.vue'), meta: { title: '监控大盘' } },
  { path: '/trace', name: 'Trace', component: () => import('../views/Trace.vue'), meta: { title: '链路查询' } },
  { path: '/tasks', name: 'Tasks', component: () => import('../views/Tasks.vue'), meta: { title: '任务引擎' } },
  { path: '/ops', name: 'Ops', component: () => import('../views/Ops.vue'), meta: { title: '运维中心' } },
]

const router = createRouter({ history: createWebHistory('/app/'), routes })
export default router
