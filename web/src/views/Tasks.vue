<template>
  <div>
    <!-- 对账徽章 -->
    <el-alert
      :title="reconcileTitle"
      :type="summary.reconcile?.consistent ? 'success' : 'error'"
      :closable="false" show-icon style="margin-bottom: 16px"
      :description="summary.reconcile?.detail || ''"
    />

    <!-- 卡片区 -->
    <el-row :gutter="20">
      <el-col :span="4" v-for="card in cards" :key="card.label">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" :style="{ background: card.bg }">
              <el-icon :size="28" :color="card.color"><component :is="card.icon" /></el-icon>
            </div>
            <div><div class="stat-value">{{ card.value }}</div><div class="stat-label">{{ card.label }}</div></div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 16px">
      <el-col :span="14">
        <el-card>
          <template #header>
            <div class="card-header">
              <span>提交速率 / 端到端 P99</span>
              <div>
                <el-select v-model="submitType" size="small" style="width: 110px">
                  <el-option label="sleep" value="sleep" />
                  <el-option label="compute" value="compute" />
                  <el-option label="fail" value="fail" />
                </el-select>
                <el-input v-model="submitPayload" size="small" style="width: 90px; margin: 0 8px" placeholder="payload" />
                <el-button type="primary" size="small" @click="submitDemo" :loading="submitting">提交 Demo 任务</el-button>
              </div>
            </div>
          </template>
          <div ref="chartRef" style="height: 300px"></div>
        </el-card>
      </el-col>
      <el-col :span="10">
        <el-card>
          <template #header>Worker 心跳（消费组）</template>
          <el-table :data="workers" size="small" max-height="340">
            <el-table-column prop="name" label="Worker" min-width="150" />
            <el-table-column prop="pending" label="Pending" width="80" />
            <el-table-column label="空闲" width="100">
              <template #default="{ row }">{{ (row.idle_ms / 1000).toFixed(0) }}s</template>
            </el-table-column>
            <el-table-column label="状态" width="80">
              <template #default="{ row }">
                <el-tag :type="row.idle_ms < 10000 ? 'success' : 'info'" size="small">
                  {{ row.idle_ms < 10000 ? '活跃' : '空闲' }}
                </el-tag>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
    </el-row>

    <el-card style="margin-top: 16px">
      <template #header>
        <div class="card-header">
          <span>最近任务（MySQL 终态凭证）</span>
          <el-button size="small" @click="fetchAll">刷新</el-button>
        </div>
      </template>
      <el-table :data="recent" size="small" max-height="320">
        <el-table-column prop="task_id" label="TaskID" min-width="200" />
        <el-table-column label="类型" width="90">
          <template #default="{ row }">{{ typeName(row.type) }}</template>
        </el-table-column>
        <el-table-column prop="state" label="状态" width="110">
          <template #default="{ row }">
            <el-tag :type="stateTag(row.state)" size="small">{{ row.state }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="retries" label="重试" width="60" />
        <el-table-column prop="worker" label="Worker" min-width="130" />
        <el-table-column prop="result_payload" label="结果" min-width="150" show-overflow-tooltip />
        <el-table-column prop="fail_reason" label="失败原因" min-width="120" show-overflow-tooltip />
        <el-table-column label="耗时" width="90">
          <template #default="{ row }">{{ duration(row) }}</template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, markRaw } from 'vue'
import { ElMessage } from 'element-plus'
import {
  DataLine, Check, Warning, Cpu, Aim,
} from '@element-plus/icons-vue'
import * as echarts from 'echarts'
import api from '../api/client'

const summary = ref<any>({ queue_depth: { high: 0, normal: 0 }, states: {}, reconcile: {} })
const workers = ref<any[]>([])
const recent = ref<any[]>([])
const submitType = ref('sleep')
const submitPayload = ref('500')
const submitting = ref(false)
const chartRef = ref<HTMLElement>()
let chart: echarts.ECharts | null = null
let timer: number | null = null

const cards = computed(() => {
  const s = summary.value
  return [
    { label: '队列堆积 HIGH', value: s.queue_depth?.high ?? 0, icon: markRaw(DataLine), bg: '#fff7e6', color: '#fa8c16' },
    { label: '队列堆积 NORMAL', value: s.queue_depth?.normal ?? 0, icon: markRaw(DataLine), bg: '#e6f7ff', color: '#1890ff' },
    { label: 'Worker 线程', value: workers.value.length, icon: markRaw(Cpu), bg: '#f6ffed', color: '#52c41a' },
    { label: 'SUCCEEDED', value: s.states?.SUCCEEDED ?? 0, icon: markRaw(Check), bg: '#f6ffed', color: '#52c41a' },
    { label: 'DEAD', value: s.states?.DEAD ?? 0, icon: markRaw(Warning), bg: '#fff1f0', color: '#f5222d' },
    { label: '任务总数', value: s.total_tasks ?? 0, icon: markRaw(Aim), bg: '#f9f0ff', color: '#722ed1' },
  ]
})

const reconcileTitle = computed(() =>
  summary.value.reconcile?.consistent
    ? '对账一致：Redis 终态与 MySQL 凭证匹配'
    : '对账不一致：落库可能滞后或存在丢失，请检查')

const typeName = (t: string) => ({ '0': 'sleep', '1': 'compute', '2': 'fail' } as any)[t] ?? t
const stateTag = (s: string) => (s === 'SUCCEEDED' ? 'success' : s === 'DEAD' || s === 'FAILED' ? 'danger' : s === 'RUNNING' ? 'primary' : 'info')
const duration = (row: any) => {
  if (!row.finished_at || !row.submitted_at) return '-'
  return `${((row.finished_at - row.submitted_at) / 1000).toFixed(0)}s`
}

async function fetchAll() {
  try {
    const [sm, wk, rc] = await Promise.all([
      api.get('/api/v1/tasks/summary'),
      api.get('/api/v1/tasks/workers'),
      api.get('/api/v1/tasks/recent', { params: { limit: 50 } }),
    ])
    summary.value = sm.data
    // 聚合同 worker 线程（两队列出现两次）
    const agg: Record<string, any> = {}
    for (const w of wk.data) {
      const k = w.name
      agg[k] = agg[k] || { name: k, pending: 0, idle_ms: w.idle_ms }
      agg[k].pending += w.pending
      agg[k].idle_ms = Math.min(agg[k].idle_ms, w.idle_ms)
    }
    workers.value = Object.values(agg)
    recent.value = rc.data
  } catch { /* backend offline */ }
}

async function refreshChart() {
  try {
    const resp = await api.get('/api/v1/tasks/trend', { params: { minutes: 30 } })
    const d = resp.data
    if (chart && d.timestamps?.length) {
      chart.setOption({
        xAxis: { data: d.timestamps },
        series: [
          { data: d.submit_rate },
          { data: d.e2e_p99_ms },
        ],
      })
    }
  } catch { /* prometheus offline */ }
}

function initChart() {
  if (!chartRef.value) return
  chart = echarts.init(chartRef.value)
  chart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: ['提交速率/s', '端到端 P99(ms)'] },
    grid: { left: 55, right: 25, top: 40, bottom: 30 },
    xAxis: { type: 'category', data: [] },
    yAxis: [
      { type: 'value', name: 'rate' },
      { type: 'value', name: 'ms' },
    ],
    series: [
      { name: '提交速率/s', type: 'line', data: [], smooth: true, areaStyle: { opacity: 0.15 } },
      { name: '端到端 P99(ms)', type: 'line', yAxisIndex: 1, data: [], smooth: true },
    ],
  })
}

async function submitDemo() {
  submitting.value = true
  try {
    const resp = await api.post('/api/v1/tasks/submit', {
      type: submitType.value,
      payload: submitPayload.value,
      count: 1,
    })
    ElMessage.success(`已提交: ${resp.data.task_ids?.[0] ?? ''}`)
    fetchAll()
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail || '提交失败')
  } finally {
    submitting.value = false
  }
}

onMounted(() => {
  fetchAll()
  initChart()
  refreshChart()
  timer = window.setInterval(() => { fetchAll(); refreshChart() }, 10000)
})

onUnmounted(() => { if (timer) clearInterval(timer); chart?.dispose() })
</script>

<style scoped>
.stat-card { display: flex; align-items: center; gap: 12px; }
.stat-icon { width: 52px; height: 52px; border-radius: 10px; display: flex; align-items: center; justify-content: center; }
.stat-value { font-size: 24px; font-weight: 600; }
.stat-label { color: #999; font-size: 12px; }
.card-header { display: flex; justify-content: space-between; align-items: center; }
</style>
