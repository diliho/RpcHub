<template>
  <el-container class="app-container">
    <el-aside width="220px" class="app-sidebar">
      <div class="logo">
        <el-icon :size="28"><Monitor /></el-icon>
        <span>Mprpc-EE</span>
      </div>
      <el-menu :default-active="route.path" router class="sidebar-menu">
        <el-menu-item index="/">
          <el-icon><DataBoard /></el-icon>
          <span>集群总览</span>
        </el-menu-item>
        <el-menu-item index="/services">
          <el-icon><Connection /></el-icon>
          <span>服务管理</span>
        </el-menu-item>
        <el-menu-item index="/governance">
          <el-icon><Setting /></el-icon>
          <span>流量治理</span>
        </el-menu-item>
        <el-menu-item index="/monitor">
          <el-icon><TrendCharts /></el-icon>
          <span>监控大盘</span>
        </el-menu-item>
        <el-menu-item index="/trace">
          <el-icon><Share /></el-icon>
          <span>链路查询</span>
        </el-menu-item>
        <el-menu-item index="/tasks">
          <el-icon><List /></el-icon>
          <span>任务引擎</span>
        </el-menu-item>
        <el-menu-item index="/ops">
          <el-icon><Tools /></el-icon>
          <span>运维中心</span>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header class="app-header">
        <span class="header-title">{{ route.meta.title || 'Mprpc-EE 控制台' }}</span>
        <div class="header-right">
          <el-tag type="success" size="small">v3.0.0</el-tag>
        </div>
      </el-header>
      <el-main class="app-main">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { useRoute } from 'vue-router'
import { Monitor, DataBoard, Connection, Setting, TrendCharts, Share, Tools, List } from '@element-plus/icons-vue'
const route = useRoute()
</script>

<style>
body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }
.app-container { height: 100vh; }
.app-sidebar {
  background: #001529;
  color: white;
  overflow-y: auto;
}
.logo {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  color: white;
  font-size: 18px;
  font-weight: bold;
  border-bottom: 1px solid #0d2137;
}
.sidebar-menu {
  border-right: none;
  background: transparent;
}
.sidebar-menu .el-menu-item {
  color: rgba(255,255,255,0.65);
}
.sidebar-menu .el-menu-item:hover,
.sidebar-menu .el-menu-item.is-active {
  color: #fff;
  background: #1890ff;
}
.app-header {
  background: #fff;
  border-bottom: 1px solid #f0f0f0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
  height: 60px;
}
.header-title { font-size: 16px; font-weight: 500; }
.app-main { background: #f5f5f5; padding: 20px; }
</style>
