# RpcHub — 自研分布式 RPC 通信与治理平台

[![CI](https://github.com/diliho/RpcHub/actions/workflows/ci.yml/badge.svg)](https://github.com/diliho/RpcHub/actions/workflows/ci.yml)

基于 **C++17 + Muduo + Protobuf + ZooKeeper + Redis** 的全栈分布式 RPC 中间件，提供从底层 RPC 通信到上层 Web 控制台的完整解决方案。

### 工程化

- **CI**：GitHub Actions 三条流水线——全量构建+单测、TSan/ASan+UBSan 并发内存检查、clang-format 风格检查（`.github/workflows/ci.yml`）
- **标准 Prometheus 直方图**：`/metrics` 输出带 `le` 累积桶的 `_bucket/_sum/_count`，`histogram_quantile` P99 告警直接可用
- **trace_id 全链路**：请求头携带 trace_id，Channel 生成（重试共用）、Provider 日志关联、响应回带；Python SDK 同协议
- **K8s 部署**：`deploy/k8s/provider.yaml`，liveness/readiness 打在 `/metrics`，Prometheus 注解自动发现
- **集成测试**：`test/integration/run.sh` 真实拉起 provider 压测并校验指标链路

---

## 有什么用

| 场景 | 说明 |
|------|------|
| **微服务架构** | 将单体应用拆分为多个独立服务，通过 RPC 进行远程通信 |
| **高性能后端** | C++ 内核 + Muduo 异步网络 + 连接池，单机 QPS 2.8 万+ |
| **多语言互通** | C++ 服务端 + Python SDK，跨语言 RPC 调用开箱即用 |
| **分布式运维** | Web 控制台管理集群、配置中心、限流熔断规则、监控大盘 |
| **面试展示** | 覆盖网络编程、分布式系统、可观测性等核心后端技能 |

---

## 功能总览

### C++ RPC 框架（核心层）

| 功能 | 实现 |
|------|------|
| RPC 通信 | Protobuf 自定义二进制协议（RpcHeader + 请求体），TCP 长连接 |
| 异步网络 | Muduo Reactor 模型，多线程 EventLoop |
| 服务注册发现 | ZooKeeper 顺序临时子节点，本地缓存 30s TTL，ZK 故障降级 |
| 负载均衡 | 加权轮询 / 一致性哈希 / 最小连接数 / 普通轮询（工厂模式） |
| 故障熔断 | 滑动窗口熔断器，方法级粒度，自动探测恢复 |
| 分布式限流 | Redis + Lua 脚本，5 种算法（固定窗口/滑动日志/滑动计数器/令牌桶/漏桶） |
| 连接池 | 复用 TCP 连接，减少握手开销 |
| 内存池 | 定长内存池 + 对象池，减少频繁 malloc/free |
| 无锁队列 | 多生产者-多消费者无锁环形缓冲区 |
| 监控指标 | Redis 按分钟聚合调用量、错误率、P99/P95 延迟 |
| 三级错误码 | 框架层 1xxx / 系统层 2xxx / 业务层 3xxx |
| 异步日志 | 按日期滚动，生产者-消费者模型 |

### Python SDK（多语言支持）

| 模块 | 功能 |
|------|------|
| `mprpc.channel.RpcChannel` | 纯 Python RPC 通道，完整线协议实现 |
| `mprpc.zk_client.ZkClient` | ZooKeeper 服务发现 |
| `mprpc.metrics.MetricsCollector` | 指标收集，Prometheus 文本格式导出 |
| `mprpc.config.RpcConfig` | 配置读写 |
| `mprpc.controller.RpcController` | RPC 控制器 |
| 跨语言互通 | Python Consumer 可调用 C++ Provider，反之亦然 |

### TaskHub 异步任务引擎（内置业务）

基于 RPC 框架的分布式任务执行引擎，provider 进程内嵌三层角色：

| 层 | 职责 | 复用的框架能力 |
|----|------|--------------|
| 接入层 | `SubmitTask/QueryTask/CancelTask`，幂等键去重，快速 ack | 限流器、Redis Lua（幂等+元数据+入队单次往返） |
| 执行层 | 多线程 Worker 池（并发=线程数），SLEEP/COMPUTE/FAIL 内置任务 | 对象池、deadline 分片超时检查 |
| 接管调度层 | Coordinator（ZK 选主）：回收失联节点 pending 消息，重入队或死信 | ZK ephemeral+sequential+watch 前驱防惊群 |

- **可靠语义**：Redis Stream 消费组 + `XPENDING/XCLAIM` 接管（兼容 Redis 6.0），retries 纪元防接管后旧结果覆盖
- **状态机**：`PENDING→RUNNING→SUCCEEDED/FAILED/DEAD/CANCELED`，失败重入队耗尽即死信
- **治理闭环**：管控面改限流规则 → Redis `rules:version` → `GovernanceWatcher` 秒级热加载（增/改/删三态验证）
- **可观测**：提交/排队/执行/端到端四级 Prometheus 直方图，对账接口（Redis 终态 vs MySQL 凭证）
- **大盘**：Web 控制台「任务引擎」页——堆积/Worker 心跳/终态卡片 + 对账徽章 + Demo 提交按钮（SDK 直发真实 RPC）

### 管控面（Control Plane）

基于 FastAPI 的 RESTful 管控 API（31 个端点）：

| API | 功能 |
|-----|------|
| 集群管理 | 节点/服务/实例 CRUD，集群概览 |
| 配置中心 | 三级配置模型（global → service → method），版本管理，回滚 |
| 流量治理 | 限流规则、熔断规则 CRUD |
| 审计日志 | 操作自动记录，可按 action/resource 过滤 |
| 监控 | 聚合指标、时间序列趋势、Prometheus/Grafana 集成 |
| 链路追踪 | TraceID 查询、调用链瀑布图 |
| 告警 | AlertManager 集成、4 条默认告警规则 |

### Web 控制台（Vue3 + Element Plus + ECharts）

| 页面 | 功能 |
|------|------|
| **集群总览** | 统计卡片 + QPS/延迟/错误率折线图 + 节点列表 |
| **服务管理** | 服务 CRUD + 实例管理 |
| **流量治理** | 限流/熔断规则表格 + 新增/编辑（限流规则热下发，秒级生效） |
| **任务引擎** | 队列堆积/Worker 心跳/终态卡片 + 对账徽章 + Demo 任务提交 + 最近任务表 |
| **监控大盘** | 指标卡片 + ECharts 趋势图 + Grafana iframe |
| **链路查询** | TraceID 搜索 + 瀑布图 + 最近追踪 |
| **运维中心** | 节点管理 + 配置下发 + 审计日志 + 告警规则 |

### 分布式部署（Docker Compose，10 容器）

```
ZooKeeper  →  服务注册发现
Redis      →  限流/监控/缓存
MySQL      →  管控面状态存储
Prometheus →  指标采集（5 个 target）
Grafana    →  监控可视化
AlertManager → 告警管理
Provider × 3  →  RPC 服务节点
Control Plane → 管控面 API
```

---

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│ Web Console (Vue3)                    控制台                  │
│  http://localhost:8080/app                                   │
└──────────────────────────┬───────────────────────────────────┘
                           │ REST API
┌──────────────────────────▼───────────────────────────────────┐
│ Control Plane (FastAPI)           管控面 API                 │
│ 端口 8080 · 31 个端点 · SQLite/MySQL                        │
│ 集群管理 · 配置中心 · 流量治理 · 监控 · 链路追踪 · 告警      │
└───────┬─────────────────────────────────────┬────────────────┘
        │                                     │
        ▼                                     ▼
┌───────────────┐  ┌──────────────────────────────────────────┐
│ ZooKeeper     │  │  Redis                                   │
│ 2181          │  │  6379                                    │
│ 服务注册发现    │  │  限流 / 监控聚合 / 缓存                  │
└───────┬───────┘  └──────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│ Consumer (C++/Python)          RPC 客户端                    │
│  CallMethod()                                               │
│    ├── 1. RateLimiter 限流检查（Consumer 侧）                │
│    ├── 2. CircuitBreaker 熔断检查（方法级）                   │
│    ├── 3. 服务发现：本地缓存(30s) → ZooKeeper 回源            │
│    ├── 4. 负载均衡：加权轮询/一致性哈希/最小连接数             │
│    ├── 5. 连接池获取 TCP 连接 + 发送 RPC 请求                 │
│    └── 6. MetricsCollector 记录监控指标                      │
└────────────────────────┬─────────────────────────────────────┘
                         │ RPC 请求
                         ▼
┌──────────────────────────────────────────────────────────────┐
│ Provider (C++)                  RPC 服务端                    │
│  OnMessage()                                                │
│    ├── 1. RateLimiter 限流检查（Provider 侧，可选）           │
│    ├── 2. 反序列化 RpcHeader + 请求参数                       │
│    ├── 3. Protobuf 反射调用业务层 CallMethod()                │
│    ├── 4. 序列化响应，回写 TCP                                │
│    └── 5. MetricsCollector 记录监控指标                      │
└──────────────────────────────────────────────────────────────┘
```

---

## 快速开始

### 一键启动（本地已编译）

```bash
./start.sh
```

自动完成：检查/启动 ZooKeeper + Redis → 启动管控面 → 启动 RPC Provider → 打开 Web 控制台

访问:
- **Web 控制台**: http://localhost:8080/app
- **API 文档**: http://localhost:8080/docs
- **RPC 调用**: `./bin/consumer -i test.conf`（输出 `Login success`）

### 编译

```bash
# 安装依赖
sudo apt install cmake build-essential libboost-all-dev \
                 protobuf-compiler libprotobuf-dev \
                 libzookeeper-mt-dev libhiredis-dev

# 一键编译
bash autobuild.sh
```

### 手动启动

```bash
# 1. 启动依赖
redis-server &
# ZooKeeper: sudo systemctl start zookeeper 或 zkServer.sh start

# 2. 启动 RPC 服务端
./bin/provider -i test.conf

# 3. 调用 RPC
./bin/consumer -i test.conf         # Login → "Login success"
./bin/consumer_friend -i test.conf  # GetFriendList → friend1 friend2 friend3

# 4. 启动管控面 + Web 控制台
cd control_plane && python3 -m app.main
# 访问 http://localhost:8080/app

# 5. 性能压测
./bin/qps_test -i test.conf
```

### Python SDK 使用

```bash
cd python_sdk
pip install -e .
python -c "
from mprpc import init, RpcChannel
init('test.conf')
channel = RpcChannel()
# 调用 C++ Provider 的 Login 方法
response = channel.call('UserServiceRpc', 'Login', request_bytes)
"
```

### Docker Compose 全栈部署

```bash
cd deploy
docker-compose up -d
# 10 容器一键启动：MySQL + ZK + Redis + 管控面 + 3×Provider + Prometheus + Grafana + AlertManager
```

---

## 性能

### QPS 压测结果（并发 8，5 秒）

| 场景 | QPS | 成功率 |
|------|-----|--------|
| 单 Provider 基线 | 28,328 | 100% |
| 3 Provider ZK 模式 | 15,284 | 100% |
| ZK 宕机（缓存降级） | 14,164 | 100% |
| 故障转移后（2 Provider） | 15,867 | 100% |

### TaskHub 压测（4 并发提交，单 Provider）

| 指标 | 数值 |
|------|------|
| 提交吞吐 | 1,889 QPS（Lua 单次往返：幂等+元数据+入队） |
| ack 延迟 | P50=1.3ms / P99=16.5ms |
| 端到端延迟（100ms sleep 任务） | P50=2.7ms / P99=5.4ms |
| 故障自愈 | kill -9 主节点 → 22s 内在跑任务自动接管重跑，零丢失 |
| 对账 | Redis 终态 = MySQL 终态 [OK] |

**故障自愈演练**（2 Provider，8 个 9s 任务均衡分布后 kill 主节点）：

```
kill 前: 8 RUNNING (8000×4 + 8001×4)
kill -9 8000 → Coordinator 回收 pending（retries 纪元+1）
+23s:        8/8 SUCCEEDED on 8001，其中 4 个 retries=1（被接管重跑）
对账:        submitted=8 accounted=8 [OK]
```

**治理热下发演练**：页面下发限流 50 QPS → 2s 内双节点生效 → 压测拦截 4522 次；改 20000 → 2s 生效 → 100% 成功恢复；删规则 → enable=false。

### 单元测试

```
负载均衡器:  4/4 PASSED
连接池:      3/3 PASSED
熔断器:      5/5 PASSED
无锁队列:    3/3 PASSED
Python SDK: 11/11 PASSED
```

---

## 项目结构

```
.
├── start.sh                   # 一键启动脚本
├── autobuild.sh               # 编译脚本
├── test.conf                  # 框架配置
├── CMakeLists.txt             # 根构建配置
│
├── src/                       # C++ 核心框架
│   ├── include/               # 头文件
│   │   ├── mprpcchannel.h     # RPC 通道（客户端）
│   │   ├── mprpcprovider.h    # RPC 提供者（服务端）
│   │   ├── mprpcapplication.h # 框架入口
│   │   ├── mprpcconfig.h      # INI 配置解析
│   │   ├── mprpccontroller.h  # RPC 控制器
│   │   ├── rpcerror.h         # 三级错误码
│   │   ├── zookeeperutil.h    # ZooKeeper C API 封装
│   │   ├── redisutil.h        # Redis 客户端
│   │   ├── ratelimiter.h      # 分布式限流器
│   │   ├── metricscollector.h # 监控指标收集器
│   │   ├── logger.h / lockqueue.h  # 日志与队列
│   │   ├── circuit_breaker.h  # 滑动窗口熔断器
│   │   ├── balance/           # 负载均衡算法
│   │   ├── pool/              # 内存池 / 对象池 / 无锁队列
│   │   ├── net/               # 连接池 / 网络缓冲区
│   │   ├── registry/          # 注册中心抽象 + ZK 实现
│   │   └── prometheus/        # Prometheus 导出器
│   └── *.cc                   # 实现文件
│
├── example/                   # 使用示例
│   ├── callee/                # 服务端（UserService + FriendService）
│   ├── caller/                # 客户端 + QPS 压测
│   ├── user.proto / friend.proto
│
├── python_sdk/                # Python SDK
│   ├── mprpc/                 # pip install 包
│   │   ├── channel.py         # RPC 通道（纯 Python 线协议）
│   │   ├── zk_client.py       # ZooKeeper 服务发现
│   │   ├── metrics.py         # 指标收集器
│   │   └── config.py / controller.py / error.py / protocol.py
│   ├── bindings/              # pybind11 C++ 绑定（可选）
│   └── tests/                 # 单元测试
│
├── control_plane/             # 管控面（FastAPI）
│   ├── app/
│   │   ├── main.py            # 入口（端口 8080）
│   │   ├── models/            # SQLAlchemy 模型（7 张表）
│   │   ├── routers/           # 9 个路由模块
│   │   ├── schemas/           # Pydantic 模型
│   │   └── config.py          # 环境配置
│   └── requirements.txt
│
├── web/                       # Web 控制台（Vue3 + TypeScript）
│   ├── dist/                  # 构建产物（生产就绪）
│   └── src/
│       ├── views/             # 6 个页面
│       ├── router/            # 路由配置
│       ├── api/               # Axios 封装
│       └── stores/            # Pinia 状态管理
│
├── deploy/                    # Docker Compose 部署
│   ├── docker-compose.yml     # 10 容器
│   ├── Dockerfile.provider    # C++ Provider 镜像构建
│   ├── Dockerfile.controlplane
│   └── config/                # Prometheus / AlertManager 配置
│
├── test/unit/                 # C++ 单元测试
│   ├── test_load_balancer.cc
│   ├── test_pool.cc
│   ├── test_circuit_breaker.cc
│   └── test_lock_free_queue.cc
│
├── bin/                       # 编译产物
│   ├── provider / consumer / consumer_friend
│   └── qps_test / test_*
│
└── docxs/                     # 设计文档
    ├── redis中间件.md
    ├── 多实例部署设计.md
    ├── Zookeeper容错设计.md
    └── 项目面试.md
```

---

## 配置

编辑 `test.conf`：

```conf
rpcserverip=127.0.0.1
rpcserverport=8000
zookeeperip=127.0.0.1
zookeeperport=2181
redisip=127.0.0.1
redisport=6379

ratelimitalgorithm=3              # 0=FixedWindow 1=SlidingLog 2=SlidingCounter
                                  # 3=TokenBucket 4=LeakyBucket
ratelimitmaxqps=1000
ratelimitenableprovider=false     # Provider 侧限流开关
ratelimitenableconsumer=false     # Consumer 侧限流开关

loadbalanceralgorithm=weighted_round_robin  # round_robin / weighted_round_robin
                                            # consistent_hash / least_connections

metricsenable=true
```

---

## 依赖

| 依赖 | 用途 |
|------|------|
| CMake ≥ 3.0 | 构建系统 |
| Muduo ≥ 2.0 | 异步网络库（Reactor 模型） |
| Protobuf ≥ 3.0 | 序列化 + RPC 服务定义 |
| ZooKeeper (C API) ≥ 3.4 | 服务注册与发现 |
| hiredis ≥ 0.14 | Redis 客户端 |
| Boost ≥ 1.50 | Muduo 依赖 |
| Python 3.9+ | 管控面 / SDK |
| FastAPI / Uvicorn | 管控面 Web 框架 |
| Vue 3 / Node.js | Web 控制台 |

---

## License

MIT
