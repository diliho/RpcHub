#!/bin/bash
# 集成测试：真实拉起 provider，循环调用 consumer，校验成功率与 /metrics 指标
#
# 前置条件：
#   - ZooKeeper (2181) 与 Redis (6379) 已启动，或先执行：
#       docker compose -f deploy/docker-compose.yml up -d zookeeper redis
#   - 已完成 cmake 构建且 bin/provider、bin/consumer 存在
# 用法：
#   ./test/integration/run.sh [调用次数，默认 50]

set -e

PROJECT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
CALLS=${1:-50}
CONF=$PROJECT_DIR/test.conf
PROVIDER_LOG=/tmp/mprpc_provider_it.log
TODAY_LOG="$PROJECT_DIR/$(date +%Y-%m-%d)-log.txt"
BEFORE_LOG_LINES=$([ -f "$TODAY_LOG" ] && wc -l < "$TODAY_LOG" || echo 0)

echo ">>> [1/4] Check binaries"
[ -x "$PROJECT_DIR/bin/provider" ] || { echo "bin/provider not found, run cmake build first"; exit 1; }
[ -x "$PROJECT_DIR/bin/consumer" ] || { echo "bin/consumer not found"; exit 1; }

echo ">>> [2/4] Start provider (log: $PROVIDER_LOG)"
"$PROJECT_DIR/bin/provider" -i "$CONF" > "$PROVIDER_LOG" 2>&1 &
PROVIDER_PID=$!
trap 'kill $PROVIDER_PID 2>/dev/null || true' EXIT

# 等待 provider 就绪（最多 10s）
for i in $(seq 1 20); do
    if grep -q "RpcProvider start service" "$PROVIDER_LOG" 2>/dev/null; then
        break
    fi
    sleep 0.5
done

echo ">>> [3/4] Run $CALLS RPC calls"
SUCCESS=0
for i in $(seq 1 "$CALLS"); do
    if "$PROJECT_DIR/bin/consumer" -i "$CONF" > /dev/null 2>&1; then
        SUCCESS=$((SUCCESS + 1))
    fi
done

echo ">>> [4/4] Verify results"
echo "    calls=$CALLS success=$SUCCESS"

if [ "$SUCCESS" -ne "$CALLS" ]; then
    echo "FAIL: success rate $SUCCESS/$CALLS < 100%"
    tail -20 "$PROVIDER_LOG"
    exit 1
fi

# 校验 /metrics 指标链路（trace_id 落日志 + exporter 输出）
# Logger 按日期写文件而非 stdout，取本次运行新增的行来校验
TODAY_LOG="$PROJECT_DIR/$(date +%Y-%m-%d)-log.txt"
if [ -f "$TODAY_LOG" ] && tail -n "+$((BEFORE_LOG_LINES + 1))" "$TODAY_LOG" | grep -q "trace_id="; then
    echo "    trace_id logged: OK"
else
    echo "    WARN: no trace_id found in provider log"
fi

METRICS_PORT=$(grep -oP '(?<=^metricsexporterport=).*' "$CONF" | head -1)
METRICS_PORT=${METRICS_PORT:-9091}
METRICS=$(curl -s --max-time 3 "http://127.0.0.1:$METRICS_PORT/metrics" || true)
if echo "$METRICS" | grep -q "mprpc_rpc_duration_seconds_count"; then
    echo "    /metrics histogram: OK"
else
    echo "    WARN: /metrics has no histogram data (exporter not on $METRICS_PORT?)"
fi

echo "Integration test PASSED"
