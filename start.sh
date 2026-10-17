#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
WEB_URL="http://localhost:8080/app"

cleanup() {
    echo ""
    echo "Shutting down..."
    [ -n "$CP_PID" ] && kill "$CP_PID" 2>/dev/null
    [ -n "$PROVIDER_PID" ] && kill "$PROVIDER_PID" 2>/dev/null
    # 兜底：清理仍占用 8080 的所有进程（uvicorn reload 双进程等残留）
    for pid in $(ss -tlnp 2>/dev/null | grep ':8080' | grep -oP 'pid=\K[0-9]+' | sort -u); do
        kill "$pid" 2>/dev/null
    done
    pkill -f "bin/provider" 2>/dev/null || true
    wait 2>/dev/null
    exit 0
}
trap cleanup SIGINT SIGTERM

# 杀掉占用端口的所有进程并等待端口释放（避免 Address already in use）
kill_port_listeners() {
    local port=$1 pids
    pids=$(ss -tlnp 2>/dev/null | grep ":$port" | grep -oP 'pid=\K[0-9]+' | sort -u)
    if [ -z "$pids" ]; then return 0; fi
    echo "Killing processes on port $port: $pids"
    kill $pids 2>/dev/null
    for i in $(seq 1 10); do
        ss -tln 2>/dev/null | grep -q ":$port " || return 0
        sleep 0.5
    done
    echo "Port $port still busy, force killing..."
    pids=$(ss -tlnp 2>/dev/null | grep ":$port" | grep -oP 'pid=\K[0-9]+' | sort -u)
    [ -n "$pids" ] && kill -9 $pids 2>/dev/null
    sleep 1
}

echo "=== Mprpc-EE Quick Start ==="

# ── Redis ──
if redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "[OK] Redis already running"
else
    echo "Starting Redis..."
    redis-server --daemonize yes
    sleep 1
    echo "[OK] Redis started"
fi

# ── ZooKeeper ──
# 用 bash /dev/tcp 探测：OpenBSD nc 半关闭不可靠，srvr 应答约一半概率丢失
zk_alive() {
    timeout 2 bash -c 'exec 3<>/dev/tcp/127.0.0.1/2181 && printf "srvr\n" >&3 && cat <&3' 2>/dev/null | grep -q "Mode:"
}

if zk_alive; then
    echo "[OK] ZooKeeper already running"
else
    echo "Starting ZooKeeper via Docker..."
    docker rm -f zookeeper 2>/dev/null || true
    if ! docker run -d --name zookeeper -p 2181:2181 zookeeper:3.7 >/dev/null 2>&1; then
        echo "[WARN] Docker ZooKeeper failed to start (native ZK may be running)"
    fi
    for i in $(seq 1 15); do
        if zk_alive; then
            break
        fi
        sleep 1
    done
    if zk_alive; then
        echo "[OK] ZooKeeper started"
    else
        echo "[WARN] ZooKeeper health check timed out, but might still be starting"
    fi
fi

# ── Prometheus（项目独立实例，与宿主机 snap 9090 共存于 9099）──
if ! ss -tln 2>/dev/null | grep -q ':9099 '; then
    echo "Starting project Prometheus (port 9099)..."
    PROM_CONF="$PROJECT_DIR/deploy/config/prometheus-standalone.yml"
    PROM_RULES="$PROJECT_DIR/deploy/config/rules.yml"
    docker rm -f mprpc-prometheus 2>/dev/null || true
    docker run -d --name mprpc-prometheus --network host \
        -v "$PROM_CONF":/etc/prometheus/prometheus.yml \
        -v "$PROM_RULES":/etc/prometheus/rules.yml \
        prom/prometheus:latest \
        --config.file=/etc/prometheus/prometheus.yml \
        --web.listen-address=0.0.0.0:9099 >/dev/null 2>&1 \
        && echo "[OK] Prometheus started" \
        || echo "[WARN] Prometheus container failed (monitor charts need it)"
fi
# 控制面查 Prometheus 的地址（已设置则尊重现有值）
export MPRPC_PROMETHEUS_URL="${MPRPC_PROMETHEUS_URL:-http://127.0.0.1:9099}"

# ── Control Plane ──
# /app 由 FastAPI 挂载 web/dist，dist 缺失（首次运行/gitignore）时自动构建
if [ ! -d "$PROJECT_DIR/web/dist" ]; then
    echo "web/dist missing, building frontend..."
    if [ ! -d "$PROJECT_DIR/web/node_modules" ]; then
        (cd "$PROJECT_DIR/web" && npm ci --no-audit --no-fund)
    fi
    (cd "$PROJECT_DIR/web" && npm run build)
fi
kill_port_listeners 8080
echo "Starting Control Plane..."
cd "$PROJECT_DIR/control_plane"
python3 -m app.main &
CP_PID=$!
# 等待端口就绪，失败则退出（set -e 不捕获后台进程的错误）
for i in $(seq 1 20); do
    ss -tln 2>/dev/null | grep -q ':8080 ' && break
    sleep 0.5
done
if ! ss -tln 2>/dev/null | grep -q ':8080 '; then
    echo "[ERROR] Control Plane failed to start on 8080"
    exit 1
fi
echo "[OK] Control Plane started (pid=$CP_PID)"

# ── RPC Provider ──
echo "Starting RPC Provider..."
cd "$PROJECT_DIR"
./bin/provider -i test.conf > /tmp/provider.log 2>&1 &
PROVIDER_PID=$!
sleep 1
if ! kill -0 "$PROVIDER_PID" 2>/dev/null; then
    echo "[ERROR] RPC Provider failed to start, see /tmp/provider.log"
    exit 1
fi
echo "[OK] RPC Provider started (pid=$PROVIDER_PID)"

echo ""
echo "====================================="
echo "  Web Console:  $WEB_URL"
echo "  API Docs:     http://localhost:8080/docs"
echo "  Press Ctrl+C to stop all services"
echo "====================================="
echo ""

# Open browser if available
if command -v xdg-open &>/dev/null; then
    xdg-open "$WEB_URL" 2>/dev/null || true
elif command -v sensible-browser &>/dev/null; then
    sensible-browser "$WEB_URL" 2>/dev/null || true
fi

wait
