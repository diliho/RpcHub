"""Monitor API - metrics aggregation from C++ Provider nodes.

Queries Prometheus for real-time metrics and returns aggregated views.
"""

import httpx
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..config import PROMETHEUS_URL, GRAFANA_URL

router = APIRouter(prefix="/api/v1/monitor", tags=["monitor"])


# ── Schemas ───────────────────────────────────────────────────

class NodeMetrics(BaseModel):
    node_ip: str
    qps: float = 0.0
    avg_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    error_rate: float = 0.0
    total_calls: int = 0
    total_errors: int = 0


class MetricsSummary(BaseModel):
    total_qps: float = 0.0
    avg_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    avg_error_rate: float = 0.0
    total_calls: int = 0
    nodes: List[NodeMetrics] = []
    timestamp: str = ""


class MetricsTrend(BaseModel):
    timestamps: List[str] = []
    qps_values: List[float] = []
    latency_values: List[float] = []
    error_values: List[float] = []


class GrafanaEmbed(BaseModel):
    url: str
    title: str = "Mprpc Dashboard"


# ── Prometheus query helpers ──────────────────────────────────

async def _prom_query(query: str) -> dict:
    """Execute a Prometheus instant query."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        return {"status": "error", "error": str(e), "data": {"result": []}}


async def _prom_query_range(query: str, start: str, end: str, step: str = "60s") -> dict:
    """Execute a Prometheus range query."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query_range",
                params={"query": query, "start": start, "end": end, "step": step},
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        return {"status": "error", "error": str(e), "data": {"result": []}}


# ── API Endpoints ─────────────────────────────────────────────

@router.get("/summary", response_model=MetricsSummary)
async def metrics_summary():
    """Aggregated metrics across all nodes."""
    result = await _prom_query('sum(rate(mprpc_rpc_calls_total[1m]))')
    total_qps = 0.0
    if result.get("status") == "success" and result["data"]["result"]:
        total_qps = float(result["data"]["result"][0]["value"][1])

    # 均值延迟：标准 PromQL 用 _sum/_count 的速率相除（exporter 不再输出 _avg）
    lat_result = await _prom_query(
        'sum(rate(mprpc_rpc_duration_seconds_sum[5m])) / sum(rate(mprpc_rpc_duration_seconds_count[5m])) * 1000')
    avg_latency = 0.0
    if lat_result.get("status") == "success" and lat_result["data"]["result"]:
        avg_latency = float(lat_result["data"]["result"][0]["value"][1])

    err_result = await _prom_query('sum(rate(mprpc_rpc_errors_total[1m])) / sum(rate(mprpc_rpc_calls_total[1m])) * 100')
    avg_error_rate = 0.0
    if err_result.get("status") == "success" and err_result["data"]["result"]:
        avg_error_rate = float(err_result["data"]["result"][0]["value"][1])

    calls_result = await _prom_query('sum(mprpc_rpc_calls_total)')
    total_calls = 0
    if calls_result.get("status") == "success" and calls_result["data"]["result"]:
        total_calls = int(float(calls_result["data"]["result"][0]["value"][1]))

    p99_result = await _prom_query(
        'histogram_quantile(0.99, sum(rate(mprpc_rpc_duration_seconds_bucket[5m])) by (le))')
    p99_latency = 0.0
    if p99_result.get("status") == "success" and p99_result["data"]["result"]:
        p99_latency = float(p99_result["data"]["result"][0]["value"][1]) * 1000

    return MetricsSummary(
        total_qps=round(total_qps, 2),
        avg_latency_ms=round(avg_latency, 3),
        p99_latency_ms=round(p99_latency, 3),
        avg_error_rate=round(avg_error_rate, 2),
        total_calls=total_calls,
        timestamp=datetime.utcnow().isoformat(),
    )


@router.get("/trend", response_model=MetricsTrend)
async def metrics_trend(
    minutes: int = Query(30, ge=1, le=1440),
):
    """Metrics trend over the last N minutes."""
    from datetime import timedelta
    import time

    end_ts = time.time()
    start_ts = end_ts - (minutes * 60)
    start = str(int(start_ts))
    end = str(int(end_ts))
    step = f"{max(1, minutes // 30)}m"

    timestamps = []
    qps_values = []
    latency_values = []
    error_values = []

    qps_resp = await _prom_query_range('sum(rate(mprpc_rpc_calls_total[1m]))', start, end, step)
    if qps_resp.get("status") == "success" and qps_resp["data"]["result"]:
        for ts, val in qps_resp["data"]["result"][0]["values"]:
            from datetime import datetime as dt
            timestamps.append(dt.fromtimestamp(ts).strftime("%H:%M"))
            qps_values.append(round(float(val), 2))

    lat_resp = await _prom_query_range('sum(rate(mprpc_rpc_duration_seconds_sum[5m])) / sum(rate(mprpc_rpc_duration_seconds_count[5m])) * 1000', start, end, step)
    if lat_resp.get("status") == "success" and lat_resp["data"]["result"]:
        latency_values = [round(float(v), 3) for _, v in lat_resp["data"]["result"][0]["values"]]

    err_resp = await _prom_query_range('sum(rate(mprpc_rpc_errors_total[1m])) / sum(rate(mprpc_rpc_calls_total[1m])) * 100', start, end, step)
    if err_resp.get("status") == "success" and err_resp["data"]["result"]:
        error_values = [round(float(v), 2) for _, v in err_resp["data"]["result"][0]["values"]]

    return MetricsTrend(
        timestamps=timestamps,
        qps_values=qps_values,
        latency_values=latency_values,
        error_values=error_values,
    )


@router.get("/node/{node_ip}", response_model=NodeMetrics)
async def node_metrics(node_ip: str):
    """Metrics for a specific node."""
    qps_result = await _prom_query(f'sum(rate(mprpc_rpc_calls_total{{instance=~"{node_ip}.*"}}[1m]))')
    qps = 0.0
    if qps_result.get("status") == "success" and qps_result["data"]["result"]:
        qps = float(qps_result["data"]["result"][0]["value"][1])

    lat_result = await _prom_query(f'sum(rate(mprpc_rpc_duration_seconds_sum{{instance=~"{node_ip}.*"}}[5m])) / sum(rate(mprpc_rpc_duration_seconds_count{{instance=~"{node_ip}.*"}}[5m])) * 1000')
    avg_lat = 0.0
    if lat_result.get("status") == "success" and lat_result["data"]["result"]:
        avg_lat = float(lat_result["data"]["result"][0]["value"][1])

    calls_result = await _prom_query(f'sum(mprpc_rpc_calls_total{{instance=~"{node_ip}.*"}})')
    total_calls = 0
    if calls_result.get("status") == "success" and calls_result["data"]["result"]:
        total_calls = int(float(calls_result["data"]["result"][0]["value"][1]))

    p99_result = await _prom_query(
        f'histogram_quantile(0.99, sum(rate(mprpc_rpc_duration_seconds_bucket{{instance=~"{node_ip}.*"}}[5m])) by (le))')
    p99_lat = 0.0
    if p99_result.get("status") == "success" and p99_result["data"]["result"]:
        p99_lat = float(p99_result["data"]["result"][0]["value"][1]) * 1000

    return NodeMetrics(
        node_ip=node_ip,
        qps=round(qps, 2),
        avg_latency_ms=round(avg_lat, 3),
        p99_latency_ms=round(p99_lat, 3),
        total_calls=total_calls,
    )


@router.get("/grafana/embed", response_model=GrafanaEmbed)
async def grafana_embed(dashboard_id: str = "mprpc-overview"):
    """Get Grafana embed URL for dashboard."""
    return GrafanaEmbed(
        url=f"{GRAFANA_URL}/d/{dashboard_id}?orgId=1&kiosk",
        title="Mprpc Overview Dashboard",
    )


@router.get("/prometheus/status")
async def prometheus_status():
    """Check if Prometheus is reachable."""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{PROMETHEUS_URL}/api/v1/status/buildinfo")
            if resp.status_code == 200:
                return {"status": "connected", "prometheus": PROMETHEUS_URL}
    except Exception:
        pass
    return {"status": "disconnected", "prometheus": PROMETHEUS_URL}
