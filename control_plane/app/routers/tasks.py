"""TaskHub 大盘 API：summary / trend / reconcile / workers / recent / submit。

数据源分层：
  - Redis（实时）：队列深度、Worker 消费组心跳、任务状态计数
  - MySQL（凭证）：终态落库、最近任务、对账基准
  - Prometheus（可选）：速率/延迟趋势曲线，未部署时返回空序列
"""

import os
import time
import logging
from collections import Counter
from datetime import datetime, timezone
from typing import List, Optional

import redis as redis_lib
import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..models.database import SessionLocal, TaskRecord
from ..services import task_bridge
from ..config import PROMETHEUS_URL

logger = logging.getLogger("tasks")

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])

REDIS_HOST = os.getenv("MPRPC_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("MPRPC_REDIS_PORT", "6379"))

_STATE_NAMES = {"0": "PENDING", "1": "RUNNING", "2": "SUCCEEDED",
                "3": "FAILED", "4": "DEAD", "5": "CANCELED"}
_QUEUES = ["task:queue:0", "task:queue:1"]


def _r() -> redis_lib.Redis:
    return redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT,
                           decode_responses=True, socket_timeout=2)


@router.get("/summary")
def tasks_summary():
    """大盘卡片：队列堆积 / Worker / 状态计数 / 对账徽章。"""
    r = _r()
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "queue_depth": {"high": 0, "normal": 0},
        "workers": [],
        "states": {v: 0 for v in _STATE_NAMES.values()},
        "total_tasks": 0,
        "reconcile": {"consistent": True, "detail": ""},
    }

    try:
        for prio, key in (("high", _QUEUES[0]), ("normal", _QUEUES[1])):
            summary["queue_depth"][prio] = r.xlen(key)
    except Exception as e:
        raise HTTPException(503, f"redis unavailable: {e}")

    # Worker：两个队列的消费组并集（同一 consumer id 视为同一 worker 线程）
    workers = {}
    for q in _QUEUES:
        try:
            for w in r.xinfo_consumers(q, "task:cg"):
                name = w.get("name", "")
                cur = workers.setdefault(
                    name, {"name": name, "pending": 0, "idle_ms": w.get("idle", 0)})
                cur["pending"] += w.get("pending", 0)
                cur["idle_ms"] = min(cur["idle_ms"], w.get("idle", 0))
        except redis_lib.ResponseError:
            continue  # 组未创建（无任务提交过）
    summary["workers"] = sorted(workers.values(), key=lambda x: x["name"])

    # 状态计数（SCAN task:item:*；任务量 ≤ 数万级可接受，量级增长后可改维护计数器）
    try:
        counts = Counter()
        for key in r.scan_iter(match="task:item:*", count=500):
            state = r.hget(key, "state")
            if state is None:
                continue
            counts[_STATE_NAMES.get(state, state)] += 1
        for name, c in counts.items():
            if name in summary["states"]:
                summary["states"][name] = c
        summary["total_tasks"] = sum(counts.values())
    except Exception as e:
        logger.warning("state scan failed: %s", e)

    # 对账：MySQL 终态数 vs Redis 终态数（succeeded/dead/canceled）
    terminal = ("SUCCEEDED", "DEAD", "CANCELED")
    redis_terminal = sum(summary["states"][s] for s in terminal)
    session = SessionLocal()
    try:
        mysql_terminal = session.query(TaskRecord).filter(
            TaskRecord.state.in_(terminal)).count()
    finally:
        session.close()
    summary["reconcile"] = {
        "consistent": redis_terminal == mysql_terminal,
        "detail": f"redis_terminal={redis_terminal} mysql_terminal={mysql_terminal}",
    }
    return summary


@router.get("/trend")
def tasks_trend(minutes: int = Query(30, ge=1, le=360)):
    """趋势曲线（Prometheus 可选数据源）：提交速率 / 端到端 P99。"""
    end = time.time()
    start = end - minutes * 60
    step = "15s" if minutes <= 30 else "60s"

    series = {"timestamps": [], "submit_rate": [], "e2e_p99_ms": [], "queue_depth": []}

    async def _noop():
        return series

    # 同步实现（FastAPI 线程池执行），Prometheus 不可达时静默返回空序列
    queries = {
        "submit_rate":
            'sum(rate(task_submitted_total[1m]))',
        "e2e_p99_ms":
            'histogram_quantile(0.99, sum(rate(task_e2e_seconds_bucket[1m])) by (le)) * 1000',
        "queue_depth":
            'sum(task_queue_depth)',
    }
    with httpx.Client(timeout=2.0) as client:
        for name, q in queries.items():
            try:
                resp = client.get(f"{PROMETHEUS_URL}/api/v1/query_range",
                                  params={"query": q, "start": start,
                                          "end": end, "step": step})
                data = resp.json().get("data", {}).get("result", [])
                if not data:
                    series[name] = []
                    continue
                values = data[0].get("values", [])
                series["timestamps"] = [
                    datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S")
                    for ts, _ in values]
                series[name] = [round(float(v), 3) for _, v in values]
            except Exception:
                series[name] = []
    # 时间轴对齐：以 e2e 序列为准补齐空
    if not series["timestamps"]:
        series["timestamps"] = []
    return series


@router.get("/workers")
def tasks_workers():
    """Worker 明细：消费组成员、pending、空闲时长。"""
    r = _r()
    out = []
    for q in _QUEUES:
        try:
            for w in r.xinfo_consumers(q, "task:cg"):
                out.append({
                    "queue": q,
                    "name": w.get("name", ""),
                    "pending": w.get("pending", 0),
                    "idle_ms": w.get("idle", 0),
                    "inactive": w.get("inactive", 0),
                })
        except redis_lib.ResponseError:
            continue
    return out


@router.get("/reconcile")
def tasks_reconcile():
    """对账明细：Redis 状态计数 vs MySQL 终态，逐状态差异。"""
    r = _r()
    redis_counts = Counter()
    for key in r.scan_iter(match="task:item:*", count=500):
        state = r.hget(key, "state")
        if state is not None:
            redis_counts[_STATE_NAMES.get(state, state)] += 1

    session = SessionLocal()
    try:
        mysql_counts = Counter(
            row[0] for row in session.query(TaskRecord.state).all())
    finally:
        session.close()

    states = ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "DEAD", "CANCELED"]
    rows = []
    consistent = True
    for s in states:
        rv, mv = redis_counts.get(s, 0), mysql_counts.get(s, 0)
        # 仅终态要求一致（PENDING/RUNNING 是瞬时态，落库可能滞后）
        if s in ("SUCCEEDED", "DEAD", "CANCELED") and rv != mv:
            consistent = False
        rows.append({"state": s, "redis": rv, "mysql": mv, "match": rv == mv})
    return {"consistent": consistent, "rows": rows}


@router.get("/recent", response_model=List[dict])
def tasks_recent(limit: int = Query(50, ge=1, le=500)):
    """最近任务（MySQL 终态凭证）。"""
    session = SessionLocal()
    try:
        rows = (session.query(TaskRecord)
                .order_by(TaskRecord.id.desc())
                .limit(limit).all())
        return [{
            "task_id": t.task_id, "type": t.type, "state": t.state,
            "priority": t.priority, "retries": t.retries, "worker": t.worker,
            "submitted_at": t.submitted_at, "finished_at": t.finished_at,
            "result_payload": t.result_payload, "fail_reason": t.fail_reason,
        } for t in rows]
    finally:
        session.close()


class SubmitRequest(BaseModel):
    type: str = "sleep"            # sleep / compute / fail
    payload: str = "500"
    count: int = 1
    priority: int = 1
    max_retry: int = 3
    idem_key: Optional[str] = None


@router.post("/submit")
def tasks_submit(req: SubmitRequest):
    """大盘页提交 Demo 任务（经 Python SDK 直发 provider）。"""
    result = task_bridge.submit_task(
        task_type=req.type, payload=req.payload, count=req.count,
        priority=req.priority, max_retry=req.max_retry,
        idem_key=req.idem_key or "")
    if not result.get("ok"):
        raise HTTPException(502, result.get("error", "submit failed"))
    return result


@router.get("/query/{task_id}")
def tasks_query(task_id: str):
    """单任务实时查询（经 SDK，Redis 状态为准）。"""
    result = task_bridge.query_task(task_id)
    if not result.get("ok"):
        raise HTTPException(404, result.get("error", "not found"))
    return result
