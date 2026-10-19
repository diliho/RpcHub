"""TaskHub 落库服务：消费 task:result Stream → MySQL task_records 表。

独立守护线程（同步 redis-py + SQLAlchemy），控制面进程内启动。
消费组 cp-persist 独立于 C++ worker 组，互不干扰；事件驱动 +
HGETALL 元数据兜底，任务状态以 Redis 为准、MySQL 为终态凭证。

事件类型（provider 侧写入）：
  succeeded / dead / retry / reclaimed / canceled
"""

import json
import threading
import time
import os
import logging
from datetime import datetime

import redis as redis_lib

from ..models.database import SessionLocal, TaskRecord

logger = logging.getLogger("task_persister")

REDIS_HOST = os.getenv("MPRPC_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("MPRPC_REDIS_PORT", "6379"))

RESULT_STREAM = "task:result"
GROUP = "cp-persist"
CONSUMER = "cp-1"

_STATE_NAMES = {"0": "PENDING", "1": "RUNNING", "2": "SUCCEEDED",
                "3": "FAILED", "4": "DEAD", "5": "CANCELED"}


def _to_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _upsert(task_id: str, item_hash: dict, event: str = ""):
    state_code = str(item_hash.get("state", ""))
    state = _STATE_NAMES.get(state_code, state_code or "UNKNOWN")
    session = SessionLocal()
    try:
        rec = session.query(TaskRecord).filter(
            TaskRecord.task_id == task_id).one_or_none()
        if rec is None:
            rec = TaskRecord(task_id=task_id)
            session.add(rec)
        rec.type = str(item_hash.get("type", "0"))
        rec.state = state
        rec.priority = str(item_hash.get("priority", "1"))
        rec.retries = _to_int(item_hash.get("retries"))
        rec.worker = str(item_hash.get("worker", ""))
        rec.submitted_at = _to_int(item_hash.get("submitted_at"))
        rec.started_at = _to_int(item_hash.get("started_at"))
        rec.finished_at = _to_int(item_hash.get("finished_at"))
        rec.result_payload = str(item_hash.get("result_payload", ""))
        rec.fail_reason = str(item_hash.get("fail_reason", ""))
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("persist task %s failed", task_id)
    finally:
        session.close()


def _run(stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT,
                                decode_responses=True, socket_timeout=3)
            try:
                r.xgroup_create(RESULT_STREAM, GROUP, id="0", mkstream=True)
            except redis_lib.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

            logger.info("task_persister started: stream=%s group=%s",
                        RESULT_STREAM, GROUP)
            while not stop_event.is_set():
                entries = r.xreadgroup(GROUP, CONSUMER, {RESULT_STREAM: ">"},
                                       count=200, block=2000)
                for _stream, msgs in entries:
                    pipe = r.pipeline()
                    for msg_id, fields in msgs:
                        task_id = fields.get("task_id", "")
                        if not task_id:
                            pipe.xack(RESULT_STREAM, GROUP, msg_id)
                            continue
                        item = r.hgetall(f"task:item:{task_id}")
                        if item:
                            _upsert(task_id, item, fields.get("event", ""))
                        else:
                            # 元数据已过期（TTL 24h）：仅事件可辨的终态落一行骨架
                            event = fields.get("event", "")
                            if event in ("succeeded", "dead", "canceled"):
                                _upsert(task_id, {"state": {"succeeded": "2", "dead": "4",
                                                            "canceled": "5"}[event],
                                                  "fail_reason": "item expired"})
                        pipe.xack(RESULT_STREAM, GROUP, msg_id)
                    pipe.execute()
        except Exception as e:
            logger.warning("task_persister redis error: %s, retry in 3s", e)
            stop_event.wait(3)


def start_persister(stop_event: threading.Event = None) -> threading.Event:
    """启动落库守护线程，返回 stop 控制事件。"""
    stop = stop_event or threading.Event()
    t = threading.Thread(target=_run, args=(stop,), daemon=True,
                         name="task-persister")
    t.start()
    return stop
