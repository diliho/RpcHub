"""TaskHub RPC 桥：控制面经 Python SDK 直连 provider 提交/查询任务。

大盘页「提交 Demo 任务」按钮的通道——页面 → REST → SDK(TCP RPC) → provider。
channel 懒加载 + 互斥保护（RpcChannel 非线程安全，串行化提交足够）。
"""

import threading
import logging

import sys
from pathlib import Path

# python_sdk 未以包形式安装时按相对路径引入
_SDK_DIR = Path(__file__).resolve().parent.parent.parent.parent / "python_sdk"
if str(_SDK_DIR) not in sys.path:
    sys.path.insert(0, str(_SDK_DIR))

from mprpc import RpcChannel, RpcController  # noqa: E402
from mprpc import task_pb2  # noqa: E402

logger = logging.getLogger("task_bridge")

_lock = threading.Lock()
_channel = None

_TYPE_NAMES = {"sleep": task_pb2.SLEEP, "compute": task_pb2.COMPUTE, "fail": task_pb2.FAIL}


def _get_channel():
    global _channel
    if _channel is None:
        _channel = RpcChannel(recv_timeout=5.0)
    return _channel


def reset_channel():
    global _channel
    with _lock:
        _channel = None


def submit_task(task_type: str = "sleep", payload: str = "500",
                count: int = 1, priority: int = 1,
                max_retry: int = 3, idem_key: str = "") -> dict:
    """经 SDK 提交任务。count>1 时批量（同 spec 不同幂等键）。"""
    with _lock:
        ch = _get_channel()
        req = task_pb2.SubmitTaskRequest()
        spec = req.spec
        spec.type = _TYPE_NAMES.get(task_type, task_pb2.SLEEP)
        spec.payload = payload.encode()
        spec.priority = priority
        spec.timeout_ms = 10000
        spec.max_retry = max_retry
        spec.batch_count = count
        if idem_key:
            spec.idempotency_key = idem_key.encode()

        ctrl = RpcController()
        resp = task_pb2.SubmitTaskResponse()
        out = ch.call_proto("TaskServiceRpc", "SubmitTask", req, resp, ctrl)
        if out is None or ctrl.failed():
            reset_channel()
            return {"ok": False, "error": ctrl.ErrorText() or "rpc failed"}
        if resp.result.errorcode != 0:
            return {"ok": False, "error": resp.result.errormsg or "rejected"}
        return {
            "ok": True,
            "task_ids": [t.decode() for t in resp.task_ids],
            "duplicate": bool(resp.duplicate),
        }


def query_task(task_id: str) -> dict:
    with _lock:
        ch = _get_channel()
        req = task_pb2.QueryTaskRequest()
        req.task_id = task_id.encode()
        resp = task_pb2.QueryTaskResponse()
        ctrl = RpcController()
        out = ch.call_proto("TaskServiceRpc", "QueryTask", req, resp, ctrl)
        if out is None or ctrl.failed():
            reset_channel()
            return {"ok": False, "error": ctrl.ErrorText() or "rpc failed"}
        if resp.result.errorcode != 0:
            return {"ok": False, "error": resp.result.errormsg}
        return {
            "ok": True,
            "task_id": resp.task_id.decode(),
            "state": task_pb2.TaskState.Name(resp.state),
            "retries": resp.retries,
            "worker": resp.worker.decode(),
            "result_payload": resp.result_payload.decode(errors="replace"),
            "fail_reason": resp.fail_reason.decode(errors="replace"),
        }
