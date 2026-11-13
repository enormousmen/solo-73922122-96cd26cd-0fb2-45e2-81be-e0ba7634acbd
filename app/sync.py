"""同步投递线程：按发件箱顺序把快照与增量变更 HTTP 投递给分析库。

关键保证：
- 后台守护线程执行网络 IO，不持有业务写锁（认领/标记只做极短的本地事务）
- 下游停收 / 网络不通 / 进程重启 -> 条目留在发件箱，稍后自动重试，不阻塞业务提交
- 投递带固定 delivery_id，下游幂等去重；丢回执后重投不会重复生效
- 固定轮询 + 事件唤醒，恢复后自动追平
"""

import threading
import time

import httpx

from . import db

POLL_INTERVAL = 2.0
MAX_BACKOFF = 30.0
HTTP_TIMEOUT = httpx.Timeout(8.0)


class SyncWorker(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True, name="sync-worker")
        self._wake = threading.Event()
        self._stop = threading.Event()

    def notify(self):
        self._wake.set()

    def stop(self):
        self._stop.set()
        self._wake.set()

    def run(self):
        backoff = POLL_INTERVAL
        while not self._stop.is_set():
            try:
                if self._pump():
                    backoff = POLL_INTERVAL  # 有进展，保持快节奏继续追
                    continue
            except Exception as e:  # 线程不能因意外退出
                backoff = min(backoff * 2, MAX_BACKOFF)
                print(f"[sync] 投递循环异常：{e}", flush=True)
            self._wake.wait(timeout=backoff)
            self._wake.clear()
            backoff = min(backoff * 2, MAX_BACKOFF)

    # ---------- 单轮泵送 ----------

    def _pump(self) -> bool:
        meta = db.get_sync_meta()
        if meta.get("enabled") != "1":
            return False
        entry = db.claim_next()
        if entry is None:
            return False
        url = meta.get("target_url", db.SYNC_TARGET).rstrip("/")
        endpoint = f"{url}/sync/snapshot" if entry["kind"] == "snapshot" else f"{url}/sync/changes"
        body = {"delivery_id": entry["delivery_id"], "data": entry["payload"]}
        try:
            with httpx.Client(timeout=HTTP_TIMEOUT) as client:
                resp = client.post(endpoint, json=body)
        except Exception as e:
            # 网络等待期间没有持任何业务锁；失败仅重新排队
            db.mark_failed(entry["id"], f"连接分析库失败：{e}")
            return False

        if resp.status_code == 200:
            # 注意：200 可能是真实成功，也可能是"保存后丢回执"重投命中幂等表
            # 两种情况对上游都意味着该 delivery_id 已在下游生效，标记送达即可
            db.mark_delivered(entry["id"])
            return True
        if resp.status_code == 503:
            db.mark_failed(entry["id"], "分析库已停收（503），等待恢复后自动重试")
        else:
            db.mark_failed(entry["id"], f"分析库拒绝投递（HTTP {resp.status_code}）：{resp.text[:200]}")
        return False


_worker: SyncWorker | None = None
_start_lock = threading.Lock()


def start_worker():
    global _worker
    with _start_lock:
        if _worker is None:
            db.reset_in_flight()
            db.after_commit_hook = lambda: _worker.notify() if _worker else None
            _worker = SyncWorker()
            _worker.start()
