"""SQLite 数据访问层。单文件库。

表：
- records/baselines/previews：业务数据（原有）
- changes：变更日志，seq 全局单调递增，同步的唯一事实来源
- outbox：事务发件箱，与业务写入同事务提交；后台线程按序投递给分析库
- sync_meta：同步开关与配置（键值）
"""

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "survey.db"))
SYNC_TARGET = os.environ.get("SYNC_TARGET", "http://127.0.0.1:8001")

# 所有写操作与确认合并共用同一把锁，保证"检查-应用"之间不被插入
LOCK = threading.RLock()

# 业务写入提交后的回调（由 sync 线程注册，用于唤醒投递线程），网络 IO 不在锁内
after_commit_hook = None


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")  # 多进程（含重启接管）并发更稳
    return conn


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _wake():
    hook = after_commit_hook
    if hook:
        try:
            hook()
        except Exception:
            pass


def init_db(seed_csv: str | None = None):
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with LOCK, connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                id      TEXT PRIMARY KEY,
                quadrat TEXT NOT NULL,
                species TEXT NOT NULL,
                count   INTEGER NOT NULL CHECK (count >= 0)
            );
            CREATE TABLE IF NOT EXISTS baselines (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                created_at TEXT NOT NULL,
                data       TEXT NOT NULL          -- JSON: {id: record}
            );
            CREATE TABLE IF NOT EXISTS previews (
                token       TEXT PRIMARY KEY,
                baseline_id INTEGER NOT NULL,
                created_at  TEXT NOT NULL,
                status      TEXT NOT NULL,        -- pending/applied/discarded
                message     TEXT NOT NULL DEFAULT '',
                revision    TEXT NOT NULL,        -- JSON: {id: record}
                snapshot    TEXT NOT NULL,        -- JSON: 预览时涉及记录的当前值 {id: record|null}
                plan        TEXT NOT NULL         -- JSON: compute_plan 的结果
            );
            CREATE TABLE IF NOT EXISTS changes (
                seq       INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        TEXT NOT NULL,
                batch_id  TEXT NOT NULL,          -- 同一次提交（尤其整批合并）共享
                kind      TEXT NOT NULL,          -- upsert / delete
                record_id TEXT NOT NULL,
                payload   TEXT                    -- JSON 记录内容；delete 为 NULL
            );
            CREATE TABLE IF NOT EXISTS outbox (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                delivery_id     TEXT NOT NULL UNIQUE,
                batch_id        TEXT NOT NULL,
                kind            TEXT NOT NULL,    -- snapshot / changes
                seq_start       INTEGER,          -- snapshot 为 NULL
                seq_end         INTEGER,
                payload         TEXT NOT NULL,    -- JSON：快照或变更条目列表
                created_at      TEXT NOT NULL,
                status          TEXT NOT NULL,    -- queued/in_flight/delivered/failed
                attempts        INTEGER NOT NULL DEFAULT 0,
                last_error      TEXT NOT NULL DEFAULT '',
                last_attempt_at TEXT NOT NULL DEFAULT '',
                delivered_at    TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS sync_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        empty = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"] == 0
    if empty and seed_csv and os.path.exists(seed_csv):
        from .csvio import parse_csv

        with open(seed_csv, encoding="utf-8") as f:
            seed = parse_csv(f.read())
        with LOCK, connect() as conn:
            for rid, r in seed.items():
                conn.execute(
                    "INSERT OR IGNORE INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                    (rid, r["quadrat"], r["species"], r["count"]),
                )
    # 上次进程退出时可能遗留在途状态，启动时全部重新排队
    reset_in_flight()


# ---------- 同步元数据 / 变更捕获 ----------


def get_sync_meta() -> dict:
    defaults = {"enabled": "0", "capturing": "0", "target_url": SYNC_TARGET}
    with connect() as conn:
        rows = conn.execute("SELECT key, value FROM sync_meta").fetchall()
    defaults.update({r["key"]: r["value"] for r in rows})
    return defaults


def _meta_dict(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT key, value FROM sync_meta").fetchall()
    return {r["key"]: r["value"] for r in rows}


def _set_meta(conn: sqlite3.Connection, key: str, value: str):
    conn.execute(
        "INSERT INTO sync_meta (key, value) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _capture(conn: sqlite3.Connection, batch_id: str, kind: str, rid: str, record) -> int:
    """写入一条变更日志并返回其 seq。调用方必须已持有事务与 LOCK。"""
    cur = conn.execute(
        "INSERT INTO changes (ts, batch_id, kind, record_id, payload) VALUES (?,?,?,?,?)",
        (now(), batch_id, kind, rid, json.dumps(record, ensure_ascii=False) if record is not None else None),
    )
    return cur.lastrowid


def _enqueue_changes(conn: sqlite3.Connection, batch_id: str, items: list):
    """把同一事务内产生的变更条目打包成一个发件箱条目（整批可见）。

    items: [(kind, rid, record_dict|None), ...]，顺序即提交顺序。
    """
    changes = []
    for kind, rid, record in items:
        seq = _capture(conn, batch_id, kind, rid, record)
        changes.append(
            {
                "seq": seq,
                "batch_id": batch_id,
                "kind": kind,
                "id": rid,
                "record": record,
                "version": seq,  # 记录版本 = 最近一次触及它的变更 seq
            }
        )
    conn.execute(
        "INSERT INTO outbox (delivery_id, batch_id, kind, seq_start, seq_end, payload, created_at, status)"
        " VALUES (?,?,?,?,?,?,?, 'queued')",
        (
            uuid.uuid4().hex,
            batch_id,
            "changes",
            changes[0]["seq"],
            changes[-1]["seq"],
            json.dumps(changes, ensure_ascii=False),
            now(),
        ),
    )


def _capturing(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM sync_meta WHERE key='capturing'").fetchone()
    return bool(row and row["value"] == "1")


def enable_sync() -> bool:
    """启用同步。首次启用在同一事务内产出全量快照发件箱；再次启用只续传。

    返回 True 表示本次新启用，False 表示原本已启用。
    """
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            meta = _meta_dict(conn)
            if meta.get("enabled") == "1":
                conn.rollback()
                return False
            _set_meta(conn, "enabled", "1")
            _set_meta(conn, "capturing", "1")  # 启用后即便停用也继续记录，保证续传不漏
            _set_meta(conn, "target_url", meta.get("target_url") or SYNC_TARGET)

            has_snapshot = conn.execute("SELECT 1 FROM outbox WHERE kind='snapshot' LIMIT 1").fetchone()
            if not has_snapshot:
                # 同一时点的全量记录：版本取各记录最近变更 seq（首次启用时 changes 为空 => 0）
                max_seq = conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM changes").fetchone()["m"]
                rows = conn.execute("SELECT * FROM records ORDER BY id").fetchall()
                snap_records = []
                for row in rows:
                    ver = conn.execute(
                        "SELECT COALESCE(MAX(seq), 0) AS v FROM changes WHERE record_id=?",
                        (row["id"],),
                    ).fetchone()["v"]
                    snap_records.append(
                        {
                            "id": row["id"],
                            "quadrat": row["quadrat"],
                            "species": row["species"],
                            "count": row["count"],
                            "version": ver,
                        }
                    )
                conn.execute(
                    "INSERT INTO outbox (delivery_id, batch_id, kind, seq_start, seq_end, payload, created_at, status)"
                    " VALUES (?,?,?,?,?,?,?, 'queued')",
                    (
                        uuid.uuid4().hex,
                        "snapshot",
                        "snapshot",
                        None,
                        max_seq,
                        json.dumps({"seq": max_seq, "records": snap_records}, ensure_ascii=False),
                        now(),
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    _wake()
    return True


def disable_sync():
    with LOCK, connect() as conn:
        conn.execute(
            "INSERT INTO sync_meta (key, value) VALUES ('enabled','0') "
            "ON CONFLICT(key) DO UPDATE SET value='0'"
        )
    _wake()


# ---------- 当前记录 ----------


def get_records() -> dict:
    with connect() as conn:
        rows = conn.execute("SELECT * FROM records ORDER BY id").fetchall()
    return {row["id"]: {"quadrat": row["quadrat"], "species": row["species"], "count": row["count"]} for row in rows}


def get_records_by_ids(ids) -> dict:
    """返回 {id: record|None}，不存在的 ID 值为 None。"""
    out = {}
    with connect() as conn:
        for rid in ids:
            row = conn.execute("SELECT * FROM records WHERE id = ?", (rid,)).fetchone()
            out[rid] = (
                {"quadrat": row["quadrat"], "species": row["species"], "count": row["count"]}
                if row
                else None
            )
    return out


def add_record(rid, quadrat, species, count):
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                (rid, quadrat, species, count),
            )
            if _capturing(conn):
                _enqueue_changes(conn, uuid.uuid4().hex, [("upsert", rid, {
                    "quadrat": quadrat, "species": species, "count": count,
                })])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    _wake()


def update_record(rid, quadrat, species, count) -> bool:
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?",
                (quadrat, species, count, rid),
            )
            ok = cur.rowcount > 0
            if ok and _capturing(conn):
                _enqueue_changes(conn, uuid.uuid4().hex, [("upsert", rid, {
                    "quadrat": quadrat, "species": species, "count": count,
                })])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    _wake()
    return ok


def delete_record(rid) -> bool:
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute("DELETE FROM records WHERE id=?", (rid,))
            ok = cur.rowcount > 0
            if ok and _capturing(conn):
                _enqueue_changes(conn, uuid.uuid4().hex, [("delete", rid, None)])
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    _wake()
    return ok


# ---------- 基线 ----------


def save_baseline(name: str, data: dict) -> int:
    with LOCK, connect() as conn:
        cur = conn.execute(
            "INSERT INTO baselines (name, created_at, data) VALUES (?,?,?)",
            (name, now(), json.dumps(data, ensure_ascii=False)),
        )
        return cur.lastrowid


def list_baselines() -> list:
    with connect() as conn:
        rows = conn.execute("SELECT id, name, created_at, data FROM baselines ORDER BY id DESC").fetchall()
    return [
        {"id": r["id"], "name": r["name"], "created_at": r["created_at"], "size": len(json.loads(r["data"]))}
        for r in rows
    ]


def get_baseline(bid: int):
    with connect() as conn:
        row = conn.execute("SELECT * FROM baselines WHERE id=?", (bid,)).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "name": row["name"],
        "created_at": row["created_at"],
        "data": json.loads(row["data"]),
    }


# ---------- 合并预览 ----------


def save_preview(token, baseline_id, revision, snapshot, plan):
    with LOCK, connect() as conn:
        conn.execute(
            "INSERT INTO previews (token, baseline_id, created_at, status, message, revision, snapshot, plan)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                token,
                baseline_id,
                now(),
                "pending",
                "",
                json.dumps(revision, ensure_ascii=False),
                json.dumps(snapshot, ensure_ascii=False),
                json.dumps(plan, ensure_ascii=False),
            ),
        )


def get_preview(token):
    with connect() as conn:
        row = conn.execute("SELECT * FROM previews WHERE token=?", (token,)).fetchone()
    if not row:
        return None
    return {
        "token": row["token"],
        "baseline_id": row["baseline_id"],
        "created_at": row["created_at"],
        "status": row["status"],
        "message": row["message"],
        "revision": json.loads(row["revision"]),
        "snapshot": json.loads(row["snapshot"]),
        "plan": json.loads(row["plan"]),
    }


def list_previews() -> list:
    with connect() as conn:
        rows = conn.execute(
            "SELECT token, baseline_id, created_at, status, message FROM previews ORDER BY created_at DESC, token DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def update_preview_plan(token, snapshot, plan, message):
    """过期预览被重新比较后，用新计划与新快照替换旧内容。"""
    with LOCK, connect() as conn:
        conn.execute(
            "UPDATE previews SET snapshot=?, plan=?, message=?, status='pending', created_at=? WHERE token=?",
            (json.dumps(snapshot, ensure_ascii=False), json.dumps(plan, ensure_ascii=False), message, now(), token),
        )


def set_preview_status(token, status, message=""):
    with LOCK, connect() as conn:
        conn.execute("UPDATE previews SET status=?, message=? WHERE token=?", (status, message, token))


def apply_actions(actions) -> int:
    """单事务应用最终动作并记录同步变更；任何失败整体回滚，当前数据与发件箱均不变。

    整批动作共享一个 batch_id，打包为同一个发件箱条目，下游要么整批可见、要么看不到。
    返回应用的动作数。
    """
    batch_id = uuid.uuid4().hex
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            captured = []  # (kind, id, record|None)
            for a in actions:
                if a["action"] == "insert":
                    r = a["record"]
                    conn.execute(
                        "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                        (a["id"], r["quadrat"], r["species"], r["count"]),
                    )
                    captured.append(("upsert", a["id"], dict(r)))
                elif a["action"] == "update":
                    r = a["to"]
                    conn.execute(
                        "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?",
                        (r["quadrat"], r["species"], r["count"], a["id"]),
                    )
                    captured.append(("upsert", a["id"], dict(r)))
                elif a["action"] == "delete":
                    conn.execute("DELETE FROM records WHERE id=?", (a["id"],))
                    captured.append(("delete", a["id"], None))
                else:
                    raise ValueError(f"未知动作：{a['action']}")
            if captured and _capturing(conn):
                _enqueue_changes(conn, batch_id, captured)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    _wake()
    return len(actions)


# ---------- 发件箱 / 投递状态 ----------


def reset_in_flight():
    """进程启动时把在途条目重新排队（恢复或重启后自动追平）。"""
    with LOCK, connect() as conn:
        conn.execute(
            "UPDATE outbox SET status='queued', last_error=last_error || ' [重启重新排队]' "
            "WHERE status='in_flight'"
        )


def outbox_counts() -> dict:
    with connect() as conn:
        rows = conn.execute("SELECT status, COUNT(*) AS n FROM outbox GROUP BY status").fetchall()
    out = {"queued": 0, "in_flight": 0, "delivered": 0, "failed": 0}
    for r in rows:
        out[r["status"]] = r["n"]
    return out


def list_outbox(limit: int = 100) -> list:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM outbox ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    result = []
    for r in rows:
        payload = json.loads(r["payload"])
        size = len(payload["records"]) if r["kind"] == "snapshot" else len(payload)
        result.append(
            {
                "id": r["id"],
                "delivery_id": r["delivery_id"],
                "kind": r["kind"],
                "batch_id": r["batch_id"],
                "seq_start": r["seq_start"],
                "seq_end": r["seq_end"],
                "size": size,
                "created_at": r["created_at"],
                "status": r["status"],
                "attempts": r["attempts"],
                "last_error": r["last_error"],
                "last_attempt_at": r["last_attempt_at"],
                "delivered_at": r["delivered_at"],
            }
        )
    return result


def claim_next() -> dict | None:
    """认领最早一条待投递条目并置为 in_flight。无则返回 None。"""
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM outbox WHERE status='queued' ORDER BY id LIMIT 1"
            ).fetchone()
            if not row:
                conn.rollback()
                return None
            conn.execute(
                "UPDATE outbox SET status='in_flight', attempts=attempts+1, last_attempt_at=? WHERE id=?",
                (now(), row["id"]),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "id": row["id"],
        "delivery_id": row["delivery_id"],
        "kind": row["kind"],
        "payload": json.loads(row["payload"]),
    }


def mark_delivered(entry_id: int):
    with LOCK, connect() as conn:
        conn.execute(
            "UPDATE outbox SET status='delivered', last_error='', delivered_at=? WHERE id=?",
            (now(), entry_id),
        )


def mark_failed(entry_id: int, error: str):
    with LOCK, connect() as conn:
        conn.execute(
            "UPDATE outbox SET status='queued', last_error=?, last_attempt_at=? WHERE id=?",
            (error[:500], now(), entry_id),
        )


def sync_overview() -> dict:
    meta = get_sync_meta()
    with connect() as conn:
        oldest = conn.execute(
            "SELECT created_at FROM outbox WHERE status IN ('queued','in_flight') ORDER BY id LIMIT 1"
        ).fetchone()
        last_delivered = conn.execute(
            "SELECT delivered_at FROM outbox WHERE status='delivered' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return {
        "meta": meta,
        "counts": outbox_counts(),
        "oldest_pending_at": oldest["created_at"] if oldest else "",
        "last_delivered_at": last_delivered["delivered_at"] if last_delivered else "",
    }
