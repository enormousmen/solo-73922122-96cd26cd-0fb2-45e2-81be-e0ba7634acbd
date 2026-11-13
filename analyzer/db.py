"""分析库 SQLite 数据访问层（下游）。

表：
- records：物种分布分析记录，带 version（= 上游变更 seq）做新旧守卫
- applied_deliveries：已生效投递去重（delivery_id 唯一），保证重投幂等
- state：运行开关（accepting 是否停收；lose_receipt 是否保存后丢回执；last_seq 高水位）
"""

import os
import sqlite3
import threading
from datetime import datetime

DB_PATH = os.environ.get("ANALYZER_DB_PATH", os.path.join(os.path.dirname(__file__), "..", "analyzer.db"))

LOCK = threading.RLock()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def init_db():
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with LOCK, connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                id      TEXT PRIMARY KEY,
                quadrat TEXT NOT NULL,
                species TEXT NOT NULL,
                count   INTEGER NOT NULL CHECK (count >= 0),
                version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS applied_deliveries (
                delivery_id TEXT PRIMARY KEY,
                kind        TEXT NOT NULL,
                applied_at  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        conn.execute("INSERT OR IGNORE INTO state (key, value) VALUES ('accepting', '1')")
        conn.execute("INSERT OR IGNORE INTO state (key, value) VALUES ('lose_receipt', '0')")
        conn.execute("INSERT OR IGNORE INTO state (key, value) VALUES ('last_seq', '0')")


def get_state() -> dict:
    with connect() as conn:
        rows = conn.execute("SELECT key, value FROM state").fetchall()
    return {r["key"]: r["value"] for r in rows}


def set_state(key: str, value: str):
    with LOCK, connect() as conn:
        conn.execute(
            "INSERT INTO state (key, value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


class GapError(Exception):
    def __init__(self, got: int, expected: int):
        self.got = got
        self.expected = expected
        super().__init__(f"变更序列不连续：期望 seq={expected}，实际从 {got} 开始；请先投递缺失批次")


def apply_snapshot(delivery_id: str, payload: dict) -> dict:
    """整批应用全量快照。同事务写入去重记录与高水位，失败整体回滚。"""
    records = payload["records"]
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(
                "SELECT 1 FROM applied_deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone():
                conn.rollback()
                return {"duplicate": True, "applied": 0}
            conn.execute("DELETE FROM records")
            for r in records:
                conn.execute(
                    "INSERT INTO records (id, quadrat, species, count, version) VALUES (?,?,?,?,?)",
                    (r["id"], r["quadrat"], r["species"], r["count"], r["version"]),
                )
            conn.execute(
                "INSERT INTO state (key, value) VALUES ('last_seq', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(payload.get("seq", 0)),),
            )
            conn.execute(
                "INSERT INTO applied_deliveries (delivery_id, kind, applied_at) VALUES (?, 'snapshot', ?)",
                (delivery_id, _now()),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {"duplicate": False, "applied": len(records)}


def apply_changes(delivery_id: str, changes: list) -> dict:
    """按提交顺序整批应用增量变更。

    - 同一 delivery_id 重投：幂等返回，不重复生效
    - 高水位 last_seq 守卫：只应用新高水位之后且连续的变更；缺口/乱序拒绝（GapError）
    - 已应用区间内的旧投递整体跳过，不覆盖新值、不复活同 ID 已删除/重建的记录
    """
    if not changes:
        raise ValueError("变更批次为空")
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(
                "SELECT 1 FROM applied_deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone():
                conn.rollback()
                return {"duplicate": True, "applied": 0, "skipped_stale": 0}

            last_seq = int(
                conn.execute("SELECT value FROM state WHERE key='last_seq'").fetchone()["value"]
            )
            ordered = sorted(changes, key=lambda x: x["seq"])
            # 允许与已投递区间重叠（FIFO 重试、整批与单条交错都会产生重叠）：
            # 高水位及以下的变更一律跳过；真正的缺口（起点跳号）才拒绝。
            fresh = [c for c in ordered if c["seq"] > last_seq]
            if not fresh:
                conn.execute(
                    "INSERT INTO applied_deliveries (delivery_id, kind, applied_at) VALUES (?, 'changes', ?)",
                    (delivery_id, _now()),
                )
                conn.commit()
                return {"duplicate": False, "applied": 0, "skipped_stale": len(ordered)}
            if fresh[0]["seq"] != last_seq + 1:
                raise GapError(fresh[0]["seq"], last_seq + 1)
            prev = last_seq
            for c in fresh:  # 新区间内部必须连续
                if c["seq"] != prev + 1:
                    raise GapError(c["seq"], prev + 1)
                prev = c["seq"]

            applied = 0
            skipped = len(ordered) - len(fresh)
            # 高水位已保证 fresh 区间内每个 seq 都是严格新于本地状态的新变更，
            # 按 seq 顺序直接应用即可（含回退到旧值、同 ID 删除后重建等合法情形）。
            for c in fresh:
                if c["kind"] == "upsert":
                    r = c["record"]
                    conn.execute(
                        "INSERT INTO records (id, quadrat, species, count, version) VALUES (?,?,?,?,?) "
                        "ON CONFLICT(id) DO UPDATE SET quadrat=excluded.quadrat, species=excluded.species, "
                        "count=excluded.count, version=excluded.version",
                        (c["id"], r["quadrat"], r["species"], r["count"], c["version"]),
                    )
                    applied += 1
                elif c["kind"] == "delete":
                    conn.execute("DELETE FROM records WHERE id=?", (c["id"],))
                    applied += 1
                else:
                    raise ValueError(f"未知变更类型：{c['kind']}")

            conn.execute(
                "INSERT INTO state (key, value) VALUES ('last_seq', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(ordered[-1]["seq"]),),
            )
            conn.execute(
                "INSERT INTO applied_deliveries (delivery_id, kind, applied_at) VALUES (?, 'changes', ?)",
                (delivery_id, _now()),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {"duplicate": False, "applied": applied, "skipped_stale": skipped}


def get_records() -> list:
    with connect() as conn:
        rows = conn.execute("SELECT * FROM records ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def list_deliveries(limit: int = 100) -> list:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM applied_deliveries ORDER BY rowid DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def max_version() -> int:
    with connect() as conn:
        return conn.execute("SELECT COALESCE(MAX(version), 0) AS m FROM records").fetchone()["m"]
