from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def insert(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    run_id: int,
    txn_id: str,
    seq_start: int,
    seq_end: int,
    event_count: int,
    status: str,
    reason: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO txn_batches(stream_id, run_id, txn_id, seq_start, seq_end, event_count, status, reason, created_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (stream_id, run_id, txn_id, seq_start, seq_end, event_count, status, reason, _now()),
    )


def list_batches(
    conn: sqlite3.Connection, *, run_id: int | None = None, limit: int = 100
) -> list[dict[str, Any]]:
    if run_id is not None:
        rows = conn.execute(
            """
            SELECT id, stream_id, run_id, txn_id, seq_start, seq_end, event_count, status, reason, created_at
            FROM txn_batches WHERE run_id=? ORDER BY id DESC LIMIT ?
            """,
            (run_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, stream_id, run_id, txn_id, seq_start, seq_end, event_count, status, reason, created_at
            FROM txn_batches ORDER BY id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]
