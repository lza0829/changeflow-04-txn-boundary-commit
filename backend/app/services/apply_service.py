from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.parse_change import parse_change_fixture
from app.modules.txn_boundary_commit import (
    BATCH_COMMIT,
    BATCH_ROLLBACK,
    BATCH_SKIP_DUPLICATE,
    REASON_APPLY_ERROR,
    classify_batch,
    group_txn_batches,
    ledger_disposition,
)
from app.repositories import cursors as cursor_repo
from app.repositories import ledger as ledger_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import runs as runs_repo
from app.repositories import settings as settings_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo
from app.repositories import txn_batches as txn_batches_repo


def preview_stream(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        path = FIXTURES_DIR / stream["fixture_path"]
        events = parse_change_fixture(path)
        return {"stream": stream, "count": len(events), "events": events[:50]}
    finally:
        conn.close()


def run_apply(stream_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")

        cursor_before = int(stream["apply_cursor"] or 0)
        gap_policy = settings_repo.get(conn, "gap_policy", "stop_quarantine") or "stop_quarantine"
        run_id = runs_repo.create_run(conn, stream_id, cursor_before)
        path = FIXTURES_DIR / stream["fixture_path"]

        try:
            events = parse_change_fixture(path)
        except Exception as exc:  # noqa: BLE001
            runs_repo.finish_run(
                conn,
                run_id,
                status="failed",
                loaded=0,
                applied=0,
                quarantined=0,
                skipped=0,
                cursor_after=cursor_before,
                report={"error": str(exc)},
            )
            conn.commit()
            raise

        loaded = len(events)
        applied = 0
        quarantined = 0
        skipped = 0
        cursor = cursor_before
        dispositions: list[str] = []
        stopped = False
        stop_reason: str | None = None
        txn = {"batch_count": 0, "committed": 0, "rolled_back": 0, "skipped": 0, "events": 0}
        batch_reports: list[dict[str, Any]] = []

        def record_batch(batch: dict[str, Any], status: str, reason: str | None) -> None:
            txn_id = batch["txn_id"]
            if not txn_id:
                return
            seqs = [int(e["seq"]) for e in batch["events"]]
            txn_batches_repo.insert(
                conn,
                stream_id=stream_id,
                run_id=run_id,
                txn_id=txn_id,
                seq_start=seqs[0],
                seq_end=seqs[-1],
                event_count=len(seqs),
                status=status,
                reason=reason,
            )
            txn["batch_count"] += 1
            txn["events"] += len(seqs)
            if status == "committed":
                txn["committed"] += 1
            elif status == "rolled_back":
                txn["rolled_back"] += 1
            else:
                txn["skipped"] += 1
            batch_reports.append(
                {
                    "txn_id": txn_id,
                    "seq_start": seqs[0],
                    "seq_end": seqs[-1],
                    "event_count": len(seqs),
                    "status": status,
                    "reason": reason,
                }
            )

        def rollback_batch(batch: dict[str, Any], disposition: str, reason: str) -> None:
            nonlocal quarantined
            for event in batch["events"]:
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=disposition
                )
                quarantine_repo.insert(
                    conn,
                    stream_id=stream_id,
                    run_id=run_id,
                    seq=int(event["seq"]),
                    entity_key=event["entity_key"],
                    reason=reason,
                    payload=event.get("payload") or {},
                )
            quarantined += len(batch["events"])
            dispositions.extend([disposition] * len(batch["events"]))
            record_batch(batch, "rolled_back", reason)

        for batch in group_txn_batches(events):
            batch_events = batch["events"]
            decision, reason = classify_batch(
                batch=batch,
                cursor=cursor,
                already_applied=lambda seq: cursor_repo.is_applied(conn, stream_id, seq),
                gap_policy=gap_policy,
            )
            disposition = ledger_disposition(batch, decision, reason)

            if decision == BATCH_SKIP_DUPLICATE:
                for event in batch_events:
                    ledger_repo.insert_ledger(
                        conn, stream_id=stream_id, run_id=run_id, event=event, disposition=disposition
                    )
                skipped += len(batch_events)
                dispositions.extend([disposition] * len(batch_events))
                record_batch(batch, "skipped_duplicate", None)
                continue

            if decision == BATCH_ROLLBACK:
                # 整批不提交：Sink 与游标保持批前，原因进隔离区与批次记录
                rollback_batch(batch, disposition, reason or REASON_APPLY_ERROR)
                stopped = True
                stop_reason = reason
                break

            # BATCH_COMMIT：整批包在一个 SAVEPOINT，任一条写失败回退到批前
            conn.execute("SAVEPOINT txn_batch")
            try:
                for event in batch_events:
                    if event["op"] == "upsert":
                        sink_repo.upsert(
                            conn,
                            stream_id=stream_id,
                            entity_key=event["entity_key"],
                            payload=event.get("payload") or {},
                            seq=int(event["seq"]),
                            event_ts=event["event_ts"],
                        )
                    else:
                        sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])
                    cursor_repo.mark_applied(conn, stream_id, int(event["seq"]), run_id)
                    ledger_repo.insert_ledger(
                        conn, stream_id=stream_id, run_id=run_id, event=event, disposition=disposition
                    )
            except Exception:  # noqa: BLE001
                conn.execute("ROLLBACK TO txn_batch")
                conn.execute("RELEASE txn_batch")
                reason = REASON_APPLY_ERROR
                rollback_batch(batch, ledger_disposition(batch, BATCH_ROLLBACK, reason), reason)
                stopped = True
                stop_reason = reason
                break
            conn.execute("RELEASE txn_batch")

            # 整批成功后才推进游标越过该批
            cursor = int(batch_events[-1]["seq"])
            streams_repo.set_cursor(conn, stream_id, cursor)
            applied += len(batch_events)
            dispositions.extend([disposition] * len(batch_events))
            record_batch(batch, "committed", None)

        status = "partial" if stopped else "succeeded"
        report = {
            "stream_id": stream_id,
            "gap_policy": gap_policy,
            "dispositions": dispositions,
            "stopped_on_gap": stopped,
            "stop_reason": stop_reason,
            "txn": {**txn, "batches": batch_reports},
            "sink_count_after": sink_repo.count_sink(conn),
            "quarantine_count_after": quarantine_repo.count_quarantine(conn),
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status=status,
            loaded=loaded,
            applied=applied,
            quarantined=quarantined,
            skipped=skipped,
            cursor_after=cursor,
            report=report,
        )
        conn.commit()
        return runs_repo.get_run(conn, run_id) or {}
    finally:
        conn.close()
