import json

import pytest

import app.db as db
from app import seed
from app.db import connect
from app.engines.parse_change import parse_change_fixture
from app.modules.txn_boundary_commit import (
    BATCH_COMMIT,
    BATCH_ROLLBACK,
    BATCH_SKIP_DUPLICATE,
    REASON_APPLY_ERROR,
    REASON_GAP_BEFORE,
    REASON_GAP_WITHIN,
    REASON_MISSING_MARKER,
    REASON_PARTIAL_REPLAY,
    classify_batch,
    group_txn_batches,
    ledger_disposition,
)
from app.repositories import ledger as ledger_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo
from app.repositories import txn_batches as txn_batches_repo
from app.services import apply_service


def ev(seq, txn_id=None, is_last=False):
    return {
        "seq": seq,
        "op": "upsert",
        "entity_key": f"E{seq}",
        "event_ts": "t",
        "payload": {},
        "txn_id": txn_id,
        "is_last": is_last,
    }


def batch(txn_id, *events, has_last=True):
    return {"txn_id": txn_id, "events": list(events), "has_last": has_last}


# ---------- 引擎：分批 ----------


def test_group_batches_singletons_and_txns():
    batches = group_txn_batches(
        [ev(1), ev(2, "T1"), ev(3, "T1", True), ev(4, "T2", True)]
    )
    assert [b["txn_id"] for b in batches] == [None, "T1", "T2"]
    assert [len(b["events"]) for b in batches] == [1, 2, 1]
    assert batches[1]["has_last"] is True


def test_group_unclosed_txn_when_next_txn_starts():
    batches = group_txn_batches([ev(1, "T1"), ev(2, "T2", True)])
    assert batches[0]["txn_id"] == "T1" and batches[0]["has_last"] is False


# ---------- 引擎：批次裁决互斥 ----------


def test_classify_commit():
    b = batch("T1", ev(2, "T1"), ev(3, "T1", True))
    decision, reason = classify_batch(batch=b, cursor=1, already_applied=lambda s: False)
    assert (decision, reason) == (BATCH_COMMIT, None)


def test_classify_gap_within_txn_rolls_back():
    b = batch("T1", ev(2, "T1"), ev(4, "T1", True))
    decision, reason = classify_batch(batch=b, cursor=1, already_applied=lambda s: False)
    assert (decision, reason) == (BATCH_ROLLBACK, REASON_GAP_WITHIN)


def test_classify_gap_before_batch_rolls_back():
    b = batch("T1", ev(5, "T1"), ev(6, "T1", True))
    decision, reason = classify_batch(batch=b, cursor=2, already_applied=lambda s: False)
    assert (decision, reason) == (BATCH_ROLLBACK, REASON_GAP_BEFORE)


def test_classify_missing_commit_marker_rolls_back():
    b = batch("T1", ev(2, "T1"), has_last=False)
    decision, reason = classify_batch(batch=b, cursor=1, already_applied=lambda s: False)
    assert (decision, reason) == (BATCH_ROLLBACK, REASON_MISSING_MARKER)


def test_classify_partial_replay_rolls_back():
    b = batch("T1", ev(2, "T1"), ev(3, "T1", True))
    decision, reason = classify_batch(batch=b, cursor=2, already_applied=lambda s: s == 2)
    assert (decision, reason) == (BATCH_ROLLBACK, REASON_PARTIAL_REPLAY)


def test_classify_skip_duplicate_when_fully_applied():
    b = batch("T1", ev(2, "T1"), ev(3, "T1", True))
    decision, _ = classify_batch(batch=b, cursor=3, already_applied=lambda s: True)
    assert decision == BATCH_SKIP_DUPLICATE


def test_ledger_disposition_mapping():
    b_txn = batch("T1", ev(1, "T1", True))
    b_single = batch(None, ev(1))
    assert ledger_disposition(b_txn, BATCH_COMMIT) == "txn_applied"
    assert ledger_disposition(b_single, BATCH_COMMIT) == "applied"
    assert ledger_disposition(b_txn, BATCH_ROLLBACK, REASON_GAP_WITHIN) == "txn_rolled_back"
    assert ledger_disposition(b_single, BATCH_ROLLBACK, REASON_GAP_BEFORE) == "quarantine_gap"
    assert ledger_disposition(b_single, BATCH_SKIP_DUPLICATE) == "skipped_duplicate"


# ---------- 解析：事务边界字段 ----------


def test_parse_txn_fields(tmp_path):
    p = tmp_path / "f.jsonl"
    p.write_text(
        json.dumps(
            {
                "seq": 1,
                "op": "upsert",
                "entity_key": "A",
                "event_ts": "t",
                "txn_id": "T1",
                "is_last": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    events = parse_change_fixture(p)
    assert events[0]["txn_id"] == "T1"
    assert events[0]["is_last"] is True


def test_parse_defaults_without_txn_fields(tmp_path):
    p = tmp_path / "f.jsonl"
    p.write_text(
        json.dumps({"seq": 1, "op": "upsert", "entity_key": "A", "event_ts": "t"}) + "\n",
        encoding="utf-8",
    )
    events = parse_change_fixture(p)
    assert events[0]["txn_id"] is None
    assert events[0]["is_last"] is False


def test_parse_is_last_without_txn_id_rejected(tmp_path):
    p = tmp_path / "f.jsonl"
    p.write_text(
        json.dumps(
            {"seq": 1, "op": "upsert", "entity_key": "A", "event_ts": "t", "is_last": True}
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="is_last without txn_id"):
        parse_change_fixture(p)


# ---------- 服务：整批提交 / 整批回滚对账 ----------


@pytest.fixture()
def fresh_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    seed.init_db()
    yield


def _stream_id(name):
    conn = connect()
    try:
        for s in streams_repo.list_streams(conn):
            if s["name"] == name:
                return s["id"]
    finally:
        conn.close()
    raise AssertionError(f"stream not seeded: {name}")


def test_clean_stream_commits_whole_batches(fresh_db):
    sid = _stream_id("Orders TXN CDC")
    run = apply_service.run_apply(sid)
    assert run["status"] == "succeeded"
    assert run["applied_count"] == 6
    assert run["cursor_before"] == 0
    assert run["cursor_after"] == 6
    txn = run["report"]["txn"]
    assert txn["batch_count"] == 3
    assert txn["committed"] == 3
    assert txn["rolled_back"] == 0
    assert txn["events"] == 5  # T-100×2 + T-101×2 + T-102×1；单条 seq1 不计入

    conn = connect()
    try:
        # Sink：ORD-5003 被 T-101 批内 delete；ORD-5002 为 T-101 paid 态
        sink = {r["entity_key"]: r for r in sink_repo.list_sink(conn) if r["stream_id"] == sid}
        assert set(sink) == {"ORD-5001", "ORD-5002", "ORD-5004"}
        assert sink["ORD-5002"]["payload"]["status"] == "paid"
        assert sink["ORD-5002"]["last_seq"] == 4
        # 账本：5 条整批提交 + 1 条单条成功，txn_id 可追溯
        led = [r for r in ledger_repo.list_ledger(conn) if r["stream_id"] == sid]
        assert sum(1 for r in led if r["disposition"] == "txn_applied") == 5
        assert sum(1 for r in led if r["disposition"] == "applied") == 1
        assert {r["txn_id"] for r in led if r["disposition"] == "txn_applied"} == {
            "T-100",
            "T-101",
            "T-102",
        }
        # 批次表持久化（刷新后仍在），与报告同源
        batches = txn_batches_repo.list_batches(conn, run_id=run["id"])
        assert len(batches) == 3
        assert all(b["status"] == "committed" for b in batches)
        assert sum(b["event_count"] for b in batches) == txn["events"]
        # 游标与已应用 seq 集合对账
        stream = streams_repo.get_stream(conn, sid)
        assert stream["apply_cursor"] == 6
    finally:
        conn.close()


def test_broken_stream_rolls_back_whole_batch(fresh_db):
    sid = _stream_id("Orders TXN Broken CDC")
    run = apply_service.run_apply(sid)
    assert run["status"] == "partial"
    assert run["applied_count"] == 2  # 仅 T-900 整批提交
    assert run["quarantined_count"] == 2  # T-901 两条全进隔离
    assert run["cursor_after"] == 2  # 游标停在批前
    txn = run["report"]["txn"]
    assert txn["batch_count"] == 2
    assert txn["committed"] == 1
    assert txn["rolled_back"] == 1
    assert txn["events"] == 4
    assert run["report"]["stop_reason"] == REASON_GAP_WITHIN

    conn = connect()
    try:
        # Sink 只剩 T-900；T-901 的 ORD-9003 不得出现（禁止前半段提交）
        keys = {r["entity_key"] for r in sink_repo.list_sink(conn) if r["stream_id"] == sid}
        assert keys == {"ORD-9001", "ORD-9002"}
        # 隔离区两条，原因批内缺口
        q = [r for r in quarantine_repo.list_quarantine(conn) if r["stream_id"] == sid]
        assert {r["seq"] for r in q} == {3, 5}
        assert all(r["reason"] == REASON_GAP_WITHIN for r in q)
        # 账本：2 条整批提交 + 2 条整批回滚，与报告计数互洽
        led = [r for r in ledger_repo.list_ledger(conn) if r["stream_id"] == sid]
        dispo = {}
        for r in led:
            dispo[r["disposition"]] = dispo.get(r["disposition"], 0) + 1
        assert dispo == {"txn_applied": 2, "txn_rolled_back": 2}
        # 批次表：T-900 committed / T-901 rolled_back
        batches = {b["txn_id"]: b for b in txn_batches_repo.list_batches(conn, run_id=run["id"])}
        assert batches["T-900"]["status"] == "committed"
        assert batches["T-901"]["status"] == "rolled_back"
        assert batches["T-901"]["reason"] == REASON_GAP_WITHIN
        # 游标与 applied_seqs 对账：只有 1、2
        stream = streams_repo.get_stream(conn, sid)
        assert stream["apply_cursor"] == 2
    finally:
        conn.close()


def test_mid_batch_sink_error_rolls_back_to_batch_start(fresh_db, monkeypatch):
    """批内第二条写 Sink 出错：整批回退到批前，游标不得越过该批。"""
    sid = _stream_id("Orders TXN CDC")
    orig_upsert = sink_repo.upsert

    def boom(conn, **kwargs):
        if kwargs["entity_key"] == "ORD-5003":
            raise RuntimeError("sink boom")
        return orig_upsert(conn, **kwargs)

    monkeypatch.setattr(sink_repo, "upsert", boom)
    run = apply_service.run_apply(sid)
    assert run["status"] == "partial"
    assert run["applied_count"] == 1  # 仅单条 seq1
    assert run["cursor_after"] == 1
    txn = run["report"]["txn"]
    assert txn["committed"] == 0
    assert txn["rolled_back"] == 1

    conn = connect()
    try:
        # T-100 批内 ORD-5002 不得残留（SAVEPOINT 回退），applied_seqs 不得含 2
        keys = {r["entity_key"] for r in sink_repo.list_sink(conn) if r["stream_id"] == sid}
        assert keys == {"ORD-5001"}
        q = [r for r in quarantine_repo.list_quarantine(conn) if r["stream_id"] == sid]
        assert {r["seq"] for r in q} == {2, 3}
        assert all(r["reason"] == REASON_APPLY_ERROR for r in q)
        led = [r for r in ledger_repo.list_ledger(conn) if r["stream_id"] == sid]
        dispo = {}
        for r in led:
            dispo[r["disposition"]] = dispo.get(r["disposition"], 0) + 1
        assert dispo == {"applied": 1, "txn_rolled_back": 2}
    finally:
        conn.close()


def test_rerun_is_idempotent_skip(fresh_db):
    sid = _stream_id("Orders TXN CDC")
    apply_service.run_apply(sid)
    run2 = apply_service.run_apply(sid)
    assert run2["status"] == "succeeded"
    assert run2["applied_count"] == 0
    assert run2["skipped_count"] == 6
    assert run2["cursor_after"] == 6
    txn = run2["report"]["txn"]
    assert txn["batch_count"] == 3
    assert txn["skipped"] == 3


def test_plain_stream_semantics_unchanged(fresh_db):
    sid = _stream_id("Orders CDC")
    run = apply_service.run_apply(sid)
    assert run["status"] == "succeeded"
    assert run["applied_count"] == 5
    assert run["cursor_after"] == 5
    assert run["report"]["txn"]["batch_count"] == 0
    conn = connect()
    try:
        led = [r for r in ledger_repo.list_ledger(conn) if r["stream_id"] == sid]
        assert {r["disposition"] for r in led} == {"applied"}
        assert all(r["txn_id"] is None for r in led)
    finally:
        conn.close()
