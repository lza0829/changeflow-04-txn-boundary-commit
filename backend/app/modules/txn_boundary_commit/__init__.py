"""事务边界整批提交（txn_boundary_commit）。

CDC 事件可携带事务边界字段：``txn_id`` 标识所属事务，``is_last`` 标记事务
末事件。Apply 时把同一事务的连续事件视为一个批次，语义互斥：

- 仅当批内每条都可应用（连续序、无重复、边界完整）才 ``commit`` —— 整批
  一次性落 Sink 并把游标推过批末；
- 任一条失败或缺口（批前缺口 / 批内缺口 / 缺事务末标记 / 新旧混杂重放）
  则 ``rollback`` —— 整批不提交，Sink 与游标停在批前，原因进隔离区与
  批次记录；
- 整批都是已应用重复时 ``skip_duplicate``。

无 ``txn_id`` 的事件视为单条批（隐含边界完整），行为与连续序单条应用一致。
"""
from __future__ import annotations

from typing import Any, Callable

# 批次裁决
BATCH_COMMIT = "commit"
BATCH_SKIP_DUPLICATE = "skip_duplicate"
BATCH_ROLLBACK = "rollback"

# 回滚原因（同时写入 quarantine.reason 与 txn_batches.reason）
REASON_GAP_BEFORE = "gap_before_seq"  # 批首 seq 越过游标期望位置
REASON_GAP_WITHIN = "gap_within_txn"  # 批内 seq 不连续（缺事件）
REASON_MISSING_MARKER = "txn_missing_commit_marker"  # 事务缺 is_last 末事件
REASON_PARTIAL_REPLAY = "txn_partially_applied"  # 批内新旧混杂（部分已应用）
REASON_APPLY_ERROR = "apply_error"  # 落 Sink 中途出错，已回退到批前

# 账本 disposition
DISPO_APPLIED = "applied"  # 单条成功
DISPO_TXN_APPLIED = "txn_applied"  # 整批提交
DISPO_TXN_ROLLED_BACK = "txn_rolled_back"  # 整批回滚
DISPO_SKIPPED = "skipped_duplicate"
DISPO_QUARANTINE_GAP = "quarantine_gap"
DISPO_APPLY_ERROR = "apply_error"


def group_txn_batches(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把按 seq 排序的事件切成批次。

    返回 [{txn_id, events, has_last}, ...]：无 txn_id 的事件各自成单条批；
    连续的同 txn_id 事件聚成一批，is_last=True 关闭当前批。批未被 is_last
    关闭时 has_last=False，裁决阶段按缺事务末标记处理。
    """
    batches: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for event in events:
        txn_id = event.get("txn_id")
        if txn_id is None:
            batches.append({"txn_id": None, "events": [event], "has_last": True})
            current = None
            continue
        if current is None or current["txn_id"] != txn_id:
            current = {"txn_id": txn_id, "events": [], "has_last": False}
            batches.append(current)
        current["events"].append(event)
        if event.get("is_last"):
            current["has_last"] = True
            current = None
    return batches


def _contiguous(seqs: list[int]) -> bool:
    return seqs == list(range(seqs[0], seqs[0] + len(seqs)))


def classify_batch(
    *,
    batch: dict[str, Any],
    cursor: int,
    already_applied: Callable[[int], bool],
    gap_policy: str = "stop_quarantine",
) -> tuple[str, str | None]:
    """裁决整个批次，返回 (decision, reason)。decision 三者互斥。"""
    events = batch["events"]
    seqs = [int(e["seq"]) for e in events]
    applied_flags = [seq <= cursor or already_applied(seq) for seq in seqs]
    if all(applied_flags):
        return BATCH_SKIP_DUPLICATE, None
    if any(applied_flags):
        # 批内新旧混杂：禁止只提交未应用部分再把游标推过整批
        return BATCH_ROLLBACK, REASON_PARTIAL_REPLAY
    expected = int(cursor) + 1
    if seqs[0] > expected and gap_policy != "skip_gap":
        return BATCH_ROLLBACK, REASON_GAP_BEFORE
    if not _contiguous(seqs):
        return BATCH_ROLLBACK, REASON_GAP_WITHIN
    if batch["txn_id"] is not None and not batch["has_last"]:
        return BATCH_ROLLBACK, REASON_MISSING_MARKER
    return BATCH_COMMIT, None


def ledger_disposition(
    batch: dict[str, Any], decision: str, reason: str | None = None
) -> str:
    """批次裁决映射到每条事件的账本 disposition。"""
    if decision == BATCH_COMMIT:
        return DISPO_TXN_APPLIED if batch["txn_id"] else DISPO_APPLIED
    if decision == BATCH_SKIP_DUPLICATE:
        return DISPO_SKIPPED
    # BATCH_ROLLBACK
    if batch["txn_id"]:
        return DISPO_TXN_ROLLED_BACK
    return DISPO_QUARANTINE_GAP if reason == REASON_GAP_BEFORE else DISPO_APPLY_ERROR
