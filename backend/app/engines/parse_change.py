from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _parse_is_last(value: Any, *, line_no: int) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise ValueError(f"invalid is_last at line {line_no}: {value!r}")


def parse_change_fixture(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"fixture missing: {path}")
    events: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid jsonl at line {line_no}: {exc}") from exc
        for key in ("seq", "op", "entity_key", "event_ts"):
            if key not in obj:
                raise ValueError(f"missing {key} at line {line_no}")
        op = str(obj["op"]).lower()
        if op not in ("upsert", "delete"):
            raise ValueError(f"unsupported op at line {line_no}: {op}")
        txn_id = obj.get("txn_id")
        if txn_id is not None:
            txn_id = str(txn_id).strip() or None
        is_last = _parse_is_last(obj.get("is_last"), line_no=line_no)
        if is_last and not txn_id:
            raise ValueError(f"is_last without txn_id at line {line_no}")
        events.append(
            {
                "seq": int(obj["seq"]),
                "op": op,
                "entity_key": str(obj["entity_key"]),
                "event_ts": str(obj["event_ts"]),
                "payload": obj.get("payload") or {},
                "txn_id": txn_id,
                "is_last": is_last,
            }
        )
    events.sort(key=lambda e: e["seq"])
    return events
