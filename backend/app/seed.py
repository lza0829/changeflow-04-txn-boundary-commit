from app.db import connect

SEED_STREAMS = [
    (
        "Orders CDC",
        "orders_cdc.jsonl",
        "order",
        0,
        1,
        "clean：seq 1→5 连续；含一次 delete",
    ),
    (
        "Inventory CDC",
        "inventory_cdc.jsonl",
        "inventory",
        0,
        1,
        "clean：seq 1→3 连续库存变更",
    ),
    (
        "Orders TXN CDC",
        "txn_orders_cdc.jsonl",
        "order",
        0,
        1,
        "clean：seq 1→6；T-100/T-101/T-102 事务批（含批内 delete），整批提交",
    ),
    (
        "Orders TXN Broken CDC",
        "txn_orders_broken_cdc.jsonl",
        "order",
        0,
        1,
        "broken：T-900 整批提交；T-901 批内缺 seq 4 → 整批回滚进隔离，游标停在批前",
    ),
]

SEED_SETTINGS = [
    ("project_label", "Changeflow"),
    ("gap_policy", "stop_quarantine"),
]


def _ensure_column(conn, table: str, column: str, ddl: str) -> None:
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_db():
    conn = connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS streams(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            fixture_path TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            apply_cursor INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS apply_runs(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            loaded_count INTEGER NOT NULL,
            applied_count INTEGER NOT NULL,
            quarantined_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL,
            cursor_before INTEGER NOT NULL,
            cursor_after INTEGER NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS change_ledger(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER,
            seq INTEGER NOT NULL,
            op TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            disposition TEXT NOT NULL,
            txn_id TEXT,
            UNIQUE(stream_id, seq, run_id)
        );
        CREATE TABLE IF NOT EXISTS sink_records(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL,
            last_event_ts TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(stream_id, entity_key)
        );
        CREATE TABLE IF NOT EXISTS quarantine(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            reason TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS applied_seqs(
            stream_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            applied_at TEXT NOT NULL,
            PRIMARY KEY(stream_id, seq)
        );
        CREATE TABLE IF NOT EXISTS txn_batches(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            txn_id TEXT NOT NULL,
            seq_start INTEGER NOT NULL,
            seq_end INTEGER NOT NULL,
            event_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    # 老库迁移：change_ledger 补 txn_id 列
    _ensure_column(conn, "change_ledger", "txn_id", "TEXT")
    for name, fixture_path, entity_type, cursor, enabled, note in SEED_STREAMS:
        conn.execute(
            """
            INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note)
            SELECT ?,?,?,?,?,?
            WHERE NOT EXISTS (SELECT 1 FROM streams WHERE name=?)
            """,
            (name, fixture_path, entity_type, cursor, enabled, note, name),
        )
    for key, value in SEED_SETTINGS:
        conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()
