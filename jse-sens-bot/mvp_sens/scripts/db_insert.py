from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mvp_sens.configs.config import DB_PATH, SCHEMA_PATH, ensure_runtime_dirs

INSERT_SQL = """
INSERT OR IGNORE INTO sens_financial_announcements
(sens_id, company, title, announcement_date, pdf_url, local_pdf_path,
 category, classification_reason, classification_version, classified_at,
 analyst_relevant, relevance_reason)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_INGEST_RUN_SQL = """
INSERT INTO ingest_runs
(run_id, source, mode, status, started_at)
VALUES (?, ?, ?, ?, ?)
"""

UPDATE_INGEST_RUN_SQL = """
UPDATE ingest_runs
SET
    status = ?,
    completed_at = ?,
    scraped_count = ?,
    inserted_count = ?,
    skipped_irrelevant_count = ?,
    skipped_existing_count = ?,
    skipped_failed_download_count = ?,
    error_message = ?
WHERE run_id = ?
"""

INSERT_INGEST_EVENT_SQL = """
INSERT INTO ingest_events
(run_id, stage, event_type, sens_id, message, metadata_json)
VALUES (?, ?, ?, ?, ?, ?)
"""

INSERT_RELEASE_SIGNAL_SQL = """
INSERT OR IGNORE INTO release_signals
(sens_id, signal_type, signal_datetime, source_text, source)
VALUES (?, ?, ?, ?, ?)
"""

UPSERT_PIPELINE_STATE_SQL = """
INSERT INTO pipeline_state
(key, value, updated_at)
VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    updated_at = excluded.updated_at
"""

SELECT_PIPELINE_STATE_SQL = """
SELECT value FROM pipeline_state WHERE key = ?
"""

GLOBAL_CURSOR_RUN_ID_KEY = "analyst_outputs.last_run_id"
GLOBAL_CURSOR_COMPLETED_AT_KEY = "analyst_outputs.last_completed_at"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open a SQLite connection configured for dict-like row access."""
    ensure_runtime_dirs()
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    """Initialize database schema if it does not exist."""
    if _table_exists(conn, "sens_financial_announcements"):
        _ensure_announcements_migration_columns(conn)
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    _ensure_announcements_migration_columns(conn)
    conn.commit()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_announcements_migration_columns(conn: sqlite3.Connection) -> None:
    """
    Backfill schema columns for legacy DBs created before Phase 2.
    """
    if not _table_exists(conn, "sens_financial_announcements"):
        return

    column_defs = {
        "category": "TEXT NOT NULL DEFAULT 'other'",
        "classification_reason": "TEXT",
        "classification_version": "TEXT",
        "classified_at": "TEXT",
        "analyst_relevant": "INTEGER NOT NULL DEFAULT 0",
        "relevance_reason": "TEXT",
    }
    existing = _table_column_names(conn, "sens_financial_announcements")
    for column_name, column_def in column_defs.items():
        if column_name in existing:
            continue
        conn.execute(
            "ALTER TABLE sens_financial_announcements "
            f"ADD COLUMN {column_name} {column_def}"
        )


def announcement_exists(conn: sqlite3.Connection, sens_id: str) -> bool:
    """Check whether a SENS announcement is already stored."""
    cursor = conn.execute(
        "SELECT 1 FROM sens_financial_announcements WHERE sens_id = ?",
        (sens_id,),
    )
    return cursor.fetchone() is not None


def _coerce_bool_to_int(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if int(value) != 0 else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "yes", "on"} else 0
    return 0


def insert_announcement(conn: sqlite3.Connection, record: Mapping[str, Any]) -> bool:
    """
    Insert one announcement row.

    Returns True when a new row was inserted, False when ignored as duplicate.
    """
    cursor = conn.execute(
        INSERT_SQL,
        (
            record["sens_id"],
            record["company"],
            record["title"],
            record["announcement_date"],
            record["pdf_url"],
            record["local_pdf_path"],
            str(record.get("category", "other")),
            record.get("classification_reason"),
            record.get("classification_version"),
            record.get("classified_at"),
            _coerce_bool_to_int(record.get("analyst_relevant", 0)),
            record.get("relevance_reason"),
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def start_ingest_run(
    conn: sqlite3.Connection,
    run_id: str,
    source: str,
    mode: str,
) -> None:
    """Create a new ingest run row with running state."""
    try:
        conn.execute(
            INSERT_INGEST_RUN_SQL,
            (run_id, source, mode, "running", _now_utc_iso()),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise ValueError(
            f"Run id '{run_id}' already exists. Use a unique run id for each pipeline execution."
        ) from exc


def complete_ingest_run(
    conn: sqlite3.Connection,
    run_id: str,
    status: str,
    scraped_count: int,
    inserted_count: int,
    skipped_irrelevant_count: int,
    skipped_existing_count: int,
    skipped_failed_download_count: int,
    error_message: str | None = None,
) -> None:
    """Finalize ingest run with counts and terminal state."""
    conn.execute(
        UPDATE_INGEST_RUN_SQL,
        (
            status,
            _now_utc_iso(),
            scraped_count,
            inserted_count,
            skipped_irrelevant_count,
            skipped_existing_count,
            skipped_failed_download_count,
            error_message,
            run_id,
        ),
    )
    conn.commit()


def log_ingest_event(
    conn: sqlite3.Connection,
    run_id: str,
    stage: str,
    event_type: str,
    message: str,
    sens_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    """Insert one ingest event row for diagnostics and auditing."""
    metadata_json = json.dumps(metadata, ensure_ascii=True) if metadata else None
    conn.execute(
        INSERT_INGEST_EVENT_SQL,
        (run_id, stage, event_type, sens_id, message, metadata_json),
    )
    conn.commit()


def insert_release_signal(
    conn: sqlite3.Connection,
    sens_id: str,
    signal_type: str,
    signal_datetime: str,
    source_text: str,
    source: str = "title",
) -> bool:
    """
    Insert one release signal row.

    Returns True when inserted, False when deduplicated by unique constraint.
    """
    cursor = conn.execute(
        INSERT_RELEASE_SIGNAL_SQL,
        (sens_id, signal_type, signal_datetime, source_text, source),
    )
    conn.commit()
    return cursor.rowcount == 1


def set_pipeline_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        UPSERT_PIPELINE_STATE_SQL,
        (key, value, _now_utc_iso()),
    )
    conn.commit()


def get_pipeline_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(SELECT_PIPELINE_STATE_SQL, (key,)).fetchone()
    if row is None:
        return None
    return str(row["value"])


def set_global_reporting_cursor(
    conn: sqlite3.Connection,
    run_id: str,
    completed_at: str,
) -> None:
    set_pipeline_state(conn, GLOBAL_CURSOR_RUN_ID_KEY, run_id)
    set_pipeline_state(conn, GLOBAL_CURSOR_COMPLETED_AT_KEY, completed_at)


def get_global_reporting_cursor(conn: sqlite3.Connection) -> dict[str, str] | None:
    run_id = get_pipeline_state(conn, GLOBAL_CURSOR_RUN_ID_KEY)
    if not run_id:
        return None
    completed_at = get_pipeline_state(conn, GLOBAL_CURSOR_COMPLETED_AT_KEY) or ""
    return {
        "run_id": run_id,
        "completed_at": completed_at,
    }


def init_database(db_path: Path = DB_PATH, schema_path: Path = SCHEMA_PATH) -> None:
    """Initialize DB from schema file in a single call."""
    with connect_db(db_path) as conn:
        initialize_db(conn, schema_path=schema_path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Initialize the SENS SQLite database schema."
    )
    parser.add_argument(
        "--db-path",
        default=str(DB_PATH),
        help="SQLite database path (default: config DB_PATH).",
    )
    parser.add_argument(
        "--schema-path",
        default=str(SCHEMA_PATH),
        help="SQL schema file path (default: config SCHEMA_PATH).",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    init_database(db_path=Path(args.db_path), schema_path=Path(args.schema_path))
    print(f"Database initialized at {args.db_path}")


if __name__ == "__main__":
    main()
