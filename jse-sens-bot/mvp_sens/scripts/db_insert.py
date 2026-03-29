from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Mapping

from mvp_sens.configs.config import DB_PATH, SCHEMA_PATH, ensure_runtime_dirs

INSERT_SQL = """
INSERT OR IGNORE INTO sens_financial_announcements
(sens_id, company, title, announcement_date, pdf_url, local_pdf_path)
VALUES (?, ?, ?, ?, ?, ?)
"""


def connect_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open a SQLite connection configured for dict-like row access."""
    ensure_runtime_dirs()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    """Initialize database schema if it does not exist."""
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    conn.commit()


def announcement_exists(conn: sqlite3.Connection, sens_id: str) -> bool:
    """Check whether a SENS announcement is already stored."""
    cursor = conn.execute(
        "SELECT 1 FROM sens_financial_announcements WHERE sens_id = ?",
        (sens_id,),
    )
    return cursor.fetchone() is not None


def insert_announcement(conn: sqlite3.Connection, record: Mapping[str, str]) -> bool:
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
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


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
