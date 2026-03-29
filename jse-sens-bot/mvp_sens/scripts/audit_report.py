from __future__ import annotations

import argparse
import json
import sqlite3

from mvp_sens.scripts.db_insert import connect_db


def fetch_recent_runs(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            run_id,
            source,
            mode,
            status,
            started_at,
            completed_at,
            scraped_count,
            inserted_count,
            skipped_irrelevant_count,
            skipped_existing_count,
            skipped_failed_download_count
        FROM ingest_runs
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()


def fetch_recent_alert_events(
    conn: sqlite3.Connection, limit: int, run_id: str | None
) -> list[sqlite3.Row]:
    if run_id:
        return conn.execute(
            """
            SELECT run_id, stage, event_type, message, metadata_json, created_at
            FROM ingest_events
            WHERE run_id = ? AND stage = 'alert'
            ORDER BY id DESC
            LIMIT ?
            """,
            (run_id, max(1, limit)),
        ).fetchall()

    return conn.execute(
        """
        SELECT run_id, stage, event_type, message, metadata_json, created_at
        FROM ingest_events
        WHERE stage = 'alert'
        ORDER BY id DESC
        LIMIT ?
        """,
        (max(1, limit),),
    ).fetchall()


def _metadata_summary(raw_json: str | None) -> str:
    if not raw_json:
        return "-"
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return raw_json

    if not isinstance(parsed, dict):
        return str(parsed)

    if "attempt_count" in parsed:
        return f"attempt_count={parsed['attempt_count']}"
    if "raw_candidate_count" in parsed and "scraped_count" in parsed:
        return (
            f"raw_candidate_count={parsed['raw_candidate_count']},"
            f"scraped_count={parsed['scraped_count']}"
        )
    keys = ",".join(sorted(parsed.keys()))
    return f"keys={keys}" if keys else "-"


def render_runs(rows: list[sqlite3.Row]) -> list[str]:
    lines = ["Recent ingest runs:"]
    if not rows:
        lines.append("  (none)")
        return lines

    for row in rows:
        lines.append(
            "  "
            f"{row['run_id']} | source={row['source']} mode={row['mode']} status={row['status']} "
            f"scraped={row['scraped_count']} inserted={row['inserted_count']} "
            f"skipped_irrelevant={row['skipped_irrelevant_count']} skipped_existing={row['skipped_existing_count']} "
            f"skipped_failed_download={row['skipped_failed_download_count']}"
        )
    return lines


def render_alerts(rows: list[sqlite3.Row]) -> list[str]:
    lines = ["Recent alert events:"]
    if not rows:
        lines.append("  (none)")
        return lines

    for row in rows:
        lines.append(
            "  "
            f"{row['created_at']} | run_id={row['run_id']} event_type={row['event_type']} "
            f"message={row['message']} metadata={_metadata_summary(row['metadata_json'])}"
        )
    return lines


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print ingest run and alert summaries from SQLite audit tables."
    )
    parser.add_argument(
        "--run-limit",
        type=int,
        default=10,
        help="How many recent ingest_runs rows to print.",
    )
    parser.add_argument(
        "--alert-limit",
        type=int,
        default=20,
        help="How many recent alert events to print.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run id filter for alert events.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    with connect_db() as conn:
        run_rows = fetch_recent_runs(conn, args.run_limit)
        alert_rows = fetch_recent_alert_events(
            conn, args.alert_limit, run_id=args.run_id
        )

    for line in render_runs(run_rows):
        print(line)
    print()
    for line in render_alerts(alert_rows):
        print(line)


if __name__ == "__main__":
    main()
