from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from mvp_sens.configs.config import EXPORT_DIR, ensure_runtime_dirs
from mvp_sens.scripts.db_insert import (
    connect_db,
    get_global_reporting_cursor,
    initialize_db,
    set_global_reporting_cursor,
)

JSE_TIMEZONE = ZoneInfo("Africa/Johannesburg")
INTRADAY_START_TIME = time(7, 5)
INTRADAY_END_TIME = time(18, 5)

DISCLOSURE_EXPORT_FIELDS = (
    "run_id",
    "run_completed_at",
    "sens_id",
    "company",
    "title",
    "announcement_date",
    "category",
    "analyst_relevant",
    "relevance_reason",
    "classification_reason",
    "classification_version",
    "classified_at",
    "observed_at_utc",
    "observed_at_jse",
    "pdf_url",
    "local_pdf_path",
    "created_at",
)

RELEASE_SIGNAL_EXPORT_FIELDS = (
    "signal_datetime",
    "signal_datetime_jse",
    "sens_id",
    "company",
    "title",
    "category",
    "analyst_relevant",
    "relevance_reason",
    "signal_type",
    "source",
    "source_text",
    "created_at",
)

FETCH_RELEVANT_DISCLOSURES_SQL = """
SELECT
    COALESCE(ann.first_seen_run_id, '') AS run_id,
    COALESCE(first_run.completed_at, '') AS run_completed_at,
    ann.sens_id,
    ann.company,
    ann.title,
    ann.announcement_date,
    ann.category,
    ann.analyst_relevant,
    ann.relevance_reason,
    ann.classification_reason,
    ann.classification_version,
    ann.classified_at,
    ann.first_seen_at,
    ann.pdf_url,
    ann.local_pdf_path,
    ann.created_at
FROM sens_financial_announcements AS ann
LEFT JOIN ingest_runs AS first_run ON first_run.run_id = ann.first_seen_run_id
WHERE ann.analyst_relevant = 1
"""

FETCH_RELEASE_SIGNALS_SQL = """
SELECT
    rs.signal_datetime,
    rs.signal_type,
    rs.source,
    rs.source_text,
    rs.created_at,
    ann.sens_id,
    ann.company,
    ann.title,
    ann.category,
    ann.analyst_relevant,
    ann.relevance_reason
FROM release_signals AS rs
INNER JOIN sens_financial_announcements AS ann ON ann.sens_id = rs.sens_id
WHERE ann.analyst_relevant = 1
"""


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(normalized, fmt)
                break
            except ValueError:
                continue
        else:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _observed_timestamp_utc(row: Mapping[str, Any]) -> datetime | None:
    for key in (
        "first_seen_at",
        "run_completed_at",
        "classified_at",
        "announcement_date",
        "created_at",
    ):
        parsed = _parse_datetime(_coerce_str(row.get(key)))
        if parsed is not None:
            return parsed
    return None


def _disclosure_to_export_row(row: Mapping[str, Any]) -> dict[str, Any]:
    observed_utc = _observed_timestamp_utc(row)
    observed_utc_str = observed_utc.isoformat() if observed_utc is not None else ""
    observed_jse_str = (
        observed_utc.astimezone(JSE_TIMEZONE).isoformat()
        if observed_utc is not None
        else ""
    )

    return {
        "run_id": _coerce_str(row.get("run_id")),
        "run_completed_at": _coerce_str(row.get("run_completed_at")),
        "sens_id": _coerce_str(row.get("sens_id")),
        "company": _coerce_str(row.get("company")),
        "title": _coerce_str(row.get("title")),
        "announcement_date": _coerce_str(row.get("announcement_date")),
        "category": _coerce_str(row.get("category")),
        "analyst_relevant": int(row.get("analyst_relevant") or 0),
        "relevance_reason": _coerce_str(row.get("relevance_reason")),
        "classification_reason": _coerce_str(row.get("classification_reason")),
        "classification_version": _coerce_str(row.get("classification_version")),
        "classified_at": _coerce_str(row.get("classified_at")),
        "observed_at_utc": observed_utc_str,
        "observed_at_jse": observed_jse_str,
        "pdf_url": _coerce_str(row.get("pdf_url")),
        "local_pdf_path": _coerce_str(row.get("local_pdf_path")),
        "created_at": _coerce_str(row.get("created_at")),
    }


def _release_signal_to_export_row(row: Mapping[str, Any]) -> dict[str, Any]:
    parsed_signal = _parse_datetime(_coerce_str(row.get("signal_datetime")))
    signal_jse = (
        parsed_signal.astimezone(JSE_TIMEZONE).isoformat() if parsed_signal is not None else ""
    )
    return {
        "signal_datetime": _coerce_str(row.get("signal_datetime")),
        "signal_datetime_jse": signal_jse,
        "sens_id": _coerce_str(row.get("sens_id")),
        "company": _coerce_str(row.get("company")),
        "title": _coerce_str(row.get("title")),
        "category": _coerce_str(row.get("category")),
        "analyst_relevant": int(row.get("analyst_relevant") or 0),
        "relevance_reason": _coerce_str(row.get("relevance_reason")),
        "signal_type": _coerce_str(row.get("signal_type")),
        "source": _coerce_str(row.get("source")),
        "source_text": _coerce_str(row.get("source_text")),
        "created_at": _coerce_str(row.get("created_at")),
    }


def _rows_sorted_by_observed_desc(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    def _sort_key(row: Mapping[str, Any]) -> tuple[str, str]:
        return (_coerce_str(row.get("observed_at_utc")), _coerce_str(row.get("sens_id")))

    return sorted(rows, key=_sort_key, reverse=True)


def fetch_relevant_disclosures(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    raw_rows = conn.execute(FETCH_RELEVANT_DISCLOSURES_SQL).fetchall()
    exported_rows = [_disclosure_to_export_row(dict(row)) for row in raw_rows]
    return _rows_sorted_by_observed_desc(exported_rows)


def _latest_successful_ingest_run(conn: sqlite3.Connection) -> dict[str, str] | None:
    row = conn.execute(
        """
        SELECT run_id, completed_at
        FROM ingest_runs
        WHERE status = 'success' AND completed_at IS NOT NULL
        ORDER BY completed_at DESC, run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return {"run_id": str(row["run_id"]), "completed_at": str(row["completed_at"])}


def _is_after_cursor(row: Mapping[str, Any], cursor: Mapping[str, str]) -> bool:
    row_timestamp = _parse_datetime(_coerce_str(row.get("observed_at_utc")))
    cursor_timestamp = _parse_datetime(cursor.get("completed_at"))
    row_run_id = _coerce_str(row.get("run_id"))
    cursor_run_id = _coerce_str(cursor.get("run_id"))

    if row_timestamp is not None and cursor_timestamp is not None:
        if row_timestamp > cursor_timestamp:
            return True
        if row_timestamp < cursor_timestamp:
            return False
        if row_run_id and cursor_run_id:
            return row_run_id > cursor_run_id
        return False

    if row_run_id and cursor_run_id:
        return row_run_id > cursor_run_id
    return True


def build_since_last_run_rows(
    conn: sqlite3.Connection,
) -> tuple[list[dict[str, Any]], dict[str, str] | None, dict[str, str] | None]:
    rows = fetch_relevant_disclosures(conn)
    cursor_before = get_global_reporting_cursor(conn)

    if cursor_before:
        rows = [row for row in rows if _is_after_cursor(row, cursor_before)]

    latest_run = _latest_successful_ingest_run(conn)
    return rows, cursor_before, latest_run


def advance_since_last_run_cursor(
    conn: sqlite3.Connection,
    cursor: Mapping[str, str] | None,
) -> None:
    if not cursor:
        return
    set_global_reporting_cursor(
        conn,
        run_id=str(cursor.get("run_id", "")),
        completed_at=str(cursor.get("completed_at", "")),
    )


def _resolve_report_date(raw_value: str | None, now_utc: datetime | None = None) -> date:
    if raw_value:
        return date.fromisoformat(raw_value)
    effective_now = now_utc or datetime.now(timezone.utc)
    return effective_now.astimezone(JSE_TIMEZONE).date()


def _in_window(observed_at_utc: str, start_local: datetime, end_local: datetime) -> bool:
    observed_dt = _parse_datetime(observed_at_utc)
    if observed_dt is None:
        return False
    observed_local = observed_dt.astimezone(JSE_TIMEZONE)
    return start_local <= observed_local <= end_local


def build_intraday_snapshot_rows(
    conn: sqlite3.Connection,
    report_date: date,
    now_utc: datetime | None = None,
) -> tuple[list[dict[str, Any]], datetime, datetime]:
    rows = fetch_relevant_disclosures(conn)
    effective_now = now_utc or datetime.now(timezone.utc)
    now_local = effective_now.astimezone(JSE_TIMEZONE)
    start_local = datetime.combine(report_date, INTRADAY_START_TIME, tzinfo=JSE_TIMEZONE)
    hard_end_local = datetime.combine(report_date, INTRADAY_END_TIME, tzinfo=JSE_TIMEZONE)
    end_local = hard_end_local
    if report_date == now_local.date() and now_local < hard_end_local:
        end_local = now_local

    filtered = [
        row
        for row in rows
        if _in_window(row.get("observed_at_utc", ""), start_local, end_local)
    ]
    return filtered, start_local, end_local


def build_daily_delta_rows(
    conn: sqlite3.Connection,
    report_date: date,
) -> tuple[list[dict[str, Any]], datetime, datetime]:
    rows = fetch_relevant_disclosures(conn)
    start_local = datetime.combine(report_date, INTRADAY_START_TIME, tzinfo=JSE_TIMEZONE)
    end_local = datetime.combine(report_date, INTRADAY_END_TIME, tzinfo=JSE_TIMEZONE)
    filtered = [
        row
        for row in rows
        if _in_window(row.get("observed_at_utc", ""), start_local, end_local)
    ]
    return filtered, start_local, end_local


def build_release_signal_rows(
    conn: sqlite3.Connection,
    include_past: bool = False,
    now_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    raw_rows = conn.execute(FETCH_RELEASE_SIGNALS_SQL).fetchall()
    rows = [_release_signal_to_export_row(dict(row)) for row in raw_rows]
    if include_past:
        include_rows = rows
    else:
        effective_now = now_utc or datetime.now(timezone.utc)
        include_rows = []
        for row in rows:
            signal_dt = _parse_datetime(_coerce_str(row.get("signal_datetime")))
            if signal_dt is None:
                continue
            if signal_dt >= effective_now.astimezone(timezone.utc):
                include_rows.append(row)
    return sorted(
        include_rows,
        key=lambda row: (
            _coerce_str(row.get("signal_datetime")),
            _coerce_str(row.get("sens_id")),
        ),
    )


def _default_output_path(report_name: str, output_format: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return EXPORT_DIR / f"{report_name}_{stamp}.{output_format}"


def _normalize_rows(
    rows: list[Mapping[str, Any]],
    fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized_rows.append({field: row.get(field, "") for field in fields})
    return normalized_rows


def write_export(
    rows: list[Mapping[str, Any]],
    fields: tuple[str, ...],
    output_format: str,
    output_path: str | None,
    report_name: str,
) -> Path:
    ensure_runtime_dirs()
    resolved = Path(output_path) if output_path else _default_output_path(report_name, output_format)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    normalized_rows = _normalize_rows(rows, fields)

    if output_format == "csv":
        with resolved.open("w", encoding="utf-8", newline="") as file_handle:
            writer = csv.DictWriter(file_handle, fieldnames=list(fields))
            writer.writeheader()
            writer.writerows(normalized_rows)
    else:
        with resolved.open("w", encoding="utf-8") as file_handle:
            json.dump(normalized_rows, file_handle, ensure_ascii=True, indent=2)
    return resolved


def export_since_last_run(
    conn: sqlite3.Connection,
    output_format: str,
    output_path: str | None,
    advance_cursor: bool = True,
) -> tuple[Path, list[dict[str, Any]], dict[str, str] | None, dict[str, str] | None]:
    rows, cursor_before, cursor_after = build_since_last_run_rows(conn)
    written_path = write_export(
        rows=rows,
        fields=DISCLOSURE_EXPORT_FIELDS,
        output_format=output_format,
        output_path=output_path,
        report_name="since_last_run",
    )
    if advance_cursor:
        advance_since_last_run_cursor(conn, cursor_after)
    return written_path, rows, cursor_before, cursor_after


def _add_common_export_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Export format (json or csv).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output file path. Defaults to timestamped file in SENS_EXPORT_DIR.",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate analyst-friendly outputs from the local SENS SQLite database."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    since_parser = subparsers.add_parser(
        "since-last-run",
        help="Export new relevant disclosures since the last reporting cursor.",
    )
    _add_common_export_args(since_parser)
    since_parser.add_argument(
        "--no-advance-cursor",
        action="store_true",
        help="Do not advance the global reporting cursor after exporting.",
    )

    intraday_parser = subparsers.add_parser(
        "intraday-snapshot",
        help="Export relevant disclosures in the JSE intraday window (07:05-18:05).",
    )
    _add_common_export_args(intraday_parser)
    intraday_parser.add_argument(
        "--date",
        default=None,
        help="Local JSE date in YYYY-MM-DD format (default: today in Africa/Johannesburg).",
    )

    daily_parser = subparsers.add_parser(
        "daily-delta",
        help="Export relevant disclosures for a full JSE daily window (07:05-18:05).",
    )
    _add_common_export_args(daily_parser)
    daily_parser.add_argument(
        "--date",
        default=None,
        help="Local JSE date in YYYY-MM-DD format (default: today in Africa/Johannesburg).",
    )

    signal_parser = subparsers.add_parser(
        "release-signals",
        help="Export upcoming analyst-relevant release signals.",
    )
    _add_common_export_args(signal_parser)
    signal_parser.add_argument(
        "--include-past",
        action="store_true",
        help="Include past signal datetimes (default exports upcoming only).",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    with connect_db() as conn:
        initialize_db(conn)

        if args.command == "since-last-run":
            output_path, rows, cursor_before, cursor_after = export_since_last_run(
                conn=conn,
                output_format=args.format,
                output_path=args.output,
                advance_cursor=not args.no_advance_cursor,
            )
            print(f"since-last-run rows={len(rows)} output={output_path}")
            print(f"cursor_before={cursor_before}")
            print(f"cursor_after={cursor_after}")
            return

        if args.command == "intraday-snapshot":
            report_date = _resolve_report_date(args.date)
            rows, start_local, end_local = build_intraday_snapshot_rows(
                conn,
                report_date=report_date,
            )
            output_path = write_export(
                rows=rows,
                fields=DISCLOSURE_EXPORT_FIELDS,
                output_format=args.format,
                output_path=args.output,
                report_name="intraday_snapshot",
            )
            print(
                f"intraday-snapshot rows={len(rows)} output={output_path} "
                f"window_start={start_local.isoformat()} window_end={end_local.isoformat()}"
            )
            return

        if args.command == "daily-delta":
            report_date = _resolve_report_date(args.date)
            rows, start_local, end_local = build_daily_delta_rows(
                conn,
                report_date=report_date,
            )
            output_path = write_export(
                rows=rows,
                fields=DISCLOSURE_EXPORT_FIELDS,
                output_format=args.format,
                output_path=args.output,
                report_name="daily_delta",
            )
            print(
                f"daily-delta rows={len(rows)} output={output_path} "
                f"window_start={start_local.isoformat()} window_end={end_local.isoformat()}"
            )
            return

        if args.command == "release-signals":
            rows = build_release_signal_rows(
                conn,
                include_past=args.include_past,
            )
            output_path = write_export(
                rows=rows,
                fields=RELEASE_SIGNAL_EXPORT_FIELDS,
                output_format=args.format,
                output_path=args.output,
                report_name="release_signals",
            )
            print(f"release-signals rows={len(rows)} output={output_path}")
            return

        raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
