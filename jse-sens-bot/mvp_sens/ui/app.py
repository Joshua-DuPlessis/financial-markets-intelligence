"""Lightweight Flask web UI for the JSE SENS Intelligence dashboard.

Exposes read-only JSON API endpoints backed by the local SQLite database and
serves the single-page HTML dashboard as the root route.

Run locally:
    python -m mvp_sens.ui.app

Or via Makefile:
    make ui
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from mvp_sens.scripts.analyst_outputs import (
    build_release_signal_rows,
    fetch_relevant_disclosures,
)
from mvp_sens.scripts.audit_report import fetch_recent_alert_events, fetch_recent_runs
from mvp_sens.scripts.db_insert import connect_db, initialize_db

_STATIC_DIR = Path(__file__).parent / "static"

app = Flask(__name__, static_folder=str(_STATIC_DIR), static_url_path="/static")

_MAX_LIMIT = 200


def _safe_limit(raw: str | None, default: int) -> int:
    try:
        return max(1, min(int(raw or default), _MAX_LIMIT))
    except (TypeError, ValueError):
        return default


def _db_connected() -> bool:
    try:
        with connect_db() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Routes — static assets
# ---------------------------------------------------------------------------


@app.route("/")
def index() -> object:
    return send_from_directory(_STATIC_DIR, "index.html")


# ---------------------------------------------------------------------------
# Routes — API
# ---------------------------------------------------------------------------


@app.route("/api/status")
def api_status() -> object:
    """Return a quick health summary of the pipeline."""
    connected = _db_connected()
    if not connected:
        return jsonify(
            {
                "db_connected": False,
                "total_disclosures": 0,
                "total_relevant": 0,
                "last_run": None,
                "pending_signals": 0,
            }
        )

    with connect_db() as conn:
        initialize_db(conn)

        total_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM sens_financial_announcements"
        ).fetchone()
        total_disclosures = int(total_row["cnt"]) if total_row else 0

        relevant_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM sens_financial_announcements WHERE analyst_relevant = 1"
        ).fetchone()
        total_relevant = int(relevant_row["cnt"]) if relevant_row else 0

        last_run_row = conn.execute(
            """
            SELECT run_id, source, mode, status, started_at, completed_at
            FROM ingest_runs
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        last_run = dict(last_run_row) if last_run_row else None

        signals = build_release_signal_rows(conn)
        pending_signals = len(signals)

    return jsonify(
        {
            "db_connected": True,
            "total_disclosures": total_disclosures,
            "total_relevant": total_relevant,
            "last_run": last_run,
            "pending_signals": pending_signals,
        }
    )


@app.route("/api/runs")
def api_runs() -> object:
    """Return recent ingest runs."""
    limit = _safe_limit(request.args.get("limit"), 10)
    if not _db_connected():
        return jsonify([])
    with connect_db() as conn:
        initialize_db(conn)
        rows = fetch_recent_runs(conn, limit)
    return jsonify([dict(row) for row in rows])


@app.route("/api/alerts")
def api_alerts() -> object:
    """Return recent alert events."""
    limit = _safe_limit(request.args.get("limit"), 20)
    run_id: str | None = request.args.get("run_id") or None
    if not _db_connected():
        return jsonify([])
    with connect_db() as conn:
        initialize_db(conn)
        rows = fetch_recent_alert_events(conn, limit, run_id)
    return jsonify([dict(row) for row in rows])


@app.route("/api/disclosures")
def api_disclosures() -> object:
    """Return analyst-relevant disclosures, newest first."""
    limit = _safe_limit(request.args.get("limit"), 50)
    if not _db_connected():
        return jsonify([])
    with connect_db() as conn:
        initialize_db(conn)
        rows = fetch_relevant_disclosures(conn)
    return jsonify(rows[:limit])


@app.route("/api/release-signals")
def api_release_signals() -> object:
    """Return upcoming analyst-relevant release signals."""
    include_past = request.args.get("include_past", "").lower() in ("1", "true", "yes")
    if not _db_connected():
        return jsonify([])
    with connect_db() as conn:
        initialize_db(conn)
        rows = build_release_signal_rows(conn, include_past=include_past)
    return jsonify(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    host = os.getenv("SENS_UI_HOST", "0.0.0.0")
    port = int(os.getenv("SENS_UI_PORT", "5000"))
    debug = os.getenv("SENS_UI_DEBUG", "").lower() in ("1", "true", "yes")
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
