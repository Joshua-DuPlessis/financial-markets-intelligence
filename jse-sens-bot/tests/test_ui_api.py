"""Tests for the Flask web UI API endpoints.

These tests use Flask's test client, so no real HTTP server is needed.
The DB is created in-memory for each test.
"""
from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from mvp_sens.scripts.db_insert import initialize_db
from mvp_sens.ui.app import _safe_limit, app


def _make_memory_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    initialize_db(conn)
    return conn


def _insert_run(conn: sqlite3.Connection, run_id: str, status: str = "success") -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO ingest_runs (
            run_id, source, mode, status,
            started_at, completed_at,
            scraped_count, inserted_count,
            skipped_irrelevant_count, skipped_existing_count,
            skipped_failed_download_count
        ) VALUES (?, 'fetch', 'live', ?, ?, ?, 5, 2, 1, 1, 0)
        """,
        (run_id, status, now, now),
    )
    conn.commit()


def _insert_announcement(conn: sqlite3.Connection, sens_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO sens_financial_announcements (
            sens_id, company, title, announcement_date, pdf_url,
            analyst_relevant, category, classification_version,
            first_seen_at, created_at
        ) VALUES (?, 'ACME Ltd', 'Annual Results 2025', '2025-03-01', 'https://senspdf.jse.co.za/test.pdf',
                  1, 'financial_results', 'v1', ?, ?)
        """,
        (sens_id, now, now),
    )
    conn.commit()


class SafeLimitTests(unittest.TestCase):
    def test_default_returned_when_none(self):
        self.assertEqual(_safe_limit(None, 10), 10)

    def test_clamps_above_max(self):
        self.assertEqual(_safe_limit("999", 10), 200)

    def test_clamps_below_minimum(self):
        self.assertEqual(_safe_limit("0", 10), 1)

    def test_parses_valid_value(self):
        self.assertEqual(_safe_limit("15", 10), 15)

    def test_invalid_string_returns_default(self):
        self.assertEqual(_safe_limit("abc", 10), 10)


class ApiStatusTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self._conn = _make_memory_conn()

    def tearDown(self):
        self._conn.close()

    def _patch_db(self):
        return patch("mvp_sens.ui.app.connect_db", return_value=self._conn)

    def test_status_returns_200(self):
        with self._patch_db():
            resp = self.client.get("/api/status")
        self.assertEqual(resp.status_code, 200)

    def test_status_schema(self):
        with self._patch_db():
            data = self.client.get("/api/status").get_json()
        self.assertIn("db_connected", data)
        self.assertIn("total_disclosures", data)
        self.assertIn("total_relevant", data)
        self.assertIn("pending_signals", data)

    def test_status_counts_relevant(self):
        _insert_announcement(self._conn, "ACME001")
        with self._patch_db():
            data = self.client.get("/api/status").get_json()
        self.assertEqual(data["total_relevant"], 1)
        self.assertEqual(data["total_disclosures"], 1)

    def test_status_last_run_populated(self):
        _insert_run(self._conn, "run-001")
        with self._patch_db():
            data = self.client.get("/api/status").get_json()
        self.assertIsNotNone(data["last_run"])
        self.assertEqual(data["last_run"]["status"], "success")


class ApiRunsTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self._conn = _make_memory_conn()

    def tearDown(self):
        self._conn.close()

    def _patch_db(self):
        return patch("mvp_sens.ui.app.connect_db", return_value=self._conn)

    def test_runs_empty(self):
        with self._patch_db():
            data = self.client.get("/api/runs").get_json()
        self.assertEqual(data, [])

    def test_runs_returns_inserted(self):
        _insert_run(self._conn, "run-abc", "success")
        with self._patch_db():
            data = self.client.get("/api/runs").get_json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["run_id"], "run-abc")

    def test_runs_limit_param(self):
        for i in range(5):
            _insert_run(self._conn, f"run-{i:03d}")
        with self._patch_db():
            data = self.client.get("/api/runs?limit=2").get_json()
        self.assertLessEqual(len(data), 2)


class ApiAlertsTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self._conn = _make_memory_conn()

    def tearDown(self):
        self._conn.close()

    def _patch_db(self):
        return patch("mvp_sens.ui.app.connect_db", return_value=self._conn)

    def test_alerts_empty(self):
        with self._patch_db():
            data = self.client.get("/api/alerts").get_json()
        self.assertEqual(data, [])

    def test_alerts_returns_alert_stage_only(self):
        _insert_run(self._conn, "run-x")
        self._conn.execute(
            """
            INSERT INTO ingest_events (run_id, stage, event_type, message)
            VALUES ('run-x', 'alert', 'dom_change', 'Unusual candidate count')
            """
        )
        self._conn.commit()
        with self._patch_db():
            data = self.client.get("/api/alerts").get_json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["event_type"], "dom_change")


class ApiDisclosuresTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self._conn = _make_memory_conn()

    def tearDown(self):
        self._conn.close()

    def _patch_db(self):
        return patch("mvp_sens.ui.app.connect_db", return_value=self._conn)

    def test_disclosures_empty(self):
        with self._patch_db():
            data = self.client.get("/api/disclosures").get_json()
        self.assertEqual(data, [])

    def test_disclosures_returns_relevant(self):
        _insert_announcement(self._conn, "DISC001")
        with self._patch_db():
            data = self.client.get("/api/disclosures").get_json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["sens_id"], "DISC001")

    def test_disclosures_has_expected_fields(self):
        _insert_announcement(self._conn, "DISC002")
        with self._patch_db():
            data = self.client.get("/api/disclosures").get_json()
        row = data[0]
        for field in ("sens_id", "company", "title", "category", "pdf_url"):
            self.assertIn(field, row)


class ApiReleaseSignalsTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self._conn = _make_memory_conn()

    def tearDown(self):
        self._conn.close()

    def _patch_db(self):
        return patch("mvp_sens.ui.app.connect_db", return_value=self._conn)

    def test_release_signals_empty(self):
        with self._patch_db():
            data = self.client.get("/api/release-signals").get_json()
        self.assertEqual(data, [])


class ApiSignalsTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self._conn = _make_memory_conn()

    def tearDown(self):
        self._conn.close()

    def _patch_db(self):
        return patch("mvp_sens.ui.app.connect_db", return_value=self._conn)

    def test_signals_empty_when_no_disclosures(self):
        with self._patch_db():
            data = self.client.get("/api/signals").get_json()
        self.assertEqual(data, [])

    def test_signals_returns_one_per_disclosure(self):
        _insert_announcement(self._conn, "SIG001")
        with self._patch_db():
            data = self.client.get("/api/signals").get_json()
        self.assertEqual(len(data), 1)

    def test_signals_schema(self):
        _insert_announcement(self._conn, "SIG002")
        with self._patch_db():
            data = self.client.get("/api/signals").get_json()
        row = data[0]
        for key in ("sens_id", "company", "signal", "confidence", "reason"):
            self.assertIn(key, row)

    def test_signals_valid_values(self):
        _insert_announcement(self._conn, "SIG003")
        with self._patch_db():
            data = self.client.get("/api/signals").get_json()
        row = data[0]
        self.assertIn(row["signal"], ("BUY", "HOLD", "SELL"))
        self.assertGreaterEqual(row["confidence"], 0.0)
        self.assertLessEqual(row["confidence"], 100.0)

    def test_signals_returns_200(self):
        with self._patch_db():
            resp = self.client.get("/api/signals")
        self.assertEqual(resp.status_code, 200)

    def test_signals_limit_param(self):
        for i in range(5):
            _insert_announcement(self._conn, f"SIG10{i}")
        with self._patch_db():
            data = self.client.get("/api/signals?limit=2").get_json()
        self.assertLessEqual(len(data), 2)


class IndexRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_index_returns_200(self):
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)

    def test_index_is_html(self):
        resp = self.client.get("/")
        self.assertIn(b"JSE SENS Intelligence", resp.data)


if __name__ == "__main__":
    unittest.main()
