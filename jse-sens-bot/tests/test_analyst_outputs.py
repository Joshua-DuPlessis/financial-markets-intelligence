from __future__ import annotations

import csv
import json
from datetime import date, datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mvp_sens.configs.config import SCHEMA_PATH
from mvp_sens.scripts import analyst_outputs
from mvp_sens.scripts.analyst_outputs import (
    DISCLOSURE_EXPORT_FIELDS,
    RELEASE_SIGNAL_EXPORT_FIELDS,
    advance_since_last_run_cursor,
    build_daily_delta_rows,
    build_intraday_snapshot_rows,
    build_release_signal_rows,
    build_since_last_run_rows,
    export_since_last_run,
    write_export,
)
from mvp_sens.scripts.db_insert import (
    connect_db,
    get_global_reporting_cursor,
    initialize_db,
    insert_announcement,
    insert_release_signal,
    log_ingest_event,
    set_global_reporting_cursor,
)


class AnalystOutputsTests(unittest.TestCase):
    def _insert_success_run(self, conn, run_id: str, completed_at: str) -> None:
        conn.execute(
            """
            INSERT INTO ingest_runs
            (run_id, source, mode, status, started_at, completed_at,
             scraped_count, inserted_count, skipped_irrelevant_count,
             skipped_existing_count, skipped_failed_download_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                "sens_web",
                "live",
                "success",
                completed_at,
                completed_at,
                1,
                1,
                0,
                0,
                0,
            ),
        )
        conn.commit()

    def _insert_relevant_announcement(
        self,
        conn,
        sens_id: str,
        run_id: str,
        completed_at: str,
        title: str = "Unaudited condensed consolidated interim financial results",
    ) -> None:
        inserted = insert_announcement(
            conn,
            {
                "sens_id": sens_id,
                "company": f"Company {sens_id}",
                "title": title,
                "announcement_date": completed_at,
                "pdf_url": f"https://senspdf.jse.co.za/documents/SENS_{sens_id}.pdf",
                "local_pdf_path": "",
                "category": "financial_results",
                "classification_reason": "kw_financial_results",
                "classification_version": "test-v1",
                "classified_at": completed_at,
                "first_seen_run_id": run_id,
                "first_seen_at": completed_at,
                "analyst_relevant": 1,
                "relevance_reason": "kw_financial_results",
            },
        )
        self.assertTrue(inserted)
        log_ingest_event(
            conn=conn,
            run_id=run_id,
            stage="classify",
            event_type="info",
            sens_id=sens_id,
            message="Classification decision",
            metadata={"category": "financial_results"},
        )

    def test_since_last_run_uses_cursor_and_advances(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                self._insert_success_run(conn, "run-001", "2026-03-27T08:00:00+00:00")
                self._insert_success_run(conn, "run-002", "2026-03-27T09:00:00+00:00")
                self._insert_relevant_announcement(
                    conn,
                    sens_id="SENS001",
                    run_id="run-001",
                    completed_at="2026-03-27T08:00:00+00:00",
                )
                self._insert_relevant_announcement(
                    conn,
                    sens_id="SENS002",
                    run_id="run-002",
                    completed_at="2026-03-27T09:00:00+00:00",
                )
                set_global_reporting_cursor(
                    conn,
                    run_id="run-001",
                    completed_at="2026-03-27T08:00:00+00:00",
                )

                rows, cursor_before, cursor_after = build_since_last_run_rows(conn)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["sens_id"], "SENS002")
                self.assertEqual(cursor_before["run_id"], "run-001")
                self.assertEqual(cursor_after["run_id"], "run-002")

                advance_since_last_run_cursor(conn, cursor_after)
                cursor_now = get_global_reporting_cursor(conn)
                self.assertIsNotNone(cursor_now)
                self.assertEqual(cursor_now["run_id"], "run-002")

    def test_since_last_run_does_not_treat_reclassification_as_new(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                self._insert_success_run(conn, "run-401", "2026-03-27T08:00:00+00:00")
                self._insert_success_run(conn, "run-402", "2026-03-27T09:00:00+00:00")
                self._insert_relevant_announcement(
                    conn,
                    sens_id="SENS401",
                    run_id="run-401",
                    completed_at="2026-03-27T08:00:00+00:00",
                )
                # Later classify event should not change first-seen newness.
                log_ingest_event(
                    conn=conn,
                    run_id="run-402",
                    stage="classify",
                    event_type="info",
                    sens_id="SENS401",
                    message="Classification decision",
                    metadata={"category": "financial_results"},
                )
                set_global_reporting_cursor(
                    conn,
                    run_id="run-401",
                    completed_at="2026-03-27T08:00:00+00:00",
                )

                rows, cursor_before, cursor_after = build_since_last_run_rows(conn)
                self.assertEqual(len(rows), 0)
                self.assertEqual(cursor_before["run_id"], "run-401")
                self.assertEqual(cursor_after["run_id"], "run-402")

    def test_cursor_not_advanced_when_export_write_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                self._insert_success_run(conn, "run-501", "2026-03-27T08:00:00+00:00")
                self._insert_success_run(conn, "run-502", "2026-03-27T09:00:00+00:00")
                self._insert_relevant_announcement(
                    conn,
                    sens_id="SENS501",
                    run_id="run-502",
                    completed_at="2026-03-27T09:00:00+00:00",
                )
                set_global_reporting_cursor(
                    conn,
                    run_id="run-501",
                    completed_at="2026-03-27T08:00:00+00:00",
                )

                _rows, _cursor_before, cursor_after = build_since_last_run_rows(conn)
                with mock.patch.object(
                    analyst_outputs,
                    "write_export",
                    side_effect=OSError("disk write failed"),
                ):
                    with self.assertRaises(OSError):
                        export_since_last_run(
                            conn=conn,
                            output_format="json",
                            output_path=str(Path(tmpdir) / "fail.json"),
                            advance_cursor=True,
                        )
                # Cursor should remain unchanged because advance is explicit post-write.
                cursor_now = get_global_reporting_cursor(conn)
                self.assertEqual(cursor_now["run_id"], "run-501")
                # Manual advance can still occur after a successful write.
                advance_since_last_run_cursor(conn, cursor_after)
                cursor_now = get_global_reporting_cursor(conn)
                self.assertEqual(cursor_now["run_id"], "run-502")

    def test_intraday_snapshot_uses_jse_window(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                self._insert_success_run(conn, "run-101", "2026-03-27T05:10:00+00:00")  # 07:10
                self._insert_success_run(conn, "run-102", "2026-03-27T04:50:00+00:00")  # 06:50
                self._insert_success_run(conn, "run-103", "2026-03-27T13:30:00+00:00")  # 15:30
                self._insert_success_run(conn, "run-104", "2026-03-27T14:30:00+00:00")  # 16:30

                self._insert_relevant_announcement(
                    conn, "SENS101", "run-101", "2026-03-27T05:10:00+00:00"
                )
                self._insert_relevant_announcement(
                    conn, "SENS102", "run-102", "2026-03-27T04:50:00+00:00"
                )
                self._insert_relevant_announcement(
                    conn, "SENS103", "run-103", "2026-03-27T13:30:00+00:00"
                )
                self._insert_relevant_announcement(
                    conn, "SENS104", "run-104", "2026-03-27T14:30:00+00:00"
                )

                rows, start_local, end_local = build_intraday_snapshot_rows(
                    conn=conn,
                    report_date=date(2026, 3, 27),
                    now_utc=datetime(2026, 3, 27, 14, 0, 0, tzinfo=timezone.utc),  # 16:00
                )
                sens_ids = {row["sens_id"] for row in rows}
                self.assertEqual(sens_ids, {"SENS101", "SENS103"})
                self.assertEqual(start_local.hour, 7)
                self.assertEqual(start_local.minute, 5)
                self.assertEqual(end_local.hour, 16)
                self.assertEqual(end_local.minute, 0)

    def test_daily_delta_uses_0705_to_1805_window(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                self._insert_success_run(conn, "run-201", "2026-03-27T05:05:00+00:00")  # 07:05
                self._insert_success_run(conn, "run-202", "2026-03-27T16:05:00+00:00")  # 18:05
                self._insert_success_run(conn, "run-203", "2026-03-27T16:06:00+00:00")  # 18:06

                self._insert_relevant_announcement(
                    conn, "SENS201", "run-201", "2026-03-27T05:05:00+00:00"
                )
                self._insert_relevant_announcement(
                    conn, "SENS202", "run-202", "2026-03-27T16:05:00+00:00"
                )
                self._insert_relevant_announcement(
                    conn, "SENS203", "run-203", "2026-03-27T16:06:00+00:00"
                )

                rows, start_local, end_local = build_daily_delta_rows(
                    conn=conn,
                    report_date=date(2026, 3, 27),
                )
                sens_ids = {row["sens_id"] for row in rows}
                self.assertEqual(sens_ids, {"SENS201", "SENS202"})
                self.assertEqual(start_local.hour, 7)
                self.assertEqual(start_local.minute, 5)
                self.assertEqual(end_local.hour, 18)
                self.assertEqual(end_local.minute, 5)

    def test_release_signal_rows_filter_upcoming_and_contract_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                self._insert_success_run(conn, "run-301", "2026-03-27T08:00:00+00:00")
                self._insert_relevant_announcement(
                    conn,
                    "SENS301",
                    "run-301",
                    "2026-03-27T08:00:00+00:00",
                    title="Annual Report and Notice of Annual General Meeting",
                )
                insert_release_signal(
                    conn,
                    sens_id="SENS301",
                    signal_type="future_release_datetime",
                    signal_datetime="2026-03-30T09:00:00+02:00",
                    source_text="Will be released on 30 March 2026 at 09:00.",
                    source="title",
                )
                insert_release_signal(
                    conn,
                    sens_id="SENS301",
                    signal_type="future_release_datetime",
                    signal_datetime="2026-03-20T09:00:00+02:00",
                    source_text="Released on 20 March 2026 at 09:00.",
                    source="title",
                )

                rows = build_release_signal_rows(
                    conn,
                    include_past=False,
                    now_utc=datetime(2026, 3, 27, 8, 0, 0, tzinfo=timezone.utc),
                )
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["sens_id"], "SENS301")
                self.assertEqual(
                    list(rows[0].keys()),
                    list(RELEASE_SIGNAL_EXPORT_FIELDS),
                )

    def test_write_export_preserves_contract_field_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [
                {
                    "run_id": "run-500",
                    "run_completed_at": "2026-03-27T09:00:00+00:00",
                    "sens_id": "SENS500",
                    "company": "Company SENS500",
                    "title": "Unaudited condensed consolidated interim financial results",
                    "announcement_date": "2026-03-27T09:00:00+00:00",
                    "category": "financial_results",
                    "analyst_relevant": 1,
                    "relevance_reason": "kw_financial_results",
                    "classification_reason": "kw_financial_results",
                    "classification_version": "test-v1",
                    "classified_at": "2026-03-27T09:00:00+00:00",
                    "observed_at_utc": "2026-03-27T09:00:00+00:00",
                    "observed_at_jse": "2026-03-27T11:00:00+02:00",
                    "pdf_url": "https://senspdf.jse.co.za/documents/SENS_500.pdf",
                    "local_pdf_path": "",
                    "created_at": "2026-03-27 09:00:00",
                }
            ]

            csv_path = write_export(
                rows=rows,
                fields=DISCLOSURE_EXPORT_FIELDS,
                output_format="csv",
                output_path=str(Path(tmpdir) / "out.csv"),
                report_name="test_report",
            )
            with csv_path.open("r", encoding="utf-8", newline="") as file_handle:
                reader = csv.reader(file_handle)
                header = next(reader)
            self.assertEqual(header, list(DISCLOSURE_EXPORT_FIELDS))

            json_path = write_export(
                rows=rows,
                fields=DISCLOSURE_EXPORT_FIELDS,
                output_format="json",
                output_path=str(Path(tmpdir) / "out.json"),
                report_name="test_report",
            )
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(list(payload[0].keys()), list(DISCLOSURE_EXPORT_FIELDS))


if __name__ == "__main__":
    unittest.main()
