from pathlib import Path
import sqlite3
import tempfile
import unittest

from mvp_sens.configs.config import SCHEMA_PATH
from mvp_sens.scripts.db_insert import (
    announcement_exists,
    complete_ingest_run,
    connect_db,
    get_global_reporting_cursor,
    get_pipeline_state,
    initialize_db,
    insert_release_signal,
    insert_announcement,
    set_global_reporting_cursor,
    set_pipeline_state,
    log_ingest_event,
    start_ingest_run,
)


class DbInsertTests(unittest.TestCase):
    def test_initialize_and_insert_round_trip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)

                record = {
                    "sens_id": "SENS123",
                    "company": "ABC LTD",
                    "title": "Audited Consolidated Results",
                    "announcement_date": "2026-03-29T10:00:00+00:00",
                    "pdf_url": "https://senspdf.jse.co.za/documents/SENS_123.pdf",
                    "local_pdf_path": "/tmp/SENS123.pdf",
                }

                inserted = insert_announcement(conn, record)
                self.assertTrue(inserted)
                self.assertTrue(announcement_exists(conn, "SENS123"))

                duplicate_insert = insert_announcement(conn, record)
                self.assertFalse(duplicate_insert)

    def test_ingest_run_lifecycle_and_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)

                run_id = "run-test-001"
                start_ingest_run(conn, run_id=run_id, source="sens_web", mode="dry-run")
                log_ingest_event(
                    conn,
                    run_id=run_id,
                    stage="scrape",
                    event_type="info",
                    message="Scrape completed",
                    metadata={"scraped_count": 3},
                )
                complete_ingest_run(
                    conn,
                    run_id=run_id,
                    status="success",
                    scraped_count=3,
                    inserted_count=2,
                    skipped_irrelevant_count=1,
                    skipped_existing_count=0,
                    skipped_failed_download_count=0,
                )

                run_row = conn.execute(
                    "SELECT status, scraped_count, inserted_count FROM ingest_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                self.assertIsNotNone(run_row)
                self.assertEqual(run_row["status"], "success")
                self.assertEqual(run_row["scraped_count"], 3)
                self.assertEqual(run_row["inserted_count"], 2)

                event_row = conn.execute(
                    "SELECT stage, event_type, message, metadata_json FROM ingest_events WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                self.assertIsNotNone(event_row)
                self.assertEqual(event_row["stage"], "scrape")
                self.assertEqual(event_row["event_type"], "info")
                self.assertEqual(event_row["message"], "Scrape completed")
                self.assertIn("scraped_count", event_row["metadata_json"])

    def test_start_ingest_run_rejects_duplicate_run_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                run_id = "run-duplicate-001"
                start_ingest_run(conn, run_id=run_id, source="sens_web", mode="dry-run")

                with self.assertRaises(ValueError):
                    start_ingest_run(
                        conn, run_id=run_id, source="sens_web", mode="dry-run"
                    )

    def test_ingest_event_requires_existing_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO ingest_events
                        (run_id, stage, event_type, sens_id, message, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "missing-run-id",
                            "scrape",
                            "info",
                            None,
                            "orphan event",
                            None,
                        ),
                    )

    def test_schema_contains_classification_and_relevance_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                rows = conn.execute("PRAGMA table_info(sens_financial_announcements)").fetchall()
                column_names = {row["name"] for row in rows}
                self.assertIn("category", column_names)
                self.assertIn("classification_reason", column_names)
                self.assertIn("classification_version", column_names)
                self.assertIn("classified_at", column_names)
                self.assertIn("analyst_relevant", column_names)
                self.assertIn("relevance_reason", column_names)
                self.assertIn("first_seen_run_id", column_names)
                self.assertIn("first_seen_at", column_names)

    def test_release_signal_insert_and_dedup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                insert_announcement(
                    conn,
                    {
                        "sens_id": "SENS500",
                        "company": "ABC LTD",
                        "title": "Annual Report and Notice of Annual General Meeting",
                        "announcement_date": "2026-03-30T10:00:00+00:00",
                        "pdf_url": "https://senspdf.jse.co.za/documents/SENS_500.pdf",
                        "local_pdf_path": "",
                    },
                )
                inserted_one = insert_release_signal(
                    conn,
                    sens_id="SENS500",
                    signal_type="future_release_datetime",
                    signal_datetime="2026-04-15T09:00:00+02:00",
                    source_text="The annual report will be released on 15 April 2026 at 09:00.",
                    source="title",
                )
                inserted_two = insert_release_signal(
                    conn,
                    sens_id="SENS500",
                    signal_type="future_release_datetime",
                    signal_datetime="2026-04-15T09:00:00+02:00",
                    source_text="The annual report will be released on 15 April 2026 at 09:00.",
                    source="title",
                )

                self.assertTrue(inserted_one)
                self.assertFalse(inserted_two)

                count_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM release_signals WHERE sens_id = ?",
                    ("SENS500",),
                ).fetchone()
                self.assertEqual(count_row["c"], 1)

    def test_pipeline_state_and_global_cursor_round_trip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)

                self.assertIsNone(get_pipeline_state(conn, "unknown-key"))
                set_pipeline_state(conn, "analyst.cursor", "run-123")
                self.assertEqual(get_pipeline_state(conn, "analyst.cursor"), "run-123")

                set_global_reporting_cursor(
                    conn,
                    run_id="run-200",
                    completed_at="2026-03-30T18:05:00+02:00",
                )
                cursor = get_global_reporting_cursor(conn)
                self.assertIsNotNone(cursor)
                self.assertEqual(cursor["run_id"], "run-200")
                self.assertEqual(cursor["completed_at"], "2026-03-30T18:05:00+02:00")

    def test_initialize_db_migrates_legacy_announcements_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            with connect_db(db_path=db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE sens_financial_announcements (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sens_id TEXT UNIQUE,
                        company TEXT,
                        title TEXT,
                        announcement_date TEXT,
                        pdf_url TEXT,
                        local_pdf_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
                conn.commit()

                initialize_db(conn, schema_path=SCHEMA_PATH)

                rows = conn.execute("PRAGMA table_info(sens_financial_announcements)").fetchall()
                column_names = {row["name"] for row in rows}
                self.assertIn("category", column_names)
                self.assertIn("analyst_relevant", column_names)
                self.assertIn("classification_reason", column_names)
                self.assertIn("first_seen_run_id", column_names)
                self.assertIn("first_seen_at", column_names)


if __name__ == "__main__":
    unittest.main()
