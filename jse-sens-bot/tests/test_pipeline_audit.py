import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mvp_sens.configs.config import SCHEMA_PATH
from mvp_sens.scripts import fetch_sens
from mvp_sens.scripts.db_insert import connect_db, initialize_db


class PipelineAuditTests(unittest.TestCase):
    def test_run_pipeline_logs_filter_summary_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)

            scrape_result = fetch_sens.ScrapeResult(
                announcements=[],
                raw_candidate_count=6,
                reject_counts={
                    "not_pdf_like_link": 2,
                    "disallowed_host": 1,
                    "not_probable_announcement_pdf": 1,
                    "missing_sens_id": 0,
                    "duplicate_sens_id": 0,
                },
            )

            def _connect_tmp_db():
                return connect_db(db_path=db_path)

            with mock.patch.object(
                fetch_sens, "connect_db", side_effect=_connect_tmp_db
            ), mock.patch.object(
                fetch_sens, "should_skip_collection_now", return_value=False
            ), mock.patch.object(
                fetch_sens,
                "scrape_announcements",
                new=mock.AsyncMock(return_value=scrape_result),
            ):
                run_id = asyncio.run(
                    fetch_sens.run_pipeline(
                        dry_run=True,
                        include_all=True,
                        run_id="release2-audit-001",
                    )
                )

            self.assertEqual(run_id, "release2-audit-001")

            with connect_db(db_path=db_path) as conn:
                run_row = conn.execute(
                    "SELECT status, scraped_count FROM ingest_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                self.assertIsNotNone(run_row)
                self.assertEqual(run_row["status"], "success")
                self.assertEqual(run_row["scraped_count"], 0)

                filter_event = conn.execute(
                    """
                    SELECT metadata_json
                    FROM ingest_events
                    WHERE run_id = ? AND stage = 'filter' AND message = 'Candidate filtering summary'
                    """,
                    (run_id,),
                ).fetchone()
                self.assertIsNotNone(filter_event)
                self.assertIn("disallowed_host", filter_event["metadata_json"])
                self.assertIn("not_pdf_like_link", filter_event["metadata_json"])

    def test_run_pipeline_logs_operator_alert_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)

            scrape_result = fetch_sens.ScrapeResult(
                announcements=[],
                raw_candidate_count=1,
                reject_counts={
                    "not_pdf_like_link": 1,
                    "disallowed_host": 0,
                    "not_probable_announcement_pdf": 0,
                    "missing_sens_id": 0,
                    "duplicate_sens_id": 0,
                },
                attempt_count=2,
                dom_change_suspected=True,
                alerts=[
                    "Potential DOM/API drift detected: low candidate volume without valid announcements."
                ],
            )

            def _connect_tmp_db():
                return connect_db(db_path=db_path)

            with mock.patch.object(
                fetch_sens, "connect_db", side_effect=_connect_tmp_db
            ), mock.patch.object(
                fetch_sens, "should_skip_collection_now", return_value=False
            ), mock.patch.object(
                fetch_sens,
                "scrape_announcements",
                new=mock.AsyncMock(return_value=scrape_result),
            ):
                run_id = asyncio.run(
                    fetch_sens.run_pipeline(
                        dry_run=True,
                        include_all=True,
                        run_id="release3-alert-001",
                    )
                )

            with connect_db(db_path=db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT stage, event_type, message, metadata_json
                    FROM ingest_events
                    WHERE run_id = ? AND stage = 'alert'
                    ORDER BY id
                    """,
                    (run_id,),
                ).fetchall()

                self.assertGreaterEqual(len(rows), 2)
                messages = [row["message"] for row in rows]
                self.assertIn("Scrape required retries before completion", messages)
                self.assertIn(
                    "Potential DOM/API drift detected: low candidate volume without valid announcements.",
                    messages,
                )

    def test_run_pipeline_persists_classification_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)

            scrape_result = fetch_sens.ScrapeResult(
                announcements=[
                    fetch_sens.Announcement(
                        sens_id="SENS900",
                        company="Visual International Holdings Limited",
                        title="INITIAL TRADING STATEMENT FOR THE YEAR ENDED 28 FEBRUARY 2026",
                        announcement_date="2026-03-30T10:00:00+00:00",
                        pdf_url="https://senspdf.jse.co.za/documents/SENS_900.pdf",
                        issuer_context="Visual International Holdings Limited | Equity Issuer (JSE)",
                        issuer_tags=("equity",),
                    )
                ],
                raw_candidate_count=1,
                reject_counts={
                    "not_pdf_like_link": 0,
                    "disallowed_host": 0,
                    "not_probable_announcement_pdf": 0,
                    "missing_sens_id": 0,
                    "duplicate_sens_id": 0,
                    "issuer_unknown": 0,
                    "issuer_non_equity": 0,
                },
            )

            def _connect_tmp_db():
                return connect_db(db_path=db_path)

            with mock.patch.object(
                fetch_sens, "connect_db", side_effect=_connect_tmp_db
            ), mock.patch.object(
                fetch_sens, "should_skip_collection_now", return_value=False
            ), mock.patch.object(
                fetch_sens,
                "scrape_announcements",
                new=mock.AsyncMock(return_value=scrape_result),
            ):
                run_id = asyncio.run(
                    fetch_sens.run_pipeline(
                        dry_run=False,
                        skip_download=True,
                        include_all=False,
                        run_id="release2-classify-001",
                    )
                )

            with connect_db(db_path=db_path) as conn:
                row = conn.execute(
                    """
                    SELECT category, classification_reason, classification_version,
                           analyst_relevant, relevance_reason
                    FROM sens_financial_announcements
                    WHERE sens_id = 'SENS900'
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["category"], "trading_statement")
                self.assertIsNotNone(row["classification_reason"])
                self.assertIsNotNone(row["classification_version"])
                self.assertEqual(row["analyst_relevant"], 1)
                self.assertIsNotNone(row["relevance_reason"])

                run_row = conn.execute(
                    "SELECT status, inserted_count FROM ingest_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                self.assertEqual(run_row["status"], "success")
                self.assertEqual(run_row["inserted_count"], 1)

    def test_run_pipeline_uses_pdf_disambiguation_for_ambiguous_title(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            fake_pdf_path = Path(tmpdir) / "SENS901.pdf"
            fake_pdf_path.write_bytes(b"%PDF-1.4\n")

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)

            scrape_result = fetch_sens.ScrapeResult(
                announcements=[
                    fetch_sens.Announcement(
                        sens_id="SENS901",
                        company="ABC Limited",
                        title="Quarterly Statement Update",
                        announcement_date="2026-03-30T10:00:00+00:00",
                        pdf_url="https://senspdf.jse.co.za/documents/SENS_901.pdf",
                        issuer_context="ABC Limited | Equity Issuer (JSE)",
                        issuer_tags=("equity",),
                    )
                ],
                raw_candidate_count=1,
                reject_counts={
                    "not_pdf_like_link": 0,
                    "disallowed_host": 0,
                    "not_probable_announcement_pdf": 0,
                    "missing_sens_id": 0,
                    "duplicate_sens_id": 0,
                    "issuer_unknown": 0,
                    "issuer_non_equity": 0,
                },
            )

            def _connect_tmp_db():
                return connect_db(db_path=db_path)

            with mock.patch.object(
                fetch_sens, "connect_db", side_effect=_connect_tmp_db
            ), mock.patch.object(
                fetch_sens, "should_skip_collection_now", return_value=False
            ), mock.patch.object(
                fetch_sens,
                "scrape_announcements",
                new=mock.AsyncMock(return_value=scrape_result),
            ), mock.patch.object(
                fetch_sens, "download_pdf", return_value=fake_pdf_path
            ) as mocked_download, mock.patch.object(
                fetch_sens,
                "extract_pdf_text_for_classification",
                return_value=(
                    "Unaudited condensed consolidated interim financial results "
                    "for the six months ended 31 December 2025"
                ),
            ) as mocked_extract:
                run_id = asyncio.run(
                    fetch_sens.run_pipeline(
                        dry_run=False,
                        skip_download=False,
                        include_all=False,
                        run_id="release3-disambiguation-001",
                    )
                )
                mocked_download.assert_called_once()
                mocked_extract.assert_called_once()

            with connect_db(db_path=db_path) as conn:
                row = conn.execute(
                    """
                    SELECT category, analyst_relevant, classification_reason
                    FROM sens_financial_announcements
                    WHERE sens_id = 'SENS901'
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["category"], "financial_results")
                self.assertEqual(row["analyst_relevant"], 1)
                self.assertTrue(str(row["classification_reason"]).startswith("pdf_"))

                classify_events = conn.execute(
                    """
                    SELECT metadata_json
                    FROM ingest_events
                    WHERE run_id = ? AND stage = 'classify' AND sens_id = 'SENS901'
                    """,
                    (run_id,),
                ).fetchall()
                self.assertEqual(len(classify_events), 1)
                self.assertIn("disambiguation_attempted", classify_events[0]["metadata_json"])


if __name__ == "__main__":
    unittest.main()
