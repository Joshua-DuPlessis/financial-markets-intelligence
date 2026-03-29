from pathlib import Path
import tempfile
import unittest

from mvp_sens.configs.config import SCHEMA_PATH
from mvp_sens.scripts.audit_report import (
    fetch_recent_alert_events,
    fetch_recent_runs,
    render_alerts,
    render_runs,
)
from mvp_sens.scripts.db_insert import (
    complete_ingest_run,
    connect_db,
    initialize_db,
    log_ingest_event,
    start_ingest_run,
)


class AuditReportTests(unittest.TestCase):
    def test_fetch_and_render_run_and_alert_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"

            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                start_ingest_run(
                    conn,
                    run_id="audit-run-001",
                    source="scheduler_loop",
                    mode="dry-run",
                )
                log_ingest_event(
                    conn,
                    run_id="audit-run-001",
                    stage="alert",
                    event_type="warning",
                    message="Scrape required retries before completion",
                    metadata={"attempt_count": 2},
                )
                complete_ingest_run(
                    conn,
                    run_id="audit-run-001",
                    status="success",
                    scraped_count=0,
                    inserted_count=0,
                    skipped_irrelevant_count=0,
                    skipped_existing_count=0,
                    skipped_failed_download_count=0,
                )

            with connect_db(db_path=db_path) as conn:
                runs = fetch_recent_runs(conn, limit=5)
                alerts = fetch_recent_alert_events(conn, limit=5, run_id=None)

            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["run_id"], "audit-run-001")
            self.assertEqual(runs[0]["status"], "success")

            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0]["stage"], "alert")
            self.assertIn("Scrape required retries", alerts[0]["message"])

            run_lines = render_runs(runs)
            alert_lines = render_alerts(alerts)
            self.assertIn("Recent ingest runs:", run_lines[0])
            self.assertIn("audit-run-001", run_lines[1])
            self.assertIn("Recent alert events:", alert_lines[0])
            self.assertIn("attempt_count=2", alert_lines[1])


if __name__ == "__main__":
    unittest.main()
