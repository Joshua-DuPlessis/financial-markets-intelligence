from datetime import datetime, timezone
from pathlib import Path
import unittest

from mvp_sens.configs.config import DB_PATH, PDF_BASE_URL, SCHEMA_PATH, ensure_runtime_dirs
from mvp_sens.scripts.fetch_sens import (
    build_pdf_url,
    extract_sens_id,
    is_allowed_pdf_url,
    is_weekend_in_jse_timezone,
    is_probable_announcement_url,
    is_relevant,
    should_skip_collection_now,
)


class SmokeTests(unittest.TestCase):
    def test_schema_sql_exists(self):
        self.assertTrue(SCHEMA_PATH.exists())
        self.assertEqual(SCHEMA_PATH.name, "schema.sql")

    def test_runtime_directories_can_be_prepared(self):
        ensure_runtime_dirs()
        self.assertTrue(DB_PATH.parent.exists())
        self.assertIsInstance(DB_PATH, Path)

    def test_build_pdf_url_handles_relative_links(self):
        pdf_url = build_pdf_url("/documents/SENS_12345.pdf")
        self.assertTrue(pdf_url.startswith(PDF_BASE_URL))

    def test_extract_sens_id(self):
        sens_id = extract_sens_id("https://senspdf.jse.co.za/documents/SENS_765432.pdf")
        self.assertEqual(sens_id, "765432")

    def test_extract_sens_id_rejects_non_announcement(self):
        sens_id = extract_sens_id("https://clientportal.jse.co.za/communication/sens-project")
        self.assertEqual(sens_id, "")

    def test_probable_announcement_url(self):
        self.assertTrue(
            is_probable_announcement_url("https://senspdf.jse.co.za/documents/SENS_765432.pdf")
        )
        self.assertFalse(
            is_probable_announcement_url("https://clientportal.jse.co.za/communication/sens-project")
        )

    def test_allowed_pdf_host(self):
        self.assertTrue(
            is_allowed_pdf_url("https://senspdf.jse.co.za/documents/SENS_123456.pdf")
        )
        self.assertFalse(
            is_allowed_pdf_url("https://example.com/documents/SENS_123456.pdf")
        )

    def test_keyword_filter(self):
        self.assertTrue(is_relevant("ABC LTD | Audited Consolidated Results for year ended"))
        self.assertFalse(is_relevant("ABC LTD | Change in directorate"))

    def test_weekend_detection_in_jse_timezone(self):
        saturday_utc = datetime(2026, 3, 28, 10, 0, 0, tzinfo=timezone.utc)
        monday_utc = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(is_weekend_in_jse_timezone(saturday_utc))
        self.assertFalse(is_weekend_in_jse_timezone(monday_utc))

    def test_should_skip_collection_now_weekend(self):
        saturday_utc = datetime(2026, 3, 28, 10, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(should_skip_collection_now(saturday_utc))


if __name__ == "__main__":
    unittest.main()
