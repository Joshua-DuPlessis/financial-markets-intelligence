from pathlib import Path
import tempfile
import unittest

from mvp_sens.configs.config import SCHEMA_PATH
from mvp_sens.scripts.db_insert import (
    announcement_exists,
    connect_db,
    initialize_db,
    insert_announcement,
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


if __name__ == "__main__":
    unittest.main()
