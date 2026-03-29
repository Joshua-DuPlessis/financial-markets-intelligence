from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mvp_sens.scripts import parse_pdf


class ParsePdfTests(unittest.TestCase):
    def test_parse_all_pdfs_creates_text_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pdf_dir = tmp_path / "pdfs"
            parsed_dir = tmp_path / "parsed"
            pdf_dir.mkdir()
            parsed_dir.mkdir()

            (pdf_dir / "A100.pdf").write_bytes(b"fake")
            (pdf_dir / "A200.pdf").write_bytes(b"fake")

            with mock.patch.object(parse_pdf, "PDF_DIR", pdf_dir), mock.patch.object(
                parse_pdf, "PARSED_TEXT_DIR", parsed_dir
            ), mock.patch.object(
                parse_pdf, "ensure_runtime_dirs", lambda: None
            ), mock.patch.object(
                parse_pdf,
                "extract_text",
                lambda pdf_path, max_pages: f"text:{pdf_path.stem}",
            ):
                parsed_count = parse_pdf.parse_all_pdfs(
                    limit=None, max_pages=2, force=False
                )

            self.assertEqual(parsed_count, 2)
            self.assertEqual(
                (parsed_dir / "A100.txt").read_text(encoding="utf-8"), "text:A100"
            )
            self.assertEqual(
                (parsed_dir / "A200.txt").read_text(encoding="utf-8"), "text:A200"
            )

    def test_parse_all_pdfs_respects_existing_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            pdf_dir = tmp_path / "pdfs"
            parsed_dir = tmp_path / "parsed"
            pdf_dir.mkdir()
            parsed_dir.mkdir()

            (pdf_dir / "X999.pdf").write_bytes(b"fake")
            existing_output = parsed_dir / "X999.txt"
            existing_output.write_text("existing", encoding="utf-8")

            with mock.patch.object(parse_pdf, "PDF_DIR", pdf_dir), mock.patch.object(
                parse_pdf, "PARSED_TEXT_DIR", parsed_dir
            ), mock.patch.object(
                parse_pdf, "ensure_runtime_dirs", lambda: None
            ), mock.patch.object(
                parse_pdf, "extract_text", lambda *_: "new"
            ):
                parsed_count = parse_pdf.parse_all_pdfs(
                    limit=None, max_pages=2, force=False
                )

            self.assertEqual(parsed_count, 0)
            self.assertEqual(existing_output.read_text(encoding="utf-8"), "existing")


if __name__ == "__main__":
    unittest.main()
