from __future__ import annotations

import argparse
from pathlib import Path

from mvp_sens.configs.config import PARSED_TEXT_DIR, PDF_DIR, ensure_runtime_dirs


def _load_pdf_reader():
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError(
            "pypdf is required for parse_pdf.py. Install dependencies from requirements.txt."
        ) from exc
    return PdfReader


def extract_text(pdf_path: Path, max_pages: int) -> str:
    PdfReader = _load_pdf_reader()
    reader = PdfReader(str(pdf_path))
    pages = reader.pages[:max_pages] if max_pages > 0 else reader.pages
    chunks: list[str] = []
    for page in pages:
        chunks.append(page.extract_text() or "")
    return "\n".join(chunks).strip()


def parse_all_pdfs(limit: int | None, max_pages: int, force: bool = False) -> int:
    ensure_runtime_dirs()
    parsed_count = 0

    for idx, pdf_path in enumerate(sorted(PDF_DIR.glob("*.pdf"))):
        if limit is not None and idx >= limit:
            break

        output_path = PARSED_TEXT_DIR / f"{pdf_path.stem}.txt"
        if output_path.exists() and not force:
            continue

        text = extract_text(pdf_path, max_pages=max_pages)
        output_path.write_text(text, encoding="utf-8")
        parsed_count += 1

    return parsed_count


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract text from downloaded SENS PDFs.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Parse only the first N PDFs (sorted by filename).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=3,
        help="Maximum number of pages to extract per PDF (0 for all pages).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-parse PDFs even when output .txt files already exist.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    total = parse_all_pdfs(limit=args.limit, max_pages=args.max_pages, force=args.force)
    print(f"Parsed {total} PDF files into text.")


if __name__ == "__main__":
    main()
