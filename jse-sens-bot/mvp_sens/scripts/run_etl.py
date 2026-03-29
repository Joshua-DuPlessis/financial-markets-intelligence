from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from uuid import uuid4

from mvp_sens.scripts.db_insert import init_database
from mvp_sens.scripts.fetch_sens import run_pipeline
from mvp_sens.scripts.gpt_review import iter_parsed_documents
from mvp_sens.scripts.parse_pdf import parse_all_pdfs


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run ETL stages for the MVP SENS pipeline."
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Do not scrape announcements from the SENS site.",
    )
    parser.add_argument(
        "--skip-parse",
        action="store_true",
        help="Do not parse downloaded PDFs to text files.",
    )
    parser.add_argument(
        "--fetch-limit",
        type=int,
        default=20,
        help="Max number of announcements to process in fetch stage.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run fetch stage without DB writes.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip PDF download during fetch stage.",
    )
    parser.add_argument(
        "--parse-limit",
        type=int,
        default=None,
        help="Optional limit for PDF parsing count.",
    )
    parser.add_argument(
        "--parse-max-pages",
        type=int,
        default=3,
        help="Pages extracted per PDF (0 for all pages).",
    )
    parser.add_argument(
        "--parse-force",
        action="store_true",
        help="Rebuild parsed text output even when target files exist.",
    )
    parser.add_argument(
        "--review-limit",
        type=int,
        default=5,
        help="How many parsed documents to print in final summary.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier for audit tracing.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    init_database()
    print("DB initialized")

    if not args.skip_fetch:
        resolved_run_id = args.run_id or (
            f"etl-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
        )
        print(f"Fetch run id: {resolved_run_id}")
        finished_run_id = asyncio.run(
            run_pipeline(
                limit=args.fetch_limit,
                dry_run=args.dry_run,
                skip_download=args.skip_download,
                run_id=resolved_run_id,
                source="run_etl",
            )
        )
        print(f"Fetch run completed: {finished_run_id}")
    else:
        print("Fetch stage skipped")

    if not args.skip_parse:
        parsed_count = parse_all_pdfs(
            limit=args.parse_limit,
            max_pages=args.parse_max_pages,
            force=args.parse_force,
        )
        print(f"Parse stage complete: {parsed_count} files parsed")
    else:
        print("Parse stage skipped")

    docs = iter_parsed_documents(limit=args.review_limit)
    print(f"Review ready: {len(docs)} parsed text files available")


if __name__ == "__main__":
    main()
