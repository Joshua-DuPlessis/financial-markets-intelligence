from __future__ import annotations

import argparse
from pathlib import Path

from mvp_sens.configs.config import PARSED_TEXT_DIR, ensure_runtime_dirs


def iter_parsed_documents(limit: int | None = None) -> list[Path]:
    ensure_runtime_dirs()
    files = sorted(PARSED_TEXT_DIR.glob("*.txt"))
    if limit is not None:
        return files[:limit]
    return files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Placeholder reviewer for parsed announcements."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of parsed announcement text files to list.",
    )
    args = parser.parse_args()

    files = iter_parsed_documents(limit=args.limit)
    if not files:
        print("No parsed files found. Run parse_pdf.py first.")
        return

    print("Parsed announcements available for downstream LLM review:")
    for file in files:
        print(f"- {file}")


if __name__ == "__main__":
    main()
