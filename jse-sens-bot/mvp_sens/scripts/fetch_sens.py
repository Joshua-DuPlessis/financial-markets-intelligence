from __future__ import annotations

import argparse
import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mvp_sens.configs.config import (
    ALLOWED_PDF_HOSTS,
    BASE_URL,
    DOWNLOAD_BACKOFF_SECONDS,
    DOWNLOAD_RETRIES,
    KEYWORDS,
    PDF_BASE_URL,
    PDF_DIR,
    REQUEST_USER_AGENT,
    REQUEST_TIMEOUT,
    ensure_runtime_dirs,
)
from mvp_sens.scripts.db_insert import (
    announcement_exists,
    connect_db,
    initialize_db,
    insert_announcement,
)

logger = logging.getLogger(__name__)
JSE_TIMEZONE = ZoneInfo("Africa/Johannesburg")

PDF_URL_RE = re.compile(
    r"(https?://[^\s\"'()<>]+?\.pdf|/[^\s\"'()<>]*?\.pdf)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Announcement:
    sens_id: str
    company: str
    title: str
    announcement_date: str
    pdf_url: str

    def as_record(self, local_pdf_path: str) -> dict[str, str]:
        return {
            "sens_id": self.sens_id,
            "company": self.company,
            "title": self.title,
            "announcement_date": self.announcement_date,
            "pdf_url": self.pdf_url,
            "local_pdf_path": local_pdf_path,
        }


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def is_relevant(title: str) -> bool:
    lowered = title.lower()
    return any(keyword in lowered for keyword in KEYWORDS)


def normalize_text(value: str) -> str:
    return " ".join(value.split())


def is_pdf_like_link(value: str) -> bool:
    lowered = (value or "").lower()
    return ".pdf" in lowered or "/documents/" in lowered or "documents/" in lowered


def build_pdf_url(href: str) -> str:
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href

    lowered = href.lower()
    if lowered.startswith("/documents/") or lowered.startswith("documents/"):
        return urljoin(PDF_BASE_URL, href)

    return urljoin(BASE_URL, href)


def is_allowed_pdf_url(pdf_url: str) -> bool:
    host = (urlparse(pdf_url).hostname or "").lower()
    if not host:
        return False
    return any(
        host == allowed_host or host.endswith(f".{allowed_host}")
        for allowed_host in ALLOWED_PDF_HOSTS
    )


def extract_sens_id(pdf_url: str) -> str:
    parsed = urlparse(pdf_url)
    filename = Path(parsed.path).name
    stem = Path(filename).stem

    prefixed = re.search(r"^sens[_-]?([a-z0-9_-]+)$", stem, flags=re.IGNORECASE)
    if prefixed and parsed.path.lower().endswith(".pdf"):
        return prefixed.group(1)

    if "_" in stem:
        trailing = stem.split("_")[-1]
        trailing_clean = "".join(ch for ch in trailing if ch.isalnum() or ch in {"-", "_"})
        if trailing_clean and re.search(r"\d", trailing_clean):
            return trailing_clean

    fallback = re.search(r"(?:sens[_-]?)([a-z0-9_-]+)", pdf_url, flags=re.IGNORECASE)
    if fallback:
        token = fallback.group(1)
        if re.search(r"\d", token) or parsed.path.lower().endswith(".pdf"):
            return token

    digits = re.search(r"(\d{5,})", pdf_url)
    if digits:
        return digits.group(1)

    return ""


def is_probable_announcement_url(pdf_url: str) -> bool:
    parsed = urlparse(pdf_url)
    path = parsed.path.lower()
    if not path.endswith(".pdf"):
        return False
    if "/documents/" in path:
        return True
    return "sens" in path


def infer_company(title: str) -> str:
    if "|" in title:
        return title.split("|", maxsplit=1)[0].strip()
    return title.strip()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_weekend_in_jse_timezone() -> bool:
    return datetime.now(timezone.utc).astimezone(JSE_TIMEZONE).weekday() >= 5


def _extract_urls_from_text(value: str) -> list[str]:
    if not value:
        return []
    return [match.group(1) for match in PDF_URL_RE.finditer(value)]


def _build_http_session() -> requests.Session:
    retry = Retry(
        total=DOWNLOAD_RETRIES,
        connect=DOWNLOAD_RETRIES,
        read=DOWNLOAD_RETRIES,
        status=DOWNLOAD_RETRIES,
        backoff_factor=DOWNLOAD_BACKOFF_SECONDS,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": REQUEST_USER_AGENT,
            "Accept": "application/pdf,*/*;q=0.8",
        }
    )
    return session


def _is_valid_pdf_file(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    with path.open("rb") as file_handle:
        return file_handle.read(5) == b"%PDF-"


async def _collect_raw_candidates(page) -> list[tuple[str, str]]:
    """
    Collect raw (href_like_value, title_text) candidates from page frames.

    This is intentionally broad to tolerate upstream DOM changes.
    """
    candidates: list[tuple[str, str]] = []

    for frame in page.frames:
        anchors = await frame.query_selector_all("a[href]")
        for anchor in anchors:
            href = (await anchor.get_attribute("href")) or ""
            title = normalize_text((await anchor.inner_text()) or "")
            if not href:
                continue
            href_l = href.lower()
            if ".pdf" in href_l or "/documents/" in href_l:
                candidates.append((href, title))

        onclick_nodes = await frame.query_selector_all("[onclick]")
        for node in onclick_nodes:
            onclick_val = (await node.get_attribute("onclick")) or ""
            text = normalize_text((await node.inner_text()) or "")
            if not onclick_val:
                continue
            onclick_l = onclick_val.lower()
            if ".pdf" in onclick_l or "/documents/" in onclick_l:
                candidates.append((onclick_val, text))

        # Fallback: inspect frame HTML in case links are embedded in script payloads.
        html = await frame.content()
        for embedded_url in _extract_urls_from_text(html):
            candidates.append((embedded_url, ""))

    return candidates


def download_pdf(pdf_url: str, sens_id: str) -> Path | None:
    try:
        if not is_allowed_pdf_url(pdf_url):
            logger.warning("Skipping download from disallowed host: %s", pdf_url)
            return None

        safe_sens_id = "".join(ch for ch in sens_id if ch.isalnum() or ch in {"-", "_"})
        if not safe_sens_id:
            logger.warning("Skipping PDF download because SENS ID is empty: %s", pdf_url)
            return None

        file_path = PDF_DIR / f"{safe_sens_id}.pdf"
        if file_path.exists() and _is_valid_pdf_file(file_path):
            return file_path

        temp_path = file_path.with_name(f"{file_path.stem}.{uuid4().hex}.tmp")
        with _build_http_session() as session:
            with session.get(pdf_url, timeout=REQUEST_TIMEOUT, stream=True) as response:
                response.raise_for_status()
                with temp_path.open("wb") as temp_file:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            temp_file.write(chunk)

        if not _is_valid_pdf_file(temp_path):
            temp_path.unlink(missing_ok=True)
            logger.warning("Downloaded file is not a valid PDF: %s", pdf_url)
            return None

        temp_path.replace(file_path)
        return file_path
    except Exception as exc:
        logger.exception("PDF download failed for %s: %s", sens_id, exc)
        return None


async def scrape_announcements(limit: int | None = None) -> list[Announcement]:
    results: list[Announcement] = []
    seen_ids: set[str] = set()

    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is required for scraping. Install dependencies and run `playwright install chromium`."
        ) from exc

    async with async_playwright() as playwright:
        try:
            browser = await playwright.chromium.launch(headless=True)
        except Exception as exc:
            msg = str(exc)
            if "Executable doesn't exist" in msg:
                raise RuntimeError(
                    "Chromium browser is not installed for Playwright. "
                    "Run `python -m playwright install chromium` "
                    "before live fetch. For Docker Compose, use "
                    "`docker compose run --rm sens-scraper python -m playwright install chromium`."
                ) from exc
            if "error while loading shared libraries" in msg or "TargetClosedError" in msg:
                raise RuntimeError(
                    "Chromium failed to start because container system libraries are missing. "
                    "Rebuild image after Dockerfile dependency updates: `docker compose build --no-cache sens-scraper`."
                ) from exc
            raise
        page = await browser.new_page()

        try:
            page.set_default_timeout(45000)
            await page.goto(BASE_URL, wait_until="networkidle")
            raw_candidates = await _collect_raw_candidates(page)
            logger.info("Collected %s raw announcement candidates", len(raw_candidates))

            for raw_value, raw_title in raw_candidates:
                urls = _extract_urls_from_text(raw_value)
                if not urls:
                    urls = [raw_value]

                for url_candidate in urls:
                    if not is_pdf_like_link(url_candidate):
                        continue

                    pdf_url = build_pdf_url(url_candidate)
                    if not is_allowed_pdf_url(pdf_url):
                        continue
                    if not is_probable_announcement_url(pdf_url):
                        continue

                    sens_id = extract_sens_id(pdf_url)

                    if not sens_id or sens_id in seen_ids:
                        continue

                    seen_ids.add(sens_id)
                    title = normalize_text(raw_title or sens_id)
                    results.append(
                        Announcement(
                            sens_id=sens_id,
                            company=infer_company(title),
                            title=title,
                            announcement_date=now_utc_iso(),
                            pdf_url=pdf_url,
                        )
                    )

                    if limit is not None and len(results) >= limit:
                        break
                if limit is not None and len(results) >= limit:
                    break

            if not results:
                if _is_weekend_in_jse_timezone():
                    logger.info(
                        "No announcements discovered on page. This can be normal on weekends (JSE timezone)."
                    )
                else:
                    logger.warning(
                        "No announcements discovered on page. This may indicate a DOM/API change at %s",
                        BASE_URL,
                    )
        finally:
            await browser.close()

    return results


def summarize_run(
    scraped: int,
    inserted: int,
    skipped_irrelevant: int,
    skipped_existing: int,
    skipped_failed_download: int,
    dry_run: bool,
) -> None:
    mode = "DRY-RUN" if dry_run else "LIVE"
    logger.info(
        "%s summary scraped=%s inserted=%s skipped_irrelevant=%s skipped_existing=%s skipped_failed_download=%s",
        mode,
        scraped,
        inserted,
        skipped_irrelevant,
        skipped_existing,
        skipped_failed_download,
    )


async def run_pipeline(
    limit: int | None = None,
    dry_run: bool = False,
    skip_download: bool = False,
    include_all: bool = False,
) -> None:
    ensure_runtime_dirs()
    announcements = await scrape_announcements(limit=limit)

    inserted = 0
    skipped_irrelevant = 0
    skipped_existing = 0
    skipped_failed_download = 0

    with connect_db() as conn:
        initialize_db(conn)

        for item in announcements:
            if not include_all and not is_relevant(item.title):
                skipped_irrelevant += 1
                logger.info(
                    "Skipping non-keyword announcement sens_id=%s title=%s",
                    item.sens_id,
                    item.title,
                )
                continue

            if announcement_exists(conn, item.sens_id):
                skipped_existing += 1
                continue

            local_pdf_path = ""
            if not skip_download:
                downloaded_path = download_pdf(item.pdf_url, item.sens_id)
                if downloaded_path is None:
                    skipped_failed_download += 1
                    continue
                local_pdf_path = str(downloaded_path)

            if dry_run:
                logger.info("Dry run announcement: %s", item.title)
                continue

            inserted_now = insert_announcement(conn, item.as_record(local_pdf_path))
            if inserted_now:
                inserted += 1
                logger.info("Saved: %s", item.title)

    summarize_run(
        scraped=len(announcements),
        inserted=inserted,
        skipped_irrelevant=skipped_irrelevant,
        skipped_existing=skipped_existing,
        skipped_failed_download=skipped_failed_download,
        dry_run=dry_run,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch and store JSE SENS announcements.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of announcements to process.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape and filter announcements without writing to DB.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip PDF download step and store records with empty local paths.",
    )
    parser.add_argument(
        "--include-all",
        action="store_true",
        help="Bypass keyword filtering and process all scraped announcements.",
    )
    return parser


def _iter_example_commands() -> Iterable[str]:
    yield "python3 -m mvp_sens.scripts.db_insert"
    yield "python3 -m mvp_sens.scripts.fetch_sens --dry-run --limit 5"
    yield "python3 -m mvp_sens.scripts.fetch_sens --limit 20"


def main() -> None:
    configure_logging()
    args = _build_parser().parse_args()
    logger.info("Starting SENS fetch pipeline")
    logger.info("Example commands: %s", " | ".join(_iter_example_commands()))
    asyncio.run(
        run_pipeline(
            limit=args.limit,
            dry_run=args.dry_run,
            skip_download=args.skip_download,
            include_all=args.include_all,
        )
    )


if __name__ == "__main__":
    main()
