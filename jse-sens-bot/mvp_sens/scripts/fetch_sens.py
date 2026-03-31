from __future__ import annotations

import argparse
import asyncio
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping
from urllib.parse import urljoin, urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mvp_sens.configs.config import (
    ALLOWED_PDF_HOSTS,
    BASE_URL,
    DOWNLOAD_MIN_INTERVAL_SECONDS,
    DOWNLOAD_BACKOFF_SECONDS,
    DOWNLOAD_RETRIES,
    KEYWORDS,
    PDF_BASE_URL,
    PDF_DIR,
    REQUEST_USER_AGENT,
    REQUEST_TIMEOUT,
    SKIP_WEEKEND_COLLECTION,
    SCRAPE_DOM_ALERT_MAX_RAW_CANDIDATES,
    SCRAPE_MAX_ATTEMPTS,
    SCRAPE_RETRY_BACKOFF_SECONDS,
    ensure_runtime_dirs,
)
from mvp_sens.scripts.classify_disclosures import (
    CLASSIFICATION_VERSION,
    classify_announcement,
    evaluate_issuer_eligibility,
)
from mvp_sens.scripts.db_insert import (
    announcement_exists,
    complete_ingest_run,
    connect_db,
    initialize_db,
    insert_release_signal,
    insert_announcement,
    log_ingest_event,
    start_ingest_run,
)
from mvp_sens.scripts.release_signals import ReleaseSignal, extract_release_signals

logger = logging.getLogger(__name__)
JSE_TIMEZONE = ZoneInfo("Africa/Johannesburg")
REJECT_REASON_NOT_PDF = "not_pdf_like_link"
REJECT_REASON_DISALLOWED_HOST = "disallowed_host"
REJECT_REASON_NOT_ANNOUNCEMENT = "not_probable_announcement_pdf"
REJECT_REASON_MISSING_SENS_ID = "missing_sens_id"
REJECT_REASON_DUPLICATE_SENS_ID = "duplicate_sens_id"
REJECT_REASON_ISSUER_UNKNOWN = "issuer_unknown"
REJECT_REASON_NON_EQUITY_ISSUER = "issuer_non_equity"

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
    issuer_context: str = ""
    issuer_tags: tuple[str, ...] = ()

    def as_record(
        self,
        local_pdf_path: str,
        extras: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "sens_id": self.sens_id,
            "company": self.company,
            "title": self.title,
            "announcement_date": self.announcement_date,
            "pdf_url": self.pdf_url,
            "local_pdf_path": local_pdf_path,
        }
        if extras:
            record.update(extras)
        return record


@dataclass(frozen=True)
class ScrapeResult:
    announcements: list[Announcement]
    raw_candidate_count: int
    reject_counts: dict[str, int]
    quarantine_candidates: list[dict[str, str]] = field(default_factory=list)
    attempt_count: int = 1
    dom_change_suspected: bool = False
    alerts: list[str] = field(default_factory=list)


_download_lock = threading.Lock()
_last_download_monotonic: float | None = None


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


def extract_company_from_context(issuer_context: str) -> str:
    """
    Extract company name from the issuer context string.

    The context is the innerText of the closest table row / container.
    The company name typically appears as the first non-empty line or
    before a known delimiter (newline, tab, date pattern).
    """
    if not issuer_context:
        return ""
    for line in issuer_context.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return issuer_context.strip()


def parse_jse_date(raw_date: str) -> str:
    """
    Parse a JSE date string into an ISO 8601 UTC string.

    Handles formats such as:
      - '31 Mar 2026 09:15'
      - '2026-03-31 09:15'
      - '2026-03-31T09:15:00'

    Date-only formats (no time component) are treated as UTC midnight for that
    calendar date to avoid date-shifting when the time is unknown.

    Returns an empty string on failure.
    """
    if not raw_date:
        return ""
    raw_date = raw_date.strip()
    # Formats that include a time component — convert from SAST to UTC.
    datetime_formats = [
        "%d %b %Y %H:%M",
        "%d %b %Y %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]
    for fmt in datetime_formats:
        try:
            dt = datetime.strptime(raw_date, fmt)
            return dt.replace(tzinfo=ZoneInfo("Africa/Johannesburg")).astimezone(
                timezone.utc
            ).isoformat()
        except ValueError:
            continue
    # Date-only formats — treat as UTC midnight to preserve the calendar date.
    date_only_formats = ["%d %b %Y", "%Y-%m-%d"]
    for fmt in date_only_formats:
        try:
            dt = datetime.strptime(raw_date, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return ""


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_weekend_in_jse_timezone(now_utc: datetime | None = None) -> bool:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    return now_utc.astimezone(JSE_TIMEZONE).weekday() >= 5


def should_skip_collection_now(now_utc: datetime | None = None) -> bool:
    return SKIP_WEEKEND_COLLECTION and is_weekend_in_jse_timezone(now_utc=now_utc)


def extract_urls_from_text(value: str) -> list[str]:
    if not value:
        return []
    return [match.group(1) for match in PDF_URL_RE.finditer(value)]


def _new_reject_counts() -> dict[str, int]:
    return {
        REJECT_REASON_NOT_PDF: 0,
        REJECT_REASON_DISALLOWED_HOST: 0,
        REJECT_REASON_NOT_ANNOUNCEMENT: 0,
        REJECT_REASON_MISSING_SENS_ID: 0,
        REJECT_REASON_DUPLICATE_SENS_ID: 0,
        REJECT_REASON_ISSUER_UNKNOWN: 0,
        REJECT_REASON_NON_EQUITY_ISSUER: 0,
    }


def _unpack_raw_candidate(
    raw_candidate: tuple[str, ...],
) -> tuple[str, str, str, str, str]:
    """Unpack a raw candidate tuple of 1-5 elements.

    Returns ``(href, title, context_text, raw_company, raw_date)``.
    """
    n = len(raw_candidate)
    href = str(raw_candidate[0]) if n >= 1 else ""
    title = str(raw_candidate[1]) if n >= 2 else ""
    context_text = str(raw_candidate[2]) if n >= 3 else ""
    raw_company = str(raw_candidate[3]) if n >= 4 else ""
    raw_date = str(raw_candidate[4]) if n >= 5 else ""
    return href, title, context_text, raw_company, raw_date


def parse_raw_candidates(
    raw_candidates: list[tuple[str, ...]],
    limit: int | None = None,
) -> tuple[list[Announcement], dict[str, int]]:
    announcements, reject_counts, _quarantine = parse_raw_candidates_with_quarantine(
        raw_candidates=raw_candidates,
        limit=limit,
    )
    return announcements, reject_counts


def parse_raw_candidates_with_quarantine(
    raw_candidates: list[tuple[str, ...]],
    limit: int | None = None,
) -> tuple[list[Announcement], dict[str, int], list[dict[str, str]]]:
    """
    Parse raw candidates into Announcement objects with explicit reject reasons.

    Unknown-issuer candidates are captured to a quarantine list for operator review.
    """
    announcements: list[Announcement] = []
    reject_counts = _new_reject_counts()
    seen_ids: set[str] = set()
    quarantine_candidates: list[dict[str, str]] = []

    for candidate in raw_candidates:
        raw_value, raw_title, raw_context, raw_company, raw_date = _unpack_raw_candidate(candidate)
        urls = extract_urls_from_text(raw_value)
        if not urls:
            urls = [raw_value]

        for url_candidate in urls:
            if not is_pdf_like_link(url_candidate):
                reject_counts[REJECT_REASON_NOT_PDF] += 1
                continue

            pdf_url = build_pdf_url(url_candidate)
            if not is_allowed_pdf_url(pdf_url):
                reject_counts[REJECT_REASON_DISALLOWED_HOST] += 1
                continue

            if not is_probable_announcement_url(pdf_url):
                reject_counts[REJECT_REASON_NOT_ANNOUNCEMENT] += 1
                continue

            sens_id = extract_sens_id(pdf_url)
            if not sens_id:
                reject_counts[REJECT_REASON_MISSING_SENS_ID] += 1
                continue

            if sens_id in seen_ids:
                reject_counts[REJECT_REASON_DUPLICATE_SENS_ID] += 1
                continue

            issuer_allowed, issuer_reason, issuer_tags = evaluate_issuer_eligibility(
                raw_title,
                raw_context,
            )
            if not issuer_allowed:
                if issuer_reason == REJECT_REASON_ISSUER_UNKNOWN:
                    reject_counts[REJECT_REASON_ISSUER_UNKNOWN] += 1
                    quarantine_candidates.append(
                        {
                            "reason": REJECT_REASON_ISSUER_UNKNOWN,
                            "raw_value": raw_value,
                            "title": normalize_text(raw_title),
                            "context": normalize_text(raw_context),
                            "pdf_url": pdf_url,
                            "sens_id": sens_id,
                        }
                    )
                else:
                    reject_counts[REJECT_REASON_NON_EQUITY_ISSUER] += 1
                continue

            seen_ids.add(sens_id)
            title = normalize_text(raw_title or sens_id)

            # Determine company: prefer raw_company from DOM, then first line of
            # issuer_context, then fall back to inferring from the title.
            company: str
            if raw_company:
                company = normalize_text(raw_company)
            else:
                ctx_company = extract_company_from_context(normalize_text(raw_context))
                company = ctx_company if ctx_company else infer_company(title)

            # Determine announcement date: prefer raw_date from DOM, fall back to now.
            announcement_date = parse_jse_date(raw_date) or now_utc_iso()

            announcements.append(
                Announcement(
                    sens_id=sens_id,
                    company=company,
                    title=title,
                    announcement_date=announcement_date,
                    pdf_url=pdf_url,
                    issuer_context=normalize_text(raw_context),
                    issuer_tags=issuer_tags,
                )
            )

            if limit is not None and len(announcements) >= limit:
                return announcements, reject_counts, quarantine_candidates

    return announcements, reject_counts, quarantine_candidates


def get_scrape_retry_delay_seconds(attempt: int, base_backoff_seconds: float) -> float:
    normalized_attempt = max(1, attempt)
    return max(0.0, base_backoff_seconds) * (2 ** (normalized_attempt - 1))


def is_dom_change_suspected(
    raw_candidate_count: int,
    scraped_count: int,
    now_utc: datetime | None = None,
) -> bool:
    if scraped_count > 0:
        return False
    if is_weekend_in_jse_timezone(now_utc=now_utc):
        return False
    return raw_candidate_count <= SCRAPE_DOM_ALERT_MAX_RAW_CANDIDATES


def should_retry_after_scrape(
    result: ScrapeResult,
    attempt: int,
    max_attempts: int,
    now_utc: datetime | None = None,
) -> bool:
    if attempt >= max_attempts:
        return False
    if result.announcements:
        return False
    if is_weekend_in_jse_timezone(now_utc=now_utc):
        return False
    return result.raw_candidate_count == 0 or is_dom_change_suspected(
        raw_candidate_count=result.raw_candidate_count,
        scraped_count=len(result.announcements),
        now_utc=now_utc,
    )


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


def _throttle_pdf_download() -> None:
    global _last_download_monotonic
    if DOWNLOAD_MIN_INTERVAL_SECONDS <= 0:
        return

    with _download_lock:
        now = time.monotonic()
        if _last_download_monotonic is not None:
            elapsed = now - _last_download_monotonic
            remaining = DOWNLOAD_MIN_INTERVAL_SECONDS - elapsed
            if remaining > 0:
                logger.info("Download throttling sleep %.2fs", remaining)
                time.sleep(remaining)
        _last_download_monotonic = time.monotonic()


async def _collect_raw_candidates(page) -> list[tuple[str, str, str, str, str]]:
    """
    Collect raw candidates from page frames.

    Each candidate is a 5-tuple:
        (href, title, context_text, raw_company, raw_date)

    ``raw_company`` and ``raw_date`` are extracted from the structured table row
    columns when the anchor lives inside a ``<tr>`` (JSE SENS table layout: col 1
    is the date, col 2 is the company name).  They default to empty strings when
    the structured extraction fails or when the anchor is not inside a ``<tr>``.

    This is intentionally broad to tolerate upstream DOM changes.
    """
    candidates: list[tuple[str, str, str, str, str]] = []

    for frame in page.frames:
        anchors = await frame.query_selector_all("a[href]")
        for anchor in anchors:
            href = (await anchor.get_attribute("href")) or ""
            title = normalize_text((await anchor.inner_text()) or "")
            row_data = await anchor.evaluate(
                """
                node => {
                  const tr = node.closest('tr');
                  const container = node.closest('tr,li,article,section,div');
                  const contextText = container ? (container.innerText || "") : "";
                  if (!tr) {
                    return { contextText, rawDate: "", rawCompany: "" };
                  }
                  const cells = tr.querySelectorAll('td');
                  const rawDate = cells.length >= 1 ? (cells[0].innerText || "").trim() : "";
                  const rawCompany = cells.length >= 2 ? (cells[1].innerText || "").trim() : "";
                  return { contextText, rawDate, rawCompany };
                }
                """
            )
            context_text = normalize_text(row_data.get("contextText", ""))
            raw_date = (row_data.get("rawDate") or "").strip()
            raw_company = (row_data.get("rawCompany") or "").strip()
            if not href:
                continue
            href_l = href.lower()
            if ".pdf" in href_l or "/documents/" in href_l:
                candidates.append((href, title, context_text, raw_company, raw_date))

        onclick_nodes = await frame.query_selector_all("[onclick]")
        for node in onclick_nodes:
            onclick_val = (await node.get_attribute("onclick")) or ""
            text = normalize_text((await node.inner_text()) or "")
            row_data = await node.evaluate(
                """
                node => {
                  const tr = node.closest('tr');
                  const container = node.closest('tr,li,article,section,div');
                  const contextText = container ? (container.innerText || "") : "";
                  if (!tr) {
                    return { contextText, rawDate: "", rawCompany: "" };
                  }
                  const cells = tr.querySelectorAll('td');
                  const rawDate = cells.length >= 1 ? (cells[0].innerText || "").trim() : "";
                  const rawCompany = cells.length >= 2 ? (cells[1].innerText || "").trim() : "";
                  return { contextText, rawDate, rawCompany };
                }
                """
            )
            context_text = normalize_text(row_data.get("contextText", ""))
            raw_date = (row_data.get("rawDate") or "").strip()
            raw_company = (row_data.get("rawCompany") or "").strip()
            if not onclick_val:
                continue
            onclick_l = onclick_val.lower()
            if ".pdf" in onclick_l or "/documents/" in onclick_l:
                candidates.append((onclick_val, text, context_text, raw_company, raw_date))

        # Fallback: inspect frame HTML in case links are embedded in script payloads.
        html = await frame.content()
        for embedded_url in extract_urls_from_text(html):
            candidates.append((embedded_url, "", "", "", ""))

    return candidates


def _normalize_scrape_exception(exc: Exception) -> Exception:
    msg = str(exc)
    if "Executable doesn't exist" in msg:
        return RuntimeError(
            "Chromium browser is not installed for Playwright. "
            "Run `python -m playwright install chromium` "
            "before live fetch. For Docker Compose, use "
            "`docker compose run --rm sens-scraper python -m playwright install chromium`."
        )
    if "error while loading shared libraries" in msg or "TargetClosedError" in msg:
        return RuntimeError(
            "Chromium failed to start because container system libraries are missing. "
            "Rebuild image after Dockerfile dependency updates: `docker compose build --no-cache sens-scraper`."
        )
    return exc


async def _scrape_once_with_playwright(
    playwright,
    limit: int | None = None,
) -> tuple[int, list[Announcement], dict[str, int], list[dict[str, str]]]:
    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()
    try:
        page.set_default_timeout(45000)
        await page.goto(BASE_URL, wait_until="networkidle")
        raw_candidates = await _collect_raw_candidates(page)
        logger.info("Collected %s raw announcement candidates", len(raw_candidates))
        announcements, reject_counts, quarantine_candidates = (
            parse_raw_candidates_with_quarantine(
                raw_candidates=raw_candidates,
                limit=limit,
            )
        )
        return len(raw_candidates), announcements, reject_counts, quarantine_candidates
    finally:
        await browser.close()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def _scrape_with_retry(
    scrape_once_fn: Callable[
        [int | None],
        Awaitable[tuple[int, list[Announcement], dict[str, int], list[dict[str, str]]]],
    ],
    limit: int | None,
    max_attempts: int,
    base_backoff_seconds: float,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    now_utc_fn: Callable[[], datetime] = _now_utc,
) -> ScrapeResult:
    attempts = max(1, max_attempts)
    dom_alert_message = (
        "Potential DOM/API drift detected: low candidate volume without valid announcements."
    )

    for attempt in range(1, attempts + 1):
        now_utc = now_utc_fn()
        try:
            raw_candidate_count, announcements, reject_counts, quarantine_candidates = (
                await scrape_once_fn(limit)
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            normalized_exc = _normalize_scrape_exception(exc)
            if attempt >= attempts:
                if normalized_exc is exc:
                    raise RuntimeError(
                        f"Scrape failed after {attempts} attempt(s): {exc}"
                    ) from exc
                raise normalized_exc from exc
            delay = get_scrape_retry_delay_seconds(
                attempt=attempt,
                base_backoff_seconds=base_backoff_seconds,
            )
            logger.warning(
                "Scrape attempt %s/%s failed: %s. Retrying in %.1fs.",
                attempt,
                attempts,
                normalized_exc,
                delay,
            )
            await sleep_fn(delay)
            continue

        logger.info(
            "Candidate filtering accepted=%s rejected=%s",
            len(announcements),
            sum(reject_counts.values()),
        )
        if sum(reject_counts.values()) > 0:
            logger.info("Candidate reject breakdown: %s", reject_counts)

        dom_change_suspected = is_dom_change_suspected(
            raw_candidate_count=raw_candidate_count,
            scraped_count=len(announcements),
            now_utc=now_utc,
        )
        alerts: list[str] = []
        if dom_change_suspected:
            alerts.append(dom_alert_message)

        result = ScrapeResult(
            announcements=announcements,
            raw_candidate_count=raw_candidate_count,
            reject_counts=reject_counts,
            quarantine_candidates=quarantine_candidates,
            attempt_count=attempt,
            dom_change_suspected=dom_change_suspected,
            alerts=alerts,
        )

        if should_retry_after_scrape(
            result=result,
            attempt=attempt,
            max_attempts=attempts,
            now_utc=now_utc,
        ):
            delay = get_scrape_retry_delay_seconds(
                attempt=attempt,
                base_backoff_seconds=base_backoff_seconds,
            )
            logger.warning(
                "Scrape attempt %s/%s returned no usable announcements (raw_candidates=%s). "
                "Retrying in %.1fs.",
                attempt,
                attempts,
                raw_candidate_count,
                delay,
            )
            await sleep_fn(delay)
            continue

        if not result.announcements:
            if is_weekend_in_jse_timezone(now_utc=now_utc):
                logger.info(
                    "No announcements discovered on page. This can be normal on weekends (JSE timezone)."
                )
            else:
                logger.warning(
                    "No announcements discovered on page. This may indicate a DOM/API change at %s",
                    BASE_URL,
                )
        return result

    raise RuntimeError("Scrape attempts exhausted unexpectedly.")


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
        _throttle_pdf_download()
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


def extract_pdf_text_for_classification(pdf_path: Path, max_pages: int = 2) -> str:
    try:
        from pypdf import PdfReader
    except Exception as exc:
        logger.warning("pypdf unavailable for classification fallback: %s", exc)
        return ""

    try:
        reader = PdfReader(str(pdf_path))
        chunks: list[str] = []
        for page_index, page in enumerate(reader.pages):
            if max_pages > 0 and page_index >= max_pages:
                break
            text = page.extract_text() or ""
            normalized = normalize_text(text)
            if normalized:
                chunks.append(normalized)
        return "\n".join(chunks)
    except Exception as exc:
        logger.warning("Failed to extract PDF text for %s: %s", pdf_path, exc)
        return ""


def _collect_release_signals(
    title: str,
    body_text: str = "",
) -> list[ReleaseSignal]:
    candidates = extract_release_signals(title, source="title")
    if body_text:
        candidates.extend(extract_release_signals(body_text, source="pdf"))

    deduped: list[ReleaseSignal] = []
    seen: set[tuple[str, str, str, str]] = set()
    for signal in candidates:
        key = (
            signal.signal_type,
            signal.signal_datetime,
            signal.source_text,
            signal.source,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(signal)
    return deduped


def _persist_release_signals(
    *,
    conn,
    run_id: str,
    sens_id: str,
    signals: list[ReleaseSignal],
) -> int:
    inserted_count = 0
    for signal in signals:
        inserted = insert_release_signal(
            conn=conn,
            sens_id=sens_id,
            signal_type=signal.signal_type,
            signal_datetime=signal.signal_datetime,
            source_text=signal.source_text,
            source=signal.source,
        )
        if inserted:
            inserted_count += 1

    if signals:
        log_ingest_event(
            conn=conn,
            run_id=run_id,
            stage="release_signal",
            event_type="info",
            sens_id=sens_id,
            message="Release signal extraction",
            metadata={
                "candidate_count": len(signals),
                "inserted_count": inserted_count,
            },
        )
    return inserted_count


async def scrape_announcements(limit: int | None = None) -> ScrapeResult:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright is required for scraping. Install dependencies and run `playwright install chromium`."
        ) from exc

    async with async_playwright() as playwright:
        return await _scrape_with_retry(
            scrape_once_fn=lambda resolved_limit: _scrape_once_with_playwright(
                playwright,
                limit=resolved_limit,
            ),
            limit=limit,
            max_attempts=SCRAPE_MAX_ATTEMPTS,
            base_backoff_seconds=SCRAPE_RETRY_BACKOFF_SECONDS,
        )


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
    dry_run_no_download: bool = False,
    include_all: bool = False,
    run_id: str | None = None,
    source: str = "sens_web",
) -> str:
    ensure_runtime_dirs()
    mode = "dry-run" if dry_run else "live"
    resolved_run_id = run_id or (
        f"sens-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    )

    scraped_count = 0
    inserted = 0
    skipped_irrelevant = 0
    skipped_existing = 0
    skipped_failed_download = 0
    run_status = "failed"
    run_error: str | None = None
    effective_skip_download = skip_download or (dry_run and dry_run_no_download)

    with connect_db() as conn:
        initialize_db(conn)
        start_ingest_run(
            conn=conn,
            run_id=resolved_run_id,
            source=source,
            mode=mode,
        )
        log_ingest_event(
            conn=conn,
            run_id=resolved_run_id,
            stage="pipeline",
            event_type="info",
            message="Pipeline started",
            metadata={
                "limit": limit,
                "skip_download": skip_download,
                "dry_run_no_download": dry_run_no_download,
                "effective_skip_download": effective_skip_download,
                "include_all": include_all,
                "dry_run": dry_run,
            },
        )

        if should_skip_collection_now():
            run_status = "skipped"
            skip_message = (
                "Fetch skipped because weekend collection is disabled "
                "(Africa/Johannesburg timezone)."
            )
            logger.info(skip_message)
            log_ingest_event(
                conn=conn,
                run_id=resolved_run_id,
                stage="scheduler",
                event_type="info",
                message=skip_message,
            )
            complete_ingest_run(
                conn=conn,
                run_id=resolved_run_id,
                status=run_status,
                scraped_count=scraped_count,
                inserted_count=inserted,
                skipped_irrelevant_count=skipped_irrelevant,
                skipped_existing_count=skipped_existing,
                skipped_failed_download_count=skipped_failed_download,
                error_message=run_error,
            )
            summarize_run(
                scraped=scraped_count,
                inserted=inserted,
                skipped_irrelevant=skipped_irrelevant,
                skipped_existing=skipped_existing,
                skipped_failed_download=skipped_failed_download,
                dry_run=dry_run,
            )
            logger.info("Pipeline completed run_id=%s status=%s", resolved_run_id, run_status)
            return resolved_run_id

        try:
            scrape_result = await scrape_announcements(limit=limit)
            announcements = scrape_result.announcements
            scraped_count = len(announcements)
            log_ingest_event(
                conn=conn,
                run_id=resolved_run_id,
                stage="scrape",
                event_type="info",
                message="Scrape completed",
                metadata={
                    "raw_candidate_count": scrape_result.raw_candidate_count,
                    "scraped_count": scraped_count,
                    "reject_counts": scrape_result.reject_counts,
                    "quarantine_count": len(scrape_result.quarantine_candidates),
                    "attempt_count": scrape_result.attempt_count,
                    "dom_change_suspected": scrape_result.dom_change_suspected,
                    "alerts": scrape_result.alerts,
                },
            )
            for quarantined in scrape_result.quarantine_candidates:
                log_ingest_event(
                    conn=conn,
                    run_id=resolved_run_id,
                    stage="quarantine",
                    event_type="warning",
                    sens_id=quarantined.get("sens_id"),
                    message="Candidate quarantined: issuer context unknown",
                    metadata=quarantined,
                )
            if scrape_result.attempt_count > 1:
                log_ingest_event(
                    conn=conn,
                    run_id=resolved_run_id,
                    stage="alert",
                    event_type="warning",
                    message="Scrape required retries before completion",
                    metadata={"attempt_count": scrape_result.attempt_count},
                )
            for alert_message in scrape_result.alerts:
                log_ingest_event(
                    conn=conn,
                    run_id=resolved_run_id,
                    stage="alert",
                    event_type="warning",
                    message=alert_message,
                    metadata={
                        "raw_candidate_count": scrape_result.raw_candidate_count,
                        "scraped_count": scraped_count,
                    },
                )
            if sum(scrape_result.reject_counts.values()) > 0:
                log_ingest_event(
                    conn=conn,
                    run_id=resolved_run_id,
                    stage="filter",
                    event_type="info",
                    message="Candidate filtering summary",
                    metadata={"reject_counts": scrape_result.reject_counts},
                )

            for item in announcements:
                classification = classify_announcement(
                    title=item.title,
                    issuer_context=item.issuer_context,
                )

                local_pdf_path = ""
                classification_pdf_text = ""
                disambiguation_attempted = False
                disambiguation_succeeded = False

                if (
                    not include_all
                    and not classification.analyst_relevant
                    and classification.ambiguous
                    and not effective_skip_download
                ):
                    disambiguation_attempted = True
                    downloaded_path = download_pdf(item.pdf_url, item.sens_id)
                    if downloaded_path is None:
                        skipped_failed_download += 1
                        log_ingest_event(
                            conn=conn,
                            run_id=resolved_run_id,
                            stage="download",
                            event_type="warning",
                            sens_id=item.sens_id,
                            message="PDF download failed during disambiguation",
                            metadata={"pdf_url": item.pdf_url},
                        )
                        continue

                    local_pdf_path = str(downloaded_path)
                    classification_pdf_text = extract_pdf_text_for_classification(
                        downloaded_path
                    )
                    classification = classify_announcement(
                        title=item.title,
                        issuer_context=item.issuer_context,
                        body_text=classification_pdf_text,
                    )
                    disambiguation_succeeded = classification.analyst_relevant

                log_ingest_event(
                    conn=conn,
                    run_id=resolved_run_id,
                    stage="classify",
                    event_type="info",
                    sens_id=item.sens_id,
                    message="Classification decision",
                    metadata={
                        "category": classification.category,
                        "classification_reason": classification.classification_reason,
                        "classification_version": CLASSIFICATION_VERSION,
                        "analyst_relevant": classification.analyst_relevant,
                        "relevance_reason": classification.relevance_reason,
                        "issuer_tags": list(classification.issuer_tags),
                        "issuer_allowed": classification.issuer_allowed,
                        "issuer_reason": classification.issuer_reason,
                        "ambiguous": classification.ambiguous,
                        "disambiguation_attempted": disambiguation_attempted,
                        "disambiguation_succeeded": disambiguation_succeeded,
                    },
                )

                if not include_all and not classification.analyst_relevant:
                    skipped_irrelevant += 1
                    logger.info(
                        "Skipping non-relevant announcement sens_id=%s title=%s reason=%s",
                        item.sens_id,
                        item.title,
                        classification.relevance_reason,
                    )
                    continue

                release_signals: list[ReleaseSignal] = []
                if classification.analyst_relevant:
                    release_signals = _collect_release_signals(
                        title=item.title,
                        body_text=classification_pdf_text,
                    )

                if announcement_exists(conn, item.sens_id):
                    skipped_existing += 1
                    if release_signals and not dry_run:
                        _persist_release_signals(
                            conn=conn,
                            run_id=resolved_run_id,
                            sens_id=item.sens_id,
                            signals=release_signals,
                        )
                    continue

                if not effective_skip_download and not local_pdf_path:
                    downloaded_path = download_pdf(item.pdf_url, item.sens_id)
                    if downloaded_path is None:
                        skipped_failed_download += 1
                        log_ingest_event(
                            conn=conn,
                            run_id=resolved_run_id,
                            stage="download",
                            event_type="warning",
                            sens_id=item.sens_id,
                            message="PDF download failed",
                            metadata={"pdf_url": item.pdf_url},
                        )
                        continue
                    local_pdf_path = str(downloaded_path)

                if dry_run:
                    logger.info(
                        "Dry run announcement: %s category=%s relevant=%s",
                        item.title,
                        classification.category,
                        classification.analyst_relevant,
                    )
                    continue

                inserted_now = insert_announcement(
                    conn,
                    item.as_record(
                        local_pdf_path,
                        extras={
                            "category": classification.category,
                            "classification_reason": classification.classification_reason,
                            "classification_version": CLASSIFICATION_VERSION,
                            "first_seen_run_id": resolved_run_id,
                            "first_seen_at": now_utc_iso(),
                            "classified_at": now_utc_iso(),
                            "analyst_relevant": classification.analyst_relevant,
                            "relevance_reason": classification.relevance_reason,
                        },
                    ),
                )
                if inserted_now:
                    inserted += 1
                    logger.info("Saved: %s", item.title)
                    if release_signals:
                        _persist_release_signals(
                            conn=conn,
                            run_id=resolved_run_id,
                            sens_id=item.sens_id,
                            signals=release_signals,
                        )

            run_status = "success"
        except Exception as exc:
            run_error = str(exc)
            log_ingest_event(
                conn=conn,
                run_id=resolved_run_id,
                stage="pipeline",
                event_type="error",
                message="Pipeline failed",
                metadata={"error": run_error},
            )
            raise
        finally:
            complete_ingest_run(
                conn=conn,
                run_id=resolved_run_id,
                status=run_status,
                scraped_count=scraped_count,
                inserted_count=inserted,
                skipped_irrelevant_count=skipped_irrelevant,
                skipped_existing_count=skipped_existing,
                skipped_failed_download_count=skipped_failed_download,
                error_message=run_error,
            )

    summarize_run(
        scraped=scraped_count,
        inserted=inserted,
        skipped_irrelevant=skipped_irrelevant,
        skipped_existing=skipped_existing,
        skipped_failed_download=skipped_failed_download,
        dry_run=dry_run,
    )
    logger.info("Pipeline completed run_id=%s status=%s", resolved_run_id, run_status)
    return resolved_run_id


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
        "--dry-run-no-download",
        action="store_true",
        help=(
            "When used with --dry-run, prevent any PDF downloads "
            "(including ambiguity fallback downloads)."
        ),
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
    run_id = asyncio.run(
        run_pipeline(
            limit=args.limit,
            dry_run=args.dry_run,
            skip_download=args.skip_download,
            dry_run_no_download=args.dry_run_no_download,
            include_all=args.include_all,
        )
    )
    logger.info("Finished run_id=%s", run_id)


if __name__ == "__main__":
    main()
