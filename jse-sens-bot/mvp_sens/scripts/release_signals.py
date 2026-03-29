from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

JSE_TIMEZONE = ZoneInfo("Africa/Johannesburg")

_SIGNAL_CONTEXT_KEYWORDS = (
    "will be released",
    "to be released",
    "will be available",
    "to be made available",
    "available on",
    "release date",
    "to be published",
    "will be published",
    "publication date",
)

_MONTH_NAME_DATE_RE = re.compile(
    r"\b(?P<day>\d{1,2})\s+"
    r"(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)\s+"
    r"(?P<year>\d{4})"
    r"(?:\s*(?:at|@)\s*(?P<hour>\d{1,2})[:h](?P<minute>\d{2}))?\b",
    re.IGNORECASE,
)

_NUMERIC_DATE_RE = re.compile(
    r"\b(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{4})"
    r"(?:\s*(?:at|@)\s*(?P<hour>\d{1,2})[:h](?P<minute>\d{2}))?\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ReleaseSignal:
    signal_type: str
    signal_datetime: str
    source_text: str
    source: str


def _normalize_text(value: str) -> str:
    return " ".join(value.lower().split())


def _has_signal_context(normalized: str) -> bool:
    return any(keyword in normalized for keyword in _SIGNAL_CONTEXT_KEYWORDS)


def _safe_datetime(
    year: int,
    month: int,
    day: int,
    hour: int | None,
    minute: int | None,
) -> datetime | None:
    try:
        if hour is None or minute is None:
            return datetime(year, month, day, 0, 0, tzinfo=JSE_TIMEZONE)
        return datetime(year, month, day, hour, minute, tzinfo=JSE_TIMEZONE)
    except ValueError:
        return None


def _parse_match_to_datetime(match: re.Match[str], month_numeric: int | None = None) -> datetime | None:
    day = int(match.group("day"))
    year = int(match.group("year"))
    hour = int(match.group("hour")) if match.group("hour") else None
    minute = int(match.group("minute")) if match.group("minute") else None

    if month_numeric is None:
        month_token = match.group("month")
        try:
            month_numeric = datetime.strptime(month_token, "%B").month
        except ValueError:
            try:
                month_numeric = datetime.strptime(month_token[:3], "%b").month
            except ValueError:
                month_numeric = None
        if month_numeric is None:
            return None
    return _safe_datetime(year, month_numeric, day, hour, minute)


def extract_release_signals(text: str, source: str = "title") -> list[ReleaseSignal]:
    if not text:
        return []

    normalized = _normalize_text(text)
    if not _has_signal_context(normalized):
        return []

    signals: list[ReleaseSignal] = []
    seen: set[tuple[str, str, str, str]] = set()

    for match in _MONTH_NAME_DATE_RE.finditer(text):
        dt = _parse_match_to_datetime(match)
        if dt is None:
            continue
        signal_type = "future_release_datetime" if match.group("hour") else "future_release_date"
        signal = ReleaseSignal(
            signal_type=signal_type,
            signal_datetime=dt.isoformat(),
            source_text=match.group(0).strip(),
            source=source,
        )
        dedupe_key = (
            signal.signal_type,
            signal.signal_datetime,
            signal.source_text,
            signal.source,
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        signals.append(signal)

    for match in _NUMERIC_DATE_RE.finditer(text):
        month_numeric = int(match.group("month"))
        dt = _parse_match_to_datetime(match, month_numeric=month_numeric)
        if dt is None:
            continue
        signal_type = "future_release_datetime" if match.group("hour") else "future_release_date"
        signal = ReleaseSignal(
            signal_type=signal_type,
            signal_datetime=dt.isoformat(),
            source_text=match.group(0).strip(),
            source=source,
        )
        dedupe_key = (
            signal.signal_type,
            signal.signal_datetime,
            signal.source_text,
            signal.source,
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        signals.append(signal)

    return signals
