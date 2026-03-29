import os
from pathlib import Path


def _int_env(name: str, default: int, minimum: int = 0) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        parsed = int(raw_value)
    except ValueError:
        parsed = default
    return max(minimum, parsed)


def _float_env(name: str, default: float, minimum: float = 0.0) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        parsed = float(raw_value)
    except ValueError:
        parsed = default
    return max(minimum, parsed)


BASE_URL = os.getenv(
    "SENS_BASE_URL", "https://clientportal.jse.co.za/communication/sens-announcements"
)
PDF_BASE_URL = os.getenv("SENS_PDF_BASE_URL", "https://senspdf.jse.co.za/")

APP_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("SENS_DATA_DIR", str(APP_ROOT / "data")))
PDF_DIR = Path(os.getenv("SENS_PDF_DIR", str(DATA_DIR / "pdfs")))
PARSED_TEXT_DIR = Path(os.getenv("SENS_PARSED_TEXT_DIR", str(DATA_DIR / "parsed")))
EXPORT_DIR = Path(os.getenv("SENS_EXPORT_DIR", str(DATA_DIR / "exports")))
DB_PATH = Path(os.getenv("SENS_DB_PATH", str(APP_ROOT / "db" / "sens.db")))
SCHEMA_PATH = Path(os.getenv("SENS_SCHEMA_PATH", str(APP_ROOT / "db" / "schema.sql")))

REQUEST_TIMEOUT = int(os.getenv("SENS_REQUEST_TIMEOUT", "30"))
SCRAPE_INTERVAL_MINUTES = _int_env("SENS_SCRAPE_INTERVAL_MINUTES", default=5, minimum=1)
DOWNLOAD_RETRIES = _int_env("SENS_DOWNLOAD_RETRIES", default=3, minimum=0)
DOWNLOAD_BACKOFF_SECONDS = _float_env(
    "SENS_DOWNLOAD_BACKOFF_SECONDS",
    default=1.0,
    minimum=0.0,
)
DOWNLOAD_MIN_INTERVAL_SECONDS = _float_env(
    "SENS_DOWNLOAD_MIN_INTERVAL_SECONDS",
    default=1.0,
    minimum=0.0,
)
SCRAPE_MAX_ATTEMPTS = _int_env("SENS_SCRAPE_MAX_ATTEMPTS", default=2, minimum=1)
SCRAPE_RETRY_BACKOFF_SECONDS = _float_env(
    "SENS_SCRAPE_RETRY_BACKOFF_SECONDS",
    default=2.0,
    minimum=0.0,
)
SCRAPE_DOM_ALERT_MAX_RAW_CANDIDATES = _int_env(
    "SENS_SCRAPE_DOM_ALERT_MAX_RAW_CANDIDATES",
    default=2,
    minimum=0,
)
REQUEST_USER_AGENT = os.getenv(
    "SENS_REQUEST_USER_AGENT",
    "jse-sens-bot/1.0 (+local-staging)",
)
SKIP_WEEKEND_COLLECTION = os.getenv("SENS_SKIP_WEEKENDS", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ALLOWED_PDF_HOSTS = tuple(
    host.strip().lower()
    for host in os.getenv("SENS_ALLOWED_PDF_HOSTS", "senspdf.jse.co.za").split(",")
    if host.strip()
)
SCHEDULER_WEEKDAY_PREOPEN_MINUTES = _int_env(
    "SENS_SCHEDULER_WEEKDAY_PREOPEN_MINUTES",
    default=10,
    minimum=1,
)
SCHEDULER_WEEKDAY_MARKET_MINUTES = _int_env(
    "SENS_SCHEDULER_WEEKDAY_MARKET_MINUTES",
    default=SCRAPE_INTERVAL_MINUTES,
    minimum=1,
)
SCHEDULER_WEEKDAY_AFTERCLOSE_MINUTES = _int_env(
    "SENS_SCHEDULER_WEEKDAY_AFTERCLOSE_MINUTES",
    default=10,
    minimum=1,
)
SCHEDULER_WEEKDAY_OFFHOURS_MINUTES = _int_env(
    "SENS_SCHEDULER_WEEKDAY_OFFHOURS_MINUTES",
    default=60,
    minimum=1,
)
SCHEDULER_WEEKEND_MINUTES = _int_env(
    "SENS_SCHEDULER_WEEKEND_MINUTES",
    default=360,
    minimum=1,
)
SCHEDULER_JITTER_SECONDS = _int_env(
    "SENS_SCHEDULER_JITTER_SECONDS",
    default=45,
    minimum=0,
)
SCHEDULER_MAX_COOLDOWN_MULTIPLIER = _int_env(
    "SENS_SCHEDULER_MAX_COOLDOWN_MULTIPLIER",
    default=8,
    minimum=1,
)

_DEFAULT_KEYWORDS = (
    "financial results",
    "financial statements",
    "annual results",
    "interim results",
    "trading statement",
    "condensed consolidated",
    "reviewed condensed",
    "audited consolidated",
)
KEYWORDS = tuple(
    keyword.strip().lower()
    for keyword in os.getenv("SENS_KEYWORDS", ",".join(_DEFAULT_KEYWORDS)).split(",")
    if keyword.strip()
)


def ensure_runtime_dirs() -> None:
    """Create local directories used by the pipeline."""
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    PARSED_TEXT_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
