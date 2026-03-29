import os
from pathlib import Path

BASE_URL = os.getenv(
    "SENS_BASE_URL", "https://clientportal.jse.co.za/communication/sens-announcements"
)
PDF_BASE_URL = os.getenv("SENS_PDF_BASE_URL", "https://senspdf.jse.co.za/")

APP_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("SENS_DATA_DIR", str(APP_ROOT / "data")))
PDF_DIR = Path(os.getenv("SENS_PDF_DIR", str(DATA_DIR / "pdfs")))
PARSED_TEXT_DIR = Path(os.getenv("SENS_PARSED_TEXT_DIR", str(DATA_DIR / "parsed")))
DB_PATH = Path(os.getenv("SENS_DB_PATH", str(APP_ROOT / "db" / "sens.db")))
SCHEMA_PATH = Path(os.getenv("SENS_SCHEMA_PATH", str(APP_ROOT / "db" / "schema.sql")))

REQUEST_TIMEOUT = int(os.getenv("SENS_REQUEST_TIMEOUT", "30"))
SCRAPE_INTERVAL_MINUTES = int(os.getenv("SENS_SCRAPE_INTERVAL_MINUTES", "5"))
DOWNLOAD_RETRIES = int(os.getenv("SENS_DOWNLOAD_RETRIES", "3"))
DOWNLOAD_BACKOFF_SECONDS = float(os.getenv("SENS_DOWNLOAD_BACKOFF_SECONDS", "1.0"))
REQUEST_USER_AGENT = os.getenv(
    "SENS_REQUEST_USER_AGENT",
    "jse-sens-bot/1.0 (+local-staging)",
)
ALLOWED_PDF_HOSTS = tuple(
    host.strip().lower()
    for host in os.getenv("SENS_ALLOWED_PDF_HOSTS", "senspdf.jse.co.za").split(",")
    if host.strip()
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
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
