# JSE SENS Bot (MVP)

This app ingests JSE SENS announcements into SQLite, downloads PDFs, and prepares parsed text for downstream analysis.

## ETL Stages

1. `db_insert.py`: Initializes the DB schema.
2. `fetch_sens.py`: Scrapes announcements, filters by financial-result keywords, downloads PDFs, and stores rows.
3. `parse_pdf.py`: Extracts text from local PDFs into `mvp_sens/data/parsed`.
4. `gpt_review.py`: Lists parsed files ready for LLM summarization/review.
5. `run_etl.py`: Orchestrates all stages.

## Dependencies

- Python 3.11+
- `playwright` + Chromium browser binaries
- `requests`
- `pypdf`
- Docker (optional for containerized run)
- Nginx (optional for local static serving of pipeline artifacts)

## Hardening Defaults

- PDF downloads are restricted to allowed hosts (`SENS_ALLOWED_PDF_HOSTS`).
- HTTP download requests use retry + backoff.
- Downloaded files are validated as real PDFs before being persisted.
- Use `.env` overrides for timeout/retry/user-agent tuning.
- A zero-announcement run is logged as info on weekends (Africa/Johannesburg timezone), warning on weekdays.

## Local Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
python3 -m playwright install chromium
```

Optional dependency check:

```bash
./check_prereqs.sh
```

## Internal (Offline) Validation

These tests do not hit the live SENS site:

```bash
python3 -m unittest discover -s tests -v
```

## Run Pipeline

Initialize schema only:

```bash
python3 -m mvp_sens.scripts.db_insert
```

Internal ETL dry-run (no DB writes):

```bash
python3 -m mvp_sens.scripts.run_etl --dry-run --fetch-limit 5
```

Offline parse/review only (skip site fetch):

```bash
python3 -m mvp_sens.scripts.run_etl --skip-fetch --parse-limit 10
```

Live fetch (hits SENS site):

```bash
python3 -m mvp_sens.scripts.run_etl --fetch-limit 20
```

If you want to validate DB insert flow regardless of keyword filtering:

```bash
python3 -m mvp_sens.scripts.fetch_sens --limit 20 --include-all --skip-download
```

## Docker + Nginx Local Staging

```bash
docker compose up --build
```

Default container command is offline-safe (`--skip-fetch`).
When ready for live site fetch, override command, for example:

```bash
# One-time browser install (persisted via docker volume playwright-cache):
docker compose run --rm sens-scraper python -m playwright install chromium

# Live fetch run(s):
docker compose run --rm sens-scraper python -m mvp_sens.scripts.fetch_sens --limit 20

# Pipeline health check regardless of keyword filtering:
docker compose run --rm sens-scraper python -m mvp_sens.scripts.fetch_sens --limit 20 --include-all --skip-download
```

Nginx serves local artifacts on `http://localhost:8080/data/`.
Health check endpoint: `http://localhost:8080/healthz`.
