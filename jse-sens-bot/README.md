# JSE SENS Bot (MVP)

This app ingests JSE SENS announcements into SQLite, downloads PDFs, and prepares parsed text for downstream analysis.

## ETL Stages

1. `db_insert.py`: Initializes the DB schema.
2. `fetch_sens.py`: Scrapes announcements, filters by financial-result keywords, downloads PDFs, and stores rows.
3. `parse_pdf.py`: Extracts text from local PDFs into `mvp_sens/data/parsed`.
4. `gpt_review.py`: Lists parsed files ready for LLM summarization/review.
5. `run_etl.py`: Orchestrates all stages.
6. `scheduler_loop.py`: Runs policy-driven fetch cadence with cooldown and jitter.
7. `audit_report.py`: Prints recent `ingest_runs` and alert events from `ingest_events`.
8. `classify_disclosures.py`: Deterministic title-based classification and issuer eligibility.
9. `analyst_outputs.py`: On-demand analyst exports (since-last-run, intraday snapshot, daily delta, release signals).

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
- PDF downloads are rate-limited with a minimum interval (`SENS_DOWNLOAD_MIN_INTERVAL_SECONDS`).
- Downloaded files are validated as real PDFs before being persisted.
- Use `.env` overrides for timeout/retry/user-agent tuning.
- Scrape stage retries transient/empty weekday results with exponential backoff (`SENS_SCRAPE_MAX_ATTEMPTS`, `SENS_SCRAPE_RETRY_BACKOFF_SECONDS`).
- DOM-change suspicion is raised when weekday candidate volume is abnormally low (`SENS_SCRAPE_DOM_ALERT_MAX_RAW_CANDIDATES`).
- Weekend collection is disabled by default (`SENS_SKIP_WEEKENDS=1`) in Africa/Johannesburg timezone.
- If weekend collection is enabled, zero-announcement runs log as info on weekends and warning on weekdays.
- Candidate filtering and operator alerts are written to `ingest_events` for auditability.
- Unknown-issuer candidates are quarantined into `ingest_events` (`stage='quarantine'`) for operator review.
- Scheduler loop applies exponential cooldown after failed iterations and random jitter to reduce burst retries.
- Candidate parsing enforces equity-issuer eligibility (mixed issuer labels are allowed when Equity Issuer is present).
- Classification is deterministic and persisted (`category`, `classification_reason`, `classification_version`, `analyst_relevant`, `relevance_reason`).
- Ambiguous title-only disclosures can use a lightweight PDF-text fallback for classification before final relevance decision.
- First-seen lineage is persisted (`first_seen_run_id`, `first_seen_at`) for stable delta outputs.
- Release-signal extraction persists future publication cues into `release_signals`.

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

Phase 1 sign-off bundle:

```bash
make phase1-signoff
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

Use explicit run id for traceability:

```bash
python3 -m mvp_sens.scripts.run_etl --dry-run --fetch-limit 5 --run-id etl-manual-001
```

Note: `run_id` values must be unique per execution.

Offline parse/review only (skip site fetch):

```bash
python3 -m mvp_sens.scripts.run_etl --skip-fetch --parse-limit 10
```

Live fetch (hits SENS site):

```bash
python3 -m mvp_sens.scripts.run_etl --fetch-limit 20
```

Run continuous scheduler loop (policy-driven, with backoff):

```bash
python3 -m mvp_sens.scripts.scheduler_loop --limit 20
```

One-iteration scheduler smoke check:

```bash
python3 -m mvp_sens.scripts.scheduler_loop --dry-run --skip-download --max-iterations 1
```

Audit tables are written per fetch run: `ingest_runs` and `ingest_events`.

View recent run/audit state:

```bash
python3 -m mvp_sens.scripts.audit_report --run-limit 10 --alert-limit 20
```

Phase 2 foundations now present in schema:

- `sens_financial_announcements` includes `category`, `classification_reason`,
  `classification_version`, `classified_at`, `analyst_relevant`, and
  `relevance_reason`.
- `release_signals` table stores extracted future disclosure signal datetimes.
- `pipeline_state` table stores lightweight global cursors (for report deltas).
- analyst exports are written to `mvp_sens/data/exports` (override with `SENS_EXPORT_DIR`).

If you want to validate DB insert flow regardless of keyword filtering:

```bash
python3 -m mvp_sens.scripts.fetch_sens --limit 20 --include-all --skip-download
```

Strict dry-run mode (never download PDFs, including ambiguity fallback):

```bash
python3 -m mvp_sens.scripts.fetch_sens --dry-run --dry-run-no-download --limit 20
```

Generate analyst-facing outputs (on-demand CLI):

```bash
# New relevant disclosures since last run cursor (advances cursor by default)
python3 -m mvp_sens.scripts.analyst_outputs since-last-run --format json

# Intraday snapshot for the JSE window (07:05-18:05 Africa/Johannesburg)
python3 -m mvp_sens.scripts.analyst_outputs intraday-snapshot --date 2026-03-30 --format csv

# Daily delta report for full 07:05-18:05 JSE window
python3 -m mvp_sens.scripts.analyst_outputs daily-delta --date 2026-03-30 --format json

# Upcoming release-signal view
python3 -m mvp_sens.scripts.analyst_outputs release-signals --format json
```

Output contract notes:

- Disclosure export fields are stable and ordered, including `run_id`, `category`, `classification_reason`, `relevance_reason`, and observed timestamps.
- Release-signal export fields are stable and ordered for downstream tooling.
- `since-last-run` uses a single global cursor in `pipeline_state` (`run_id` + `completed_at`) for idempotent analyst deltas.
- `since-last-run` advances cursor only after successful export write.

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

# Start continuous scheduler loop (opt-in profile):
docker compose --profile scheduler up -d sens-scheduler

# Stop scheduler loop:
docker compose --profile scheduler stop sens-scheduler
```

Nginx serves local artifacts on `http://localhost:8080/data/`.
Health check endpoint: `http://localhost:8080/healthz`.

## Phase 2 Decision-Support Features

### BUY / HOLD / SELL Signal Engine (`mvp_sens/signals.py`)

The signal engine analyses each analyst-relevant disclosure and produces an
actionable verdict:

| Signal | Colour | Meaning |
|--------|--------|---------|
| **BUY**  | 🟢 green  | Positive sentiment or confirmed uptrend |
| **HOLD** | 🟡 yellow | Mixed or neutral evidence |
| **SELL** | 🔴 red    | Negative sentiment or confirmed downtrend |

Each signal also carries a **confidence score** (0–100 %) and a short **reason**
string (e.g. `"Positive sentiment"` or `"Uptrend + positive sentiment"`).

When price history is available the engine computes short and long simple
moving averages (defaults: 5-period and 20-period) and combines the resulting
trend score with a sentiment score using configurable weights.  When no price
history is available the engine falls back to sentiment derived directly from
the disclosure's `category` and `analyst_relevant` fields.

**Sentiment mapping (no price data needed):**

| Category | Base sentiment |
|----------|----------------|
| `financial_results` | +0.30 |
| `earnings_update`   | +0.25 |
| `trading_statement` | 0.00  |
| `other`             | 0.00  |

An extra +0.10 boost is applied when `analyst_relevant = 1`.

**Example output:**

```json
{
  "sens_id": "20260330-001",
  "company": "ACME Ltd",
  "signal": "BUY",
  "confidence": 70.0,
  "reason": "Positive sentiment"
}
```

**API endpoint:** `GET /api/signals[?limit=N]`

Returns signals for up to *N* analyst-relevant disclosures (default 50, max 200).

```bash
curl http://localhost:5000/api/signals | python3 -m json.tool
```

**All threshold constants** are defined at the top of `mvp_sens/signals.py` and
can be overridden without touching the core logic.

---

### Alert System (`mvp_sens/alerts.py`)

`check_alerts(asset_data, thresholds)` evaluates a snapshot of asset data
against four configurable conditions and returns a list of triggered alerts.

**Supported alert types:**

| Type | Condition |
|------|-----------|
| `price_above` | `price > thresholds["price_above"]` |
| `price_below` | `price < thresholds["price_below"]` |
| `pct_change`  | `|% change vs prev_price| ≥ threshold` (default 5 %) |
| `volume_spike` | `volume / avg_volume ≥ factor` (default 2×) |

**Example usage:**

```python
from mvp_sens.alerts import check_alerts

alerts = check_alerts(
    {"symbol": "ACM", "price": 110.0, "prev_price": 100.0,
     "volume": 300_000, "avg_volume": 100_000},
    {"price_above": 105.0, "pct_change": 5.0, "volume_spike_factor": 2.0},
)
# [
#   {"type": "price_above", "symbol": "ACM",
#    "message": "ACM price 110.00 crossed above threshold 105.00", "value": 110.0},
#   {"type": "pct_change",  "symbol": "ACM",
#    "message": "ACM moved up 10.0% (threshold: 5.0%)", "value": 10.0},
#   {"type": "volume_spike","symbol": "ACM",
#    "message": "ACM volume spike: 300,000 (3.0x avg, threshold: 2.0x)", "value": 3.0}
# ]
```

**In-app alert display:** triggered alerts are displayed in the *Alert Events*
tab of the dashboard.

**API endpoint:** `GET /api/alerts[?limit=N&run_id=<run_id>]`

Returns recent pipeline alert events from the `ingest_events` table.

**Default thresholds** are constants at the top of `mvp_sens/alerts.py` —
no hardcoded magic numbers anywhere else.

---


## Phase 1 Closeout

- Release labeling: this is tracked as **Phase 1 Release 4** (final Phase 1 release).
- Weekend fetch policy for production/local staging is strict skip (`SENS_SKIP_WEEKENDS=1`).
- Operator alerts are DB/log based for now (external notifications deferred to Phase 2).
- Weekday live-data validation should be run **Monday-Friday (Africa/Johannesburg timezone)**.
