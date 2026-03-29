from __future__ import annotations

import argparse
import asyncio
import logging
import random
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from mvp_sens.configs.config import (
    DB_PATH,
    SCHEDULER_JITTER_SECONDS,
    SCHEDULER_MAX_COOLDOWN_MULTIPLIER,
    SCHEDULER_WEEKDAY_AFTERCLOSE_MINUTES,
    SCHEDULER_WEEKDAY_MARKET_MINUTES,
    SCHEDULER_WEEKDAY_OFFHOURS_MINUTES,
    SCHEDULER_WEEKDAY_PREOPEN_MINUTES,
    SCHEDULER_WEEKEND_MINUTES,
    SKIP_WEEKEND_COLLECTION,
)
from mvp_sens.scripts.db_insert import connect_db
from mvp_sens.scripts.fetch_sens import run_pipeline

logger = logging.getLogger(__name__)

JSE_TIMEZONE = ZoneInfo("Africa/Johannesburg")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def to_jse_datetime(now_utc: datetime | None = None) -> datetime:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    return now_utc.astimezone(JSE_TIMEZONE)


def get_scheduler_interval_minutes(now_jse: datetime) -> int:
    if now_jse.weekday() >= 5:
        return SCHEDULER_WEEKEND_MINUTES

    current_time = now_jse.time()
    if time(7, 0) <= current_time < time(9, 0):
        return SCHEDULER_WEEKDAY_PREOPEN_MINUTES
    if time(9, 0) <= current_time < time(17, 30):
        return SCHEDULER_WEEKDAY_MARKET_MINUTES
    if time(17, 30) <= current_time < time(20, 0):
        return SCHEDULER_WEEKDAY_AFTERCLOSE_MINUTES
    return SCHEDULER_WEEKDAY_OFFHOURS_MINUTES


def compute_cooldown_multiplier(consecutive_failures: int, max_multiplier: int) -> int:
    if consecutive_failures <= 0:
        return 1
    return min(2**consecutive_failures, max_multiplier)


def compute_sleep_seconds(
    base_minutes: int,
    jitter_seconds: int,
    cooldown_multiplier: int,
    randint_fn=random.randint,
) -> int:
    base_seconds = base_minutes * 60 * max(1, cooldown_multiplier)
    jitter = randint_fn(0, max(0, jitter_seconds)) if jitter_seconds > 0 else 0
    return base_seconds + jitter


def read_run_status(run_id: str) -> str:
    with connect_db(db_path=DB_PATH) as conn:
        row = conn.execute(
            "SELECT status FROM ingest_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    if row is None:
        return "failed"
    return str(row["status"])


async def run_scheduler(
    limit: int | None,
    dry_run: bool,
    skip_download: bool,
    include_all: bool,
    max_iterations: int | None,
) -> None:
    iteration = 0
    consecutive_failures = 0

    logger.info(
        "Scheduler started dry_run=%s skip_download=%s include_all=%s skip_weekends=%s",
        dry_run,
        skip_download,
        include_all,
        SKIP_WEEKEND_COLLECTION,
    )

    while True:
        iteration += 1
        run_id: str | None = None
        status = "failed"
        try:
            run_id = await run_pipeline(
                limit=limit,
                dry_run=dry_run,
                skip_download=skip_download,
                include_all=include_all,
                source="scheduler_loop",
            )
            status = read_run_status(run_id)
        except Exception as exc:
            logger.exception(
                "Scheduler iteration=%s failed before run completion: %s",
                iteration,
                exc,
            )

        if status == "failed":
            consecutive_failures += 1
        else:
            consecutive_failures = 0

        logger.info(
            "Scheduler iteration=%s run_id=%s status=%s consecutive_failures=%s",
            iteration,
            run_id or "-",
            status,
            consecutive_failures,
        )

        if max_iterations is not None and iteration >= max_iterations:
            logger.info("Scheduler reached max_iterations=%s", max_iterations)
            return

        now_jse = to_jse_datetime()
        base_minutes = get_scheduler_interval_minutes(now_jse)
        cooldown_multiplier = compute_cooldown_multiplier(
            consecutive_failures,
            max_multiplier=SCHEDULER_MAX_COOLDOWN_MULTIPLIER,
        )
        sleep_seconds = compute_sleep_seconds(
            base_minutes=base_minutes,
            jitter_seconds=SCHEDULER_JITTER_SECONDS,
            cooldown_multiplier=cooldown_multiplier,
        )
        logger.info(
            "Scheduler sleeping for %s seconds (base_minutes=%s, multiplier=%s)",
            sleep_seconds,
            base_minutes,
            cooldown_multiplier,
        )
        await asyncio.sleep(sleep_seconds)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run policy-driven scheduler loop for SENS ingestion."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max announcements per run iteration.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run scheduler in dry-run mode (no DB announcement inserts).",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip PDF downloads during each run.",
    )
    parser.add_argument(
        "--include-all",
        action="store_true",
        help="Include non-keyword announcements during each run.",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Optional loop cap for testing/validation.",
    )
    return parser


def main() -> None:
    configure_logging()
    args = _build_parser().parse_args()
    asyncio.run(
        run_scheduler(
            limit=args.limit,
            dry_run=args.dry_run,
            skip_download=args.skip_download,
            include_all=args.include_all,
            max_iterations=args.max_iterations,
        )
    )


if __name__ == "__main__":
    main()
