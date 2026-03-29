from datetime import datetime, timezone
import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from mvp_sens.configs.config import SCHEMA_PATH
from mvp_sens.scripts.db_insert import connect_db, initialize_db, start_ingest_run
from mvp_sens.scripts import scheduler_loop


class SchedulerPolicyTests(unittest.TestCase):
    def test_interval_weekday_preopen(self):
        dt = datetime(2026, 3, 30, 6, 0, tzinfo=timezone.utc)  # 08:00 JSE (Mon)
        now_jse = scheduler_loop.to_jse_datetime(dt)
        self.assertEqual(
            scheduler_loop.get_scheduler_interval_minutes(now_jse),
            scheduler_loop.SCHEDULER_WEEKDAY_PREOPEN_MINUTES,
        )

    def test_interval_weekday_market_hours(self):
        dt = datetime(2026, 3, 30, 8, 0, tzinfo=timezone.utc)  # 10:00 JSE (Mon)
        now_jse = scheduler_loop.to_jse_datetime(dt)
        self.assertEqual(
            scheduler_loop.get_scheduler_interval_minutes(now_jse),
            scheduler_loop.SCHEDULER_WEEKDAY_MARKET_MINUTES,
        )

    def test_interval_weekend(self):
        dt = datetime(2026, 3, 29, 8, 0, tzinfo=timezone.utc)  # Sunday
        now_jse = scheduler_loop.to_jse_datetime(dt)
        self.assertEqual(
            scheduler_loop.get_scheduler_interval_minutes(now_jse),
            scheduler_loop.SCHEDULER_WEEKEND_MINUTES,
        )

    def test_cooldown_multiplier_bounds(self):
        self.assertEqual(
            scheduler_loop.compute_cooldown_multiplier(0, max_multiplier=8), 1
        )
        self.assertEqual(
            scheduler_loop.compute_cooldown_multiplier(2, max_multiplier=8), 4
        )
        self.assertEqual(
            scheduler_loop.compute_cooldown_multiplier(8, max_multiplier=8), 8
        )

    def test_compute_sleep_seconds_with_deterministic_jitter(self):
        seconds = scheduler_loop.compute_sleep_seconds(
            base_minutes=5,
            jitter_seconds=30,
            cooldown_multiplier=2,
            randint_fn=lambda _a, _b: 7,
        )
        self.assertEqual(seconds, (5 * 60 * 2) + 7)

    def test_read_run_status_missing_row_returns_failed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)

            with mock.patch.object(scheduler_loop, "DB_PATH", db_path):
                self.assertEqual(
                    scheduler_loop.read_run_status("does-not-exist"),
                    "failed",
                )

    def test_read_run_status_returns_saved_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sens_test.db"
            with connect_db(db_path=db_path) as conn:
                initialize_db(conn, schema_path=SCHEMA_PATH)
                start_ingest_run(
                    conn,
                    run_id="sched-run-001",
                    source="scheduler_loop",
                    mode="live",
                )
                conn.execute(
                    "UPDATE ingest_runs SET status = ? WHERE run_id = ?",
                    ("success", "sched-run-001"),
                )
                conn.commit()

            with mock.patch.object(scheduler_loop, "DB_PATH", db_path):
                self.assertEqual(
                    scheduler_loop.read_run_status("sched-run-001"),
                    "success",
                )


class SchedulerLoopTests(unittest.TestCase):
    def test_scheduler_runs_single_iteration(self):
        async def _run():
            with mock.patch.object(
                scheduler_loop, "run_pipeline", new=mock.AsyncMock(return_value="r-1")
            ) as mocked_pipeline, mock.patch.object(
                scheduler_loop, "read_run_status", return_value="success"
            ):
                await scheduler_loop.run_scheduler(
                    limit=5,
                    dry_run=True,
                    skip_download=True,
                    include_all=False,
                    max_iterations=1,
                )
                self.assertEqual(mocked_pipeline.await_count, 1)

        asyncio.run(_run())

    def test_scheduler_handles_pipeline_exception_then_recovers(self):
        async def _run():
            with mock.patch.object(
                scheduler_loop,
                "run_pipeline",
                new=mock.AsyncMock(side_effect=[RuntimeError("boom"), "r-2"]),
            ) as mocked_pipeline, mock.patch.object(
                scheduler_loop, "read_run_status", return_value="success"
            ) as mocked_status, mock.patch.object(
                scheduler_loop, "to_jse_datetime", return_value=datetime.now(timezone.utc)
            ), mock.patch.object(
                scheduler_loop, "get_scheduler_interval_minutes", return_value=5
            ), mock.patch.object(
                scheduler_loop, "compute_sleep_seconds", return_value=600
            ) as mocked_compute_sleep, mock.patch.object(
                scheduler_loop.logger, "exception"
            ), mock.patch.object(
                scheduler_loop.asyncio, "sleep", new=mock.AsyncMock()
            ) as mocked_sleep:
                await scheduler_loop.run_scheduler(
                    limit=5,
                    dry_run=True,
                    skip_download=True,
                    include_all=False,
                    max_iterations=2,
                )
                self.assertEqual(mocked_pipeline.await_count, 2)
                mocked_status.assert_called_once_with("r-2")
                self.assertEqual(mocked_compute_sleep.call_count, 1)
                kwargs = mocked_compute_sleep.call_args.kwargs
                self.assertEqual(kwargs["cooldown_multiplier"], 2)
                mocked_sleep.assert_awaited_once_with(600)

        asyncio.run(_run())

    def test_scheduler_propagates_task_cancellation(self):
        async def _run():
            with mock.patch.object(
                scheduler_loop,
                "run_pipeline",
                new=mock.AsyncMock(side_effect=asyncio.CancelledError()),
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await scheduler_loop.run_scheduler(
                        limit=5,
                        dry_run=True,
                        skip_download=True,
                        include_all=False,
                        max_iterations=1,
                    )

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
