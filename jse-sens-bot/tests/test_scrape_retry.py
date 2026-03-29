import asyncio
from datetime import datetime, timezone
import unittest
from unittest import mock

from mvp_sens.scripts.fetch_sens import (
    Announcement,
    _scrape_with_retry,
)


def _reject_counts() -> dict[str, int]:
    return {
        "not_pdf_like_link": 0,
        "disallowed_host": 0,
        "not_probable_announcement_pdf": 0,
        "missing_sens_id": 0,
        "duplicate_sens_id": 0,
    }


class ScrapeRetryTests(unittest.TestCase):
    def test_scrape_with_retry_recovers_after_empty_first_attempt(self):
        monday_utc = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        results = [
            (0, [], _reject_counts()),
            (
                4,
                [
                    Announcement(
                        sens_id="123456",
                        company="ABC LTD",
                        title="ABC LTD | Trading Statement",
                        announcement_date="2026-03-30T10:00:00+00:00",
                        pdf_url="https://senspdf.jse.co.za/documents/SENS_123456.pdf",
                    )
                ],
                _reject_counts(),
            ),
        ]

        async def _fake_scrape_once(_limit):
            return results.pop(0)

        async def _run():
            mocked_sleep = mock.AsyncMock()
            result = await _scrape_with_retry(
                scrape_once_fn=_fake_scrape_once,
                limit=10,
                max_attempts=3,
                base_backoff_seconds=1.0,
                sleep_fn=mocked_sleep,
                now_utc_fn=lambda: monday_utc,
            )
            self.assertEqual(result.attempt_count, 2)
            self.assertEqual(len(result.announcements), 1)
            mocked_sleep.assert_awaited_once_with(1.0)

        asyncio.run(_run())

    def test_scrape_with_retry_raises_after_max_attempts(self):
        monday_utc = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)

        async def _fake_scrape_once(_limit):
            raise RuntimeError("temporary scrape failure")

        async def _run():
            mocked_sleep = mock.AsyncMock()
            with self.assertRaises(RuntimeError):
                await _scrape_with_retry(
                    scrape_once_fn=_fake_scrape_once,
                    limit=10,
                    max_attempts=2,
                    base_backoff_seconds=1.0,
                    sleep_fn=mocked_sleep,
                    now_utc_fn=lambda: monday_utc,
                )
            mocked_sleep.assert_awaited_once_with(1.0)

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
