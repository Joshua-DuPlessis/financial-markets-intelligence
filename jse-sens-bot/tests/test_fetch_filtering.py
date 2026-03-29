from datetime import datetime, timezone
from pathlib import Path
import unittest

from mvp_sens.scripts.fetch_sens import (
    ScrapeResult,
    extract_urls_from_text,
    get_scrape_retry_delay_seconds,
    is_dom_change_suspected,
    parse_raw_candidates,
    parse_raw_candidates_with_quarantine,
    should_retry_after_scrape,
)


class FetchFilteringTests(unittest.TestCase):
    def test_parse_raw_candidates_accepts_valid_pdf(self):
        raw_candidates = [
            (
                "/documents/SENS_123456.pdf",
                "ABC LTD | Trading Statement",
                "ABC LTD | Equity Issuer (JSE)",
            )
        ]
        announcements, reject_counts = parse_raw_candidates(raw_candidates)

        self.assertEqual(len(announcements), 1)
        self.assertEqual(announcements[0].sens_id, "123456")
        self.assertEqual(announcements[0].title, "ABC LTD | Trading Statement")
        self.assertIn("equity", announcements[0].issuer_tags)
        self.assertEqual(sum(reject_counts.values()), 0)

    def test_parse_raw_candidates_rejects_navigation_link(self):
        raw_candidates = [("/communication/sens-project", "SENS Project")]
        announcements, reject_counts = parse_raw_candidates(raw_candidates)

        self.assertEqual(len(announcements), 0)
        self.assertEqual(reject_counts["not_pdf_like_link"], 1)

    def test_parse_raw_candidates_rejects_disallowed_host(self):
        raw_candidates = [("https://example.com/documents/SENS_777777.pdf", "Bad Host")]
        announcements, reject_counts = parse_raw_candidates(raw_candidates)

        self.assertEqual(len(announcements), 0)
        self.assertEqual(reject_counts["disallowed_host"], 1)

    def test_parse_raw_candidates_rejects_duplicate_sens_id(self):
        raw_candidates = [
            (
                "/documents/SENS_111111.pdf",
                "First",
                "ABC LTD | Equity Issuer (JSE)",
            ),
            (
                "https://senspdf.jse.co.za/documents/SENS_111111.pdf",
                "Second",
                "ABC LTD | Equity Issuer (JSE)",
            ),
        ]
        announcements, reject_counts = parse_raw_candidates(raw_candidates)

        self.assertEqual(len(announcements), 1)
        self.assertEqual(reject_counts["duplicate_sens_id"], 1)

    def test_parse_raw_candidates_rejects_non_equity_issuer(self):
        raw_candidates = [
            (
                "/documents/SENS_555555.pdf",
                "Listing of Additional Satrix Global Prop ETF Securities",
                "Satrix Collective Investment Scheme | ETF Issuer (JSE)",
            )
        ]
        announcements, reject_counts = parse_raw_candidates(raw_candidates)

        self.assertEqual(len(announcements), 0)
        self.assertEqual(reject_counts["issuer_non_equity"], 1)

    def test_parse_raw_candidates_quarantines_unknown_issuer(self):
        raw_candidates = [
            (
                "/documents/SENS_777777.pdf",
                "Issuer missing context trading statement",
                "",
            )
        ]
        announcements, reject_counts, quarantine_candidates = (
            parse_raw_candidates_with_quarantine(raw_candidates)
        )

        self.assertEqual(len(announcements), 0)
        self.assertEqual(reject_counts["issuer_unknown"], 1)
        self.assertEqual(len(quarantine_candidates), 1)
        self.assertEqual(quarantine_candidates[0]["reason"], "issuer_unknown")
        self.assertEqual(quarantine_candidates[0]["sens_id"], "777777")

    def test_parse_raw_candidates_accepts_mixed_issuer_labels_with_equity(self):
        raw_candidates = [
            (
                "/documents/SENS_666666.pdf",
                "Nedbank Group Limited | Trading Statement",
                "Nedbank Group Limited | Interest Rate Issuer (JSE) | Equity Issuer (JSE)",
            )
        ]
        announcements, reject_counts = parse_raw_candidates(raw_candidates)

        self.assertEqual(len(announcements), 1)
        self.assertIn("equity", announcements[0].issuer_tags)
        self.assertIn("interest_rate", announcements[0].issuer_tags)
        self.assertEqual(sum(reject_counts.values()), 0)

    def test_extract_urls_from_fixture_html(self):
        fixture_path = (
            Path(__file__).resolve().parent / "fixtures" / "sens_listing_sample.html"
        )
        html = fixture_path.read_text(encoding="utf-8")

        urls = extract_urls_from_text(html)

        self.assertIn("/documents/SENS_123456.pdf", urls)
        self.assertIn("https://senspdf.jse.co.za/documents/SENS_654321.pdf", urls)
        self.assertIn("https://example.com/documents/SENS_777777.pdf", urls)

    def test_dom_change_suspected_on_weekday_low_candidates(self):
        monday_utc = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(
            is_dom_change_suspected(
                raw_candidate_count=2,
                scraped_count=0,
                now_utc=monday_utc,
            )
        )

    def test_dom_change_not_suspected_on_weekend(self):
        saturday_utc = datetime(2026, 3, 28, 10, 0, 0, tzinfo=timezone.utc)
        self.assertFalse(
            is_dom_change_suspected(
                raw_candidate_count=0,
                scraped_count=0,
                now_utc=saturday_utc,
            )
        )

    def test_should_retry_after_scrape_for_dom_change(self):
        monday_utc = datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc)
        result = ScrapeResult(
            announcements=[],
            raw_candidate_count=1,
            reject_counts={
                "not_pdf_like_link": 1,
                "disallowed_host": 0,
                "not_probable_announcement_pdf": 0,
                "missing_sens_id": 0,
                "duplicate_sens_id": 0,
            },
        )
        self.assertTrue(
            should_retry_after_scrape(
                result=result,
                attempt=1,
                max_attempts=3,
                now_utc=monday_utc,
            )
        )

    def test_retry_delay_seconds_exponential(self):
        self.assertEqual(get_scrape_retry_delay_seconds(1, 1.5), 1.5)
        self.assertEqual(get_scrape_retry_delay_seconds(2, 1.5), 3.0)
        self.assertEqual(get_scrape_retry_delay_seconds(3, 1.5), 6.0)


if __name__ == "__main__":
    unittest.main()
