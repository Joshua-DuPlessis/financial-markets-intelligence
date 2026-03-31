from datetime import datetime, timezone
from pathlib import Path
import unittest

from mvp_sens.scripts.fetch_sens import (
    ScrapeResult,
    _unpack_raw_candidate,
    extract_company_from_context,
    extract_urls_from_text,
    get_scrape_retry_delay_seconds,
    is_dom_change_suspected,
    parse_jse_date,
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


class ExtractCompanyFromContextTests(unittest.TestCase):
    def test_returns_first_non_empty_line(self):
        self.assertEqual(
            extract_company_from_context("Absa Group Limited\n31 Mar 2026\nTrading Statement"),
            "Absa Group Limited",
        )

    def test_skips_leading_blank_lines(self):
        self.assertEqual(
            extract_company_from_context("\n\nNaspers Limited\nSome detail"),
            "Naspers Limited",
        )

    def test_single_line_with_no_newline(self):
        self.assertEqual(
            extract_company_from_context("Standard Bank Group Limited"),
            "Standard Bank Group Limited",
        )

    def test_empty_string_returns_empty(self):
        self.assertEqual(extract_company_from_context(""), "")

    def test_whitespace_only_returns_empty(self):
        self.assertEqual(extract_company_from_context("   \n  \n  "), "")

    def test_strips_surrounding_whitespace(self):
        self.assertEqual(
            extract_company_from_context("  Investec Limited  \nother stuff"),
            "Investec Limited",
        )


class ParseJseDateTests(unittest.TestCase):
    def test_jse_day_mon_year_hhmm(self):
        result = parse_jse_date("31 Mar 2026 09:15")
        # JSE is UTC+2 (Africa/Johannesburg), so 09:15 SAST = 07:15 UTC
        self.assertIn("07:15", result)
        self.assertIn("2026-03-31", result)

    def test_iso_date_hhmm(self):
        result = parse_jse_date("2026-03-31 09:15")
        self.assertIn("07:15", result)
        self.assertIn("2026-03-31", result)

    def test_iso_datetime(self):
        result = parse_jse_date("2026-03-31T09:15:00")
        self.assertIn("2026-03-31", result)
        self.assertTrue(result.endswith("+00:00") or "07:15" in result)

    def test_date_only_jse_format(self):
        result = parse_jse_date("31 Mar 2026")
        self.assertIn("2026-03-31", result)

    def test_empty_string_returns_empty(self):
        self.assertEqual(parse_jse_date(""), "")

    def test_garbage_returns_empty(self):
        self.assertEqual(parse_jse_date("not a date"), "")

    def test_whitespace_returns_empty(self):
        self.assertEqual(parse_jse_date("   "), "")


class UnpackRawCandidateTests(unittest.TestCase):
    def test_2_tuple(self):
        href, title, ctx, company, date = _unpack_raw_candidate(
            ("/documents/SENS_1.pdf", "Title")
        )
        self.assertEqual(href, "/documents/SENS_1.pdf")
        self.assertEqual(title, "Title")
        self.assertEqual(ctx, "")
        self.assertEqual(company, "")
        self.assertEqual(date, "")

    def test_3_tuple(self):
        href, title, ctx, company, date = _unpack_raw_candidate(
            ("/documents/SENS_1.pdf", "Title", "Context blob")
        )
        self.assertEqual(ctx, "Context blob")
        self.assertEqual(company, "")
        self.assertEqual(date, "")

    def test_5_tuple(self):
        href, title, ctx, company, date = _unpack_raw_candidate(
            ("/documents/SENS_1.pdf", "Title", "Context", "Absa Group", "31 Mar 2026 09:15")
        )
        self.assertEqual(company, "Absa Group")
        self.assertEqual(date, "31 Mar 2026 09:15")

    def test_1_tuple(self):
        href, title, ctx, company, date = _unpack_raw_candidate(("/documents/SENS_1.pdf",))
        self.assertEqual(href, "/documents/SENS_1.pdf")
        self.assertEqual(title, "")

    def test_empty_tuple(self):
        href, title, ctx, company, date = _unpack_raw_candidate(())
        self.assertEqual(href, "")
        self.assertEqual(title, "")


class ParseRawCandidatesMetadataTests(unittest.TestCase):
    """Verify company and announcement_date are populated from the new tuple fields."""

    _CONTEXT = "Absa Group Limited | Equity Issuer (JSE)"

    def _make_5tuple(self, raw_company: str, raw_date: str) -> tuple:
        return (
            "/documents/SENS_999001.pdf",
            "Absa Group Limited | Trading Statement",
            self._CONTEXT,
            raw_company,
            raw_date,
        )

    def test_company_from_raw_company_field(self):
        candidates = [self._make_5tuple("Absa Group Limited", "")]
        announcements, _ = parse_raw_candidates(candidates)
        self.assertEqual(len(announcements), 1)
        self.assertEqual(announcements[0].company, "Absa Group Limited")

    def test_company_falls_back_to_context_first_line(self):
        # No raw_company; the context first line should be used.
        candidates = [self._make_5tuple("", "")]
        announcements, _ = parse_raw_candidates(candidates)
        self.assertEqual(len(announcements), 1)
        self.assertEqual(announcements[0].company, "Absa Group Limited | Equity Issuer (JSE)")

    def test_announcement_date_from_raw_date(self):
        candidates = [self._make_5tuple("Absa Group Limited", "31 Mar 2026 09:15")]
        announcements, _ = parse_raw_candidates(candidates)
        self.assertEqual(len(announcements), 1)
        # Should contain the parsed UTC date, not just today's date.
        self.assertIn("2026-03-31", announcements[0].announcement_date)
        self.assertIn("07:15", announcements[0].announcement_date)

    def test_announcement_date_falls_back_to_now_when_empty(self):
        candidates = [self._make_5tuple("Absa Group Limited", "")]
        announcements, _ = parse_raw_candidates(candidates)
        self.assertEqual(len(announcements), 1)
        # Should be a valid ISO datetime string containing today's year.
        self.assertRegex(announcements[0].announcement_date, r"\d{4}-\d{2}-\d{2}T")

    def test_announcement_date_falls_back_to_now_when_unparseable(self):
        candidates = [self._make_5tuple("Absa Group Limited", "not-a-date")]
        announcements, _ = parse_raw_candidates(candidates)
        self.assertEqual(len(announcements), 1)
        self.assertRegex(announcements[0].announcement_date, r"\d{4}-\d{2}-\d{2}T")

    def test_3tuple_backward_compat_company_inferred_from_title(self):
        # 3-tuple (old format): company should still fall back to infer_company(title).
        candidates = [
            (
                "/documents/SENS_999002.pdf",
                "FallbackCo | Trading Statement",
                "FallbackCo | Equity Issuer (JSE)",
            )
        ]
        announcements, _ = parse_raw_candidates(candidates)
        self.assertEqual(len(announcements), 1)
        # context first line is "FallbackCo | Equity Issuer (JSE)" — that's what
        # extract_company_from_context returns (it takes the first non-empty line).
        self.assertEqual(announcements[0].company, "FallbackCo | Equity Issuer (JSE)")


if __name__ == "__main__":
    unittest.main()
