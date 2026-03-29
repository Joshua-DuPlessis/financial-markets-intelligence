import unittest

from mvp_sens.scripts.classify_disclosures import (
    CATEGORY_EARNINGS_UPDATE,
    CATEGORY_FINANCIAL_RESULTS,
    CATEGORY_OTHER,
    CATEGORY_TRADING_STATEMENT,
    classify_announcement,
    evaluate_issuer_eligibility,
)


class ClassificationTests(unittest.TestCase):
    def test_trading_statement_classification(self):
        result = classify_announcement(
            title="INITIAL TRADING STATEMENT FOR THE YEAR ENDED 28 FEBRUARY 2026",
            issuer_context="Visual International Holdings Limited | Equity Issuer (JSE)",
        )
        self.assertEqual(result.category, CATEGORY_TRADING_STATEMENT)
        self.assertTrue(result.analyst_relevant)

    def test_financial_results_classification(self):
        result = classify_announcement(
            title=(
                "Unaudited condensed consolidated interim financial results "
                "for the six months ended 31 December 2025"
            ),
            issuer_context="York Timber Holdings Limited | Equity Issuer (JSE)",
        )
        self.assertEqual(result.category, CATEGORY_FINANCIAL_RESULTS)
        self.assertTrue(result.analyst_relevant)

    def test_earnings_update_classification(self):
        result = classify_announcement(
            title="Headline earnings per share update for the year ended 31 March 2026",
            issuer_context="ABC Limited | Equity Issuer (JSE)",
        )
        self.assertEqual(result.category, CATEGORY_EARNINGS_UPDATE)
        self.assertTrue(result.analyst_relevant)

    def test_beneficial_ownership_not_relevant(self):
        result = classify_announcement(
            title="Schedule 13G/A: Statement of Beneficial Ownership by Certain Investors",
            issuer_context="ASP Isotopes Inc | Equity Issuer (JSE)",
        )
        self.assertEqual(result.category, CATEGORY_OTHER)
        self.assertFalse(result.analyst_relevant)
        self.assertIn("excluded", result.relevance_reason)

    def test_annual_report_and_agm_is_relevant(self):
        result = classify_announcement(
            title="Annual Report and Notice of Annual General Meeting 26 March 2026",
            issuer_context="Hammerson Plc | Equity Issuer (JSE)",
        )
        self.assertEqual(result.category, CATEGORY_OTHER)
        self.assertTrue(result.analyst_relevant)
        self.assertEqual(result.relevance_reason, "kw_annual_report")

    def test_non_equity_issuer_not_relevant(self):
        result = classify_announcement(
            title="Listing of Additional Satrix Global Prop ETF Securities",
            issuer_context="Satrix Collective Investment Scheme | ETF Issuer (JSE)",
        )
        self.assertFalse(result.analyst_relevant)
        self.assertEqual(result.relevance_reason, "issuer_non_equity")

    def test_statement_alone_is_ambiguous(self):
        result = classify_announcement(
            title="Quarterly Statement Update",
            issuer_context="ABC Limited | Equity Issuer (JSE)",
        )
        self.assertEqual(result.category, CATEGORY_OTHER)
        self.assertFalse(result.analyst_relevant)
        self.assertTrue(result.ambiguous)

    def test_pdf_text_can_disambiguate_ambiguous_title(self):
        result = classify_announcement(
            title="Quarterly Statement Update",
            issuer_context="ABC Limited | Equity Issuer (JSE)",
            body_text=(
                "This document includes unaudited condensed consolidated interim "
                "financial results for the six months ended 31 December 2025."
            ),
        )
        self.assertEqual(result.category, CATEGORY_FINANCIAL_RESULTS)
        self.assertTrue(result.analyst_relevant)
        self.assertTrue(result.classification_reason.startswith("pdf_"))

    def test_mixed_issuer_labels_are_allowed_if_equity_present(self):
        allowed, reason, tags = evaluate_issuer_eligibility(
            "Nedbank Group Limited | Interest Rate Issuer (JSE) | Equity Issuer (JSE)"
        )
        self.assertTrue(allowed)
        self.assertEqual(reason, "issuer_equity")
        self.assertIn("equity", tags)
        self.assertIn("interest_rate", tags)


if __name__ == "__main__":
    unittest.main()
