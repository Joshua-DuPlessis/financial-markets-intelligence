"""Tests for the BUY / HOLD / SELL signal engine (mvp_sens.signals)."""
from __future__ import annotations

import unittest

from mvp_sens.signals import (
    ANALYST_RELEVANT_BOOST,
    BUY_THRESHOLD,
    CATEGORY_SENTIMENT,
    SELL_THRESHOLD,
    derive_sentiment_from_disclosure,
    generate_signal,
    generate_signal_for_disclosure,
)


class MovingAverageSignalTests(unittest.TestCase):
    """Test generate_signal with price data present."""

    def test_uptrend_positive_sentiment_returns_buy(self):
        # Short MA > Long MA → uptrend; positive sentiment → BUY
        prices = [100.0] * 15 + [105.0] * 5  # short avg > long avg
        result = generate_signal({"prices": prices}, sentiment_score=0.5)
        self.assertEqual(result["signal"], "BUY")

    def test_downtrend_negative_sentiment_returns_sell(self):
        # Short MA < Long MA → downtrend; negative sentiment → SELL
        prices = [110.0] * 15 + [95.0] * 5  # short avg < long avg
        result = generate_signal({"prices": prices}, sentiment_score=-0.5)
        self.assertEqual(result["signal"], "SELL")

    def test_flat_neutral_returns_hold(self):
        prices = [100.0] * 25
        result = generate_signal({"prices": prices}, sentiment_score=0.0)
        self.assertEqual(result["signal"], "HOLD")

    def test_insufficient_prices_falls_back_to_sentiment(self):
        # Only 3 prices — shorter than any reasonable window → no trend
        result = generate_signal({"prices": [100.0, 101.0, 102.0]}, sentiment_score=0.5)
        # Positive sentiment without price data → BUY
        self.assertEqual(result["signal"], "BUY")

    def test_custom_windows_respected(self):
        prices = [100.0] * 3 + [110.0] * 3  # 6 prices total
        # short_window=3, long_window=6 → short avg > long avg
        result = generate_signal(
            {"prices": prices, "short_window": 3, "long_window": 6},
            sentiment_score=0.3,
        )
        self.assertEqual(result["signal"], "BUY")

    def test_confidence_in_range(self):
        result = generate_signal({"prices": []}, sentiment_score=0.0)
        self.assertGreaterEqual(result["confidence"], 0.0)
        self.assertLessEqual(result["confidence"], 100.0)

    def test_result_has_required_keys(self):
        result = generate_signal({"prices": [100.0] * 25})
        for key in ("signal", "confidence", "reason"):
            self.assertIn(key, result)

    def test_signal_values_are_valid(self):
        for sent in (-1.0, 0.0, 1.0):
            result = generate_signal({}, sentiment_score=sent)
            self.assertIn(result["signal"], ("BUY", "HOLD", "SELL"))

    def test_no_price_data_high_sentiment_buys(self):
        result = generate_signal({}, sentiment_score=1.0)
        self.assertEqual(result["signal"], "BUY")
        self.assertGreater(result["confidence"], 50.0)

    def test_no_price_data_negative_sentiment_sells(self):
        result = generate_signal({}, sentiment_score=-1.0)
        self.assertEqual(result["signal"], "SELL")

    def test_uptrend_no_sentiment_buys(self):
        prices = [100.0] * 15 + [110.0] * 5
        result = generate_signal({"prices": prices})
        self.assertEqual(result["signal"], "BUY")

    def test_reason_is_non_empty_string(self):
        result = generate_signal({"prices": [100.0] * 25}, sentiment_score=0.3)
        self.assertIsInstance(result["reason"], str)
        self.assertGreater(len(result["reason"]), 0)


class DeriveSentimentTests(unittest.TestCase):
    """Test derive_sentiment_from_disclosure."""

    def test_financial_results_analyst_relevant(self):
        row = {"category": "financial_results", "analyst_relevant": 1}
        score = derive_sentiment_from_disclosure(row)
        expected = CATEGORY_SENTIMENT["financial_results"] + ANALYST_RELEVANT_BOOST
        self.assertAlmostEqual(score, expected)

    def test_trading_statement_not_relevant(self):
        row = {"category": "trading_statement", "analyst_relevant": 0}
        score = derive_sentiment_from_disclosure(row)
        self.assertEqual(score, CATEGORY_SENTIMENT["trading_statement"])

    def test_unknown_category_defaults_to_zero(self):
        row = {"category": "mystery_category", "analyst_relevant": 0}
        score = derive_sentiment_from_disclosure(row)
        self.assertEqual(score, 0.0)

    def test_missing_category_defaults_to_other(self):
        row = {"analyst_relevant": 0}
        score = derive_sentiment_from_disclosure(row)
        self.assertEqual(score, 0.0)

    def test_analyst_relevant_boost_applied(self):
        row_without = {"category": "earnings_update", "analyst_relevant": 0}
        row_with = {"category": "earnings_update", "analyst_relevant": 1}
        score_without = derive_sentiment_from_disclosure(row_without)
        score_with = derive_sentiment_from_disclosure(row_with)
        self.assertAlmostEqual(score_with - score_without, ANALYST_RELEVANT_BOOST)

    def test_score_within_bounds(self):
        for cat in (*CATEGORY_SENTIMENT.keys(), "other"):
            for relevant in (0, 1):
                score = derive_sentiment_from_disclosure(
                    {"category": cat, "analyst_relevant": relevant}
                )
                self.assertGreaterEqual(score, -1.0)
                self.assertLessEqual(score, 1.0)


class GenerateSignalForDisclosureTests(unittest.TestCase):
    """Test generate_signal_for_disclosure."""

    def _make_row(self, sens_id: str, category: str, relevant: int) -> dict:
        return {
            "sens_id": sens_id,
            "company": "Test Co",
            "category": category,
            "analyst_relevant": relevant,
        }

    def test_financial_results_relevant_returns_buy(self):
        row = self._make_row("S001", "financial_results", 1)
        result = generate_signal_for_disclosure(row)
        self.assertEqual(result["signal"], "BUY")

    def test_result_includes_sens_id_and_company(self):
        row = self._make_row("S002", "trading_statement", 0)
        result = generate_signal_for_disclosure(row)
        self.assertEqual(result["sens_id"], "S002")
        self.assertEqual(result["company"], "Test Co")

    def test_result_has_all_signal_keys(self):
        row = self._make_row("S003", "other", 0)
        result = generate_signal_for_disclosure(row)
        for key in ("sens_id", "company", "signal", "confidence", "reason"):
            self.assertIn(key, result)

    def test_missing_sens_id_defaults_to_empty_string(self):
        result = generate_signal_for_disclosure({"category": "other", "analyst_relevant": 0})
        self.assertEqual(result["sens_id"], "")

    def test_buy_threshold_consistency(self):
        # financial_results + analyst_relevant must cross BUY_THRESHOLD
        sentiment = (
            CATEGORY_SENTIMENT.get("financial_results", 0.0) + ANALYST_RELEVANT_BOOST
        )
        self.assertGreater(sentiment, BUY_THRESHOLD)

    def test_hold_for_neutral_category(self):
        row = self._make_row("S004", "trading_statement", 0)
        result = generate_signal_for_disclosure(row)
        # trading_statement with no analyst_relevant → neutral → HOLD
        self.assertEqual(result["signal"], "HOLD")


if __name__ == "__main__":
    unittest.main()
