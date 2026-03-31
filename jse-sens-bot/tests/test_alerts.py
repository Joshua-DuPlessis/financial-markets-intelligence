"""Tests for the alert system (mvp_sens.alerts)."""
from __future__ import annotations

import unittest

from mvp_sens.alerts import (
    DEFAULT_PCT_CHANGE_THRESHOLD,
    DEFAULT_VOLUME_SPIKE_FACTOR,
    check_alerts,
)


class PriceAboveAlertTests(unittest.TestCase):
    def test_triggers_when_price_exceeds_threshold(self):
        alerts = check_alerts(
            {"symbol": "ACM", "price": 110.0},
            {"price_above": 100.0},
        )
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "price_above")
        self.assertEqual(alerts[0]["symbol"], "ACM")

    def test_no_trigger_when_price_equals_threshold(self):
        alerts = check_alerts(
            {"symbol": "ACM", "price": 100.0},
            {"price_above": 100.0},
        )
        self.assertEqual(len(alerts), 0)

    def test_no_trigger_when_price_below_threshold(self):
        alerts = check_alerts(
            {"symbol": "ACM", "price": 90.0},
            {"price_above": 100.0},
        )
        self.assertEqual(len(alerts), 0)

    def test_alert_contains_required_keys(self):
        alerts = check_alerts(
            {"symbol": "ACM", "price": 110.0},
            {"price_above": 100.0},
        )
        for key in ("type", "symbol", "message", "value"):
            self.assertIn(key, alerts[0])


class PriceBelowAlertTests(unittest.TestCase):
    def test_triggers_when_price_below_threshold(self):
        alerts = check_alerts(
            {"symbol": "XYZ", "price": 45.0},
            {"price_below": 50.0},
        )
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "price_below")

    def test_no_trigger_when_price_equals_threshold(self):
        alerts = check_alerts(
            {"symbol": "XYZ", "price": 50.0},
            {"price_below": 50.0},
        )
        self.assertEqual(len(alerts), 0)

    def test_no_trigger_when_price_above_threshold(self):
        alerts = check_alerts(
            {"symbol": "XYZ", "price": 55.0},
            {"price_below": 50.0},
        )
        self.assertEqual(len(alerts), 0)


class PctChangeAlertTests(unittest.TestCase):
    def test_triggers_on_large_positive_move(self):
        alerts = check_alerts(
            {"symbol": "ABC", "price": 110.0, "prev_price": 100.0},
            {"pct_change": 5.0},
        )
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "pct_change")
        self.assertAlmostEqual(alerts[0]["value"], 10.0)

    def test_triggers_on_large_negative_move(self):
        alerts = check_alerts(
            {"symbol": "ABC", "price": 88.0, "prev_price": 100.0},
            {"pct_change": 5.0},
        )
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "pct_change")
        self.assertAlmostEqual(alerts[0]["value"], -12.0)

    def test_no_trigger_below_threshold(self):
        alerts = check_alerts(
            {"symbol": "ABC", "price": 102.0, "prev_price": 100.0},
            {"pct_change": 5.0},
        )
        self.assertEqual(len(alerts), 0)

    def test_triggers_exactly_at_threshold(self):
        alerts = check_alerts(
            {"symbol": "ABC", "price": 105.0, "prev_price": 100.0},
            {"pct_change": 5.0},
        )
        self.assertEqual(len(alerts), 1)

    def test_uses_default_pct_threshold_when_not_specified(self):
        # No explicit pct_change in thresholds → default applies
        # Move > DEFAULT_PCT_CHANGE_THRESHOLD (5%) should trigger
        alerts = check_alerts(
            {"symbol": "DEF", "price": 107.0, "prev_price": 100.0},
            {},
        )
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "pct_change")

    def test_no_trigger_if_no_prev_price(self):
        alerts = check_alerts(
            {"symbol": "DEF", "price": 120.0},
            {"pct_change": 5.0},
        )
        self.assertEqual(len(alerts), 0)


class VolumeSpikeAlertTests(unittest.TestCase):
    def test_triggers_on_volume_spike(self):
        alerts = check_alerts(
            {"symbol": "VOL", "price": 100.0, "volume": 300_000.0, "avg_volume": 100_000.0},
            {"volume_spike_factor": 2.0},
        )
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "volume_spike")
        self.assertAlmostEqual(alerts[0]["value"], 3.0)

    def test_no_trigger_below_factor(self):
        alerts = check_alerts(
            {"symbol": "VOL", "price": 100.0, "volume": 150_000.0, "avg_volume": 100_000.0},
            {"volume_spike_factor": 2.0},
        )
        self.assertEqual(len(alerts), 0)

    def test_triggers_exactly_at_factor(self):
        alerts = check_alerts(
            {"symbol": "VOL", "price": 100.0, "volume": 200_000.0, "avg_volume": 100_000.0},
            {"volume_spike_factor": 2.0},
        )
        self.assertEqual(len(alerts), 1)

    def test_uses_default_volume_factor_when_not_specified(self):
        alerts = check_alerts(
            {
                "symbol": "VOL",
                "price": 100.0,
                "volume": DEFAULT_VOLUME_SPIKE_FACTOR * 100_000.0,
                "avg_volume": 100_000.0,
            },
            {},
        )
        self.assertEqual(len(alerts), 1)

    def test_no_trigger_without_avg_volume(self):
        alerts = check_alerts(
            {"symbol": "VOL", "price": 100.0, "volume": 999_999.0},
            {"volume_spike_factor": 2.0},
        )
        self.assertEqual(len(alerts), 0)


class EdgeCaseTests(unittest.TestCase):
    def test_no_price_returns_empty_list(self):
        alerts = check_alerts(
            {"symbol": "X"},
            {"price_above": 50.0, "pct_change": 5.0},
        )
        self.assertEqual(alerts, [])

    def test_multiple_alerts_can_trigger_simultaneously(self):
        alerts = check_alerts(
            {"symbol": "MUL", "price": 120.0, "prev_price": 100.0},
            {"price_above": 110.0, "pct_change": 5.0},
        )
        types = {a["type"] for a in alerts}
        self.assertIn("price_above", types)
        self.assertIn("pct_change", types)

    def test_default_symbol_when_missing(self):
        alerts = check_alerts(
            {"price": 200.0},
            {"price_above": 100.0},
        )
        self.assertEqual(alerts[0]["symbol"], "UNKNOWN")

    def test_non_numeric_price_returns_empty(self):
        alerts = check_alerts(
            {"symbol": "BAD", "price": "not_a_number"},
            {"price_above": 50.0},
        )
        self.assertEqual(alerts, [])

    def test_empty_thresholds_no_pct_change_without_prev_price(self):
        alerts = check_alerts({"symbol": "NOOP", "price": 100.0}, {})
        self.assertEqual(alerts, [])


if __name__ == "__main__":
    unittest.main()
