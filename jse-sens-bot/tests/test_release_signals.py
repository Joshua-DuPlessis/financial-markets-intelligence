import unittest

from mvp_sens.scripts.release_signals import extract_release_signals


class ReleaseSignalExtractionTests(unittest.TestCase):
    def test_extract_release_signal_with_context_and_time(self):
        text = "The annual report will be released on 30 March 2026 at 09:00."
        signals = extract_release_signals(text, source="title")
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].signal_type, "future_release_datetime")
        self.assertTrue(signals[0].signal_datetime.endswith("+02:00"))
        self.assertEqual(signals[0].source, "title")

    def test_extract_release_signal_with_context_and_date_only(self):
        text = "Results will be available on 30 March 2026."
        signals = extract_release_signals(text, source="title")
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].signal_type, "future_release_date")

    def test_extract_release_signal_without_context_returns_none(self):
        text = "Unaudited condensed interim financial results for the year ended 31 December 2025."
        signals = extract_release_signals(text, source="title")
        self.assertEqual(signals, [])

    def test_extract_release_signal_deduplicates(self):
        text = (
            "The annual report will be released on 30 March 2026 at 09:00. "
            "Reminder: annual report will be released on 30 March 2026 at 09:00."
        )
        signals = extract_release_signals(text, source="title")
        self.assertEqual(len(signals), 1)


if __name__ == "__main__":
    unittest.main()
