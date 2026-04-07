# -*- coding: utf-8 -*-
"""Tests for session-aware activity metrics."""

import unittest

from factors.market_activity import calculate_volume_profile


class MarketActivityTests(unittest.TestCase):
    def test_calculate_volume_profile_normalizes_intraday_progress(self) -> None:
        history = []
        for index in range(1, 7):
            history.append(
                {
                    "date": f"2026-04-0{index}",
                    "open": 10.0,
                    "close": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "volume": 1_000_000,
                    "amount": 10_000_000,
                }
            )

        quote = {
            "price": 10.2,
            "open": 10.0,
            "prev_close": 10.0,
            "volume": 500_000,
            "volume_shares": 500_000,
            "updated_at": "20260407103000",
        }

        profile = calculate_volume_profile(quote, history)

        self.assertAlmostEqual(profile["trading_progress"], 0.25, places=6)
        self.assertAlmostEqual(profile["raw_volume_ratio"], 0.5, places=6)
        self.assertAlmostEqual(profile["volume_ratio"], 2.0, places=6)
        self.assertAlmostEqual(profile["turnover_ratio"], 2.0, places=6)
        self.assertAlmostEqual(profile["estimated_full_day_volume"], 2_000_000, places=6)


if __name__ == "__main__":
    unittest.main()
