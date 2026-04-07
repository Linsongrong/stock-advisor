# -*- coding: utf-8 -*-
"""Tests for backtest parameter scan helpers."""

import unittest

import backtest_scan


def _build_rows(start_price, daily_returns, base_volume):
    rows = []
    price = start_price
    for index, daily_return in enumerate(daily_returns, start=1):
        open_price = round(price, 2)
        close_price = round(price * (1.0 + daily_return), 2)
        high_price = round(max(open_price, close_price) * 1.01, 2)
        low_price = round(min(open_price, close_price) * 0.99, 2)
        volume = base_volume + index * 1000
        rows.append(
            {
                "date": f"2026-01-{index:02d}",
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price,
                "volume": volume,
                "amount": round(close_price * volume, 2),
            }
        )
        price = close_price
    return rows


class BacktestScanTests(unittest.TestCase):
    def test_parse_csv_ints_and_dates(self):
        self.assertEqual(backtest_scan._parse_csv_ints("5, 10,20", [1]), [5, 10, 20])
        self.assertEqual(backtest_scan._parse_csv_dates("2016-01-01, 2020-01-01", ["x"]), ["2016-01-01", "2020-01-01"])
        self.assertEqual(backtest_scan._parse_csv_ints("", [1, 2]), [1, 2])

    def test_scan_backtests_on_histories_returns_grouped_results(self):
        histories = {
            "AAA001": {
                "code": "AAA001",
                "name": "leader",
                "sector": "test",
                "rows": _build_rows(10.0, [0.008, -0.003, 0.01, -0.002, 0.009, 0.004] * 2, 120000),
            },
            "BBB001": {
                "code": "BBB001",
                "name": "laggard",
                "sector": "test",
                "rows": _build_rows(10.0, [-0.008, 0.001, -0.01, 0.0, -0.006, -0.003] * 2, 90000),
            },
            "CCC001": {
                "code": "CCC001",
                "name": "flat",
                "sector": "test",
                "rows": _build_rows(10.0, [0.001, 0.0, 0.002, -0.001, 0.001, 0.0] * 2, 100000),
            },
        }
        for stock in histories.values():
            stock["date_index"] = {row["date"]: index for index, row in enumerate(stock["rows"])}

        result = backtest_scan.scan_backtests_on_histories(
            histories,
            start_dates=["2026-01-05"],
            top_ns=[1, 2],
            hold_days_list=[2],
            rebalance_days_list=[2],
            keep_ranks=[0],
            min_history=5,
            min_candidates=1,
            enabled_groups=("capital", "technical"),
        )

        self.assertEqual(len(result["results"]), 2)
        self.assertIn("2026-01-05", result["grouped"])
        self.assertEqual(len(result["grouped"]["2026-01-05"]), 2)

    def test_scan_backtests_on_histories_supports_named_windows(self):
        histories = {
            "AAA001": {
                "code": "AAA001",
                "name": "leader",
                "sector": "test",
                "rows": _build_rows(10.0, [0.008, -0.003, 0.01, -0.002, 0.009, 0.004] * 3, 120000),
            },
            "BBB001": {
                "code": "BBB001",
                "name": "laggard",
                "sector": "test",
                "rows": _build_rows(10.0, [-0.008, 0.001, -0.01, 0.0, -0.006, -0.003] * 3, 90000),
            },
        }
        for stock in histories.values():
            stock["date_index"] = {row["date"]: index for index, row in enumerate(stock["rows"])}

        result = backtest_scan.scan_backtests_on_histories(
            histories,
            windows=[("phase-a", "2026-01-05", "2026-01-10"), ("phase-b", "2026-01-11", "")],
            top_ns=[1],
            hold_days_list=[2],
            rebalance_days_list=[2],
            keep_ranks=[0],
            min_history=5,
            min_candidates=1,
            enabled_groups=("capital", "technical"),
        )

        self.assertIn("phase-a", result["grouped"])
        self.assertIn("phase-b", result["grouped"])
        self.assertEqual(result["results"][0]["window_label"] in {"phase-a", "phase-b"}, True)
        self.assertGreaterEqual(result["grouped"]["phase-b"][0]["summary"]["period_count"], 1)


if __name__ == "__main__":
    unittest.main()
