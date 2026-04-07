# -*- coding: utf-8 -*-
"""Tests for the historical backtesting helpers."""

import unittest

import backtester


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


class BacktesterHelperTests(unittest.TestCase):
    def test_build_historical_quote_uses_current_row_and_previous_close(self):
        rows = _build_rows(10.0, [0.01, 0.02, -0.01], 100000)
        quote = backtester._build_historical_quote("000001", "fixture", rows, 2)

        self.assertIsNotNone(quote)
        self.assertEqual(quote["updated_at"], "2026-01-03")
        self.assertAlmostEqual(quote["price"], rows[2]["close"], places=4)
        self.assertAlmostEqual(quote["prev_close"], rows[1]["close"], places=4)
        self.assertEqual(quote["volume"], rows[2]["volume"])

    def test_combine_group_scores_renormalizes_selected_groups(self):
        score_result = {
            "capital_score": 80.0,
            "technical_score": 60.0,
            "fundamental_score": 40.0,
            "sentiment_score": 20.0,
            "weight_totals": {
                "capital": 0.48,
                "technical": 0.32,
                "fundamental": 0.12,
                "sentiment": 0.08,
            },
        }
        combined = backtester._combine_group_scores(score_result, ["capital", "technical"])
        self.assertAlmostEqual(combined, 72.0, places=2)

    def test_compute_turnover_for_equal_weight_rebalance(self):
        turnover = backtester._compute_turnover(["AAA", "BBB"], ["BBB", "CCC"])
        self.assertAlmostEqual(turnover["buy_turnover"], 0.5, places=6)
        self.assertAlmostEqual(turnover["sell_turnover"], 0.5, places=6)
        self.assertAlmostEqual(turnover["turnover"], 0.5, places=6)

    def test_apply_transaction_costs_reduces_gross_return(self):
        turnover = {"buy_turnover": 0.5, "sell_turnover": 0.5, "turnover": 0.5}
        result = backtester._apply_transaction_costs(0.02, turnover, 0.0003, 0.0005, 0.0005)

        self.assertAlmostEqual(result["cost_rate"], 0.00105, places=6)
        self.assertLess(result["net_return"], result["gross_return"])
        self.assertAlmostEqual(result["gross_return"], 0.02, places=6)

    def test_compute_forward_return_skips_outlier_sample(self):
        rows = [
            {"date": "2026-01-01", "open": 10.0, "close": 10.0, "high": 10.1, "low": 9.9, "volume": 1, "amount": 10},
            {"date": "2026-01-02", "open": 10.0, "close": 10.0, "high": 10.1, "low": 9.9, "volume": 1, "amount": 10},
            {"date": "2026-01-03", "open": 18.0, "close": 18.0, "high": 18.1, "low": 17.9, "volume": 1, "amount": 18},
        ]

        result = backtester._compute_forward_return(rows, 0, hold_days=2, max_forward_return_pct=50.0)
        self.assertIsNone(result)

    def test_select_with_keep_rank_keeps_previous_holding_within_buffer(self):
        ranked = [
            {"code": "NEW001", "score": 95.0, "capital_score": 95.0, "technical_score": 95.0},
            {"code": "OLD001", "score": 90.0, "capital_score": 90.0, "technical_score": 90.0},
            {"code": "OLD002", "score": 85.0, "capital_score": 85.0, "technical_score": 85.0},
        ]
        selected = backtester._select_with_keep_rank(
            ranked,
            top_n=2,
            previous_selected_codes=["OLD001", "OLD002"],
            keep_rank=3,
        )

        self.assertEqual([item["code"] for item in selected], ["OLD001", "OLD002"])


class BacktesterRunTests(unittest.TestCase):
    def test_run_backtest_on_histories_produces_periods_and_prefers_stronger_trend(self):
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

        result = backtester.run_backtest_on_histories(
            histories,
            top_n=1,
            hold_days=2,
            rebalance_days=2,
            min_history=5,
            min_candidates=1,
            enabled_groups=("capital", "technical"),
        )

        self.assertGreater(result["summary"]["period_count"], 0)
        self.assertEqual(result["summary"]["universe_size"], 3)
        self.assertEqual(result["periods"][0]["selected_count"], 1)
        self.assertEqual(result["periods"][0]["picks"][0]["code"], "AAA001")
        self.assertGreaterEqual(result["summary"]["average_turnover_pct"], 0.0)
        self.assertGreaterEqual(result["summary"]["average_cost_pct"], 0.0)
        self.assertLessEqual(
            result["summary"]["average_net_return_pct"],
            result["summary"]["average_gross_return_pct"],
        )

    def test_outlier_filter_counts_skipped_samples(self):
        histories = {
            "AAA001": {
                "code": "AAA001",
                "name": "outlier",
                "sector": "test",
                "rows": [
                    {"date": "2026-01-01", "open": 10.0, "close": 10.0, "high": 10.1, "low": 9.9, "volume": 1, "amount": 10},
                    {"date": "2026-01-02", "open": 10.0, "close": 10.0, "high": 10.1, "low": 9.9, "volume": 1, "amount": 10},
                    {"date": "2026-01-03", "open": 10.0, "close": 10.0, "high": 10.1, "low": 9.9, "volume": 1, "amount": 10},
                    {"date": "2026-01-04", "open": 18.0, "close": 18.0, "high": 18.1, "low": 17.9, "volume": 1, "amount": 18},
                ],
            }
        }
        for stock in histories.values():
            stock["date_index"] = {row["date"]: index for index, row in enumerate(stock["rows"])}

        result = backtester.run_backtest_on_histories(
            histories,
            top_n=1,
            hold_days=2,
            rebalance_days=1,
            min_history=2,
            min_candidates=1,
            enabled_groups=("capital", "technical"),
            max_forward_return_pct=50.0,
        )

        self.assertGreater(result["summary"]["skipped_outlier_count"], 0)
        self.assertGreaterEqual(result["summary"]["period_count"], 0)

    def test_keep_rank_reduces_turnover(self):
        histories = {
            "AAA001": {
                "code": "AAA001",
                "name": "leader_a",
                "sector": "test",
                "rows": _build_rows(10.0, [0.008, -0.003, 0.01, -0.002, 0.009, 0.004] * 2, 120000),
            },
            "BBB001": {
                "code": "BBB001",
                "name": "leader_b",
                "sector": "test",
                "rows": _build_rows(10.2, [0.007, -0.002, 0.009, -0.001, 0.008, 0.003] * 2, 118000),
            },
            "CCC001": {
                "code": "CCC001",
                "name": "challenger",
                "sector": "test",
                "rows": _build_rows(10.1, [0.009, -0.004, 0.008, -0.003, 0.01, 0.005] * 2, 121000),
            },
        }
        for stock in histories.values():
            stock["date_index"] = {row["date"]: index for index, row in enumerate(stock["rows"])}

        baseline = backtester.run_backtest_on_histories(
            histories,
            top_n=2,
            hold_days=2,
            rebalance_days=2,
            min_history=5,
            min_candidates=1,
            enabled_groups=("capital", "technical"),
            keep_rank=0,
        )
        buffered = backtester.run_backtest_on_histories(
            histories,
            top_n=2,
            hold_days=2,
            rebalance_days=2,
            min_history=5,
            min_candidates=1,
            enabled_groups=("capital", "technical"),
            keep_rank=3,
        )

        self.assertGreaterEqual(baseline["summary"]["average_turnover_pct"], buffered["summary"]["average_turnover_pct"])


if __name__ == "__main__":
    unittest.main()
