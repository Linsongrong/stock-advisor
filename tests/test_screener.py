# -*- coding: utf-8 -*-
"""Tests for staged screening behavior."""

import unittest
from unittest import mock

import screener


class ScreenerTests(unittest.TestCase):
    def test_screen_market_full_fundamental_mode_fetches_all_codes(self) -> None:
        stocks = []
        for index in range(8):
            code = f"{index:06d}"
            stocks.append(
                {
                    "code": code,
                    "name": code,
                    "sector": "auto",
                    "quote": {"price": 10.0},
                    "kline": [{"date": "2026-04-07", "close": 10.0}],
                }
            )

        market_data = {"stocks": stocks, "failed": []}

        def fake_score(bundle, fund_data=None, sentiment_data=None):
            return {
                "code": bundle["code"],
                "name": bundle["name"],
                "sector": bundle["sector"],
                "price": 10.0,
                "change_pct": 1.0,
                "capital_score": 60.0,
                "technical_score": 60.0,
                "fundamental_score": 50.0 if fund_data is None else 70.0,
                "sentiment_score": 50.0,
                "total_score": 60.0,
                "technical_indicators": {"rsi": 50.0},
                "quote": bundle["quote"],
                "kline": bundle["kline"],
                "filtered_out": False,
            }

        with mock.patch.object(screener, "FUNDAMENTAL_PREFETCH_POOL", 0):
            with mock.patch("screener.score_stock", side_effect=fake_score):
                with mock.patch("screener.batch_fetch_sentiment", return_value={}):
                    with mock.patch(
                        "screener.batch_fetch_fundamentals",
                        side_effect=lambda codes: {code: {"bonus": 1.0} for code in codes},
                    ) as batch_fund:
                        result = screener.screen_market(market_data, top_n=3, fetch_sentiment=False)

        self.assertEqual(len(batch_fund.call_args.args[0]), 8)
        self.assertEqual(result["fundamental_pool"]["selected_count"], 8)
        self.assertEqual(result["fundamental_pool"]["input_count"], 8)

    def test_screen_market_limits_fundamental_fetch_pool(self) -> None:
        stocks = []
        for index in range(50):
            code = f"{index:06d}"
            stocks.append(
                {
                    "code": code,
                    "name": code,
                    "sector": "auto",
                    "quote": {"price": 10.0},
                    "kline": [{"date": "2026-04-07", "close": 10.0}],
                }
            )

        market_data = {"stocks": stocks, "failed": []}

        def fake_score(bundle, fund_data=None, sentiment_data=None):
            code_score = int(bundle["code"])
            total = float(code_score)
            if fund_data is not None:
                total += float(fund_data.get("bonus", 0.0))
            if sentiment_data is not None:
                total += float(sentiment_data.get("bonus", 0.0))
            return {
                "code": bundle["code"],
                "name": bundle["name"],
                "sector": bundle["sector"],
                "price": 10.0,
                "change_pct": 1.0,
                "capital_score": total,
                "technical_score": total,
                "fundamental_score": 50.0 if fund_data is None else 80.0,
                "sentiment_score": 50.0 if sentiment_data is None else 80.0,
                "total_score": total,
                "technical_indicators": {"rsi": 50.0},
                "quote": bundle["quote"],
                "kline": bundle["kline"],
                "filtered_out": False,
            }

        with mock.patch.object(screener, "FUNDAMENTAL_PREFETCH_POOL", 20):
            with mock.patch("screener.score_stock", side_effect=fake_score):
                with mock.patch("screener.batch_fetch_sentiment", return_value={}):
                    with mock.patch(
                        "screener.batch_fetch_fundamentals",
                        side_effect=lambda codes: {code: {"bonus": 1000.0} for code in codes},
                    ) as batch_fund:
                        result = screener.screen_market(market_data, top_n=1, fetch_sentiment=False)

        requested_codes = batch_fund.call_args.args[0]
        self.assertEqual(len(requested_codes), 20)
        self.assertEqual(result["fundamental_pool"]["selected_count"], 20)
        self.assertEqual(result["fundamental_pool"]["input_count"], 50)


if __name__ == "__main__":
    unittest.main()
