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

    def test_screen_market_applies_sector_cap_to_top_list(self) -> None:
        stocks = [
            {
                "code": "000001",
                "name": "A1",
                "sector": "alpha",
                "quote": {"price": 10.0},
                "kline": [{"date": "2026-04-07", "close": 10.0}],
            },
            {
                "code": "000002",
                "name": "A2",
                "sector": "alpha",
                "quote": {"price": 10.0},
                "kline": [{"date": "2026-04-07", "close": 10.0}],
            },
            {
                "code": "000003",
                "name": "B1",
                "sector": "beta",
                "quote": {"price": 10.0},
                "kline": [{"date": "2026-04-07", "close": 10.0}],
            },
        ]
        market_data = {"stocks": stocks, "failed": []}
        score_map = {"000001": 95.0, "000002": 90.0, "000003": 80.0}

        def fake_score(bundle, fund_data=None, sentiment_data=None):
            total = score_map[bundle["code"]]
            return {
                "code": bundle["code"],
                "name": bundle["name"],
                "sector": bundle["sector"],
                "price": 10.0,
                "change_pct": 1.0,
                "capital_score": total,
                "technical_score": total,
                "fundamental_score": 50.0,
                "sentiment_score": 50.0,
                "total_score": total,
                "technical_indicators": {"rsi": 50.0},
                "quote": bundle["quote"],
                "kline": bundle["kline"],
                "filtered_out": False,
            }

        with mock.patch.object(screener, "FUNDAMENTAL_PREFETCH_POOL", 0):
            with mock.patch("screener.score_stock", side_effect=fake_score):
                with mock.patch("screener.batch_fetch_sentiment", return_value={}):
                    with mock.patch("screener.batch_fetch_fundamentals", return_value={}):
                        result = screener.screen_market(
                            market_data,
                            top_n=2,
                            fetch_sentiment=False,
                            max_per_sector=1,
                        )

        self.assertEqual([item["code"] for item in result["top_stocks"]], ["000001", "000003"])
        self.assertEqual(result["max_per_sector"], 1)

    def test_screen_market_filters_low_price_candidates(self) -> None:
        stocks = [
            {
                "code": "000001",
                "name": "cheap",
                "sector": "alpha",
                "quote": {"price": 3.0},
                "kline": [{"date": "2026-04-07", "close": 3.0}],
            },
            {
                "code": "000002",
                "name": "normal",
                "sector": "beta",
                "quote": {"price": 10.0},
                "kline": [{"date": "2026-04-07", "close": 10.0}],
            },
        ]
        market_data = {"stocks": stocks, "failed": []}

        def fake_score(bundle, fund_data=None, sentiment_data=None):
            return {
                "code": bundle["code"],
                "name": bundle["name"],
                "sector": bundle["sector"],
                "price": float(bundle["quote"]["price"]),
                "change_pct": 1.0,
                "capital_score": 80.0,
                "technical_score": 80.0,
                "fundamental_score": 50.0,
                "sentiment_score": 50.0,
                "total_score": 80.0,
                "technical_indicators": {"rsi": 50.0},
                "quote": bundle["quote"],
                "kline": bundle["kline"],
                "filtered_out": False,
            }

        with mock.patch.object(screener, "FUNDAMENTAL_PREFETCH_POOL", 0):
            with mock.patch("screener.score_stock", side_effect=fake_score):
                with mock.patch("screener.batch_fetch_sentiment", return_value={}):
                    with mock.patch("screener.batch_fetch_fundamentals", return_value={}):
                        result = screener.screen_market(
                            market_data,
                            top_n=2,
                            fetch_sentiment=False,
                            min_price_filter=5.0,
                        )

        self.assertEqual([item["code"] for item in result["top_stocks"]], ["000002"])
        self.assertEqual(result["filtered_count"], 1)
        self.assertEqual(result["min_price_filter"], 5.0)

    def test_screen_market_can_reduce_exposure_from_breadth(self) -> None:
        stocks = [
            {
                "code": "000001",
                "name": "leader",
                "sector": "alpha",
                "quote": {"price": 10.0},
                "kline": [{"date": "2026-04-07", "close": 10.0}],
            },
            {
                "code": "000002",
                "name": "laggard",
                "sector": "beta",
                "quote": {"price": 8.0},
                "kline": [{"date": "2026-04-07", "close": 8.0}],
            },
        ]
        market_data = {"stocks": stocks, "failed": []}

        def fake_score(bundle, fund_data=None, sentiment_data=None):
            ma20 = 9.0 if bundle["code"] == "000001" else 9.5
            return {
                "code": bundle["code"],
                "name": bundle["name"],
                "sector": bundle["sector"],
                "price": float(bundle["quote"]["price"]),
                "change_pct": 1.0,
                "capital_score": 80.0,
                "technical_score": 80.0,
                "fundamental_score": 50.0,
                "sentiment_score": 50.0,
                "total_score": 80.0,
                "technical_indicators": {"rsi": 50.0, "ma20": ma20},
                "quote": bundle["quote"],
                "kline": bundle["kline"],
                "filtered_out": False,
            }

        with mock.patch.object(screener, "FUNDAMENTAL_PREFETCH_POOL", 0):
            with mock.patch("screener.score_stock", side_effect=fake_score):
                with mock.patch("screener.batch_fetch_sentiment", return_value={}):
                    with mock.patch("screener.batch_fetch_fundamentals", return_value={}):
                        result = screener.screen_market(
                            market_data,
                            top_n=2,
                            fetch_sentiment=False,
                            min_price_filter=0.0,
                            min_breadth_above_ma20=0.75,
                            low_exposure=0.6,
                        )

        self.assertEqual(len(result["top_stocks"]), 2)
        self.assertTrue(result["risk_off"])
        self.assertEqual(result["market_breadth"]["above_ma20_ratio"], 0.5)
        self.assertEqual(result["suggested_exposure"], 0.6)

    def test_screen_market_prefers_strong_flow_sector_when_liquidity_is_good(self) -> None:
        def rows(close: float, volume: int, amount: float):
            return [
                {"date": "2026-04-01", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
                {"date": "2026-04-02", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
                {"date": "2026-04-03", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
                {"date": "2026-04-06", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
                {"date": "2026-04-07", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
            ]

        stocks = [
            {
                "code": "000001",
                "name": "Weak Leader",
                "sector": "weak",
                "quote": {"price": 10.0, "amount": 1_000_000, "volume": 100_000, "change_pct": 0.2},
                "kline": rows(10.0, 100_000, 1_000_000),
            },
            {
                "code": "000002",
                "name": "Strong One",
                "sector": "strong",
                "quote": {"price": 10.0, "amount": 4_000_000, "volume": 400_000, "change_pct": 2.5},
                "kline": rows(10.0, 100_000, 1_000_000),
            },
            {
                "code": "000003",
                "name": "Strong Two",
                "sector": "strong",
                "quote": {"price": 10.0, "amount": 3_500_000, "volume": 350_000, "change_pct": 1.8},
                "kline": rows(10.0, 100_000, 1_000_000),
            },
        ]
        market_data = {"stocks": stocks, "failed": []}
        base_scores = {"000001": 90.0, "000002": 86.0, "000003": 84.0}
        capital_scores = {"000001": 20.0, "000002": 95.0, "000003": 92.0}

        def fake_score(bundle, fund_data=None, sentiment_data=None):
            total = base_scores[bundle["code"]]
            return {
                "code": bundle["code"],
                "name": bundle["name"],
                "sector": bundle["sector"],
                "price": float(bundle["quote"]["price"]),
                "change_pct": float(bundle["quote"]["change_pct"]),
                "capital_score": capital_scores[bundle["code"]],
                "technical_score": total,
                "fundamental_score": 50.0,
                "sentiment_score": 50.0,
                "total_score": total,
                "selection_score": total,
                "technical_indicators": {"rsi": 50.0, "ma20": 9.0},
                "quote": bundle["quote"],
                "kline": bundle["kline"],
                "filtered_out": False,
            }

        with mock.patch.object(screener, "FUNDAMENTAL_PREFETCH_POOL", 0):
            with mock.patch("screener.score_stock", side_effect=fake_score):
                with mock.patch("screener.batch_fetch_sentiment", return_value={}):
                    with mock.patch("screener.batch_fetch_fundamentals", return_value={}):
                        result = screener.screen_market(
                            market_data,
                            top_n=2,
                            fetch_sentiment=False,
                            max_per_sector=2,
                            min_price_filter=0.0,
                            min_market_liquidity_ratio=1.05,
                            sector_flow_top_n=1,
                            sector_flow_bonus=10.0,
                        )

        self.assertEqual(result["market_liquidity"]["status"], "good")
        self.assertTrue(result["market_liquidity"]["sector_flow_active"])
        self.assertEqual(result["sector_flows"][0]["sector"], "strong")
        self.assertEqual(result["top_stocks"][0]["code"], "000002")
        self.assertGreater(result["top_stocks"][0]["sector_flow_adjustment"], 0.0)

    def test_screen_market_restricts_picks_to_strong_flow_sector_even_when_liquidity_is_weak(self) -> None:
        def rows(close: float, volume: int, amount: float):
            return [
                {"date": "2026-04-01", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
                {"date": "2026-04-02", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
                {"date": "2026-04-03", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
                {"date": "2026-04-06", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
                {"date": "2026-04-07", "open": close, "close": close, "high": close, "low": close, "volume": volume, "amount": amount},
            ]

        stocks = [
            {
                "code": "000001",
                "name": "High Score Outside",
                "sector": "weak",
                "quote": {"price": 10.0, "amount": 500_000, "volume": 50_000, "change_pct": 0.1},
                "kline": rows(10.0, 100_000, 1_000_000),
            },
            {
                "code": "000002",
                "name": "Strong Sector Pick",
                "sector": "strong",
                "quote": {"price": 10.0, "amount": 700_000, "volume": 70_000, "change_pct": 2.0},
                "kline": rows(10.0, 100_000, 1_000_000),
            },
        ]
        market_data = {"stocks": stocks, "failed": []}

        def fake_score(bundle, fund_data=None, sentiment_data=None):
            total = 95.0 if bundle["code"] == "000001" else 80.0
            capital = 20.0 if bundle["code"] == "000001" else 95.0
            return {
                "code": bundle["code"],
                "name": bundle["name"],
                "sector": bundle["sector"],
                "price": float(bundle["quote"]["price"]),
                "change_pct": float(bundle["quote"]["change_pct"]),
                "capital_score": capital,
                "technical_score": total,
                "fundamental_score": 50.0,
                "sentiment_score": 50.0,
                "total_score": total,
                "selection_score": total,
                "technical_indicators": {"rsi": 50.0, "ma20": 9.0},
                "quote": bundle["quote"],
                "kline": bundle["kline"],
                "filtered_out": False,
            }

        with mock.patch.object(screener, "FUNDAMENTAL_PREFETCH_POOL", 0):
            with mock.patch("screener.score_stock", side_effect=fake_score):
                with mock.patch("screener.batch_fetch_sentiment", return_value={}):
                    with mock.patch("screener.batch_fetch_fundamentals", return_value={}):
                        result = screener.screen_market(
                            market_data,
                            top_n=1,
                            fetch_sentiment=False,
                            min_price_filter=0.0,
                            min_market_liquidity_ratio=2.0,
                            sector_flow_top_n=1,
                            sector_flow_bonus=8.0,
                        )

        self.assertEqual(result["market_liquidity"]["status"], "weak")
        self.assertTrue(result["market_liquidity"]["sector_flow_active"])
        self.assertEqual(result["sector_flows"][0]["sector"], "strong")
        self.assertEqual([item["code"] for item in result["top_stocks"]], ["000002"])


if __name__ == "__main__":
    unittest.main()
