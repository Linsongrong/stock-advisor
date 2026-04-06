# -*- coding: utf-8 -*-
"""Regression tests for parser, cache, and scoring behavior."""

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import config
import factors.fundamental as fundamental
import factors.sentiment as sentiment
from scorer import score_stock


BULLISH_TITLE = "\u516c\u53f8\u8ba2\u5355\u589e\u957f\uff0c\u7a81\u7834\u65b0\u9ad8"
BEARISH_TITLE = "\u80a1\u4e1c\u51cf\u6301\u5e26\u6765\u98ce\u9669"
OLD_TITLE = "\u65e7\u95fb\u4e0d\u5e94\u7eb3\u5165\u7edf\u8ba1"


class FakeResponse:
    """Minimal requests-like response object for parser tests."""

    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = "utf-8"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class ConfigRegressionTests(unittest.TestCase):
    def test_global_factor_weights_are_normalized(self) -> None:
        self.assertAlmostEqual(config.RAW_TOTAL_WEIGHT, 1.04, places=6)
        self.assertAlmostEqual(config.TOTAL_WEIGHT, 1.0, places=10)

        expected_capital = (
            config.RAW_FACTOR_WEIGHTS["capital"]["net_inflow_rate"] / config.RAW_TOTAL_WEIGHT
        )
        expected_sentiment = (
            config.RAW_FACTOR_WEIGHTS["sentiment"]["sentiment"] / config.RAW_TOTAL_WEIGHT
        )

        self.assertAlmostEqual(
            config.FACTOR_WEIGHTS["capital"]["net_inflow_rate"],
            expected_capital,
            places=10,
        )
        self.assertAlmostEqual(
            config.FACTOR_WEIGHTS["sentiment"]["sentiment"],
            expected_sentiment,
            places=10,
        )


class FundamentalRegressionTests(unittest.TestCase):
    def test_batch_fetch_fundamentals_parses_jsvar_and_reuses_cache(self) -> None:
        jsvar_text = """
        var fourQ_mgsy = 0.7437;
        var lastyear_mgsy = 0.7400;
        var mgjzc = 5.326077;
        """

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "fundamental_cache.json"

            with mock.patch.object(fundamental, "FUNDAMENTAL_CACHE_PATH", cache_path):
                with mock.patch("factors.fundamental.requests.get", return_value=FakeResponse(jsvar_text)):
                    first = fundamental.batch_fetch_fundamentals(["601872"])

                self.assertTrue(cache_path.exists())
                self.assertIn("601872", first)
                self.assertEqual(first["601872"]["source"], "live")
                self.assertAlmostEqual(first["601872"]["eps_ttm"], 0.7437, places=4)
                self.assertAlmostEqual(first["601872"]["eps_last_year"], 0.74, places=4)
                self.assertAlmostEqual(first["601872"]["bvps"], 5.326077, places=6)
                self.assertAlmostEqual(first["601872"]["roe"], 13.96, places=2)

                with mock.patch(
                    "factors.fundamental.requests.get",
                    side_effect=AssertionError("fresh cache should avoid live request"),
                ):
                    second = fundamental.batch_fetch_fundamentals(["601872"])

                self.assertEqual(second["601872"]["source"], "cache")
                self.assertAlmostEqual(second["601872"]["roe"], 13.96, places=2)


class SentimentRegressionTests(unittest.TestCase):
    def test_fetch_stock_news_sina_parses_recent_datelist_items(self) -> None:
        recent_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        old_date = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d")
        html_text = f"""
        <div class="datelist">
            <ul>
                &nbsp;&nbsp;&nbsp;&nbsp;{recent_date}&nbsp;15:35&nbsp;&nbsp;<a target='_blank' href='https://example.com/1'>{BULLISH_TITLE}</a> <br>
                &nbsp;&nbsp;&nbsp;&nbsp;{recent_date}&nbsp;09:20&nbsp;&nbsp;<a target='_blank' href='https://example.com/2'>{BEARISH_TITLE}</a> <br>
                &nbsp;&nbsp;&nbsp;&nbsp;{recent_date}&nbsp;08:15&nbsp;&nbsp;<a target='_blank' href='https://example.com/3'>{BULLISH_TITLE}</a> <br>
                &nbsp;&nbsp;&nbsp;&nbsp;{old_date}&nbsp;07:47&nbsp;&nbsp;<a target='_blank' href='https://example.com/old'>{OLD_TITLE}</a> <br>
            </ul>
        </div>
        """

        with mock.patch("factors.sentiment.requests.get", return_value=FakeResponse(html_text)):
            articles = sentiment._fetch_stock_news_sina("601872")

        self.assertEqual(len(articles), 2)
        self.assertEqual(articles[0]["title"], BULLISH_TITLE)
        self.assertEqual(articles[1]["title"], BEARISH_TITLE)

    def test_fetch_stock_sentiment_scores_recent_titles_and_uses_cache(self) -> None:
        recent_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        html_text = f"""
        <div class="datelist">
            <ul>
                &nbsp;&nbsp;&nbsp;&nbsp;{recent_date}&nbsp;15:35&nbsp;&nbsp;<a target='_blank' href='https://example.com/1'>{BULLISH_TITLE}</a> <br>
                &nbsp;&nbsp;&nbsp;&nbsp;{recent_date}&nbsp;09:20&nbsp;&nbsp;<a target='_blank' href='https://example.com/2'>{BEARISH_TITLE}</a> <br>
            </ul>
        </div>
        """

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "sentiment_cache.json"

            with mock.patch.object(sentiment, "SENTIMENT_CACHE_PATH", cache_path):
                with mock.patch("factors.sentiment.requests.get", return_value=FakeResponse(html_text)):
                    first = sentiment.fetch_stock_sentiment("601872")

                self.assertTrue(cache_path.exists())
                self.assertEqual(first["source"], "live")
                self.assertTrue(first["has_data"])
                self.assertEqual(first["news_count"], 2)
                self.assertEqual(first["bullish_hits"], 4)
                self.assertEqual(first["bearish_hits"], 2)
                self.assertAlmostEqual(first["sentiment_score"], 66.67, places=2)

                with mock.patch(
                    "factors.sentiment.requests.get",
                    side_effect=AssertionError("fresh cache should avoid live request"),
                ):
                    second = sentiment.fetch_stock_sentiment("601872")

                self.assertEqual(second["source"], "cache")
                self.assertAlmostEqual(second["sentiment_score"], 66.67, places=2)


class ScoreSnapshotTests(unittest.TestCase):
    @staticmethod
    def _build_fixture_bundle():
        returns = [0.01, -0.005, 0.012, 0.0, 0.008, -0.006, 0.011, 0.004, -0.007, 0.009] * 3
        kline = []
        price = 10.0

        for index, day_return in enumerate(returns, start=1):
            close_price = round(price * (1 + day_return), 2)
            open_price = round(price * (1 + day_return / 3), 2)
            high_price = round(max(open_price, close_price) * 1.01, 2)
            low_price = round(min(open_price, close_price) * 0.99, 2)
            volume = 100000 + index * 2500 + (5000 if day_return > 0.008 else 0)
            kline.append(
                {
                    "date": f"2026-03-{index:02d}",
                    "open": open_price,
                    "close": close_price,
                    "high": high_price,
                    "low": low_price,
                    "volume": volume,
                    "amount": round(close_price * volume, 2),
                }
            )
            price = close_price

        quote = {
            "code": "999999",
            "name": "fixture",
            "price": kline[-1]["close"],
            "prev_close": kline[-2]["close"],
            "open": kline[-1]["open"],
            "volume": int(kline[-1]["volume"] * 1.35),
            "change_amount": round(kline[-1]["close"] - kline[-2]["close"], 2),
            "change_pct": round((kline[-1]["close"] / kline[-2]["close"] - 1) * 100, 2),
        }

        bundle = {
            "code": "999999",
            "name": "fixture",
            "sector": "test",
            "quote": quote,
            "kline": kline,
        }
        fund_data = {
            "eps_ttm": 0.9,
            "eps_last_year": 0.75,
            "bvps": 4.2,
            "roe": 21.43,
            "source": "fixture",
        }
        sentiment_data = {
            "sentiment_score": 68.0,
            "bullish_hits": 5,
            "bearish_hits": 2,
            "news_count": 6,
            "has_data": True,
            "source": "fixture",
        }
        return bundle, fund_data, sentiment_data

    def test_score_stock_snapshot_for_reference_bundle(self) -> None:
        bundle, fund_data, sentiment_data = self._build_fixture_bundle()
        result = score_stock(bundle, fund_data=fund_data, sentiment_data=sentiment_data)

        self.assertFalse(result["filtered_out"])
        self.assertAlmostEqual(result["capital_score"], 81.38, places=2)
        self.assertAlmostEqual(result["technical_score"], 68.86, places=2)
        self.assertAlmostEqual(result["fundamental_score"], 90.0, places=2)
        self.assertAlmostEqual(result["sentiment_score"], 68.0, places=2)
        self.assertAlmostEqual(result["total_score"], 77.26, places=2)
        self.assertAlmostEqual(result["technical_indicators"]["rsi"], 75.89, places=2)


if __name__ == "__main__":
    unittest.main()
