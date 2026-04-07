# -*- coding: utf-8 -*-
"""Tests for stock universe fetching and cache semantics."""

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import stock_universe


class FakeResponse:
    def __init__(self, payload=None, text: str = "", status_code: int = 200) -> None:
        self._payload = payload
        self.text = text
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class StockUniverseTests(unittest.TestCase):
    def test_fetch_eastmoney_universe_parses_dict_diff_and_sector(self) -> None:
        payload = {
            "data": {
                "diff": {
                    "0": {"f12": "000001", "f14": "平安银行", "f100": "银行"},
                    "1": {"f12": "600519", "f14": "贵州茅台", "f100": "白酒"},
                }
            }
        }

        fake_session = mock.Mock()
        fake_session.get.return_value = FakeResponse(payload=payload)
        fake_session.mount.return_value = None

        with mock.patch("stock_universe.requests.Session", return_value=fake_session):
            stocks = stock_universe._fetch_eastmoney_universe()

        self.assertEqual(
            stocks,
            [
                {"code": "000001", "name": "平安银行", "sector": "银行"},
                {"code": "600519", "name": "贵州茅台", "sector": "白酒"},
            ],
        )

    def test_fetch_sina_universe_filters_non_sh_sz_and_uses_sector_map(self) -> None:
        count_response = FakeResponse(text='"3"')
        page_response = FakeResponse(
            text=json.dumps(
                [
                    {"symbol": "sh600519", "code": "600519", "name": "贵州茅台"},
                    {"symbol": "sz000001", "code": "000001", "name": "平安银行"},
                    {"symbol": "bj920000", "code": "920000", "name": "安徽凤凰"},
                ],
                ensure_ascii=False,
            )
        )
        fake_session = mock.Mock()
        fake_session.get.side_effect = [count_response, page_response]
        fake_session.mount.return_value = None

        with mock.patch("stock_universe.requests.Session", return_value=fake_session):
            stocks = stock_universe._fetch_sina_universe()

        self.assertEqual([item["code"] for item in stocks], ["000001", "600519"])
        self.assertEqual(stocks[0]["sector"], "auto_broad_sina")
        self.assertEqual(stocks[1]["sector"], "白酒食品")
        self.assertEqual(stocks[0]["quote_snapshot"]["source"], "universe_snapshot")
        self.assertEqual(stocks[0]["quote_snapshot"]["volume_unit"], "shares")
        self.assertIn("quote_snapshot_saved_at", stocks[0])

    def test_get_stock_universe_with_source_falls_back_to_sina(self) -> None:
        with mock.patch.object(stock_universe, "_load_universe_cache_payload", return_value=([], "", "")):
            with mock.patch.object(stock_universe, "_fetch_eastmoney_universe", return_value=[]):
                with mock.patch.object(
                    stock_universe,
                    "_fetch_sina_universe",
                    return_value=[{"code": "000001", "name": "平安银行", "sector": "银行"}],
                ):
                    with mock.patch.object(stock_universe, "_save_universe_cache") as save_cache:
                        stocks, source = stock_universe.get_stock_universe_with_source()

        self.assertEqual(source, "sina_broad_market_live")
        self.assertEqual(stocks, [{"code": "000001", "name": "平安银行", "sector": "银行"}])
        save_cache.assert_called_once_with(stocks, "sina_broad_market")

    def test_get_stock_universe_with_source_refreshes_stale_sina_snapshot_cache(self) -> None:
        cached_stocks = [{"code": "000001", "name": "平安银行", "sector": "银行"}]
        stale_saved_at = "2026-04-07T09:00:00"

        with mock.patch.object(
            stock_universe,
            "_load_universe_cache_payload",
            return_value=(cached_stocks, "sina_broad_market", stale_saved_at),
        ):
            with mock.patch("stock_universe._snapshot_cache_is_fresh", return_value=False):
                with mock.patch.object(
                    stock_universe,
                    "_fetch_sina_universe",
                    return_value=[{"code": "600519", "name": "贵州茅台", "sector": "白酒食品"}],
                ) as fetch_sina:
                    with mock.patch.object(stock_universe, "_save_universe_cache") as save_cache:
                        stocks, source = stock_universe.get_stock_universe_with_source()

        self.assertEqual(source, "sina_broad_market_live")
        self.assertEqual(stocks, [{"code": "600519", "name": "贵州茅台", "sector": "白酒食品"}])
        fetch_sina.assert_called_once()
        save_cache.assert_called_once()

    def test_load_universe_cache_requires_current_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "universe_cache.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "version": stock_universe.UNIVERSE_CACHE_VERSION - 1,
                        "saved_at": datetime.now().isoformat(timespec="seconds"),
                        "stocks": [{"code": "000001", "name": "平安银行", "sector": "银行"}],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with mock.patch.object(stock_universe, "UNIVERSE_CACHE_PATH", cache_path):
                stocks = stock_universe._load_universe_cache()

        self.assertEqual(stocks, [])

    def test_save_and_load_universe_cache_preserves_quote_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "universe_cache.json"
            stocks = [
                {
                    "code": "600519",
                    "name": "贵州茅台",
                    "sector": "白酒食品",
                    "quote_snapshot_saved_at": datetime.now().isoformat(timespec="seconds"),
                    "quote_snapshot": {
                        "code": "600519",
                        "name": "贵州茅台",
                        "price": 1500.0,
                        "prev_close": 1490.0,
                        "open": 1495.0,
                        "volume": 123456,
                        "volume_unit": "shares",
                        "source": "universe_snapshot",
                    },
                }
            ]

            with mock.patch.object(stock_universe, "UNIVERSE_CACHE_PATH", cache_path):
                stock_universe._save_universe_cache(stocks, "sina_broad_market")
                loaded_stocks, loaded_source, _ = stock_universe._load_universe_cache_payload()

        self.assertEqual(loaded_source, "sina_broad_market")
        self.assertEqual(loaded_stocks[0]["quote_snapshot"]["source"], "universe_snapshot")


if __name__ == "__main__":
    unittest.main()
