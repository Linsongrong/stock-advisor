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
        payload_page_1 = {
            "data": {
                "total": 200,
                "diff": {
                    "0": {"f12": "000001", "f14": "平安银行", "f100": "银行"},
                    "1": {"f12": "300001", "f14": "特锐德", "f100": "创业板"},
                },
            }
        }
        payload_page_2 = {
            "data": {
                "total": 200,
                "diff": {
                    "0": {"f12": "600519", "f14": "贵州茅台", "f100": "白酒"},
                    "1": {"f12": "000004", "f14": "*ST国华", "f100": "软件"},
                },
            }
        }

        fake_session = mock.Mock()
        fake_session.get.side_effect = [
            FakeResponse(payload=payload_page_1),
            FakeResponse(payload=payload_page_2),
        ]
        fake_session.mount.return_value = None

        with mock.patch("stock_universe.requests.Session", return_value=fake_session), mock.patch.object(
            stock_universe,
            "MIN_COMPLETE_UNIVERSE_SIZE",
            2,
        ):
            stocks = stock_universe._fetch_eastmoney_universe()

        self.assertEqual(
            stocks,
            [
                {"code": "000001", "name": "平安银行", "sector": "银行"},
                {"code": "600519", "name": "贵州茅台", "sector": "白酒"},
            ],
        )

    def test_fetch_sina_universe_filters_non_sh_sz_and_uses_sector_map(self) -> None:
        count_response = FakeResponse(text='"4"')
        page_response = FakeResponse(
            text=json.dumps(
                [
                    {"symbol": "sh600519", "code": "600519", "name": "贵州茅台"},
                    {"symbol": "sz000001", "code": "000001", "name": "平安银行"},
                    {"symbol": "sz300001", "code": "300001", "name": "特锐德"},
                    {"symbol": "sz000004", "code": "000004", "name": "*ST国华"},
                    {"symbol": "bj920000", "code": "920000", "name": "安徽凤凰"},
                ],
                ensure_ascii=False,
            )
        )
        fake_session = mock.Mock()
        fake_session.get.side_effect = [count_response, page_response]
        fake_session.mount.return_value = None

        with mock.patch("stock_universe.requests.Session", return_value=fake_session), mock.patch.object(
            stock_universe,
            "MIN_COMPLETE_UNIVERSE_SIZE",
            2,
        ):
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

    def test_get_stock_universe_with_source_refreshes_incomplete_eastmoney_cache(self) -> None:
        cached_stocks = [{"code": "000001", "name": "平安银行", "sector": "银行"}]
        fetched_stocks = [{"code": "600519", "name": "贵州茅台", "sector": "白酒食品"}]

        with mock.patch.object(
            stock_universe,
            "_load_universe_cache_payload",
            return_value=(cached_stocks, "eastmoney_broad_market", "2026-04-22T09:00:00"),
        ), mock.patch.object(
            stock_universe,
            "MIN_COMPLETE_UNIVERSE_SIZE",
            2,
        ), mock.patch.object(
            stock_universe,
            "_fetch_eastmoney_universe",
            return_value=fetched_stocks,
        ) as fetch_eastmoney, mock.patch.object(stock_universe, "_save_universe_cache") as save_cache:
            stocks, source = stock_universe.get_stock_universe_with_source()

        self.assertEqual(source, "eastmoney_broad_market_live")
        self.assertEqual(stocks, fetched_stocks)
        fetch_eastmoney.assert_called_once()
        save_cache.assert_called_once_with(fetched_stocks, "eastmoney_broad_market")

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

    def test_is_excluded_stock_keeps_only_main_board_and_filters_st(self) -> None:
        self.assertTrue(stock_universe._is_excluded_stock("300001", "特锐德"))
        self.assertTrue(stock_universe._is_excluded_stock("301001", "凯淳股份"))
        self.assertTrue(stock_universe._is_excluded_stock("688001", "华兴源创"))
        self.assertTrue(stock_universe._is_excluded_stock("920000", "安徽凤凰"))
        self.assertTrue(stock_universe._is_excluded_stock("000004", "*ST国华"))
        self.assertFalse(stock_universe._is_excluded_stock("600519", "贵州茅台"))
        self.assertFalse(stock_universe._is_excluded_stock("002371", "北方华创"))


if __name__ == "__main__":
    unittest.main()
