# -*- coding: utf-8 -*-
"""Tests for market data fetching behavior."""

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import data_fetcher
from data_fetcher import MarketDataFetcher


class FakeResponse:
    """Minimal response object for mocked requests."""

    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = "utf-8"
        self.apparent_encoding = "utf-8"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class DataFetcherTests(unittest.TestCase):
    def test_fetch_quote_live_normalizes_volume_to_shares(self) -> None:
        raw_text = (
            'v_sh601866="1~中远海发~601866~3.05~2.86~3.00~4688143~2883423~1804150~3.04~21289~3.03~17887~'
            '3.02~29388~3.01~28496~3.00~50521~3.05~3783~3.06~14680~3.07~9752~3.08~15175~3.09~13249~~'
            '20260407141848~0.19~6.64~3.15~2.88~3.05/4688143/1426070118~4688143~142607~4.81";'
        )
        fetcher = MarketDataFetcher()

        with mock.patch.object(fetcher.session, "get", return_value=FakeResponse(raw_text)):
            quote = fetcher.fetch_quote_live("601866")

        self.assertIsNotNone(quote)
        self.assertEqual(quote["volume"], 468814300)
        self.assertEqual(quote["volume_shares"], 468814300)
        self.assertEqual(quote["volume_input"], 4688143)
        self.assertEqual(quote["volume_input_unit"], "lots")
        self.assertEqual(quote["volume_unit"], "shares")
        self.assertEqual(quote["updated_at"], "20260407141848")

    def test_fetch_kline_live_prefers_longer_history(self) -> None:
        short_text = (
            'quotebridge_v6_line_hs_000001_01_last({"data":"20260101,10,10.1,10.2,9.9,1000,10000;'
            '20260102,10.1,10.2,10.3,10.0,1200,12000"});'
        )
        long_text = (
            'quotebridge_v6_line_hs_000001_01_last36000({"data":"20251230,9.8,9.9,10.0,9.7,900,9000;'
            '20251231,9.9,10.0,10.1,9.8,950,9500;'
            '20260101,10,10.1,10.2,9.9,1000,10000;'
            '20260102,10.1,10.2,10.3,10.0,1200,12000"});'
        )
        fetcher = MarketDataFetcher()

        with mock.patch.object(fetcher.session, "get", side_effect=[FakeResponse(short_text), FakeResponse(long_text)]):
            rows = fetcher.fetch_kline_live("000001", prefer_long_history=True)

        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["date"], "2025-12-30")
        self.assertEqual(rows[-1]["date"], "2026-01-02")

    def test_fetch_kline_live_short_mode_stops_after_sufficient_first_response(self) -> None:
        first_text = (
            'quotebridge_v6_line_hs_000001_01_last({"data":"20260101,10,10.1,10.2,9.9,1000,10000;'
            '20260102,10.1,10.2,10.3,10.0,1200,12000"});'
        )
        fetcher = MarketDataFetcher()

        with mock.patch.object(fetcher.session, "get", return_value=FakeResponse(first_text)) as mocked_get:
            rows = fetcher.fetch_kline_live("000001", prefer_long_history=False, min_rows=2)

        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 2)
        self.assertEqual(mocked_get.call_count, 1)

    def test_refresh_kline_cache_for_universe_updates_and_tracks_failures(self) -> None:
        fetcher = MarketDataFetcher()
        universe = [{"code": "000001"}, {"code": "000002"}, {"code": "000003"}]
        rows = [{"date": "2026-01-01", "open": 1, "close": 1, "high": 1, "low": 1, "volume": 1, "amount": 1}]

        with mock.patch.object(fetcher, "fetch_kline_live", side_effect=[rows, None, rows]):
            with mock.patch.object(fetcher, "_write_cache") as write_cache:
                result = fetcher.refresh_kline_cache_for_universe(universe, max_workers=2)

        self.assertEqual(result["requested_count"], 3)
        self.assertEqual(result["updated_count"], 2)
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(result["failed_codes"], ["000002"])
        self.assertEqual(write_cache.call_count, 2)

    def test_get_quote_normalizes_legacy_cached_live_volume(self) -> None:
        fetcher = MarketDataFetcher()
        saved_at = (datetime.now() - timedelta(minutes=1)).isoformat(timespec="seconds")

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_path = cache_dir / "601866.json"
            cache_path.write_text(
                (
                    f'{{"saved_at":"{saved_at}","data":{{"code":"601866","name":"中远海发","price":3.05,'
                    '"prev_close":2.86,"open":3.0,"volume":4688143,"updated_at":"20260407141848","source":"live"}}'
                ),
                encoding="utf-8",
            )

            with mock.patch.object(data_fetcher, "QUOTE_CACHE_DIR", cache_dir):
                quote = fetcher.get_quote("601866", allow_live=False)

        self.assertIsNotNone(quote)
        self.assertEqual(quote["source"], "cache_fresh")
        self.assertEqual(quote["cache_origin_source"], "live")
        self.assertEqual(quote["volume"], 468814300)
        self.assertEqual(quote["volume_input_unit"], "lots")

    def test_get_quote_with_meta_prefers_fresh_cache_when_requested(self) -> None:
        fetcher = MarketDataFetcher()
        saved_at = (datetime.now() - timedelta(minutes=1)).isoformat(timespec="seconds")

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)
            cache_path = cache_dir / "601866.json"
            cache_path.write_text(
                (
                    f'{{"saved_at":"{saved_at}","data":{{"code":"601866","name":"中远海发","price":3.05,'
                    '"prev_close":2.86,"open":3.0,"volume":4688143,"updated_at":"20260407141848","source":"live"}}'
                ),
                encoding="utf-8",
            )

            with mock.patch.object(data_fetcher, "QUOTE_CACHE_DIR", cache_dir):
                with mock.patch.object(fetcher, "fetch_quote_live", side_effect=AssertionError("should prefer fresh cache")):
                    quote, meta = fetcher.get_quote_with_meta("601866", allow_live=True, prefer_cache=True)

        self.assertIsNotNone(quote)
        self.assertEqual(meta["source"], "cache_fresh")
        self.assertTrue(meta["cache_is_fresh"])

    def test_get_kline_with_meta_short_live_does_not_overwrite_shared_cache(self) -> None:
        fetcher = MarketDataFetcher()
        rows = [{"date": "2026-01-01", "open": 1, "close": 1, "high": 1, "low": 1, "volume": 1, "amount": 1}]

        with mock.patch.object(fetcher, "fetch_kline_live", return_value=rows):
            with mock.patch.object(fetcher, "_write_cache") as write_cache:
                result, meta = fetcher.get_kline_with_meta("000001", allow_live=True, prefer_cache=False, prefer_long_history=False)

        self.assertEqual(result, rows)
        self.assertEqual(meta["source"], "live")
        write_cache.assert_not_called()

    def test_fetch_universe_data_records_source_usage(self) -> None:
        fetcher = MarketDataFetcher()
        universe = [{"code": "000001", "name": "A", "sector": "s"}, {"code": "000002", "name": "B", "sector": "s"}]
        stock_result = {
            "code": "000001",
            "name": "A",
            "sector": "s",
            "quote": {"price": 1},
            "kline": [{"date": "2026-01-01"}],
            "data_sources": {
                "quote": {"source": "live"},
                "kline": {"source": "cache_stale"},
            },
        }
        failed_result = {
            "code": "000002",
            "name": "B",
            "sector": "s",
            "error": "quote_unavailable",
            "data_sources": {
                "quote": {"source": "unavailable"},
                "kline": {"source": "cache_fresh"},
            },
        }

        with mock.patch.object(fetcher, "probe_sources", return_value={"quote": False, "kline": True}):
            with mock.patch.object(fetcher, "fetch_stock_bundle", side_effect=[stock_result, failed_result]):
                result = fetcher.fetch_universe_data(universe, max_workers=2)

        self.assertEqual(result["source_usage"]["quote"]["live"], 1)
        self.assertEqual(result["source_usage"]["quote"]["unavailable"], 1)
        self.assertEqual(result["source_usage"]["kline"]["cache_stale"], 1)
        self.assertEqual(result["source_usage"]["kline"]["cache_fresh"], 1)

    def test_fetch_stock_bundle_uses_prefetched_quote_snapshot(self) -> None:
        fetcher = MarketDataFetcher()
        stock = {
            "code": "600519",
            "name": "贵州茅台",
            "sector": "白酒",
            "quote_snapshot_saved_at": datetime.now().isoformat(timespec="seconds"),
            "quote_snapshot": {
                "code": "600519",
                "name": "贵州茅台",
                "price": 1500.0,
                "prev_close": 1490.0,
                "open": 1495.0,
                "volume": 123456,
                "volume_input": 123456,
                "volume_input_unit": "shares",
                "volume_unit": "shares",
                "change_amount": 10.0,
                "change_pct": 0.67,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "source": "universe_snapshot",
            },
        }
        kline = [
            {"date": "2026-04-06", "open": 1480.0, "close": 1490.0, "high": 1495.0, "low": 1470.0, "volume": 100000, "amount": 1},
            {"date": "2026-04-07", "open": 1495.0, "close": 1500.0, "high": 1505.0, "low": 1490.0, "volume": 123456, "amount": 1},
        ]

        with mock.patch.object(fetcher, "get_kline_with_meta", return_value=(kline, {"source": "cache_fresh"})):
            with mock.patch.object(fetcher, "get_quote_with_meta", side_effect=AssertionError("should not fetch live quote")):
                bundle = fetcher.fetch_stock_bundle(stock, source_status={"quote": True, "kline": True})

        self.assertEqual(bundle["quote"]["source"], "universe_snapshot")
        self.assertEqual(bundle["data_sources"]["quote"]["source"], "universe_snapshot")
        self.assertEqual(bundle["quote"]["volume"], 123456)

    def test_select_prefilter_targets_limits_large_snapshot_universe(self) -> None:
        fetcher = MarketDataFetcher()
        universe = []
        for index in range(1500):
            code = f"{index:06d}"
            universe.append(
                {
                    "code": code,
                    "name": code,
                    "sector": "auto",
                    "quote_snapshot_saved_at": datetime.now().isoformat(timespec="seconds"),
                    "quote_snapshot": {
                        "code": code,
                        "name": code,
                        "price": 10.0,
                        "prev_close": 9.8,
                        "open": 9.9,
                        "volume": 1000 + index,
                        "volume_input": 1000 + index,
                        "volume_input_unit": "shares",
                        "volume_unit": "shares",
                        "amount": 100000 + index * 1000,
                        "change_amount": 0.2,
                        "change_pct": index / 100.0,
                        "updated_at": datetime.now().isoformat(timespec="seconds"),
                        "source": "universe_snapshot",
                    },
                }
            )

        selected, prefilter = fetcher._select_prefilter_targets(universe)

        self.assertTrue(prefilter["enabled"])
        self.assertEqual(prefilter["input_count"], 1500)
        self.assertEqual(prefilter["seed_quote_count"], 1500)
        self.assertEqual(prefilter["selected_count"], len(selected))
        self.assertLess(len(selected), len(universe))

    def test_select_prefilter_targets_keeps_small_universe_unchanged(self) -> None:
        fetcher = MarketDataFetcher()
        universe = [{"code": "000001", "name": "A", "sector": "s"} for _ in range(10)]

        selected, prefilter = fetcher._select_prefilter_targets(universe)

        self.assertFalse(prefilter["enabled"])
        self.assertEqual(selected, universe)

    def test_fetch_stock_bundle_trims_screening_kline_window(self) -> None:
        fetcher = MarketDataFetcher()
        stock = {
            "code": "600519",
            "name": "MOUTAI",
            "sector": "test",
            "quote_snapshot_saved_at": datetime.now().isoformat(timespec="seconds"),
            "quote_snapshot": {
                "code": "600519",
                "name": "MOUTAI",
                "price": 1500.0,
                "prev_close": 1490.0,
                "open": 1495.0,
                "volume": 123456,
                "volume_input": 123456,
                "volume_input_unit": "shares",
                "volume_unit": "shares",
                "change_amount": 10.0,
                "change_pct": 0.67,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "source": "universe_snapshot",
            },
        }
        kline = []
        for index in range(220):
            kline.append(
                {
                    "date": f"2026-02-{(index % 28) + 1:02d}",
                    "open": 10.0,
                    "close": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "volume": 1000,
                    "amount": 10000,
                }
            )

        with mock.patch.object(fetcher, "get_kline_with_meta", return_value=(kline, {"source": "cache_fresh"})):
            bundle = fetcher.fetch_stock_bundle(stock, source_status={"quote": True, "kline": True})

        self.assertEqual(len(bundle["kline"]), 180)


if __name__ == "__main__":
    unittest.main()
