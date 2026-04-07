# -*- coding: utf-8 -*-
"""Tests for market data fetching behavior."""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

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

        with mock.patch(
            "data_fetcher.requests.get",
            side_effect=[FakeResponse(short_text), FakeResponse(long_text)],
        ):
            rows = fetcher.fetch_kline_live("000001")

        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["date"], "2025-12-30")
        self.assertEqual(rows[-1]["date"], "2026-01-02")

    def test_refresh_kline_cache_for_universe_updates_and_tracks_failures(self) -> None:
        fetcher = MarketDataFetcher()
        universe = [{"code": "000001"}, {"code": "000002"}, {"code": "000003"}]
        rows = [{"date": "2026-01-01", "open": 1, "close": 1, "high": 1, "low": 1, "volume": 1, "amount": 1}]

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir)

            with mock.patch.object(fetcher, "fetch_kline_live", side_effect=[rows, None, rows]):
                with mock.patch.object(fetcher, "_write_cache") as write_cache:
                    result = fetcher.refresh_kline_cache_for_universe(universe, max_workers=2)

        self.assertEqual(result["requested_count"], 3)
        self.assertEqual(result["updated_count"], 2)
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(result["failed_codes"], ["000002"])
        self.assertEqual(write_cache.call_count, 2)


if __name__ == "__main__":
    unittest.main()
