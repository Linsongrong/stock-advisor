# -*- coding: utf-8 -*-
"""Main entrypoint: fetch -> score -> screen -> report."""

import argparse
import sys

from config import DEFAULT_TOP_N, ensure_runtime_directories
from data_fetcher import MarketDataFetcher
from reporter import generate_report
from screener import screen_market
from stock_universe import get_stock_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A-share stock recommendation system MVP")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N, help="Number of top stocks to output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    top_n = max(1, int(args.top))

    ensure_runtime_directories()
    universe = get_stock_universe()
    fetcher = MarketDataFetcher()
    market_data = fetcher.fetch_universe_data(universe)
    screened_data = screen_market(market_data, top_n=top_n)
    report_path = generate_report(screened_data, top_n=top_n)

    print("A-share Stock Advisor MVP")
    print(f"Universe size: {len(universe)}")
    print(
        "Source status: quote={quote}, kline={kline}".format(
            quote="up" if market_data.get("source_status", {}).get("quote") else "down",
            kline="up" if market_data.get("source_status", {}).get("kline") else "down",
        )
    )
    print(f"Fetched successfully: {screened_data['fetched_count']}")
    print(f"Qualified candidates: {screened_data['qualified_count']}")
    print(f"Filtered out (RSI > 80): {screened_data['filtered_count']}")
    print(f"Failed fetch/scoring: {screened_data['failed_count']}")
    print(f"Report saved to: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
