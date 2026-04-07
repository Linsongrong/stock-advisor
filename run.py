# -*- coding: utf-8 -*-
"""Main entrypoint: fetch -> score -> screen -> report."""

import argparse
import sys

from backtest_scan import (
    DEFAULT_SCAN_HOLD_DAYS,
    DEFAULT_SCAN_KEEP_RANKS,
    DEFAULT_SCAN_REBALANCE_DAYS,
    DEFAULT_SCAN_START_DATES,
    DEFAULT_SCAN_WINDOWS,
    DEFAULT_SCAN_TOP_NS,
    _parse_csv_dates,
    _parse_csv_ints,
    run_backtest_scan,
)
from backtester import (
    DEFAULT_COMMISSION_RATE,
    DEFAULT_ENABLED_GROUPS,
    DEFAULT_KEEP_RANK,
    DEFAULT_MAX_FORWARD_RETURN_PCT,
    DEFAULT_SELL_TAX_RATE,
    DEFAULT_SLIPPAGE_RATE,
    run_backtest,
)
from config import DEFAULT_TOP_N, MIN_KLINE_DAYS, ensure_runtime_directories
from data_fetcher import MarketDataFetcher
from reporter import generate_report
from screener import screen_market
from stock_universe import get_stock_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A-share stock recommendation system MVP")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N, help="Number of top stocks to output")
    parser.add_argument("--backtest", action="store_true", help="Run historical backtest from local K-line cache")
    parser.add_argument("--scan-backtests", action="store_true", help="Run a small parameter scan over recent backtest windows")
    parser.add_argument("--scan-regimes", action="store_true", help="Run a regime-based scan over predefined market windows")
    parser.add_argument("--refresh-history", action="store_true", help="Refresh local K-line cache with longest available live history before backtest")
    parser.add_argument("--refresh-max-stocks", type=int, default=0, help="Optional cap on number of stocks to refresh before backtest; 0 means all")
    parser.add_argument("--start-date", type=str, default="", help="Backtest start date in YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default="", help="Backtest end date in YYYY-MM-DD")
    parser.add_argument("--hold-days", type=int, default=5, help="Backtest holding period in trading days")
    parser.add_argument("--rebalance-days", type=int, default=5, help="Backtest rebalance interval in trading days")
    parser.add_argument("--min-history", type=int, default=MIN_KLINE_DAYS, help="Minimum history length required for backtest scoring")
    parser.add_argument("--min-candidates", type=int, default=20, help="Minimum number of scored candidates required per backtest period")
    parser.add_argument("--max-periods", type=int, default=0, help="Optional cap on backtest periods; 0 means all")
    parser.add_argument("--max-stocks", type=int, default=0, help="Optional cap on number of local cache stocks for backtest; 0 means all")
    parser.add_argument(
        "--scan-start-dates",
        type=str,
        default=",".join(DEFAULT_SCAN_START_DATES),
        help="Comma-separated scan start dates, e.g. 2016-01-01,2020-01-01",
    )
    parser.add_argument(
        "--scan-top-options",
        type=str,
        default=",".join(str(item) for item in DEFAULT_SCAN_TOP_NS),
        help="Comma-separated Top N options for backtest scan",
    )
    parser.add_argument(
        "--scan-hold-options",
        type=str,
        default=",".join(str(item) for item in DEFAULT_SCAN_HOLD_DAYS),
        help="Comma-separated holding period options for backtest scan",
    )
    parser.add_argument(
        "--scan-rebalance-options",
        type=str,
        default=",".join(str(item) for item in DEFAULT_SCAN_REBALANCE_DAYS),
        help="Comma-separated rebalance period options for backtest scan",
    )
    parser.add_argument(
        "--scan-keep-options",
        type=str,
        default=",".join(str(item) for item in DEFAULT_SCAN_KEEP_RANKS),
        help="Comma-separated keep-rank options for backtest scan",
    )
    parser.add_argument(
        "--max-forward-return-pct",
        type=float,
        default=DEFAULT_MAX_FORWARD_RETURN_PCT,
        help="Skip forward-return samples whose absolute return exceeds this threshold; 0 disables filtering",
    )
    parser.add_argument(
        "--keep-rank",
        type=int,
        default=DEFAULT_KEEP_RANK,
        help="Keep previous holdings if they still rank within this threshold; 0 disables turnover buffer",
    )
    parser.add_argument(
        "--commission-rate",
        type=float,
        default=DEFAULT_COMMISSION_RATE,
        help="Backtest commission assumption, e.g. 0.0003 for 0.03%%",
    )
    parser.add_argument(
        "--slippage-rate",
        type=float,
        default=DEFAULT_SLIPPAGE_RATE,
        help="Backtest slippage assumption, e.g. 0.0005 for 0.05%%",
    )
    parser.add_argument(
        "--sell-tax-rate",
        type=float,
        default=DEFAULT_SELL_TAX_RATE,
        help="Backtest sell-side extra cost assumption, e.g. 0.0005 for 0.05%%",
    )
    parser.add_argument(
        "--groups",
        nargs="+",
        default=list(DEFAULT_ENABLED_GROUPS),
        help="Factor groups to use in backtest scoring: capital technical fundamental sentiment",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    top_n = max(1, int(args.top))

    if args.scan_backtests:
        scan_windows = list(DEFAULT_SCAN_WINDOWS) if args.scan_regimes else None
        result = run_backtest_scan(
            start_dates=_parse_csv_dates(args.scan_start_dates, DEFAULT_SCAN_START_DATES),
            end_date=args.end_date or None,
            windows=scan_windows,
            top_ns=[value for value in _parse_csv_ints(args.scan_top_options, DEFAULT_SCAN_TOP_NS) if value > 0],
            hold_days_list=[value for value in _parse_csv_ints(args.scan_hold_options, DEFAULT_SCAN_HOLD_DAYS) if value > 0],
            rebalance_days_list=[value for value in _parse_csv_ints(args.scan_rebalance_options, DEFAULT_SCAN_REBALANCE_DAYS) if value > 0],
            keep_ranks=_parse_csv_ints(args.scan_keep_options, DEFAULT_SCAN_KEEP_RANKS),
            min_history=max(2, int(args.min_history)),
            min_candidates=max(1, int(args.min_candidates)),
            commission_rate=max(0.0, float(args.commission_rate)),
            slippage_rate=max(0.0, float(args.slippage_rate)),
            sell_tax_rate=max(0.0, float(args.sell_tax_rate)),
            max_forward_return_pct=max(0.0, float(args.max_forward_return_pct)),
            enabled_groups=args.groups,
            max_stocks=max(0, int(args.max_stocks)),
        )
        grouped = result.get("grouped", {})

        print("A-share Backtest Scan")
        if scan_windows:
            print(
                "Windows: {windows}".format(
                    windows=", ".join(f"{label}({start}->{end or 'auto'})" for label, start, end in scan_windows)
                )
            )
        else:
            print(f"Windows: {', '.join(result.get('scan_params', {}).get('start_dates', []))}")
        print(f"Result count: {len(result.get('results', []))}")
        for window_label, items in grouped.items():
            if not items:
                continue
            best = items[0]
            summary = best["summary"]
            print(
                "Best {window}: {label} | cum_net={cum:.2f}% | sharpe={sharpe:.2f} | max_dd={dd:.2f}%".format(
                    window=window_label,
                    label="top{top}_hold{hold}_step{step}_keep{keep}".format(
                        top=best["top_n"],
                        hold=best["hold_days"],
                        step=best["rebalance_days"],
                        keep=best["keep_rank"],
                    ),
                    cum=summary.get("cumulative_net_return_pct", 0.0),
                    sharpe=summary.get("sharpe_ratio", 0.0),
                    dd=summary.get("max_drawdown_pct", 0.0),
                )
            )
        print(f"Report saved to: {result.get('report_path')}")
        return 0

    if args.backtest:
        if args.refresh_history:
            ensure_runtime_directories()
            universe = get_stock_universe()
            fetcher = MarketDataFetcher()
            refresh_result = fetcher.refresh_kline_cache_for_universe(
                universe,
                max_stocks=max(0, int(args.refresh_max_stocks)),
            )
            print("Backtest History Refresh")
            print(f"Requested: {refresh_result.get('requested_count', 0)}")
            print(f"Updated: {refresh_result.get('updated_count', 0)}")
            print(f"Failed: {refresh_result.get('failed_count', 0)}")

        result = run_backtest(
            top_n=top_n,
            start_date=args.start_date or None,
            end_date=args.end_date or None,
            hold_days=max(1, int(args.hold_days)),
            rebalance_days=max(1, int(args.rebalance_days)),
            min_history=max(2, int(args.min_history)),
            min_candidates=max(top_n, int(args.min_candidates)),
            max_periods=max(0, int(args.max_periods)),
            max_stocks=max(0, int(args.max_stocks)),
            enabled_groups=args.groups,
            max_forward_return_pct=max(0.0, float(args.max_forward_return_pct)),
            keep_rank=max(0, int(args.keep_rank)),
            commission_rate=max(0.0, float(args.commission_rate)),
            slippage_rate=max(0.0, float(args.slippage_rate)),
            sell_tax_rate=max(0.0, float(args.sell_tax_rate)),
        )
        summary = result.get("summary", {})

        print("A-share Stock Advisor Backtest")
        print(f"Universe size: {summary.get('universe_size', 0)}")
        print(
            "Signal range: {start} -> {end}".format(
                start=summary.get("actual_start") or "N/A",
                end=summary.get("actual_end") or "N/A",
            )
        )
        print(f"Evaluated periods: {summary.get('period_count', 0)}")
        print(f"Skipped outlier samples: {summary.get('skipped_outlier_count', 0)}")
        print(f"Avg retained positions: {summary.get('average_retained', 0)}")
        print(f"Avg replacements: {summary.get('average_replacements', 0)}")
        print(f"Avg turnover: {summary.get('average_turnover_pct', 0):.2f}%")
        print(f"Avg cost drag: {summary.get('average_cost_pct', 0):.2f}%")
        print(f"Strategy avg gross return: {summary.get('average_gross_return_pct', 0):.2f}%")
        print(f"Strategy avg net return: {summary.get('average_net_return_pct', 0):.2f}%")
        print(f"Benchmark avg net return: {summary.get('benchmark_average_net_return_pct', 0):.2f}%")
        print(f"Strategy cumulative net return: {summary.get('cumulative_net_return_pct', 0):.2f}%")
        print(f"Benchmark cumulative net return: {summary.get('benchmark_cumulative_net_return_pct', 0):.2f}%")
        print(f"Win rate: {summary.get('win_rate_pct', 0):.2f}%")
        print(f"Max drawdown: {summary.get('max_drawdown_pct', 0):.2f}%")
        print(f"Report saved to: {result.get('report_path')}")
        return 0

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
