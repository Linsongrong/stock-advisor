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
    DEFAULT_BACKTEST_TOP_N,
    DEFAULT_COMMISSION_RATE,
    DEFAULT_ENABLED_GROUPS,
    DEFAULT_HOLD_DAYS,
    DEFAULT_KEEP_RANK,
    DEFAULT_LOW_EXPOSURE,
    DEFAULT_MAX_PER_SECTOR,
    DEFAULT_MAX_FORWARD_RETURN_PCT,
    DEFAULT_MIN_PRICE_FILTER,
    DEFAULT_MIN_BREADTH_ABOVE_MA20,
    DEFAULT_REBALANCE_DAYS,
    DEFAULT_SELL_TAX_RATE,
    DEFAULT_SLIPPAGE_RATE,
    run_backtest,
)
from config import DEFAULT_TOP_N, MIN_KLINE_DAYS, ensure_runtime_directories
from data_fetcher import MarketDataFetcher
from reporter import generate_report
from screener import screen_market
from stock_universe import get_stock_universe_with_source


def _format_source_usage(source_usage, source_name: str) -> str:
    usage = source_usage.get(source_name, {}) if isinstance(source_usage, dict) else {}
    return (
        "{name}: live={live}, snapshot={snapshot}, cache_fresh={fresh}, cache_stale={stale}, derived={derived}, unavailable={unavailable}".format(
            name=source_name,
            live=int(usage.get("live", 0)),
            snapshot=int(usage.get("universe_snapshot", 0)),
            fresh=int(usage.get("cache_fresh", 0)),
            stale=int(usage.get("cache_stale", 0)),
            derived=int(usage.get("derived_kline", 0)),
            unavailable=int(usage.get("unavailable", 0)),
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A-share stock recommendation system MVP")
    parser.add_argument("--top", type=int, default=None, help="Number of top stocks to output; defaults to 10 for screening and 3 for backtest")
    parser.add_argument("--backtest", action="store_true", help="Run historical backtest from local K-line cache")
    parser.add_argument("--scan-backtests", action="store_true", help="Run a small parameter scan over recent backtest windows")
    parser.add_argument("--scan-regimes", action="store_true", help="Run a regime-based scan over predefined market windows")
    parser.add_argument("--refresh-history", action="store_true", help="Refresh local K-line cache with longest available live history before backtest")
    parser.add_argument("--refresh-max-stocks", type=int, default=0, help="Optional cap on number of stocks to refresh before backtest; 0 means all")
    parser.add_argument("--start-date", type=str, default="", help="Backtest start date in YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default="", help="Backtest end date in YYYY-MM-DD")
    parser.add_argument("--hold-days", type=int, default=None, help="Backtest holding period in trading days; defaults to 10")
    parser.add_argument("--rebalance-days", type=int, default=None, help="Backtest rebalance interval in trading days; defaults to 10")
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
        default=None,
        help="Keep previous holdings if they still rank within this threshold; defaults to 18, 0 disables turnover buffer",
    )
    parser.add_argument(
        "--max-per-sector",
        type=int,
        default=None,
        help="Limit final picks per sector; defaults to 2, 0 disables sector diversification",
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=None,
        help="Filter out stocks below this price; defaults to 5.5, 0 disables the price floor",
    )
    parser.add_argument(
        "--min-breadth-ma20",
        type=float,
        default=None,
        help="Reduce exposure when the share of stocks above MA20 is below this threshold; defaults to 0.55",
    )
    parser.add_argument(
        "--low-exposure",
        type=float,
        default=None,
        help="Portfolio exposure to use in weak regime; defaults to 0.6, 0 means full cash",
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
    top_n = max(1, int(args.top)) if args.top is not None else DEFAULT_TOP_N

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
            max_per_sector=DEFAULT_MAX_PER_SECTOR if args.max_per_sector is None else max(0, int(args.max_per_sector)),
            min_price_filter=DEFAULT_MIN_PRICE_FILTER if args.min_price is None else max(0.0, float(args.min_price)),
            min_breadth_above_ma20=DEFAULT_MIN_BREADTH_ABOVE_MA20 if args.min_breadth_ma20 is None else max(0.0, float(args.min_breadth_ma20)),
            low_exposure=DEFAULT_LOW_EXPOSURE if args.low_exposure is None else min(1.0, max(0.0, float(args.low_exposure))),
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
        overall = result.get("cross_window", [])
        if overall:
            best = overall[0]
            print(
                "Best overall: {label} | avg_ann={avg_ann:.2f}% | worst_ann={worst_ann:.2f}% | worst_dd={worst_dd:.2f}%".format(
                    label=best.get("label", "N/A"),
                    avg_ann=float(best.get("average_annualized_net_return_pct", 0.0)),
                    worst_ann=float(best.get("worst_annualized_net_return_pct", 0.0)),
                    worst_dd=float(best.get("worst_max_drawdown_pct", 0.0)),
                )
            )
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
        top_n = max(1, int(args.top)) if args.top is not None else DEFAULT_BACKTEST_TOP_N
        hold_days = max(1, int(args.hold_days)) if args.hold_days is not None else DEFAULT_HOLD_DAYS
        rebalance_days = max(1, int(args.rebalance_days)) if args.rebalance_days is not None else DEFAULT_REBALANCE_DAYS
        keep_rank = max(0, int(args.keep_rank)) if args.keep_rank is not None else DEFAULT_KEEP_RANK
        max_per_sector = max(0, int(args.max_per_sector)) if args.max_per_sector is not None else DEFAULT_MAX_PER_SECTOR
        min_price_filter = max(0.0, float(args.min_price)) if args.min_price is not None else DEFAULT_MIN_PRICE_FILTER
        min_breadth_above_ma20 = max(0.0, float(args.min_breadth_ma20)) if args.min_breadth_ma20 is not None else DEFAULT_MIN_BREADTH_ABOVE_MA20
        low_exposure = min(1.0, max(0.0, float(args.low_exposure))) if args.low_exposure is not None else DEFAULT_LOW_EXPOSURE
        if args.refresh_history:
            ensure_runtime_directories()
            universe, _ = get_stock_universe_with_source()
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
            hold_days=hold_days,
            rebalance_days=rebalance_days,
            min_history=max(2, int(args.min_history)),
            min_candidates=max(top_n, int(args.min_candidates)),
            max_periods=max(0, int(args.max_periods)),
            max_stocks=max(0, int(args.max_stocks)),
            enabled_groups=args.groups,
            max_forward_return_pct=max(0.0, float(args.max_forward_return_pct)),
            keep_rank=keep_rank,
            max_per_sector=max_per_sector,
            min_price_filter=min_price_filter,
            min_breadth_above_ma20=min_breadth_above_ma20,
            low_exposure=low_exposure,
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
        print(f"Reduced-exposure periods: {summary.get('risk_off_period_count', 0)}")
        print(f"Avg exposure: {summary.get('average_exposure_pct', 0):.2f}%")
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
    universe, universe_source = get_stock_universe_with_source()
    fetcher = MarketDataFetcher()
    market_data = fetcher.fetch_universe_data(universe)
    market_data["universe_source"] = universe_source
    screened_data = screen_market(
        market_data,
        top_n=top_n,
        max_per_sector=DEFAULT_MAX_PER_SECTOR if args.max_per_sector is None else max(0, int(args.max_per_sector)),
        min_price_filter=DEFAULT_MIN_PRICE_FILTER if args.min_price is None else max(0.0, float(args.min_price)),
        min_breadth_above_ma20=DEFAULT_MIN_BREADTH_ABOVE_MA20 if args.min_breadth_ma20 is None else max(0.0, float(args.min_breadth_ma20)),
        low_exposure=DEFAULT_LOW_EXPOSURE if args.low_exposure is None else min(1.0, max(0.0, float(args.low_exposure))),
    )
    report_path = generate_report(screened_data, top_n=top_n)
    prefilter = market_data.get("prefilter", {})
    fundamental_pool = screened_data.get("fundamental_pool", {})

    print("A-share Stock Advisor MVP")
    print(f"Universe size: {len(universe)}")
    print(f"Universe source: {universe_source}")
    print(
        "Quote prefilter: {status} ({selected}/{input_count}, seed_quotes={seed_quotes})".format(
            status="on" if prefilter.get("enabled") else "off",
            selected=int(prefilter.get("selected_count", len(universe)) or 0),
            input_count=int(prefilter.get("input_count", len(universe)) or 0),
            seed_quotes=int(prefilter.get("seed_quote_count", 0) or 0),
        )
    )
    print(
        "Fundamental pool: {selected}/{input_count}".format(
            selected=int(fundamental_pool.get("selected_count", screened_data.get("fetched_count", 0)) or 0),
            input_count=int(fundamental_pool.get("input_count", screened_data.get("fetched_count", 0)) or 0),
        )
    )
    print(
        "Source status: quote={quote}, kline={kline}".format(
            quote="up" if market_data.get("source_status", {}).get("quote") else "down",
            kline="up" if market_data.get("source_status", {}).get("kline") else "down",
        )
    )
    print(f"Source usage: {_format_source_usage(market_data.get('source_usage', {}), 'quote')}")
    print(f"Source usage: {_format_source_usage(market_data.get('source_usage', {}), 'kline')}")
    print(f"Fetched successfully: {screened_data['fetched_count']}")
    print(f"Qualified candidates: {screened_data['qualified_count']}")
    print(f"Filtered out by rules: {screened_data['filtered_count']}")
    print(
        "Market breadth above MA20: {value:.2f}%".format(
            value=float(screened_data.get("market_breadth", {}).get("above_ma20_ratio", 0.0)) * 100.0
        )
    )
    print(f"Suggested exposure: {float(screened_data.get('suggested_exposure', 1.0)) * 100.0:.0f}%")
    if screened_data.get("risk_off"):
        print(f"Risk regime: {screened_data.get('risk_regime')} ({screened_data.get('risk_off_reason')})")
    print(f"Failed fetch/scoring: {screened_data['failed_count']}")
    print(f"Report saved to: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
