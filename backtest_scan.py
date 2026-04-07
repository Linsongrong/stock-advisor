# -*- coding: utf-8 -*-
"""Parameter scan utilities for historical backtests."""

from itertools import product
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backtester import (
    BACKTEST_REPORTS_DIR,
    _annualized_return,
    _annualized_volatility,
    _max_drawdown,
    _sharpe_ratio,
    _summarize_symbols,
    _summarize_years,
    load_backtest_histories,
    run_backtest_on_histories,
)


SWEEP_REPORTS_DIR = BACKTEST_REPORTS_DIR / "sweeps"
DEFAULT_SCAN_START_DATES = ("2016-01-01", "2020-01-01")
DEFAULT_SCAN_TOP_NS = (5,)
DEFAULT_SCAN_HOLD_DAYS = (5, 10, 20)
DEFAULT_SCAN_REBALANCE_DAYS = (5, 10, 20)
DEFAULT_SCAN_KEEP_RANKS = (0,)
DEFAULT_SCAN_WINDOWS: Tuple[Tuple[str, str, str], ...] = (
    ("2016-2019", "2016-01-01", "2019-12-31"),
    ("2020-2022", "2020-01-01", "2022-12-31"),
    ("2023-now", "2023-01-01", ""),
)


def _parse_csv_ints(value: str, default: Sequence[int]) -> List[int]:
    if not value.strip():
        return list(default)
    result = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        result.append(max(0, int(part)))
    return result or list(default)


def _parse_csv_dates(value: str, default: Sequence[str]) -> List[str]:
    if not value.strip():
        return list(default)
    result = [part.strip() for part in value.split(",") if part.strip()]
    return result or list(default)


def _config_label(scan: Dict[str, Any]) -> str:
    return "top{top}_hold{hold}_step{step}_keep{keep}".format(
        top=scan["top_n"],
        hold=scan["hold_days"],
        step=scan["rebalance_days"],
        keep=scan["keep_rank"],
    )


def _rank_key(scan: Dict[str, Any]):
    summary = scan["summary"]
    return (
        float(summary.get("annualized_net_return_pct", 0.0)),
        float(summary.get("sharpe_ratio", 0.0)),
        -float(summary.get("max_drawdown_pct", 0.0)),
        float(summary.get("cumulative_net_return_pct", 0.0)),
    )


def _filter_periods(periods: Sequence[Dict[str, Any]], start_date: str, end_date: str) -> List[Dict[str, Any]]:
    filtered = [period for period in periods if period.get("signal_date", "") >= start_date]
    if end_date:
        filtered = [period for period in filtered if period.get("signal_date", "") <= end_date]
    return filtered


def _summarize_period_subset(
    periods: Sequence[Dict[str, Any]],
    universe_size: int,
    rebalance_days: int,
) -> Dict[str, Any]:
    period_list = list(periods)
    net_period_returns = [float(period.get("portfolio_net_return", 0.0)) for period in period_list]
    gross_period_returns = [float(period.get("portfolio_gross_return", 0.0)) for period in period_list]
    benchmark_gross_returns = [float(period.get("benchmark_gross_return", 0.0)) for period in period_list]
    benchmark_net_returns = [float(period.get("benchmark_net_return", 0.0)) for period in period_list]

    cumulative_gross_return = 1.0
    cumulative_net_return = 1.0
    cumulative_benchmark_gross = 1.0
    cumulative_benchmark_net = 1.0
    for value in gross_period_returns:
        cumulative_gross_return *= 1.0 + value
    for value in net_period_returns:
        cumulative_net_return *= 1.0 + value
    for value in benchmark_gross_returns:
        cumulative_benchmark_gross *= 1.0 + value
    for value in benchmark_net_returns:
        cumulative_benchmark_net *= 1.0 + value

    cumulative_gross_return -= 1.0
    cumulative_net_return -= 1.0
    cumulative_benchmark_gross -= 1.0
    cumulative_benchmark_net -= 1.0

    return {
        "universe_size": universe_size,
        "period_count": len(period_list),
        "actual_start": period_list[0]["signal_date"] if period_list else "",
        "actual_end": period_list[-1]["signal_date"] if period_list else "",
        "skipped_outlier_count": 0,
        "average_candidates": round(mean(period["candidate_count"] for period in period_list), 2) if period_list else 0.0,
        "average_selected": round(mean(period["selected_count"] for period in period_list), 2) if period_list else 0.0,
        "average_retained": round(mean(period["retained_count"] for period in period_list), 2) if period_list else 0.0,
        "average_replacements": round(mean(period["replacement_count"] for period in period_list), 2) if period_list else 0.0,
        "average_turnover_pct": round(mean(period["turnover"] for period in period_list) * 100.0, 2) if period_list else 0.0,
        "average_cost_pct": round(mean(period["cost_rate"] for period in period_list) * 100.0, 2) if period_list else 0.0,
        "total_cost_pct": round(sum(period["cost_rate"] for period in period_list) * 100.0, 2) if period_list else 0.0,
        "average_gross_return_pct": round(mean(gross_period_returns) * 100.0, 2) if gross_period_returns else 0.0,
        "average_return_pct": round(mean(net_period_returns) * 100.0, 2) if net_period_returns else 0.0,
        "average_net_return_pct": round(mean(net_period_returns) * 100.0, 2) if net_period_returns else 0.0,
        "benchmark_average_gross_return_pct": round(mean(benchmark_gross_returns) * 100.0, 2) if benchmark_gross_returns else 0.0,
        "benchmark_average_return_pct": round(mean(benchmark_net_returns) * 100.0, 2) if benchmark_net_returns else 0.0,
        "benchmark_average_net_return_pct": round(mean(benchmark_net_returns) * 100.0, 2) if benchmark_net_returns else 0.0,
        "cumulative_gross_return_pct": round(cumulative_gross_return * 100.0, 2),
        "cumulative_return_pct": round(cumulative_net_return * 100.0, 2),
        "cumulative_net_return_pct": round(cumulative_net_return * 100.0, 2),
        "benchmark_cumulative_gross_return_pct": round(cumulative_benchmark_gross * 100.0, 2),
        "benchmark_cumulative_return_pct": round(cumulative_benchmark_net * 100.0, 2),
        "benchmark_cumulative_net_return_pct": round(cumulative_benchmark_net * 100.0, 2),
        "annualized_gross_return_pct": round(_annualized_return(cumulative_gross_return, len(period_list), rebalance_days) * 100.0, 2),
        "annualized_return_pct": round(_annualized_return(cumulative_net_return, len(period_list), rebalance_days) * 100.0, 2),
        "annualized_net_return_pct": round(_annualized_return(cumulative_net_return, len(period_list), rebalance_days) * 100.0, 2),
        "benchmark_annualized_gross_return_pct": round(_annualized_return(cumulative_benchmark_gross, len(period_list), rebalance_days) * 100.0, 2),
        "benchmark_annualized_return_pct": round(_annualized_return(cumulative_benchmark_net, len(period_list), rebalance_days) * 100.0, 2),
        "benchmark_annualized_net_return_pct": round(_annualized_return(cumulative_benchmark_net, len(period_list), rebalance_days) * 100.0, 2),
        "volatility_pct": round(_annualized_volatility(net_period_returns, rebalance_days) * 100.0, 2),
        "sharpe_ratio": round(_sharpe_ratio(net_period_returns, rebalance_days), 2),
        "win_rate_pct": round(sum(ret > 0 for ret in net_period_returns) / len(net_period_returns) * 100.0, 2)
        if net_period_returns
        else 0.0,
        "excess_win_rate_pct": round(
            sum(portfolio > benchmark for portfolio, benchmark in zip(net_period_returns, benchmark_net_returns))
            / len(net_period_returns)
            * 100.0,
            2,
        )
        if net_period_returns
        else 0.0,
        "max_drawdown_pct": round(_max_drawdown(net_period_returns) * 100.0, 2),
        "benchmark_max_drawdown_pct": round(_max_drawdown(benchmark_net_returns) * 100.0, 2),
    }


def scan_backtests_on_histories(
    histories: Dict[str, Dict[str, Any]],
    start_dates: Sequence[str] = DEFAULT_SCAN_START_DATES,
    end_date: Optional[str] = None,
    windows: Optional[Sequence[Tuple[str, str, str]]] = None,
    top_ns: Sequence[int] = DEFAULT_SCAN_TOP_NS,
    hold_days_list: Sequence[int] = DEFAULT_SCAN_HOLD_DAYS,
    rebalance_days_list: Sequence[int] = DEFAULT_SCAN_REBALANCE_DAYS,
    keep_ranks: Sequence[int] = DEFAULT_SCAN_KEEP_RANKS,
    min_history: int = 60,
    min_candidates: int = 20,
    commission_rate: float = 0.0003,
    slippage_rate: float = 0.0005,
    sell_tax_rate: float = 0.0005,
    max_forward_return_pct: float = 50.0,
    enabled_groups: Sequence[str] = ("capital", "technical"),
) -> Dict[str, Any]:
    """Run a limited grid scan over backtest parameters."""
    results: List[Dict[str, Any]] = []
    scan_windows = list(windows or [])
    if not scan_windows:
        scan_windows = [(start_date, start_date, end_date or "") for start_date in start_dates]

    full_scan_start = min(window[1] for window in scan_windows)
    full_scan_end = "" if any(not window[2] for window in scan_windows) else max(window[2] for window in scan_windows)
    universe_size = len(histories)

    for top_n, hold_days, rebalance_days, keep_rank in product(
        top_ns,
        hold_days_list,
        rebalance_days_list,
        keep_ranks,
    ):
        if rebalance_days <= 0 or hold_days <= 0 or top_n <= 0:
            continue

        full_result = run_backtest_on_histories(
            histories,
            top_n=top_n,
            start_date=full_scan_start,
            end_date=full_scan_end or None,
            hold_days=hold_days,
            rebalance_days=rebalance_days,
            min_history=min_history,
            min_candidates=max(top_n, min_candidates),
            enabled_groups=enabled_groups,
            commission_rate=commission_rate,
            slippage_rate=slippage_rate,
            sell_tax_rate=sell_tax_rate,
            keep_rank=keep_rank,
            max_forward_return_pct=max_forward_return_pct,
        )
        for window_label, window_start, window_end in scan_windows:
            filtered_periods = _filter_periods(full_result["periods"], window_start, window_end)
            summary = _summarize_period_subset(filtered_periods, universe_size, rebalance_days)
            results.append(
                {
                    "window_label": window_label,
                    "window_start": window_start,
                    "window_end": window_end or summary.get("actual_end") or "",
                    "top_n": top_n,
                    "hold_days": hold_days,
                    "rebalance_days": rebalance_days,
                    "keep_rank": keep_rank,
                    "summary": summary,
                    "params": full_result["params"],
                    "yearly_summary": _summarize_years(filtered_periods),
                    "top_symbols": _summarize_symbols(filtered_periods)[:10],
                }
            )

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in results:
        grouped.setdefault(item["window_label"], []).append(item)

    for items in grouped.values():
        items.sort(key=_rank_key, reverse=True)

    return {
        "results": results,
        "grouped": grouped,
        "scan_params": {
            "windows": scan_windows,
            "start_dates": list(start_dates),
            "end_date": end_date or "",
            "top_ns": list(top_ns),
            "hold_days_list": list(hold_days_list),
            "rebalance_days_list": list(rebalance_days_list),
            "keep_ranks": list(keep_ranks),
            "enabled_groups": list(enabled_groups),
            "min_history": min_history,
            "min_candidates": min_candidates,
            "commission_rate": commission_rate,
            "slippage_rate": slippage_rate,
            "sell_tax_rate": sell_tax_rate,
            "max_forward_return_pct": max_forward_return_pct,
        },
    }


def _scan_filename(scan_params: Dict[str, Any]) -> str:
    windows = scan_params.get("windows", [])
    starts = "-".join(window[0] for window in windows) if windows else "-".join(date[:4] for date in scan_params.get("start_dates", []))
    starts = starts or "na"
    groups = "-".join(scan_params.get("enabled_groups", [])) or "none"
    tops = "-".join(str(item) for item in scan_params.get("top_ns", [])) or "na"
    holds = "-".join(str(item) for item in scan_params.get("hold_days_list", [])) or "na"
    steps = "-".join(str(item) for item in scan_params.get("rebalance_days_list", [])) or "na"
    keeps = "-".join(str(item) for item in scan_params.get("keep_ranks", [])) or "na"
    return f"scan_{starts}_{groups}_top{tops}_hold{holds}_step{steps}_keep{keeps}.md"


def write_backtest_scan_report(scan_result: Dict[str, Any]) -> Path:
    SWEEP_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    scan_params = scan_result["scan_params"]
    grouped = scan_result["grouped"]
    report_path = SWEEP_REPORTS_DIR / _scan_filename(scan_params)

    lines = [
        "# Backtest Parameter Scan",
        "",
        "## Scan Parameters",
        f"- Windows: {scan_params.get('windows')}",
        f"- Start dates: {', '.join(scan_params.get('start_dates', []))}",
        f"- End date: {scan_params.get('end_date') or 'auto'}",
        f"- Top N options: {scan_params.get('top_ns')}",
        f"- Hold day options: {scan_params.get('hold_days_list')}",
        f"- Rebalance day options: {scan_params.get('rebalance_days_list')}",
        f"- Keep rank options: {scan_params.get('keep_ranks')}",
        f"- Enabled groups: {', '.join(scan_params.get('enabled_groups', []))}",
        f"- Max forward return filter: +/-{float(scan_params.get('max_forward_return_pct', 0.0)):.2f}%",
        "",
    ]

    for window_label, items in grouped.items():
        window_desc = "{start} -> {end}".format(
            start=items[0]["window_start"] if items else "",
            end=items[0]["window_end"] if items else "",
        )
        lines.extend(
            [
                f"## Window {window_label} ({window_desc})",
                "",
                "| Rank | Config | Periods | Avg Turnover | Avg Cost | Avg Net | Cum Net | Bench Cum Net | Sharpe | Max DD |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for index, item in enumerate(items, start=1):
            summary = item["summary"]
            lines.append(
                "| {rank} | {label} | {periods} | {turnover:.2f}% | {cost:.2f}% | {avg_net:.2f}% | {cum_net:.2f}% | {bench_cum:.2f}% | {sharpe:.2f} | {dd:.2f}% |".format(
                    rank=index,
                    label=_config_label(item),
                    periods=summary.get("period_count", 0),
                    turnover=summary.get("average_turnover_pct", 0.0),
                    cost=summary.get("average_cost_pct", 0.0),
                    avg_net=summary.get("average_net_return_pct", 0.0),
                    cum_net=summary.get("cumulative_net_return_pct", 0.0),
                    bench_cum=summary.get("benchmark_cumulative_net_return_pct", 0.0),
                    sharpe=summary.get("sharpe_ratio", 0.0),
                    dd=summary.get("max_drawdown_pct", 0.0),
                )
            )
        lines.append("")

    report_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return report_path


def run_backtest_scan(
    start_dates: Sequence[str] = DEFAULT_SCAN_START_DATES,
    end_date: Optional[str] = None,
    windows: Optional[Sequence[Tuple[str, str, str]]] = None,
    top_ns: Sequence[int] = DEFAULT_SCAN_TOP_NS,
    hold_days_list: Sequence[int] = DEFAULT_SCAN_HOLD_DAYS,
    rebalance_days_list: Sequence[int] = DEFAULT_SCAN_REBALANCE_DAYS,
    keep_ranks: Sequence[int] = DEFAULT_SCAN_KEEP_RANKS,
    min_history: int = 60,
    min_candidates: int = 20,
    commission_rate: float = 0.0003,
    slippage_rate: float = 0.0005,
    sell_tax_rate: float = 0.0005,
    max_forward_return_pct: float = 50.0,
    enabled_groups: Sequence[str] = ("capital", "technical"),
    max_stocks: int = 0,
) -> Dict[str, Any]:
    histories = load_backtest_histories(max_stocks=max_stocks)
    result = scan_backtests_on_histories(
        histories,
        start_dates=start_dates,
        end_date=end_date,
        windows=windows,
        top_ns=top_ns,
        hold_days_list=hold_days_list,
        rebalance_days_list=rebalance_days_list,
        keep_ranks=keep_ranks,
        min_history=min_history,
        min_candidates=min_candidates,
        commission_rate=commission_rate,
        slippage_rate=slippage_rate,
        sell_tax_rate=sell_tax_rate,
        max_forward_return_pct=max_forward_return_pct,
        enabled_groups=enabled_groups,
    )
    result["report_path"] = write_backtest_scan_report(result)
    return result
