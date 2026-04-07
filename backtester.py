# -*- coding: utf-8 -*-
"""Historical backtesting using local K-line cache data."""

import json
import math
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence

from config import (
    KLINE_CACHE_DIR,
    MIN_KLINE_DAYS,
    QUOTE_CACHE_DIR,
    REPORTS_DIR,
    ensure_runtime_directories,
)
from scorer import score_stock
from stock_universe import _get_hardcoded_universe, _load_universe_cache


BACKTEST_REPORTS_DIR = REPORTS_DIR / "backtests"
DEFAULT_ENABLED_GROUPS = ("capital", "technical")
ALL_GROUPS = ("capital", "technical", "fundamental", "sentiment")
DEFAULT_COMMISSION_RATE = 0.0003
DEFAULT_SLIPPAGE_RATE = 0.0005
DEFAULT_SELL_TAX_RATE = 0.0005
DEFAULT_KEEP_RANK = 0
DEFAULT_MAX_FORWARD_RETURN_PCT = 50.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None, "--"):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _load_stock_metadata() -> Dict[str, Dict[str, str]]:
    metadata: Dict[str, Dict[str, str]] = {}

    universe = _load_universe_cache() or _get_hardcoded_universe()
    for item in universe:
        code = str(item.get("code", ""))
        if not code:
            continue
        metadata[code] = {
            "name": str(item.get("name", "") or code),
            "sector": str(item.get("sector", "") or ""),
        }

    for path in QUOTE_CACHE_DIR.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        data = payload.get("data", {})
        if not isinstance(data, dict):
            continue
        code = str(data.get("code", "") or path.stem)
        if not code:
            continue
        name = str(data.get("name", "") or metadata.get(code, {}).get("name", code))
        sector = metadata.get(code, {}).get("sector", "")
        metadata[code] = {"name": name, "sector": sector}

    return metadata


def load_backtest_histories(max_stocks: int = 0) -> Dict[str, Dict[str, Any]]:
    """Load local K-line cache into a backtest-friendly structure."""
    metadata = _load_stock_metadata()
    histories: Dict[str, Dict[str, Any]] = {}

    for path in sorted(KLINE_CACHE_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        rows = payload.get("data", [])
        if not isinstance(rows, list) or len(rows) < 2:
            continue

        code = path.stem
        meta = metadata.get(code, {"name": code, "sector": ""})
        histories[code] = {
            "code": code,
            "name": meta.get("name", code) or code,
            "sector": meta.get("sector", "") or "",
            "rows": rows,
            "date_index": {str(row.get("date", "")): index for index, row in enumerate(rows)},
        }
        if max_stocks and len(histories) >= max_stocks:
            break

    return histories


def _build_historical_quote(code: str, name: str, rows: List[Dict[str, Any]], index: int) -> Optional[Dict[str, Any]]:
    if index < 1 or index >= len(rows):
        return None

    current_row = rows[index]
    previous_row = rows[index - 1]

    price = _safe_float(current_row.get("close"))
    prev_close = _safe_float(previous_row.get("close"))
    open_price = _safe_float(current_row.get("open"))
    volume = int(_safe_float(current_row.get("volume")))

    if price <= 0 or prev_close <= 0 or open_price <= 0:
        return None

    change_amount = price - prev_close
    change_pct = (price / prev_close - 1.0) * 100.0
    return {
        "code": code,
        "name": name,
        "price": round(price, 4),
        "prev_close": round(prev_close, 4),
        "open": round(open_price, 4),
        "volume": volume,
        "change_amount": round(change_amount, 4),
        "change_pct": round(change_pct, 2),
        "updated_at": str(current_row.get("date", "")),
        "source": "historical_kline",
    }


def _build_historical_bundle(stock: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
    rows = stock.get("rows", [])
    code = stock.get("code", "")
    name = stock.get("name", code)
    quote = _build_historical_quote(code, name, rows, index)
    if quote is None:
        return None

    return {
        "code": code,
        "name": name,
        "sector": stock.get("sector", ""),
        "quote": quote,
        "kline": rows[: index + 1],
    }


def _compute_forward_return(
    rows: List[Dict[str, Any]],
    signal_index: int,
    hold_days: int,
    max_forward_return_pct: float = DEFAULT_MAX_FORWARD_RETURN_PCT,
) -> Optional[Dict[str, Any]]:
    entry_index = signal_index + 1
    exit_index = signal_index + hold_days
    if entry_index >= len(rows) or exit_index >= len(rows):
        return None

    entry_price = _safe_float(rows[entry_index].get("open"))
    exit_price = _safe_float(rows[exit_index].get("close"))
    if entry_price <= 0 or exit_price <= 0:
        return None

    return_pct = (exit_price / entry_price - 1.0) * 100.0
    if max_forward_return_pct > 0 and abs(return_pct) > max_forward_return_pct:
        return None

    return {
        "entry_date": str(rows[entry_index].get("date", "")),
        "exit_date": str(rows[exit_index].get("date", "")),
        "entry_price": round(entry_price, 4),
        "exit_price": round(exit_price, 4),
        "return_pct": round(return_pct, 2),
        "return_decimal": exit_price / entry_price - 1.0,
    }


def _normalize_enabled_groups(enabled_groups: Sequence[str]) -> List[str]:
    groups = []
    for group in enabled_groups:
        name = str(group).strip().lower()
        if name and name in ALL_GROUPS and name not in groups:
            groups.append(name)
    if not groups:
        raise ValueError("enabled_groups must include at least one valid group")
    return groups


def _combine_group_scores(score_result: Dict[str, Any], enabled_groups: Sequence[str]) -> float:
    groups = _normalize_enabled_groups(enabled_groups)
    weight_totals = score_result.get("weight_totals", {})
    active_weight = sum(float(weight_totals.get(group, 0.0)) for group in groups)
    if active_weight <= 0:
        return 0.0

    weighted_score = sum(
        float(score_result.get(f"{group}_score", 0.0)) * float(weight_totals.get(group, 0.0))
        for group in groups
    )
    return round(weighted_score / active_weight, 2)


def _equal_weight_map(codes: Sequence[str]) -> Dict[str, float]:
    unique_codes = [code for code in dict.fromkeys(codes) if code]
    if not unique_codes:
        return {}
    weight = 1.0 / len(unique_codes)
    return {code: weight for code in unique_codes}


def _compute_turnover(previous_codes: Sequence[str], current_codes: Sequence[str]) -> Dict[str, float]:
    previous_weights = _equal_weight_map(previous_codes)
    current_weights = _equal_weight_map(current_codes)
    symbols = set(previous_weights) | set(current_weights)

    buy_turnover = sum(
        max(current_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0), 0.0)
        for symbol in symbols
    )
    sell_turnover = sum(
        max(previous_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0), 0.0)
        for symbol in symbols
    )
    turnover = max(buy_turnover, sell_turnover)

    return {
        "buy_turnover": round(buy_turnover, 6),
        "sell_turnover": round(sell_turnover, 6),
        "turnover": round(turnover, 6),
    }


def _apply_transaction_costs(
    gross_return: float,
    turnover_stats: Dict[str, float],
    commission_rate: float,
    slippage_rate: float,
    sell_tax_rate: float,
) -> Dict[str, float]:
    buy_cost_rate = max(0.0, commission_rate) + max(0.0, slippage_rate)
    sell_cost_rate = max(0.0, commission_rate) + max(0.0, slippage_rate) + max(0.0, sell_tax_rate)
    total_cost_rate = turnover_stats["buy_turnover"] * buy_cost_rate + turnover_stats["sell_turnover"] * sell_cost_rate
    net_return = (1.0 + gross_return) * (1.0 - total_cost_rate) - 1.0

    return {
        "gross_return": round(gross_return, 6),
        "net_return": round(net_return, 6),
        "cost_rate": round(total_cost_rate, 6),
    }


def _select_with_keep_rank(
    ranked: Sequence[Dict[str, Any]],
    top_n: int,
    previous_selected_codes: Sequence[str],
    keep_rank: int,
) -> List[Dict[str, Any]]:
    if top_n <= 0:
        return []
    if keep_rank <= 0 or not previous_selected_codes:
        return list(ranked[:top_n])

    keep_rank = max(int(keep_rank), top_n)
    rank_map = {item["code"]: index for index, item in enumerate(ranked, start=1)}
    item_map = {item["code"]: item for item in ranked}

    kept_items: List[Dict[str, Any]] = []
    kept_codes = set()
    for code in previous_selected_codes:
        item = item_map.get(code)
        rank = rank_map.get(code)
        if item is None or rank is None or rank > keep_rank or code in kept_codes:
            continue
        kept_items.append(item)
        kept_codes.add(code)

    kept_items.sort(key=lambda item: (item["score"], item["capital_score"], item["technical_score"]), reverse=True)
    selected = kept_items[:top_n]
    selected_codes = {item["code"] for item in selected}

    for item in ranked:
        if len(selected) >= top_n:
            break
        code = item["code"]
        if code in selected_codes:
            continue
        selected.append(item)
        selected_codes.add(code)

    return selected


def _equity_curve(period_returns: List[float]) -> List[float]:
    equity = 1.0
    curve = []
    for period_return in period_returns:
        equity *= 1.0 + period_return
        curve.append(equity)
    return curve


def _max_drawdown(period_returns: List[float]) -> float:
    peak = 1.0
    max_drawdown = 0.0
    for equity in _equity_curve(period_returns):
        peak = max(peak, equity)
        if peak <= 0:
            continue
        drawdown = equity / peak - 1.0
        max_drawdown = min(max_drawdown, drawdown)
    return abs(max_drawdown)


def _annualized_return(cumulative_return: float, period_count: int, rebalance_days: int) -> float:
    if period_count <= 0 or cumulative_return <= -1.0:
        return 0.0
    periods_per_year = 252.0 / max(1, rebalance_days)
    return (1.0 + cumulative_return) ** (periods_per_year / period_count) - 1.0


def _annualized_volatility(period_returns: List[float], rebalance_days: int) -> float:
    if len(period_returns) <= 1:
        return 0.0
    periods_per_year = 252.0 / max(1, rebalance_days)
    return pstdev(period_returns) * math.sqrt(periods_per_year)


def _sharpe_ratio(period_returns: List[float], rebalance_days: int) -> float:
    if len(period_returns) <= 1:
        return 0.0
    volatility = _annualized_volatility(period_returns, rebalance_days)
    if volatility <= 0:
        return 0.0
    periods_per_year = 252.0 / max(1, rebalance_days)
    return mean(period_returns) * periods_per_year / volatility


def _rate_to_bps(rate: float) -> int:
    return int(round(max(0.0, rate) * 10000))


def _summarize_symbols(periods: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    symbol_stats: Dict[str, Dict[str, Any]] = {}

    for period in periods:
        for pick in period.get("picks", []):
            code = pick["code"]
            stats = symbol_stats.setdefault(
                code,
                {
                    "code": code,
                    "name": pick.get("name", code),
                    "count": 0,
                    "score_sum": 0.0,
                    "return_sum": 0.0,
                },
            )
            stats["count"] += 1
            stats["score_sum"] += float(pick.get("score", 0.0))
            stats["return_sum"] += float(pick.get("return_pct", 0.0))

    summary = []
    for stats in symbol_stats.values():
        count = stats["count"] or 1
        summary.append(
            {
                "code": stats["code"],
                "name": stats["name"],
                "selection_count": stats["count"],
                "avg_score": round(stats["score_sum"] / count, 2),
                "avg_return_pct": round(stats["return_sum"] / count, 2),
            }
        )

    summary.sort(
        key=lambda item: (item["selection_count"], item["avg_return_pct"], item["avg_score"]),
        reverse=True,
    )
    return summary


def _summarize_years(periods: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    year_groups: Dict[str, List[Dict[str, Any]]] = {}
    for period in periods:
        year = str(period.get("signal_date", ""))[:4]
        if len(year) != 4:
            continue
        year_groups.setdefault(year, []).append(period)

    summary = []
    for year, items in sorted(year_groups.items()):
        strategy_returns = [float(item.get("portfolio_net_return", 0.0)) for item in items]
        benchmark_returns = [float(item.get("benchmark_net_return", 0.0)) for item in items]
        strategy_cumulative = _equity_curve(strategy_returns)[-1] - 1.0 if strategy_returns else 0.0
        benchmark_cumulative = _equity_curve(benchmark_returns)[-1] - 1.0 if benchmark_returns else 0.0

        summary.append(
            {
                "year": year,
                "period_count": len(items),
                "avg_turnover_pct": round(mean(item.get("turnover", 0.0) for item in items) * 100.0, 2),
                "avg_cost_pct": round(mean(item.get("cost_rate", 0.0) for item in items) * 100.0, 2),
                "avg_net_return_pct": round(mean(strategy_returns) * 100.0, 2) if strategy_returns else 0.0,
                "benchmark_avg_net_return_pct": round(mean(benchmark_returns) * 100.0, 2) if benchmark_returns else 0.0,
                "cumulative_net_return_pct": round(strategy_cumulative * 100.0, 2),
                "benchmark_cumulative_net_return_pct": round(benchmark_cumulative * 100.0, 2),
                "win_rate_pct": round(sum(ret > 0 for ret in strategy_returns) / len(strategy_returns) * 100.0, 2)
                if strategy_returns
                else 0.0,
            }
        )

    return summary


def run_backtest_on_histories(
    histories: Dict[str, Dict[str, Any]],
    top_n: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    hold_days: int = 5,
    rebalance_days: int = 5,
    min_history: int = MIN_KLINE_DAYS,
    min_candidates: int = 20,
    max_periods: int = 0,
    enabled_groups: Sequence[str] = DEFAULT_ENABLED_GROUPS,
    commission_rate: float = DEFAULT_COMMISSION_RATE,
    slippage_rate: float = DEFAULT_SLIPPAGE_RATE,
    sell_tax_rate: float = DEFAULT_SELL_TAX_RATE,
    keep_rank: int = DEFAULT_KEEP_RANK,
    max_forward_return_pct: float = DEFAULT_MAX_FORWARD_RETURN_PCT,
) -> Dict[str, Any]:
    """Run a close-to-close historical backtest over in-memory histories."""
    groups = _normalize_enabled_groups(enabled_groups)
    top_n = max(1, int(top_n))
    hold_days = max(1, int(hold_days))
    rebalance_days = max(1, int(rebalance_days))
    min_history = max(2, int(min_history))
    min_candidates = max(top_n, int(min_candidates))
    max_periods = max(0, int(max_periods))
    keep_rank = max(0, int(keep_rank))
    max_forward_return_pct = max(0.0, float(max_forward_return_pct))

    if not histories:
        raise ValueError("no historical data available for backtest")

    trading_dates = sorted(
        {
            str(row.get("date", ""))
            for stock in histories.values()
            for row in stock.get("rows", [])
            if row.get("date")
        }
    )
    if start_date:
        trading_dates = [date for date in trading_dates if date >= start_date]
    if end_date:
        trading_dates = [date for date in trading_dates if date <= end_date]

    rebalance_dates = trading_dates[::rebalance_days]
    periods: List[Dict[str, Any]] = []
    previous_selected_codes: List[str] = []
    previous_benchmark_codes: List[str] = []
    skipped_outlier_count = 0

    for rebalance_date in rebalance_dates:
        ranked: List[Dict[str, Any]] = []

        for stock in histories.values():
            date_index = stock.get("date_index", {})
            signal_index = date_index.get(rebalance_date)
            if signal_index is None:
                continue
            if signal_index < min_history - 1:
                continue
            if signal_index + hold_days >= len(stock.get("rows", [])):
                continue

            bundle = _build_historical_bundle(stock, signal_index)
            forward_info = _compute_forward_return(
                stock["rows"],
                signal_index,
                hold_days,
                max_forward_return_pct=max_forward_return_pct,
            )
            if bundle is None:
                continue
            if forward_info is None:
                skipped_outlier_count += 1
                continue

            score_result = score_stock(bundle, fund_data=None, sentiment_data=None)
            if score_result.get("error") or score_result.get("filtered_out"):
                continue

            ranked.append(
                {
                    "code": score_result["code"],
                    "name": score_result["name"],
                    "sector": score_result.get("sector", ""),
                    "score": _combine_group_scores(score_result, groups),
                    "capital_score": float(score_result.get("capital_score", 0.0)),
                    "technical_score": float(score_result.get("technical_score", 0.0)),
                    "entry_date": forward_info["entry_date"],
                    "exit_date": forward_info["exit_date"],
                    "entry_price": forward_info["entry_price"],
                    "exit_price": forward_info["exit_price"],
                    "return_pct": forward_info["return_pct"],
                    "return_decimal": forward_info["return_decimal"],
                }
            )

        if len(ranked) < min_candidates:
            continue

        ranked.sort(
            key=lambda item: (item["score"], item["capital_score"], item["technical_score"]),
            reverse=True,
        )
        picks = _select_with_keep_rank(ranked, top_n, previous_selected_codes, keep_rank)
        selected_codes = [item["code"] for item in picks]
        benchmark_codes = [item["code"] for item in ranked]

        portfolio_gross_return = mean(item["return_decimal"] for item in picks)
        benchmark_gross_return = mean(item["return_decimal"] for item in ranked)

        portfolio_turnover = _compute_turnover(previous_selected_codes, selected_codes)
        benchmark_turnover = _compute_turnover(previous_benchmark_codes, benchmark_codes)

        portfolio_performance = _apply_transaction_costs(
            portfolio_gross_return,
            portfolio_turnover,
            commission_rate,
            slippage_rate,
            sell_tax_rate,
        )
        benchmark_performance = _apply_transaction_costs(
            benchmark_gross_return,
            benchmark_turnover,
            commission_rate,
            slippage_rate,
            sell_tax_rate,
        )

        periods.append(
            {
                "signal_date": rebalance_date,
                "entry_date": picks[0]["entry_date"],
                "exit_date": picks[0]["exit_date"],
                "candidate_count": len(ranked),
                "selected_count": len(picks),
                "retained_count": sum(1 for code in selected_codes if code in previous_selected_codes),
                "replacement_count": sum(1 for code in selected_codes if code not in previous_selected_codes),
                "turnover": portfolio_turnover["turnover"],
                "buy_turnover": portfolio_turnover["buy_turnover"],
                "sell_turnover": portfolio_turnover["sell_turnover"],
                "cost_rate": portfolio_performance["cost_rate"],
                "benchmark_turnover": benchmark_turnover["turnover"],
                "benchmark_cost_rate": benchmark_performance["cost_rate"],
                "portfolio_gross_return": portfolio_performance["gross_return"],
                "portfolio_net_return": portfolio_performance["net_return"],
                "benchmark_gross_return": benchmark_performance["gross_return"],
                "benchmark_net_return": benchmark_performance["net_return"],
                "portfolio_return": portfolio_performance["net_return"],
                "benchmark_return": benchmark_performance["net_return"],
                "picks": picks,
            }
        )
        previous_selected_codes = selected_codes
        previous_benchmark_codes = benchmark_codes
        if max_periods and len(periods) >= max_periods:
            break

    gross_period_returns = [period["portfolio_gross_return"] for period in periods]
    net_period_returns = [period["portfolio_net_return"] for period in periods]
    benchmark_gross_returns = [period["benchmark_gross_return"] for period in periods]
    benchmark_net_returns = [period["benchmark_net_return"] for period in periods]
    cumulative_gross_return = _equity_curve(gross_period_returns)[-1] - 1.0 if gross_period_returns else 0.0
    cumulative_net_return = _equity_curve(net_period_returns)[-1] - 1.0 if net_period_returns else 0.0
    cumulative_benchmark_gross = _equity_curve(benchmark_gross_returns)[-1] - 1.0 if benchmark_gross_returns else 0.0
    cumulative_benchmark_net = _equity_curve(benchmark_net_returns)[-1] - 1.0 if benchmark_net_returns else 0.0

    return {
        "params": {
            "top_n": top_n,
            "start_date": start_date or "",
            "end_date": end_date or "",
            "hold_days": hold_days,
            "rebalance_days": rebalance_days,
            "min_history": min_history,
            "min_candidates": min_candidates,
            "enabled_groups": groups,
            "commission_rate": commission_rate,
            "slippage_rate": slippage_rate,
            "sell_tax_rate": sell_tax_rate,
            "keep_rank": keep_rank,
            "max_forward_return_pct": max_forward_return_pct,
        },
        "summary": {
            "universe_size": len(histories),
            "period_count": len(periods),
            "actual_start": periods[0]["signal_date"] if periods else "",
            "actual_end": periods[-1]["signal_date"] if periods else "",
            "skipped_outlier_count": skipped_outlier_count,
            "average_candidates": round(mean(period["candidate_count"] for period in periods), 2) if periods else 0.0,
            "average_selected": round(mean(period["selected_count"] for period in periods), 2) if periods else 0.0,
            "average_retained": round(mean(period["retained_count"] for period in periods), 2) if periods else 0.0,
            "average_replacements": round(mean(period["replacement_count"] for period in periods), 2) if periods else 0.0,
            "average_turnover_pct": round(mean(period["turnover"] for period in periods) * 100.0, 2) if periods else 0.0,
            "average_cost_pct": round(mean(period["cost_rate"] for period in periods) * 100.0, 2) if periods else 0.0,
            "total_cost_pct": round(sum(period["cost_rate"] for period in periods) * 100.0, 2) if periods else 0.0,
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
            "annualized_gross_return_pct": round(_annualized_return(cumulative_gross_return, len(periods), rebalance_days) * 100.0, 2),
            "annualized_return_pct": round(_annualized_return(cumulative_net_return, len(periods), rebalance_days) * 100.0, 2),
            "annualized_net_return_pct": round(_annualized_return(cumulative_net_return, len(periods), rebalance_days) * 100.0, 2),
            "benchmark_annualized_gross_return_pct": round(_annualized_return(cumulative_benchmark_gross, len(periods), rebalance_days) * 100.0, 2),
            "benchmark_annualized_return_pct": round(_annualized_return(cumulative_benchmark_net, len(periods), rebalance_days) * 100.0, 2),
            "benchmark_annualized_net_return_pct": round(_annualized_return(cumulative_benchmark_net, len(periods), rebalance_days) * 100.0, 2),
            "volatility_pct": round(_annualized_volatility(net_period_returns, rebalance_days) * 100.0, 2),
            "sharpe_ratio": round(_sharpe_ratio(net_period_returns, rebalance_days), 2),
            "win_rate_pct": round(sum(ret > 0 for ret in net_period_returns) / len(net_period_returns) * 100.0, 2) if net_period_returns else 0.0,
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
        },
        "periods": periods,
        "top_symbols": _summarize_symbols(periods)[:20],
        "yearly_summary": _summarize_years(periods),
    }


def _report_filename(summary: Dict[str, Any], params: Dict[str, Any]) -> str:
    start = summary.get("actual_start") or params.get("start_date") or "na"
    end = summary.get("actual_end") or params.get("end_date") or "na"
    groups = "-".join(params.get("enabled_groups", [])) or "none"
    cost_tag = "c{commission}_s{slippage}_t{tax}".format(
        commission=_rate_to_bps(float(params.get("commission_rate", 0.0))),
        slippage=_rate_to_bps(float(params.get("slippage_rate", 0.0))),
        tax=_rate_to_bps(float(params.get("sell_tax_rate", 0.0))),
    )
    keep_rank = params.get("keep_rank", 0)
    outlier_tag = "fwd{threshold}".format(
        threshold=int(round(float(params.get("max_forward_return_pct", 0.0)))),
    )
    return (
        f"backtest_{start}_to_{end}_top{params['top_n']}_hold{params['hold_days']}"
        f"_step{params['rebalance_days']}_{groups}_{cost_tag}_keep{keep_rank}_{outlier_tag}.md"
    )


def write_backtest_report(result: Dict[str, Any]) -> Path:
    """Write a Markdown report for a backtest run."""
    ensure_runtime_directories()
    BACKTEST_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    params = result.get("params", {})
    summary = result.get("summary", {})
    periods = result.get("periods", [])
    top_symbols = result.get("top_symbols", [])
    yearly_summary = result.get("yearly_summary", [])
    report_path = BACKTEST_REPORTS_DIR / _report_filename(summary, params)

    lines = [
        "# A-share Backtest Report",
        "",
        "## Parameters",
        f"- Enabled groups: {', '.join(params.get('enabled_groups', []))}",
        f"- Top N: {params.get('top_n', 0)}",
        f"- Hold days: {params.get('hold_days', 0)}",
        f"- Rebalance days: {params.get('rebalance_days', 0)}",
        f"- Min history: {params.get('min_history', 0)}",
        f"- Min candidates: {params.get('min_candidates', 0)}",
        f"- Keep rank: {params.get('keep_rank', 0) or 'disabled'}",
        f"- Max forward return filter: +/-{float(params.get('max_forward_return_pct', 0.0)):.2f}%",
        f"- Requested date range: {params.get('start_date') or 'auto'} -> {params.get('end_date') or 'auto'}",
        f"- Commission rate: {float(params.get('commission_rate', 0.0)) * 100:.3f}%",
        f"- Slippage rate: {float(params.get('slippage_rate', 0.0)) * 100:.3f}%",
        f"- Sell tax rate: {float(params.get('sell_tax_rate', 0.0)) * 100:.3f}%",
        "",
        "## Summary",
        f"- Universe size: {summary.get('universe_size', 0)}",
        f"- Evaluated periods: {summary.get('period_count', 0)}",
        f"- Actual signal range: {summary.get('actual_start') or 'N/A'} -> {summary.get('actual_end') or 'N/A'}",
        f"- Skipped outlier samples: {summary.get('skipped_outlier_count', 0)}",
        f"- Avg candidates: {summary.get('average_candidates', 0)}",
        f"- Avg selected: {summary.get('average_selected', 0)}",
        f"- Avg retained positions: {summary.get('average_retained', 0)}",
        f"- Avg replacements: {summary.get('average_replacements', 0)}",
        f"- Avg turnover: {summary.get('average_turnover_pct', 0):.2f}%",
        f"- Avg cost drag: {summary.get('average_cost_pct', 0):.2f}%",
        f"- Total cost drag: {summary.get('total_cost_pct', 0):.2f}%",
        f"- Strategy avg gross return: {summary.get('average_gross_return_pct', 0):.2f}%",
        f"- Strategy avg net return: {summary.get('average_net_return_pct', 0):.2f}%",
        f"- Benchmark avg gross return: {summary.get('benchmark_average_gross_return_pct', 0):.2f}%",
        f"- Benchmark avg net return: {summary.get('benchmark_average_net_return_pct', 0):.2f}%",
        f"- Strategy cumulative gross return: {summary.get('cumulative_gross_return_pct', 0):.2f}%",
        f"- Strategy cumulative net return: {summary.get('cumulative_net_return_pct', 0):.2f}%",
        f"- Benchmark cumulative gross return: {summary.get('benchmark_cumulative_gross_return_pct', 0):.2f}%",
        f"- Benchmark cumulative net return: {summary.get('benchmark_cumulative_net_return_pct', 0):.2f}%",
        f"- Strategy annualized gross return: {summary.get('annualized_gross_return_pct', 0):.2f}%",
        f"- Strategy annualized net return: {summary.get('annualized_net_return_pct', 0):.2f}%",
        f"- Benchmark annualized gross return: {summary.get('benchmark_annualized_gross_return_pct', 0):.2f}%",
        f"- Benchmark annualized net return: {summary.get('benchmark_annualized_net_return_pct', 0):.2f}%",
        f"- Strategy volatility: {summary.get('volatility_pct', 0):.2f}%",
        f"- Sharpe ratio: {summary.get('sharpe_ratio', 0):.2f}",
        f"- Win rate: {summary.get('win_rate_pct', 0):.2f}%",
        f"- Excess win rate: {summary.get('excess_win_rate_pct', 0):.2f}%",
        f"- Max drawdown: {summary.get('max_drawdown_pct', 0):.2f}%",
        f"- Benchmark max drawdown: {summary.get('benchmark_max_drawdown_pct', 0):.2f}%",
        "",
    ]

    if top_symbols:
        lines.extend(
            [
                "## Most Selected Symbols",
                "",
                "| Rank | Code | Name | Count | Avg Score | Avg Return |",
                "| --- | --- | --- | ---: | ---: | ---: |",
            ]
        )
        for index, item in enumerate(top_symbols, start=1):
            lines.append(
                "| {rank} | {code} | {name} | {count} | {score:.2f} | {ret:.2f}% |".format(
                    rank=index,
                    code=item["code"],
                    name=item["name"],
                    count=item["selection_count"],
                    score=item["avg_score"],
                    ret=item["avg_return_pct"],
                )
            )
        lines.append("")

    if yearly_summary:
        lines.extend(
            [
                "## Yearly Breakdown",
                "",
                "| Year | Periods | Avg Turnover | Avg Cost | Avg Net | Bench Avg Net | Cum Net | Bench Cum Net | Win Rate |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for item in yearly_summary:
            lines.append(
                "| {year} | {periods} | {turnover:.2f}% | {cost:.2f}% | {net:.2f}% | {bench_avg:.2f}% | {cum:.2f}% | {bench_cum:.2f}% | {win:.2f}% |".format(
                    year=item["year"],
                    periods=item["period_count"],
                    turnover=item["avg_turnover_pct"],
                    cost=item["avg_cost_pct"],
                    net=item["avg_net_return_pct"],
                    bench_avg=item["benchmark_avg_net_return_pct"],
                    cum=item["cumulative_net_return_pct"],
                    bench_cum=item["benchmark_cumulative_net_return_pct"],
                    win=item["win_rate_pct"],
                )
            )
        lines.append("")

    if periods:
        lines.extend(
            [
                "## Recent Periods",
                "",
                "| Signal | Entry | Exit | Candidates | Picks | Kept | New | Turnover | Cost | Gross | Net | Bench Gross | Bench Net | Selected Codes |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for period in periods[-20:]:
            selected_codes = ", ".join(pick["code"] for pick in period.get("picks", []))
            lines.append(
                "| {signal} | {entry} | {exit} | {candidates} | {picks} | {kept} | {new} | {turnover:.2f}% | {cost:.2f}% | {gross:.2f}% | {net:.2f}% | {bench_gross:.2f}% | {bench_net:.2f}% | {codes} |".format(
                    signal=period["signal_date"],
                    entry=period["entry_date"],
                    exit=period["exit_date"],
                    candidates=period["candidate_count"],
                    picks=period["selected_count"],
                    kept=period["retained_count"],
                    new=period["replacement_count"],
                    turnover=period["turnover"] * 100.0,
                    cost=period["cost_rate"] * 100.0,
                    gross=period["portfolio_gross_return"] * 100.0,
                    net=period["portfolio_net_return"] * 100.0,
                    bench_gross=period["benchmark_gross_return"] * 100.0,
                    bench_net=period["benchmark_net_return"] * 100.0,
                    codes=selected_codes,
                )
            )
        lines.append("")
    else:
        lines.extend(["## Result", "", "No valid backtest periods were produced.", ""])

    report_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return report_path


def run_backtest(
    top_n: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    hold_days: int = 5,
    rebalance_days: int = 5,
    min_history: int = MIN_KLINE_DAYS,
    min_candidates: int = 20,
    max_periods: int = 0,
    max_stocks: int = 0,
    enabled_groups: Sequence[str] = DEFAULT_ENABLED_GROUPS,
    commission_rate: float = DEFAULT_COMMISSION_RATE,
    slippage_rate: float = DEFAULT_SLIPPAGE_RATE,
    sell_tax_rate: float = DEFAULT_SELL_TAX_RATE,
    keep_rank: int = DEFAULT_KEEP_RANK,
    max_forward_return_pct: float = DEFAULT_MAX_FORWARD_RETURN_PCT,
) -> Dict[str, Any]:
    """Run a backtest from local cache and persist a Markdown report."""
    histories = load_backtest_histories(max_stocks=max_stocks)
    result = run_backtest_on_histories(
        histories,
        top_n=top_n,
        start_date=start_date,
        end_date=end_date,
        hold_days=hold_days,
        rebalance_days=rebalance_days,
        min_history=min_history,
        min_candidates=min_candidates,
        max_periods=max_periods,
        enabled_groups=enabled_groups,
        commission_rate=commission_rate,
        slippage_rate=slippage_rate,
        sell_tax_rate=sell_tax_rate,
        keep_rank=keep_rank,
        max_forward_return_pct=max_forward_return_pct,
    )
    result["report_path"] = write_backtest_report(result)
    return result
