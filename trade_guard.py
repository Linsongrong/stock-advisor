# -*- coding: utf-8 -*-
"""Runtime data-quality guardrails for daily screening runs."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from config import (
    MAX_FAILURE_RATIO,
    MIN_FETCH_SUCCESS_RATIO,
    MIN_PREFILTER_SEED_RATIO,
    MIN_QUALIFIED_COUNT,
    MIN_SCREEN_UNIVERSE_SIZE,
    QUALITY_REPORTS_DIR,
    ensure_runtime_directories,
)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def evaluate_screen_quality(
    universe: List[Dict[str, Any]],
    market_data: Dict[str, Any],
    screened_data: Dict[str, Any],
    top_n: int,
) -> Dict[str, Any]:
    """Return pass/fail quality status plus concrete reasons for the daily run."""
    universe_size = int(len(universe))
    prefilter = market_data.get("prefilter", {}) if isinstance(market_data, dict) else {}
    source_status = market_data.get("source_status", {}) if isinstance(market_data, dict) else {}
    source_usage = market_data.get("source_usage", {}) if isinstance(market_data, dict) else {}

    selected_count = int(prefilter.get("selected_count", universe_size) or 0)
    seed_quote_count = int(prefilter.get("seed_quote_count", 0) or 0)
    fetched_count = int(screened_data.get("fetched_count", 0) or 0)
    failed_count = int(screened_data.get("failed_count", 0) or 0)
    qualified_count = int(screened_data.get("qualified_count", 0) or 0)

    quote_usage = source_usage.get("quote", {}) if isinstance(source_usage, dict) else {}
    kline_usage = source_usage.get("kline", {}) if isinstance(source_usage, dict) else {}
    quote_success_count = sum(
        int(quote_usage.get(key, 0) or 0)
        for key in ("live", "universe_snapshot", "cache_fresh", "cache_stale", "derived_kline")
    )
    kline_success_count = sum(
        int(kline_usage.get(key, 0) or 0)
        for key in ("live", "universe_snapshot", "cache_fresh", "cache_stale", "derived_kline")
    )

    seed_ratio = _ratio(seed_quote_count, universe_size)
    fetch_ratio = _ratio(fetched_count, selected_count)
    failure_ratio = _ratio(failed_count, selected_count)
    quote_unavailable = int(quote_usage.get("unavailable", 0) or 0)
    kline_unavailable = int(kline_usage.get("unavailable", 0) or 0)
    quote_stale = int(quote_usage.get("cache_stale", 0) or 0)
    kline_stale = int(kline_usage.get("cache_stale", 0) or 0)
    quote_derived = int(quote_usage.get("derived_kline", 0) or 0)

    reasons: List[str] = []

    if universe_size < MIN_SCREEN_UNIVERSE_SIZE:
        reasons.append(
            "股票池只有 {actual} 只，低于最低要求 {expected} 只".format(
                actual=universe_size,
                expected=MIN_SCREEN_UNIVERSE_SIZE,
            )
        )
    if not bool(source_status.get("quote", False)) and quote_success_count <= 0:
        reasons.append("行情源探测失败且没有可用行情")
    if not bool(source_status.get("kline", False)) and kline_success_count <= 0:
        reasons.append("日线源探测失败且没有可用日线")
    if seed_ratio < MIN_PREFILTER_SEED_RATIO:
        reasons.append(
            "预筛行情覆盖率只有 {actual:.2%}，低于要求 {expected:.2%}".format(
                actual=seed_ratio,
                expected=MIN_PREFILTER_SEED_RATIO,
            )
        )
    if fetch_ratio < MIN_FETCH_SUCCESS_RATIO:
        reasons.append(
            "预筛后抓取完成率只有 {actual:.2%}，低于要求 {expected:.2%}".format(
                actual=fetch_ratio,
                expected=MIN_FETCH_SUCCESS_RATIO,
            )
        )
    if failure_ratio > MAX_FAILURE_RATIO:
        reasons.append(
            "抓取或打分失败比例达到 {actual:.2%}，超过上限 {expected:.2%}".format(
                actual=failure_ratio,
                expected=MAX_FAILURE_RATIO,
            )
        )
    if quote_unavailable > 0:
        reasons.append(f"有 {quote_unavailable} 只股票缺少行情")
    if kline_unavailable > 0:
        reasons.append(f"有 {kline_unavailable} 只股票缺少日线")
    if quote_stale > 0:
        reasons.append(f"有 {quote_stale} 只股票用了过期行情缓存")
    if kline_stale > 0:
        reasons.append(f"有 {kline_stale} 只股票用了过期日线缓存")
    if quote_derived > 0:
        reasons.append(f"有 {quote_derived} 只股票只能用日线推导行情")
    if qualified_count < max(int(top_n), MIN_QUALIFIED_COUNT):
        reasons.append(
            "合格候选只有 {actual} 只，低于最低要求 {expected} 只".format(
                actual=qualified_count,
                expected=max(int(top_n), MIN_QUALIFIED_COUNT),
            )
        )

    return {
        "passed": len(reasons) == 0,
        "report_date": screened_data.get("report_date") or date.today().isoformat(),
        "reasons": reasons,
        "metrics": {
            "universe_size": universe_size,
            "selected_count": selected_count,
            "seed_quote_count": seed_quote_count,
            "seed_ratio": round(seed_ratio, 4),
            "fetched_count": fetched_count,
            "fetch_ratio": round(fetch_ratio, 4),
            "failed_count": failed_count,
            "failure_ratio": round(failure_ratio, 4),
            "qualified_count": qualified_count,
            "quote_unavailable": quote_unavailable,
            "kline_unavailable": kline_unavailable,
            "quote_stale": quote_stale,
            "kline_stale": kline_stale,
            "quote_derived": quote_derived,
            "quote_success_count": quote_success_count,
            "kline_success_count": kline_success_count,
        },
    }


def write_screen_quality_report(quality: Dict[str, Any]) -> Path:
    """Persist a markdown quality report for the latest daily run."""
    ensure_runtime_directories()
    report_date = str(quality.get("report_date") or date.today().isoformat())
    report_path = QUALITY_REPORTS_DIR / f"{report_date}.md"
    metrics = quality.get("metrics", {}) if isinstance(quality, dict) else {}
    reasons = quality.get("reasons", []) if isinstance(quality, dict) else []

    lines = [
        f"# Screening Quality Report - {report_date}",
        "",
        f"- Status: {'PASS' if quality.get('passed') else 'FAIL'}",
        f"- Universe size: {metrics.get('universe_size', 0)}",
        f"- Prefilter seed coverage: {float(metrics.get('seed_ratio', 0.0)) * 100.0:.2f}%",
        f"- Fetch completion: {float(metrics.get('fetch_ratio', 0.0)) * 100.0:.2f}%",
        f"- Failure ratio: {float(metrics.get('failure_ratio', 0.0)) * 100.0:.2f}%",
        f"- Qualified candidates: {metrics.get('qualified_count', 0)}",
        "",
        "## Reasons",
        "",
    ]
    if reasons:
        lines.extend(f"- {item}" for item in reasons)
    else:
        lines.append("- All quality checks passed.")
    report_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return report_path
