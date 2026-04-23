# -*- coding: utf-8 -*-
"""Strategy health checks and daily trade-plan rendering."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from config import (
    STRATEGY_HEALTH_DIR,
    STRATEGY_HEALTH_LOOKBACK_RUNS,
    STRATEGY_HEALTH_MAX_CONSECUTIVE_LOSSES,
    STRATEGY_HEALTH_MAX_DRAWDOWN_PCT,
    STRATEGY_HEALTH_MIN_AVERAGE_RUN_RETURN_PCT,
    STRATEGY_HEALTH_MIN_CLOSED_RUNS,
    STRATEGY_HEALTH_MIN_WIN_RATE_PCT,
    TRADE_PLANS_DIR,
    ensure_runtime_directories,
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None, "--"):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _compute_max_drawdown_pct(returns_pct: List[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for item in returns_pct:
        equity *= 1.0 + float(item) / 100.0
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
    return round(max_drawdown * 100.0, 2)


def _compute_consecutive_losses(returns_pct: List[float]) -> int:
    streak = 0
    worst = 0
    for item in returns_pct:
        if float(item) < 0:
            streak += 1
            worst = max(worst, streak)
        else:
            streak = 0
    return worst


def evaluate_strategy_health(review: Dict[str, Any]) -> Dict[str, Any]:
    """Judge whether recent paper-trade history is healthy enough for live deployment."""
    runs = list(review.get("runs", []))
    closed_runs = [item for item in runs if item.get("run_return_pct") is not None]
    recent_closed = closed_runs[-STRATEGY_HEALTH_LOOKBACK_RUNS :]
    recent_returns = [float(item.get("run_return_pct", 0.0) or 0.0) for item in recent_closed]

    status = "pass"
    reasons: List[str] = []
    deployment_mode = "live_candidate"

    if len(closed_runs) < STRATEGY_HEALTH_MIN_CLOSED_RUNS:
        status = "warmup"
        deployment_mode = "paper_only"
        reasons.append(
            "纸面跟踪已关闭样本只有 {actual} 组，低于最低要求 {expected} 组".format(
                actual=len(closed_runs),
                expected=STRATEGY_HEALTH_MIN_CLOSED_RUNS,
            )
        )

    recent_avg = round(sum(recent_returns) / len(recent_returns), 2) if recent_returns else 0.0
    recent_win_rate = round(
        (sum(1 for item in recent_returns if item > 0) / len(recent_returns)) * 100.0,
        2,
    ) if recent_returns else 0.0
    recent_drawdown = _compute_max_drawdown_pct(recent_returns)
    recent_loss_streak = _compute_consecutive_losses(recent_returns)

    if len(closed_runs) >= STRATEGY_HEALTH_MIN_CLOSED_RUNS:
        if recent_avg < STRATEGY_HEALTH_MIN_AVERAGE_RUN_RETURN_PCT:
            status = "fail"
            deployment_mode = "blocked"
            reasons.append(
                "最近 {count} 组平均收益只有 {actual:.2f}%，低于要求 {expected:.2f}%".format(
                    count=len(recent_returns),
                    actual=recent_avg,
                    expected=STRATEGY_HEALTH_MIN_AVERAGE_RUN_RETURN_PCT,
                )
            )
        if recent_win_rate < STRATEGY_HEALTH_MIN_WIN_RATE_PCT:
            status = "fail"
            deployment_mode = "blocked"
            reasons.append(
                "最近 {count} 组胜率只有 {actual:.2f}%，低于要求 {expected:.2f}%".format(
                    count=len(recent_returns),
                    actual=recent_win_rate,
                    expected=STRATEGY_HEALTH_MIN_WIN_RATE_PCT,
                )
            )
        if recent_drawdown > STRATEGY_HEALTH_MAX_DRAWDOWN_PCT:
            status = "fail"
            deployment_mode = "blocked"
            reasons.append(
                "最近 {count} 组最大回撤达到 {actual:.2f}%，超过上限 {expected:.2f}%".format(
                    count=len(recent_returns),
                    actual=recent_drawdown,
                    expected=STRATEGY_HEALTH_MAX_DRAWDOWN_PCT,
                )
            )
        if recent_loss_streak > STRATEGY_HEALTH_MAX_CONSECUTIVE_LOSSES:
            status = "fail"
            deployment_mode = "blocked"
            reasons.append(
                "最近连续亏损最多 {actual} 组，超过上限 {expected} 组".format(
                    actual=recent_loss_streak,
                    expected=STRATEGY_HEALTH_MAX_CONSECUTIVE_LOSSES,
                )
            )

    if status == "pass":
        reasons.append("最近纸面跟踪表现通过健康检查，可以进入实盘候选状态。")

    return {
        "report_date": review.get("as_of_date") or date.today().isoformat(),
        "status": status,
        "deployment_mode": deployment_mode,
        "reasons": reasons,
        "metrics": {
            "closed_run_count": len(closed_runs),
            "lookback_run_count": len(recent_returns),
            "recent_average_run_return_pct": recent_avg,
            "recent_win_rate_pct": recent_win_rate,
            "recent_max_drawdown_pct": recent_drawdown,
            "recent_max_consecutive_losses": recent_loss_streak,
        },
    }


def write_strategy_health_report(health: Dict[str, Any]) -> Path:
    """Persist a markdown strategy-health report."""
    ensure_runtime_directories()
    report_date = str(health.get("report_date") or date.today().isoformat())
    path = STRATEGY_HEALTH_DIR / f"{report_date}.md"
    metrics = health.get("metrics", {}) if isinstance(health, dict) else {}
    reasons = health.get("reasons", []) if isinstance(health, dict) else []

    lines = [
        f"# Strategy Health Report - {report_date}",
        "",
        f"- Status: {str(health.get('status', 'unknown')).upper()}",
        f"- Deployment mode: {health.get('deployment_mode', 'unknown')}",
        f"- Closed runs: {metrics.get('closed_run_count', 0)}",
        f"- Lookback runs: {metrics.get('lookback_run_count', 0)}",
        f"- Recent average run return: {float(metrics.get('recent_average_run_return_pct', 0.0)):.2f}%",
        f"- Recent win rate: {float(metrics.get('recent_win_rate_pct', 0.0)):.2f}%",
        f"- Recent max drawdown: {float(metrics.get('recent_max_drawdown_pct', 0.0)):.2f}%",
        f"- Recent max consecutive losses: {int(metrics.get('recent_max_consecutive_losses', 0) or 0)}",
        "",
        "## Reasons",
        "",
    ]
    lines.extend(f"- {item}" for item in reasons)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return path


def write_daily_trade_plan(
    screened_data: Dict[str, Any],
    quality: Dict[str, Any],
    health: Dict[str, Any],
    top_n: int,
    hold_days: int,
) -> Path:
    """Write a daily execution-oriented trade plan."""
    ensure_runtime_directories()
    report_date = str(screened_data.get("report_date") or date.today().isoformat())
    top_stocks = list(screened_data.get("top_stocks", []))[: max(0, int(top_n))]
    model_exposure = max(0.0, min(1.0, float(screened_data.get("suggested_exposure", 1.0) or 0.0)))
    deployment_mode = str(health.get("deployment_mode", "paper_only"))
    live_exposure = model_exposure if deployment_mode == "live_candidate" and quality.get("passed") else 0.0
    position_weight = live_exposure / len(top_stocks) if top_stocks and live_exposure > 0 else 0.0

    path = TRADE_PLANS_DIR / f"{report_date}.md"
    lines = [
        f"# Daily Trade Plan - {report_date}",
        "",
        f"- Data quality: {'PASS' if quality.get('passed') else 'FAIL'}",
        f"- Strategy health: {str(health.get('status', 'unknown')).upper()}",
        f"- Deployment mode: {deployment_mode}",
        f"- Model suggested exposure: {model_exposure * 100.0:.0f}%",
        f"- Live exposure cap: {live_exposure * 100.0:.0f}%",
        f"- Planned hold days: {hold_days}",
        "",
        "## Action",
        "",
    ]

    if not quality.get("passed"):
        lines.append("- Stop. Data quality failed, no live or paper deployment today.")
    elif deployment_mode == "blocked":
        lines.append("- Stop. Strategy health failed, today's output only serves as observation data.")
    elif deployment_mode == "paper_only":
        lines.append("- Continue paper tracking only. Do not deploy live capital yet.")
    else:
        lines.append("- Live deployment candidate. Use the capped exposure and equal-weight position sizing below.")

    lines.extend(["", "## Reasons", ""])
    for item in quality.get("reasons", []):
        lines.append(f"- 数据质量：{item}")
    for item in health.get("reasons", []):
        lines.append(f"- 策略健康：{item}")

    lines.extend(
        [
            "",
            "## Picks",
            "",
            "| Rank | Code | Name | Sector | Price | Score | Live Weight |",
            "| --- | --- | --- | --- | ---: | ---: | ---: |",
        ]
    )

    for index, item in enumerate(top_stocks, start=1):
        lines.append(
            "| {rank} | {code} | {name} | {sector} | {price:.2f} | {score:.2f} | {weight:.2f}% |".format(
                rank=index,
                code=item.get("code", ""),
                name=item.get("name", ""),
                sector=item.get("sector", ""),
                price=_safe_float(item.get("price")),
                score=_safe_float(item.get("selection_score", item.get("total_score"))),
                weight=position_weight * 100.0,
            )
        )

    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return path
