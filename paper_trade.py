# -*- coding: utf-8 -*-
"""Paper-trade logging and review helpers."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backtester import DEFAULT_HOLD_DAYS
from config import KLINE_CACHE_DIR, PAPER_TRADE_REVIEWS_DIR, PAPER_TRADES_DIR, ensure_runtime_directories


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None, "--"):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _paper_trade_path(report_date: str) -> Path:
    return PAPER_TRADES_DIR / f"{report_date}.json"


def record_paper_trade_run(
    screened_data: Dict[str, Any],
    top_n: int,
    report_path: Path,
    quality_report_path: Path,
    hold_days: int = DEFAULT_HOLD_DAYS,
) -> Path:
    """Write one paper-trade snapshot for the current screening date."""
    ensure_runtime_directories()
    report_date = str(screened_data.get("report_date") or date.today().isoformat())
    top_stocks = list(screened_data.get("top_stocks", []))[: max(0, int(top_n))]
    suggested_exposure = max(0.0, min(1.0, float(screened_data.get("suggested_exposure", 1.0) or 0.0)))
    position_weight = round(suggested_exposure / len(top_stocks), 6) if top_stocks else 0.0

    payload = {
        "report_date": report_date,
        "hold_days": max(1, int(hold_days)),
        "report_path": str(report_path),
        "quality_report_path": str(quality_report_path),
        "suggested_exposure": suggested_exposure,
        "position_weight": position_weight,
        "stocks": [
            {
                "rank": index,
                "code": item.get("code", ""),
                "name": item.get("name", ""),
                "sector": item.get("sector", ""),
                "entry_price": round(float(item.get("price", 0.0) or 0.0), 4),
                "total_score": round(float(item.get("total_score", 0.0) or 0.0), 4),
            }
            for index, item in enumerate(top_stocks, start=1)
        ],
    }

    path = _paper_trade_path(report_date)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _load_paper_trade_runs() -> List[Dict[str, Any]]:
    ensure_runtime_directories()
    runs: List[Dict[str, Any]] = []
    for path in sorted(PAPER_TRADES_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and payload.get("report_date"):
            payload["_path"] = str(path)
            runs.append(payload)
    runs.sort(key=lambda item: str(item.get("report_date", "")))
    return runs


def _load_kline_rows(code: str) -> List[Dict[str, Any]]:
    path = KLINE_CACHE_DIR / f"{code}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = payload.get("data", [])
    return rows if isinstance(rows, list) else []


def _compute_stock_review(
    stock: Dict[str, Any],
    report_date: str,
    hold_days: int,
    as_of_date: str,
) -> Dict[str, Any]:
    code = str(stock.get("code", ""))
    rows = _load_kline_rows(code)
    entry_price = _safe_float(stock.get("entry_price"))
    if not code or not rows or entry_price <= 0:
        return {
            "code": code,
            "name": stock.get("name", ""),
            "status": "missing_data",
        }

    entry_index: Optional[int] = None
    for index, row in enumerate(rows):
        row_date = str(row.get("date", ""))
        if row_date >= report_date:
            entry_index = index
            break

    if entry_index is None:
        return {
            "code": code,
            "name": stock.get("name", ""),
            "status": "missing_entry",
        }

    target_index = entry_index + max(1, int(hold_days))
    if target_index >= len(rows):
        return {
            "code": code,
            "name": stock.get("name", ""),
            "status": "open",
        }

    exit_row = rows[target_index]
    exit_date = str(exit_row.get("date", ""))
    if as_of_date and exit_date > as_of_date:
        return {
            "code": code,
            "name": stock.get("name", ""),
            "status": "open",
        }

    exit_price = _safe_float(exit_row.get("close"))
    if exit_price <= 0:
        return {
            "code": code,
            "name": stock.get("name", ""),
            "status": "missing_exit",
        }

    return_pct = (exit_price / entry_price - 1.0) * 100.0
    return {
        "code": code,
        "name": stock.get("name", ""),
        "status": "closed",
        "entry_price": round(entry_price, 4),
        "exit_price": round(exit_price, 4),
        "exit_date": exit_date,
        "return_pct": round(return_pct, 2),
    }


def review_paper_trade_runs(
    hold_days: int = DEFAULT_HOLD_DAYS,
    as_of_date: str = "",
) -> Dict[str, Any]:
    """Review all saved paper-trade runs against cached K-line history."""
    runs = _load_paper_trade_runs()
    reviewed_runs: List[Dict[str, Any]] = []
    closed_run_returns: List[float] = []
    closed_pick_returns: List[float] = []

    for run in runs:
        report_date = str(run.get("report_date", ""))
        suggested_exposure = max(0.0, min(1.0, float(run.get("suggested_exposure", 1.0) or 0.0)))
        reviewed_stocks = [
            _compute_stock_review(stock, report_date=report_date, hold_days=hold_days, as_of_date=as_of_date)
            for stock in list(run.get("stocks", []))
        ]
        closed = [item for item in reviewed_stocks if item.get("status") == "closed"]
        pending_count = sum(1 for item in reviewed_stocks if item.get("status") != "closed")
        run_return = None
        if closed:
            avg_pick_return = sum(float(item.get("return_pct", 0.0)) for item in closed) / len(closed)
            run_return = round(avg_pick_return * suggested_exposure, 2)
            closed_run_returns.append(run_return)
            closed_pick_returns.extend(float(item.get("return_pct", 0.0)) for item in closed)
        reviewed_runs.append(
            {
                "report_date": report_date,
                "suggested_exposure": suggested_exposure,
                "run_return_pct": run_return,
                "closed_count": len(closed),
                "open_count": pending_count,
                "stocks": reviewed_stocks,
                "path": run.get("_path", ""),
            }
        )

    return {
        "hold_days": hold_days,
        "as_of_date": as_of_date or date.today().isoformat(),
        "run_count": len(reviewed_runs),
        "closed_run_count": len(closed_run_returns),
        "open_run_count": sum(1 for item in reviewed_runs if item.get("run_return_pct") is None),
        "average_run_return_pct": round(sum(closed_run_returns) / len(closed_run_returns), 2) if closed_run_returns else 0.0,
        "average_pick_return_pct": round(sum(closed_pick_returns) / len(closed_pick_returns), 2) if closed_pick_returns else 0.0,
        "win_rate_pct": round(
            (sum(1 for item in closed_run_returns if item > 0) / len(closed_run_returns)) * 100.0,
            2,
        )
        if closed_run_returns
        else 0.0,
        "runs": reviewed_runs,
    }


def write_paper_trade_review(review: Dict[str, Any]) -> Path:
    """Persist a markdown review of paper-trade history."""
    ensure_runtime_directories()
    as_of_date = str(review.get("as_of_date") or date.today().isoformat())
    hold_days = int(review.get("hold_days", DEFAULT_HOLD_DAYS) or DEFAULT_HOLD_DAYS)
    path = PAPER_TRADE_REVIEWS_DIR / f"paper_review_{as_of_date}_hold{hold_days}.md"

    lines = [
        f"# Paper Trade Review - {as_of_date}",
        "",
        f"- Hold days: {hold_days}",
        f"- Runs tracked: {review.get('run_count', 0)}",
        f"- Closed runs: {review.get('closed_run_count', 0)}",
        f"- Open runs: {review.get('open_run_count', 0)}",
        f"- Average run return: {float(review.get('average_run_return_pct', 0.0)):.2f}%",
        f"- Average pick return: {float(review.get('average_pick_return_pct', 0.0)):.2f}%",
        f"- Win rate: {float(review.get('win_rate_pct', 0.0)):.2f}%",
        "",
        "## Runs",
        "",
        "| Date | Closed | Open | Run Return |",
        "| --- | ---: | ---: | ---: |",
    ]

    for item in review.get("runs", []):
        run_return = item.get("run_return_pct")
        lines.append(
            "| {date} | {closed} | {open_} | {run_return} |".format(
                date=item.get("report_date", ""),
                closed=int(item.get("closed_count", 0) or 0),
                open_=int(item.get("open_count", 0) or 0),
                run_return=f"{float(run_return):.2f}%" if run_return is not None else "pending",
            )
        )

    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return path
