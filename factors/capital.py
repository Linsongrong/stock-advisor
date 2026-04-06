# -*- coding: utf-8 -*-
"""Capital-flow proxy factor calculations."""

from typing import Any, Dict, List

import numpy as np


def _score(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def _mean(values: List[float]) -> float:
    return float(np.mean(values)) if values else 0.0


def _extract_volume(rows: List[Dict[str, Any]], lookback: int) -> float:
    volumes = [float(item.get("volume", 0) or 0) for item in rows[-lookback:] if float(item.get("volume", 0) or 0) > 0]
    return _mean(volumes)


def calculate_capital_factors(quote: Dict[str, Any], kline: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate proxy capital-flow factors from quote and daily K-line data."""
    if not quote or not kline:
        return {"factors": {}, "metrics": {}}

    latest = kline[-1]
    history = kline[:-1] if len(kline) > 1 else kline

    current_price = float(quote.get("price") or latest.get("close") or 0)
    open_price = float(quote.get("open") or latest.get("open") or 0)
    prev_close = float(quote.get("prev_close") or (history[-1]["close"] if history else 0) or 0)
    current_volume = float(quote.get("volume") or latest.get("volume") or 0)

    avg5_volume = _extract_volume(history or kline, 5)
    avg10_volume = _extract_volume(history or kline, 10)
    volume_ratio = current_volume / avg5_volume if avg5_volume > 0 else 1.0
    turnover_ratio = current_volume / avg10_volume if avg10_volume > 0 else volume_ratio

    intraday_return = (current_price / open_price - 1.0) if open_price > 0 else 0.0
    day_change = (current_price / prev_close - 1.0) if prev_close > 0 else 0.0

    net_inflow_rate = _score(
        50
        + (20 if intraday_return > 0 else -20 if intraday_return < 0 else 0)
        + max(-15, min(25, (volume_ratio - 1.0) * 30))
        + max(-15, min(20, day_change * 300))
    )

    volume_ratio_score = _score(
        30
        + min(volume_ratio, 3.0) / 1.5 * 50
        + (10 if intraday_return > 0 else -10 if intraday_return < 0 else 0)
    )

    turnover_anomaly = _score(
        25
        + min(turnover_ratio, 3.0) / 2.0 * 50
        + (10 if abs(day_change) >= 0.02 else 0)
        + (10 if intraday_return > 0 else -10 if intraday_return < 0 else 0)
    )

    if volume_ratio >= 2.0 and intraday_return > 0:
        volume_breakout = 100.0
    elif volume_ratio >= 1.5 and intraday_return > 0:
        volume_breakout = 80.0
    elif volume_ratio >= 1.2 and intraday_return > 0:
        volume_breakout = 65.0
    elif volume_ratio >= 1.5 and intraday_return <= 0:
        volume_breakout = 35.0
    else:
        volume_breakout = 20.0 + min(volume_ratio, 1.2) * 20

    return {
        "factors": {
            "net_inflow_rate": net_inflow_rate,
            "volume_ratio": _score(volume_ratio_score),
            "turnover_anomaly": _score(turnover_anomaly),
            "volume_breakout": _score(volume_breakout),
        },
        "metrics": {
            "current_price": round(current_price, 4),
            "open_price": round(open_price, 4),
            "prev_close": round(prev_close, 4),
            "current_volume": int(current_volume),
            "avg5_volume": round(avg5_volume, 2),
            "avg10_volume": round(avg10_volume, 2),
            "volume_ratio": round(volume_ratio, 3),
            "turnover_ratio": round(turnover_ratio, 3),
            "intraday_return_pct": round(intraday_return * 100, 2),
            "day_change_pct": round(day_change * 100, 2),
        },
    }
