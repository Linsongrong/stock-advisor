# -*- coding: utf-8 -*-
"""Shared helpers for market-session-aware activity metrics."""

from __future__ import annotations

import re
from datetime import datetime, time
from typing import Any, Dict, List, Optional

import numpy as np


A_SHARE_SESSION_MINUTES = 240.0
MORNING_OPEN = time(9, 30)
MORNING_CLOSE = time(11, 30)
AFTERNOON_OPEN = time(13, 0)
AFTERNOON_CLOSE = time(15, 0)


def _series_mean(values: List[float], window: int) -> float:
    if not values:
        return 0.0
    usable = values[-window:] if len(values) >= window else values
    return float(np.mean(usable))


def _parse_updated_at(raw_value: Any) -> Optional[datetime]:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return None

    digits = re.sub(r"[^0-9]", "", raw_text)
    for fmt, size in (("%Y%m%d%H%M%S", 14), ("%Y%m%d%H%M", 12), ("%Y%m%d", 8)):
        if len(digits) >= size:
            try:
                return datetime.strptime(digits[:size], fmt)
            except ValueError:
                continue

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_text, fmt)
        except ValueError:
            continue
    return None


def _has_intraday_timestamp(raw_value: Any) -> bool:
    digits = re.sub(r"[^0-9]", "", str(raw_value or ""))
    return len(digits) >= 12


def _session_progress(raw_value: Any) -> float:
    if not _has_intraday_timestamp(raw_value):
        return 1.0

    updated_at = _parse_updated_at(raw_value)
    if updated_at is None:
        return 1.0

    current_time = updated_at.time()
    if current_time <= MORNING_OPEN:
        return 0.0
    if current_time <= MORNING_CLOSE:
        elapsed = (updated_at.hour * 60 + updated_at.minute) - (MORNING_OPEN.hour * 60 + MORNING_OPEN.minute)
        return max(0.0, min(1.0, elapsed / A_SHARE_SESSION_MINUTES))
    if current_time < AFTERNOON_OPEN:
        return 120.0 / A_SHARE_SESSION_MINUTES
    if current_time <= AFTERNOON_CLOSE:
        elapsed_afternoon = (updated_at.hour * 60 + updated_at.minute) - (AFTERNOON_OPEN.hour * 60 + AFTERNOON_OPEN.minute)
        return max(0.0, min(1.0, (120.0 + elapsed_afternoon) / A_SHARE_SESSION_MINUTES))
    return 1.0


def calculate_volume_profile(quote: Dict[str, Any], kline: List[Dict[str, Any]]) -> Dict[str, float]:
    """Align live quote volume with K-line volume and normalize intraday progress."""
    if not kline:
        return {
            "current_volume": 0.0,
            "avg5_volume": 0.0,
            "avg10_volume": 0.0,
            "raw_volume_ratio": 1.0,
            "volume_ratio": 1.0,
            "turnover_ratio": 1.0,
            "trading_progress": 1.0,
            "estimated_full_day_volume": 0.0,
        }

    history = kline[:-1] if len(kline) > 1 else kline
    history_volumes = [float(item.get("volume", 0) or 0) for item in history if float(item.get("volume", 0) or 0) > 0]
    avg5_volume = _series_mean(history_volumes, 5)
    avg10_volume = _series_mean(history_volumes, 10)

    fallback_volume = float(kline[-1].get("volume", 0) or 0)
    current_volume = float(
        quote.get("volume_shares")
        or quote.get("volume")
        or fallback_volume
        or 0
    )

    progress = _session_progress(quote.get("updated_at"))
    estimated_full_day_volume = current_volume
    if 0.0 < progress < 1.0:
        estimated_full_day_volume = current_volume / progress

    raw_volume_ratio = current_volume / avg5_volume if avg5_volume > 0 else 1.0
    normalized_volume_ratio = estimated_full_day_volume / avg5_volume if avg5_volume > 0 else 1.0
    turnover_ratio = estimated_full_day_volume / avg10_volume if avg10_volume > 0 else normalized_volume_ratio

    return {
        "current_volume": current_volume,
        "avg5_volume": avg5_volume,
        "avg10_volume": avg10_volume,
        "raw_volume_ratio": raw_volume_ratio,
        "volume_ratio": normalized_volume_ratio,
        "turnover_ratio": turnover_ratio,
        "trading_progress": progress,
        "estimated_full_day_volume": estimated_full_day_volume,
    }
