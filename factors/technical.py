# -*- coding: utf-8 -*-
"""Technical factor calculations based on daily K-line data."""

from typing import Any, Dict, List

import numpy as np

from config import BREAKOUT_LOOKBACK_DAYS
from factors.market_activity import calculate_volume_profile


def _score(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def _series_mean(values: np.ndarray, window: int) -> float:
    if values.size == 0:
        return 0.0
    usable = values[-window:] if values.size >= window else values
    return float(np.mean(usable))


def _ema(values: np.ndarray, period: int) -> np.ndarray:
    if values.size == 0:
        return np.array([])
    alpha = 2.0 / (period + 1.0)
    ema_values = np.zeros_like(values, dtype=float)
    ema_values[0] = values[0]
    for index in range(1, len(values)):
        ema_values[index] = alpha * values[index] + (1.0 - alpha) * ema_values[index - 1]
    return ema_values


def _rsi(values: np.ndarray, period: int = 14) -> float:
    if values.size <= 1:
        return 50.0
    deltas = np.diff(values)
    recent = deltas[-period:] if deltas.size >= period else deltas
    if recent.size == 0:
        return 50.0
    gains = np.where(recent > 0, recent, 0.0)
    losses = np.where(recent < 0, -recent, 0.0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def calculate_technical_factors(
    quote: Dict[str, Any],
    kline: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Calculate MACD, RSI, MA alignment, breakout, and trend strength."""
    if not kline:
        return {"factors": {}, "indicators": {}}

    closes = np.array([float(item.get("close", 0) or 0) for item in kline], dtype=float)
    highs = np.array([float(item.get("high", 0) or 0) for item in kline], dtype=float)
    lows = np.array([float(item.get("low", 0) or 0) for item in kline], dtype=float)
    volumes = np.array([float(item.get("volume", 0) or 0) for item in kline], dtype=float)

    if closes.size == 0:
        return {"factors": {}, "indicators": {}}

    adjusted_closes = closes.copy()
    live_price = float(quote.get("price") or 0)
    if live_price > 0:
        adjusted_closes[-1] = live_price

    current_close = float(adjusted_closes[-1])
    volume_profile = calculate_volume_profile(quote, kline)
    current_volume = float(volume_profile.get("current_volume", 0.0))
    avg5_volume = float(volume_profile.get("avg5_volume", 0.0)) or (float(volumes[-1]) if volumes.size else 0.0)
    volume_ratio = float(volume_profile.get("volume_ratio", 1.0))
    raw_volume_ratio = float(volume_profile.get("raw_volume_ratio", volume_ratio))
    estimated_full_day_volume = float(volume_profile.get("estimated_full_day_volume", current_volume))
    trading_progress = float(volume_profile.get("trading_progress", 1.0))

    ema12 = _ema(adjusted_closes, 12)
    ema26 = _ema(adjusted_closes, 26)
    macd_line = ema12 - ema26
    signal_line = _ema(macd_line, 9) if macd_line.size else np.array([])
    histogram = (macd_line - signal_line) * 2 if signal_line.size else np.array([])

    macd_now = float(macd_line[-1]) if macd_line.size else 0.0
    signal_now = float(signal_line[-1]) if signal_line.size else 0.0
    hist_now = float(histogram[-1]) if histogram.size else 0.0
    hist_prev = float(histogram[-2]) if histogram.size > 1 else hist_now
    macd_prev = float(macd_line[-2]) if macd_line.size > 1 else macd_now
    signal_prev = float(signal_line[-2]) if signal_line.size > 1 else signal_now

    if macd_now > signal_now and macd_prev <= signal_prev and hist_now > 0:
        macd_score = 100.0
    elif macd_now > signal_now and hist_now > hist_prev:
        macd_score = 85.0
    elif macd_now > signal_now:
        macd_score = 70.0
    elif macd_now < signal_now and macd_prev >= signal_prev:
        macd_score = 20.0
    else:
        macd_score = 35.0 if hist_now < 0 else 50.0

    rsi_value = float(_rsi(adjusted_closes, 14))
    if rsi_value < 20:
        rsi_score = 90.0
    elif rsi_value < 30:
        rsi_score = 100.0
    elif rsi_value < 45:
        rsi_score = 80.0
    elif rsi_value < 60:
        rsi_score = 65.0
    elif rsi_value < 70:
        rsi_score = 45.0
    elif rsi_value < 80:
        rsi_score = 20.0
    else:
        rsi_score = 0.0

    ma5 = _series_mean(adjusted_closes, 5)
    ma10 = _series_mean(adjusted_closes, 10)
    ma20 = _series_mean(adjusted_closes, 20)
    ma60 = _series_mean(adjusted_closes, 60)
    if current_close > ma5 > ma10 > ma20 > ma60:
        ma_alignment = 100.0
    elif current_close > ma10 > ma20 and ma5 > ma10:
        ma_alignment = 80.0
    elif current_close > ma20 and ma5 > ma20:
        ma_alignment = 65.0
    elif current_close > ma20:
        ma_alignment = 55.0
    else:
        ma_alignment = 20.0

    if highs.size > BREAKOUT_LOOKBACK_DAYS:
        previous_high = float(np.max(highs[-(BREAKOUT_LOOKBACK_DAYS + 1):-1]))
    else:
        previous_high = float(np.max(highs[:-1])) if highs.size > 1 else current_close

    if current_close > previous_high and volume_ratio >= 1.2:
        breakout = 100.0
    elif previous_high > 0 and current_close >= previous_high * 0.99 and volume_ratio >= 1.0:
        breakout = 75.0
    elif current_close > ma20:
        breakout = 55.0
    else:
        breakout = 20.0

    previous_ma20 = _series_mean(adjusted_closes[:-1], 20) if adjusted_closes.size > 1 else ma20
    if current_close > ma20 and ma20 >= previous_ma20:
        trend_strength = 100.0
    elif current_close > ma20:
        trend_strength = 80.0
    elif current_close >= ma20 * 0.98:
        trend_strength = 55.0
    else:
        trend_strength = 20.0

    return {
        "factors": {
            "macd": _score(macd_score),
            "rsi": _score(rsi_score),
            "ma_alignment": _score(ma_alignment),
            "breakout": _score(breakout),
            "trend_strength": _score(trend_strength),
        },
        "indicators": {
            "close": round(current_close, 4),
            "rsi": round(rsi_value, 2),
            "macd_line": round(macd_now, 4),
            "signal_line": round(signal_now, 4),
            "macd_hist": round(hist_now, 4),
            "ma5": round(ma5, 4),
            "ma10": round(ma10, 4),
            "ma20": round(ma20, 4),
            "ma60": round(ma60, 4),
            "breakout_level": round(previous_high, 4),
            "current_volume": round(current_volume, 2),
            "avg5_volume": round(avg5_volume, 2),
            "estimated_full_day_volume": round(estimated_full_day_volume, 2),
            "trading_progress_pct": round(trading_progress * 100, 2),
            "raw_volume_ratio": round(raw_volume_ratio, 3),
            "volume_ratio": round(volume_ratio, 3),
            "latest_high": round(float(highs[-1]), 4),
            "latest_low": round(float(lows[-1]), 4),
        },
    }
