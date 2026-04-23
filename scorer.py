# -*- coding: utf-8 -*-
"""Multi-factor weighted scoring engine."""

from typing import Any, Dict

from config import (
    CAPITAL_WEIGHT_TOTAL,
    FACTOR_WEIGHTS,
    FUNDAMENTAL_WEIGHT_TOTAL,
    SENTIMENT_WEIGHT_TOTAL,
    RSI_FILTER_MAX,
    TECHNICAL_WEIGHT_TOTAL,
)
from factors import (
    calculate_capital_factors,
    calculate_fundamental_factors,
    calculate_technical_factors,
    calculate_sentiment_factors,
)


def _weighted_score(factor_scores: Dict[str, float], weights: Dict[str, float]) -> Dict[str, float]:
    weighted_sum = sum(float(factor_scores.get(name, 0.0)) * weight for name, weight in weights.items())
    weight_total = sum(weights.values()) or 1.0
    normalized = weighted_sum / weight_total
    return {
        "weighted_sum": round(weighted_sum, 2),
        "normalized": round(normalized, 2),
    }


def compute_selection_adjustment(score_result: Dict[str, Any]) -> float:
    """Return a ranking adjustment that discourages chasing overextended moves."""
    technical_indicators = score_result.get("technical_indicators", {}) if isinstance(score_result, dict) else {}
    price = float(score_result.get("price", 0.0) or 0.0)
    change_pct = float(score_result.get("change_pct", 0.0) or 0.0)
    rsi_value = float(technical_indicators.get("rsi", 50.0) or 50.0)
    ma20 = float(technical_indicators.get("ma20", 0.0) or 0.0)
    ma60 = float(technical_indicators.get("ma60", 0.0) or 0.0)
    breakout_level = float(technical_indicators.get("breakout_level", 0.0) or 0.0)
    adjustment = 0.0

    premium_pct = ((price / ma20) - 1.0) * 100.0 if price > 0 and ma20 > 0 else 0.0
    if premium_pct >= 18.0:
        adjustment -= 18.0
    elif premium_pct >= 12.0:
        adjustment -= 11.0
    elif premium_pct >= 8.0:
        adjustment -= 6.0
    elif 1.0 <= premium_pct <= 5.0:
        adjustment += 2.0

    is_hot_setup = premium_pct >= 5.0 or change_pct >= 4.0
    if rsi_value >= 78.0 and is_hot_setup:
        adjustment -= 16.0
    elif rsi_value >= 72.0 and is_hot_setup:
        adjustment -= 10.0
    elif 45.0 <= rsi_value <= 62.0:
        adjustment += 3.0
    elif 62.0 < rsi_value <= 68.0:
        adjustment += 2.0

    if change_pct >= 9.0:
        adjustment -= 16.0
    elif change_pct >= 6.0:
        adjustment -= 10.0
    elif change_pct >= 4.0:
        adjustment -= 5.0
    elif -1.5 <= change_pct <= 3.0:
        adjustment += 1.5

    if breakout_level > 0 and price > breakout_level * 1.08:
        adjustment -= 6.0

    if price > 0 and ma60 > 0 and price < ma60:
        adjustment -= 5.0

    return round(adjustment, 2)


def score_stock(stock_bundle: Dict[str, Any], fund_data: Dict[str, float] = None, sentiment_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """Score a single stock bundle and apply the RSI filter."""
    quote = stock_bundle.get("quote")
    kline = stock_bundle.get("kline")
    code = stock_bundle.get("code", "")
    if not quote or not kline:
        return {
            "code": code,
            "name": stock_bundle.get("name", ""),
            "error": "missing_quote_or_kline",
        }

    capital_result = calculate_capital_factors(quote, kline)
    technical_result = calculate_technical_factors(quote, kline)
    fundamental_result = calculate_fundamental_factors(quote, kline, code=code, fund_data=fund_data)
    sentiment_result = calculate_sentiment_factors(quote, kline, code=code, sentiment_data=sentiment_data)

    capital_scores = capital_result.get("factors", {})
    technical_scores = technical_result.get("factors", {})
    fundamental_scores = fundamental_result.get("factors", {})
    sentiment_scores = sentiment_result.get("factors", {})

    if not capital_scores or not technical_scores:
        return {
            "code": code,
            "name": stock_bundle.get("name", ""),
            "error": "insufficient_factor_data",
        }

    capital_weighted = _weighted_score(capital_scores, FACTOR_WEIGHTS["capital"])
    technical_weighted = _weighted_score(technical_scores, FACTOR_WEIGHTS["technical"])
    fundamental_weighted = _weighted_score(fundamental_scores, FACTOR_WEIGHTS["fundamental"])
    sentiment_weighted = _weighted_score(sentiment_scores, FACTOR_WEIGHTS["sentiment"])

    total_score = round(
        capital_weighted["weighted_sum"]
        + technical_weighted["weighted_sum"]
        + fundamental_weighted["weighted_sum"]
        + sentiment_weighted["weighted_sum"],
        2,
    )
    selection_adjustment = compute_selection_adjustment(
        {
            "price": round(float(quote.get("price", 0.0)), 2),
            "change_pct": round(float(quote.get("change_pct", 0.0)), 2),
            "technical_indicators": technical_result.get("indicators", {}),
        }
    )

    rsi_value = float(technical_result.get("indicators", {}).get("rsi", 50.0))
    filtered_out = rsi_value > RSI_FILTER_MAX

    return {
        "code": code,
        "name": stock_bundle.get("name", quote.get("name", "")),
        "sector": stock_bundle.get("sector", ""),
        "price": round(float(quote.get("price", 0.0)), 2),
        "change_pct": round(float(quote.get("change_pct", 0.0)), 2),
        "capital_score": capital_weighted["normalized"],
        "technical_score": technical_weighted["normalized"],
        "fundamental_score": fundamental_weighted["normalized"],
        "sentiment_score": sentiment_weighted["normalized"],
        "total_score": total_score,
        "selection_adjustment": selection_adjustment,
        "selection_score": round(total_score + selection_adjustment, 2),
        "capital_factors": capital_scores,
        "technical_factors": technical_scores,
        "fundamental_factors": fundamental_scores,
        "sentiment_factors": sentiment_scores,
        "capital_metrics": capital_result.get("metrics", {}),
        "technical_indicators": technical_result.get("indicators", {}),
        "fundamental_metrics": fundamental_result.get("metrics", {}),
        "sentiment_metrics": sentiment_result.get("metrics", {}),
        "quote": quote,
        "kline": kline,
        "filtered_out": filtered_out,
        "filter_reason": f"RSI>{RSI_FILTER_MAX}" if filtered_out else "",
        "weight_totals": {
            "capital": CAPITAL_WEIGHT_TOTAL,
            "technical": TECHNICAL_WEIGHT_TOTAL,
            "fundamental": FUNDAMENTAL_WEIGHT_TOTAL,
            "sentiment": SENTIMENT_WEIGHT_TOTAL,
        },
    }
