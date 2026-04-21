# -*- coding: utf-8 -*-
"""Market screening and Top N selection with two-pass sentiment."""

from datetime import date
from typing import Any, Dict, List

from config import (
    DEFAULT_LOW_EXPOSURE,
    DEFAULT_MAX_PER_SECTOR,
    DEFAULT_MIN_BREADTH_ABOVE_MA20,
    DEFAULT_MIN_PRICE_FILTER,
    FUNDAMENTAL_PREFETCH_POOL,
)
from factors.fundamental import batch_fetch_fundamentals
from factors.sentiment import batch_fetch_sentiment
from scorer import score_stock


def _select_top_with_sector_limit(
    ranked: List[Dict[str, Any]],
    top_n: int,
    max_per_sector: int = DEFAULT_MAX_PER_SECTOR,
) -> List[Dict[str, Any]]:
    if top_n <= 0:
        return []
    if max_per_sector <= 0:
        return list(ranked[:top_n])

    selected: List[Dict[str, Any]] = []
    selected_codes = set()
    sector_counts: Dict[str, int] = {}

    for item in ranked:
        if len(selected) >= top_n:
            break
        code = item.get("code", "")
        if not code or code in selected_codes:
            continue

        sector = str(item.get("sector", "") or "").strip()
        if sector and sector_counts.get(sector, 0) >= max_per_sector:
            continue

        selected.append(item)
        selected_codes.add(code)
        if sector:
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

    if len(selected) >= top_n:
        return selected

    for item in ranked:
        if len(selected) >= top_n:
            break
        code = item.get("code", "")
        if not code or code in selected_codes:
            continue
        selected.append(item)
        selected_codes.add(code)

    return selected


def _fails_price_filter(result: Dict[str, Any], min_price_filter: float) -> bool:
    if min_price_filter <= 0:
        return False
    return float(result.get("price", 0.0) or 0.0) < float(min_price_filter)


def _mark_filtered(result: Dict[str, Any], reason: str) -> Dict[str, Any]:
    item = dict(result)
    item["filtered_out"] = True
    item["filter_reason"] = reason
    return item


def _compute_market_breadth(results: List[Dict[str, Any]], min_price_filter: float) -> Dict[str, float]:
    eligible = []
    for result in results:
        price = float(result.get("price", 0.0) or 0.0)
        if min_price_filter > 0 and price < min_price_filter:
            continue
        ma20 = float(result.get("technical_indicators", {}).get("ma20", 0.0) or 0.0)
        if ma20 <= 0:
            continue
        eligible.append(1.0 if price > ma20 else 0.0)

    total = len(eligible)
    return {
        "sample_size": total,
        "above_ma20_ratio": round(sum(eligible) / total, 4) if total else 0.0,
    }


def _suggest_exposure(
    market_breadth: Dict[str, float],
    min_breadth_above_ma20: float,
    low_exposure: float,
) -> float:
    if min_breadth_above_ma20 <= 0:
        return 1.0
    ratio = float(market_breadth.get("above_ma20_ratio", 0.0))
    return max(0.0, min(1.0, low_exposure)) if ratio < min_breadth_above_ma20 else 1.0


def screen_market(
    market_data: Dict[str, Any],
    top_n: int,
    fetch_sentiment: bool = True,
    max_per_sector: int = DEFAULT_MAX_PER_SECTOR,
    min_price_filter: float = DEFAULT_MIN_PRICE_FILTER,
    min_breadth_above_ma20: float = DEFAULT_MIN_BREADTH_ABOVE_MA20,
    low_exposure: float = DEFAULT_LOW_EXPOSURE,
) -> Dict[str, Any]:
    """Two-pass scoring: first pass without sentiment, then sentiment for top candidates."""
    filtered = []
    failed = list(market_data.get("failed", []))
    stocks = market_data.get("stocks", [])
    min_price_filter = max(0.0, float(min_price_filter))
    min_breadth_above_ma20 = max(0.0, float(min_breadth_above_ma20))
    low_exposure = max(0.0, min(1.0, float(low_exposure)))

    if FUNDAMENTAL_PREFETCH_POOL <= 0:
        codes = [item.get("code", "") for item in stocks if item.get("code")]
        fund_map = batch_fetch_fundamentals(codes) if codes else {}
        pass1_results = []
        breadth_results = []
        for stock_bundle in stocks:
            code = stock_bundle.get("code", "")
            try:
                result = score_stock(stock_bundle, fund_data=fund_map.get(code), sentiment_data=None)
            except Exception as exc:
                failed.append({"code": code, "name": stock_bundle.get("name", ""), "error": str(exc)})
                continue
            if result.get("error"):
                failed.append(result)
                continue
            breadth_results.append(result)
            if result.get("filtered_out"):
                filtered.append(result)
                continue
            if _fails_price_filter(result, min_price_filter):
                filtered.append(_mark_filtered(result, f"price<{float(min_price_filter):.2f}"))
                continue
            pass1_results.append(result)

        pass1_results.sort(key=lambda item: item["total_score"], reverse=True)
        sentiment_map = {}
        if fetch_sentiment:
            sentiment_codes = [s["code"] for s in pass1_results[:30]]
            if sentiment_codes:
                sentiment_map = batch_fetch_sentiment(sentiment_codes)

        final_ranked = []
        for result in pass1_results:
            code = result["code"]
            if code in sentiment_map:
                try:
                    rescored = score_stock(
                        {"quote": result["quote"], "kline": result["kline"], "code": code, "name": result["name"], "sector": result["sector"]},
                        fund_data=fund_map.get(code),
                        sentiment_data=sentiment_map.get(code),
                    )
                except Exception:
                    rescored = result
                if rescored.get("error") or rescored.get("filtered_out"):
                    rescored = result
                result = rescored
            final_ranked.append(result)

        final_ranked.sort(key=lambda item: (item["total_score"], item["capital_score"], item["technical_score"]), reverse=True)
        filtered.sort(key=lambda item: item.get("technical_indicators", {}).get("rsi", 0.0), reverse=True)
        market_breadth = _compute_market_breadth(breadth_results, min_price_filter=min_price_filter)
        suggested_exposure = _suggest_exposure(
            market_breadth,
            min_breadth_above_ma20=min_breadth_above_ma20,
            low_exposure=low_exposure,
        )
        risk_off = suggested_exposure < 1.0
        top_stocks = _select_top_with_sector_limit(
            final_ranked,
            top_n=top_n,
            max_per_sector=max(0, int(max_per_sector)),
        )
        if suggested_exposure <= 0:
            top_stocks = []
        risk_regime = "off" if suggested_exposure <= 0 else ("reduced" if risk_off else "on")
        return {
            "report_date": date.today().isoformat(),
            "universe_source": market_data.get("universe_source", ""),
            "source_status": market_data.get("source_status", {}),
            "source_usage": market_data.get("source_usage", {}),
            "prefilter": market_data.get("prefilter", {}),
            "fundamental_pool": {
                "selected_count": len(pass1_results),
                "input_count": len(pass1_results),
            },
            "universe_size": int(market_data.get("total_universe_size", len(stocks) + len(market_data.get("failed", [])))),
            "fetched_count": len(stocks),
            "failed_count": len(failed),
            "filtered_count": len(filtered),
            "qualified_count": len(final_ranked),
            "top_stocks": top_stocks,
            "ranked_stocks": final_ranked,
            "filtered_stocks": filtered,
            "failed_stocks": failed,
            "max_per_sector": max(0, int(max_per_sector)),
            "min_price_filter": min_price_filter,
            "market_breadth": market_breadth,
            "min_breadth_above_ma20": min_breadth_above_ma20,
            "low_exposure": low_exposure,
            "suggested_exposure": suggested_exposure,
            "risk_off": risk_off,
            "risk_regime": risk_regime,
            "risk_off_reason": (
                f"breadth_above_ma20<{min_breadth_above_ma20:.2f}"
                if risk_off
                else ""
            ),
        }

    # Pass 0: lightweight ranking without fetched fundamentals or sentiment
    pass0_results = []
    breadth_results = []
    for stock_bundle in stocks:
        code = stock_bundle.get("code", "")
        try:
            result = score_stock(stock_bundle, fund_data=None, sentiment_data=None)
        except Exception as exc:
            failed.append({"code": code, "name": stock_bundle.get("name", ""), "error": str(exc)})
            continue
        if result.get("error"):
            failed.append(result)
            continue
        breadth_results.append(result)
        if result.get("filtered_out"):
            filtered.append(result)
            continue
        if _fails_price_filter(result, min_price_filter):
            filtered.append(_mark_filtered(result, f"price<{float(min_price_filter):.2f}"))
            continue
        pass0_results.append(result)

    pass0_results.sort(key=lambda item: item["total_score"], reverse=True)

    # Pass 1: fetch fundamentals only for the top pre-ranked candidate pool
    if FUNDAMENTAL_PREFETCH_POOL > 0:
        fundamental_pool_size = min(len(pass0_results), max(int(top_n) * 20, FUNDAMENTAL_PREFETCH_POOL))
    else:
        fundamental_pool_size = len(pass0_results)
    fundamental_codes = [item["code"] for item in pass0_results[:fundamental_pool_size]]
    fund_map = batch_fetch_fundamentals(fundamental_codes) if fundamental_codes else {}

    pass1_results = []
    for result in pass0_results:
        code = result["code"]
        if code not in fund_map:
            pass1_results.append(result)
            continue
        try:
            rescored = score_stock(
                {"quote": result["quote"], "kline": result["kline"], "code": code, "name": result["name"], "sector": result["sector"]},
                fund_data=fund_map.get(code),
                sentiment_data=None,
            )
        except Exception:
            rescored = result
        if rescored.get("error") or rescored.get("filtered_out"):
            rescored = result
        pass1_results.append(rescored)

    pass1_results.sort(key=lambda item: item["total_score"], reverse=True)

    # Pass 2: Fetch sentiment only for the top 30 candidates after fundamentals
    sentiment_map = {}
    if fetch_sentiment:
        sentiment_codes = [s["code"] for s in pass1_results[:30]]
        if sentiment_codes:
            sentiment_map = batch_fetch_sentiment(sentiment_codes)

    final_ranked = []
    for result in pass1_results:
        code = result["code"]
        if code in sentiment_map:
            try:
                rescored = score_stock(
                    {"quote": result["quote"], "kline": result["kline"], "code": code, "name": result["name"], "sector": result["sector"]},
                    fund_data=fund_map.get(code),
                    sentiment_data=sentiment_map.get(code),
                )
            except Exception:
                rescored = result
            if rescored.get("error") or rescored.get("filtered_out"):
                rescored = result
            result = rescored
        final_ranked.append(result)

    # Final sort
    final_ranked.sort(key=lambda item: (item["total_score"], item["capital_score"], item["technical_score"]), reverse=True)
    filtered.sort(key=lambda item: item.get("technical_indicators", {}).get("rsi", 0.0), reverse=True)
    market_breadth = _compute_market_breadth(breadth_results, min_price_filter=min_price_filter)
    suggested_exposure = _suggest_exposure(
        market_breadth,
        min_breadth_above_ma20=min_breadth_above_ma20,
        low_exposure=low_exposure,
    )
    risk_off = suggested_exposure < 1.0

    top_stocks = _select_top_with_sector_limit(
        final_ranked,
        top_n=top_n,
        max_per_sector=max(0, int(max_per_sector)),
    )
    if suggested_exposure <= 0:
        top_stocks = []
    risk_regime = "off" if suggested_exposure <= 0 else ("reduced" if risk_off else "on")
    return {
        "report_date": date.today().isoformat(),
        "universe_source": market_data.get("universe_source", ""),
        "source_status": market_data.get("source_status", {}),
        "source_usage": market_data.get("source_usage", {}),
        "prefilter": market_data.get("prefilter", {}),
        "fundamental_pool": {
            "selected_count": fundamental_pool_size,
            "input_count": len(pass0_results),
        },
        "universe_size": int(market_data.get("total_universe_size", len(stocks) + len(market_data.get("failed", [])))),
        "fetched_count": len(stocks),
        "failed_count": len(failed),
        "filtered_count": len(filtered),
        "qualified_count": len(final_ranked),
        "top_stocks": top_stocks,
        "ranked_stocks": final_ranked,
        "filtered_stocks": filtered,
        "failed_stocks": failed,
        "max_per_sector": max(0, int(max_per_sector)),
        "min_price_filter": min_price_filter,
        "market_breadth": market_breadth,
        "min_breadth_above_ma20": min_breadth_above_ma20,
        "low_exposure": low_exposure,
        "suggested_exposure": suggested_exposure,
        "risk_off": risk_off,
        "risk_regime": risk_regime,
        "risk_off_reason": (
            f"breadth_above_ma20<{min_breadth_above_ma20:.2f}"
            if risk_off
            else ""
        ),
    }
