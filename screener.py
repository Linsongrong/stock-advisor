# -*- coding: utf-8 -*-
"""Market screening and Top N selection with two-pass sentiment."""

from datetime import date
from typing import Any, Dict, List

from config import (
    DEFAULT_LOW_EXPOSURE,
    DEFAULT_MAX_PER_SECTOR,
    DEFAULT_MIN_MARKET_LIQUIDITY_RATIO,
    DEFAULT_MIN_BREADTH_ABOVE_MA20,
    DEFAULT_MIN_PRICE_FILTER,
    DEFAULT_SECTOR_FLOW_BONUS,
    DEFAULT_SECTOR_FLOW_TOP_N,
    FUNDAMENTAL_PREFETCH_POOL,
)
from factors.fundamental import batch_fetch_fundamentals
from factors.sentiment import batch_fetch_sentiment
from scorer import score_stock


def _selection_value(item: Dict[str, Any]) -> float:
    return float(item.get("selection_score", item.get("total_score", 0.0)) or 0.0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None, "--"):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _quote_amount(result: Dict[str, Any]) -> float:
    quote = result.get("quote", {}) if isinstance(result, dict) else {}
    price = _safe_float(result.get("price") or quote.get("price"))
    volume = _safe_float(quote.get("volume_shares") or quote.get("volume"))
    amount = _safe_float(quote.get("amount")) or (price * volume)
    return max(0.0, amount or price)


def _kline_amount(row: Dict[str, Any]) -> float:
    amount = _safe_float(row.get("amount"))
    if amount > 0:
        return amount
    close = _safe_float(row.get("close"))
    volume = _safe_float(row.get("volume"))
    return (close * volume) or close


def _average_history_amount(result: Dict[str, Any], window: int = 5) -> float:
    kline = result.get("kline", []) if isinstance(result, dict) else []
    if not kline:
        return 0.0
    history = kline[:-1] if len(kline) > 1 else kline
    amounts = [_kline_amount(item) for item in history[-window:]]
    amounts = [item for item in amounts if item > 0]
    return sum(amounts) / len(amounts) if amounts else 0.0


def _stock_amount_ratio(result: Dict[str, Any]) -> float:
    avg_amount = _average_history_amount(result)
    if avg_amount <= 0:
        return 1.0
    return _quote_amount(result) / avg_amount


def _is_actionable_sector(sector: str) -> bool:
    normalized = str(sector or "").strip().lower()
    if not normalized:
        return False
    return not (
        normalized == "unknown"
        or normalized.startswith("auto_broad_")
        or normalized.startswith("auto_")
    )


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


def _compute_market_liquidity(
    results: List[Dict[str, Any]],
    min_price_filter: float,
    min_liquidity_ratio: float,
) -> Dict[str, Any]:
    total_amount = 0.0
    total_avg_amount = 0.0
    positive_amount = 0.0
    sample_size = 0

    for result in results:
        price = _safe_float(result.get("price"))
        if min_price_filter > 0 and price < min_price_filter:
            continue

        amount = _quote_amount(result)
        avg_amount = _average_history_amount(result)
        if amount <= 0 or avg_amount <= 0:
            continue

        sample_size += 1
        total_amount += amount
        total_avg_amount += avg_amount
        if _safe_float(result.get("change_pct")) > 0:
            positive_amount += amount

    amount_ratio = total_amount / total_avg_amount if total_avg_amount > 0 else 0.0
    positive_amount_ratio = positive_amount / total_amount if total_amount > 0 else 0.0
    is_liquid = sample_size > 0 and amount_ratio >= max(0.0, float(min_liquidity_ratio))
    return {
        "sample_size": sample_size,
        "total_amount": round(total_amount, 2),
        "avg5_amount": round(total_avg_amount, 2),
        "amount_ratio": round(amount_ratio, 4),
        "positive_amount_ratio": round(positive_amount_ratio, 4),
        "min_liquidity_ratio": max(0.0, float(min_liquidity_ratio)),
        "status": "good" if is_liquid else "weak",
        "liquidity_gate_passed": is_liquid,
        "sector_flow_active": sample_size > 0,
    }


def _compute_sector_flows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sectors: Dict[str, Dict[str, Any]] = {}
    for result in results:
        sector = str(result.get("sector", "") or "unknown").strip() or "unknown"
        if not _is_actionable_sector(sector):
            continue
        amount = _quote_amount(result)
        if amount <= 0:
            continue
        bucket = sectors.setdefault(
            sector,
            {
                "sector": sector,
                "stock_count": 0,
                "amount": 0.0,
                "positive_amount": 0.0,
                "weighted_change": 0.0,
                "volume_ratio_sum": 0.0,
                "capital_score_sum": 0.0,
            },
        )
        bucket["stock_count"] += 1
        bucket["amount"] += amount
        if _safe_float(result.get("change_pct")) > 0:
            bucket["positive_amount"] += amount
        bucket["weighted_change"] += amount * _safe_float(result.get("change_pct"))
        bucket["volume_ratio_sum"] += _stock_amount_ratio(result)
        bucket["capital_score_sum"] += _safe_float(result.get("capital_score"))

    if not sectors:
        return []

    max_amount = max(float(item["amount"]) for item in sectors.values()) or 1.0
    rows: List[Dict[str, Any]] = []
    for bucket in sectors.values():
        stock_count = int(bucket["stock_count"]) or 1
        amount = float(bucket["amount"])
        positive_amount_ratio = float(bucket["positive_amount"]) / amount if amount > 0 else 0.0
        avg_change_pct = float(bucket["weighted_change"]) / amount if amount > 0 else 0.0
        avg_volume_ratio = float(bucket["volume_ratio_sum"]) / stock_count
        avg_capital_score = float(bucket["capital_score_sum"]) / stock_count
        amount_score = min(100.0, amount / max_amount * 100.0)
        volume_score = min(100.0, max(0.0, avg_volume_ratio) / 2.0 * 100.0)
        flow_score = (
            avg_capital_score * 0.35
            + positive_amount_ratio * 100.0 * 0.25
            + volume_score * 0.20
            + amount_score * 0.20
        )
        rows.append(
            {
                "sector": str(bucket["sector"]),
                "stock_count": stock_count,
                "amount": round(amount, 2),
                "positive_amount_ratio": round(positive_amount_ratio, 4),
                "avg_change_pct": round(avg_change_pct, 4),
                "avg_volume_ratio": round(avg_volume_ratio, 4),
                "avg_capital_score": round(avg_capital_score, 2),
                "flow_score": round(flow_score, 2),
            }
        )

    rows.sort(
        key=lambda item: (
            float(item.get("flow_score", 0.0)),
            float(item.get("amount", 0.0)),
            float(item.get("positive_amount_ratio", 0.0)),
        ),
        reverse=True,
    )
    for index, item in enumerate(rows, start=1):
        item["rank"] = index
    return rows


def _apply_sector_flow_adjustments(
    ranked: List[Dict[str, Any]],
    market_liquidity: Dict[str, Any],
    sector_flows: List[Dict[str, Any]],
    sector_flow_top_n: int,
    sector_flow_bonus: float,
) -> List[Dict[str, Any]]:
    sector_flow_top_n = max(0, int(sector_flow_top_n))
    sector_flow_bonus = max(0.0, float(sector_flow_bonus))
    sector_map = {str(item.get("sector", "")): item for item in sector_flows}
    leader_sectors = {str(item.get("sector", "")) for item in sector_flows[:sector_flow_top_n]}

    adjusted: List[Dict[str, Any]] = []
    for item in ranked:
        result = dict(item)
        base_selection = _selection_value(item)
        sector = str(result.get("sector", "") or "unknown").strip() or "unknown"
        flow = sector_map.get(sector, {})
        adjustment = 0.0
        if sector in leader_sectors and sector_flow_bonus > 0:
            adjustment = sector_flow_bonus * (_safe_float(flow.get("flow_score")) / 100.0)
        result["base_selection_score"] = round(base_selection, 2)
        result["sector_flow_score"] = _safe_float(flow.get("flow_score"))
        result["sector_flow_rank"] = int(flow.get("rank", 0) or 0)
        result["sector_flow_adjustment"] = round(adjustment, 2)
        result["selection_score"] = round(base_selection + adjustment, 2)
        adjusted.append(result)
    return adjusted


def _filter_to_strong_flow_sectors(
    ranked: List[Dict[str, Any]],
    sector_flows: List[Dict[str, Any]],
    sector_flow_top_n: int,
) -> List[Dict[str, Any]]:
    leader_sectors = {
        str(item.get("sector", ""))
        for item in sector_flows[: max(0, int(sector_flow_top_n))]
        if str(item.get("sector", "")).strip()
    }
    if not leader_sectors:
        return []
    return [
        item
        for item in ranked
        if str(item.get("sector", "") or "unknown").strip() in leader_sectors
    ]


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
    min_market_liquidity_ratio: float = DEFAULT_MIN_MARKET_LIQUIDITY_RATIO,
    sector_flow_top_n: int = DEFAULT_SECTOR_FLOW_TOP_N,
    sector_flow_bonus: float = DEFAULT_SECTOR_FLOW_BONUS,
) -> Dict[str, Any]:
    """Two-pass scoring: first pass without sentiment, then sentiment for top candidates."""
    filtered = []
    failed = list(market_data.get("failed", []))
    stocks = market_data.get("stocks", [])
    min_price_filter = max(0.0, float(min_price_filter))
    min_breadth_above_ma20 = max(0.0, float(min_breadth_above_ma20))
    low_exposure = max(0.0, min(1.0, float(low_exposure)))
    min_market_liquidity_ratio = max(0.0, float(min_market_liquidity_ratio))
    sector_flow_top_n = max(0, int(sector_flow_top_n))
    sector_flow_bonus = max(0.0, float(sector_flow_bonus))

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

        pass1_results.sort(key=lambda item: (_selection_value(item), item["capital_score"], item["technical_score"]), reverse=True)
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

        sector_flows = _compute_sector_flows(final_ranked)
        market_liquidity = _compute_market_liquidity(
            breadth_results,
            min_price_filter=min_price_filter,
            min_liquidity_ratio=min_market_liquidity_ratio,
        )
        final_ranked = _apply_sector_flow_adjustments(
            final_ranked,
            market_liquidity=market_liquidity,
            sector_flows=sector_flows,
            sector_flow_top_n=sector_flow_top_n,
            sector_flow_bonus=sector_flow_bonus,
        )
        final_ranked.sort(key=lambda item: (_selection_value(item), item["capital_score"], item["technical_score"]), reverse=True)
        filtered.sort(key=lambda item: item.get("technical_indicators", {}).get("rsi", 0.0), reverse=True)
        market_breadth = _compute_market_breadth(breadth_results, min_price_filter=min_price_filter)
        suggested_exposure = _suggest_exposure(
            market_breadth,
            min_breadth_above_ma20=min_breadth_above_ma20,
            low_exposure=low_exposure,
        )
        risk_off = suggested_exposure < 1.0
        strong_sector_ranked = _filter_to_strong_flow_sectors(
            final_ranked,
            sector_flows=sector_flows,
            sector_flow_top_n=sector_flow_top_n,
        )
        top_stocks = _select_top_with_sector_limit(
            strong_sector_ranked,
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
            "sector_flow_ranked_stocks": strong_sector_ranked,
            "filtered_stocks": filtered,
            "failed_stocks": failed,
            "max_per_sector": max(0, int(max_per_sector)),
            "min_price_filter": min_price_filter,
            "market_breadth": market_breadth,
            "min_breadth_above_ma20": min_breadth_above_ma20,
            "market_liquidity": market_liquidity,
            "sector_flows": sector_flows,
            "sector_flow_top_n": sector_flow_top_n,
            "sector_flow_bonus": sector_flow_bonus,
            "sector_flow_candidate_count": len(strong_sector_ranked),
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

    pass0_results.sort(key=lambda item: (_selection_value(item), item["capital_score"], item["technical_score"]), reverse=True)

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

    pass1_results.sort(key=lambda item: (_selection_value(item), item["capital_score"], item["technical_score"]), reverse=True)

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
    sector_flows = _compute_sector_flows(final_ranked)
    market_liquidity = _compute_market_liquidity(
        breadth_results,
        min_price_filter=min_price_filter,
        min_liquidity_ratio=min_market_liquidity_ratio,
    )
    final_ranked = _apply_sector_flow_adjustments(
        final_ranked,
        market_liquidity=market_liquidity,
        sector_flows=sector_flows,
        sector_flow_top_n=sector_flow_top_n,
        sector_flow_bonus=sector_flow_bonus,
    )
    final_ranked.sort(key=lambda item: (_selection_value(item), item["capital_score"], item["technical_score"]), reverse=True)
    filtered.sort(key=lambda item: item.get("technical_indicators", {}).get("rsi", 0.0), reverse=True)
    market_breadth = _compute_market_breadth(breadth_results, min_price_filter=min_price_filter)
    suggested_exposure = _suggest_exposure(
        market_breadth,
        min_breadth_above_ma20=min_breadth_above_ma20,
        low_exposure=low_exposure,
    )
    risk_off = suggested_exposure < 1.0

    strong_sector_ranked = _filter_to_strong_flow_sectors(
        final_ranked,
        sector_flows=sector_flows,
        sector_flow_top_n=sector_flow_top_n,
    )
    top_stocks = _select_top_with_sector_limit(
        strong_sector_ranked,
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
        "sector_flow_ranked_stocks": strong_sector_ranked,
        "filtered_stocks": filtered,
        "failed_stocks": failed,
        "max_per_sector": max(0, int(max_per_sector)),
        "min_price_filter": min_price_filter,
        "market_breadth": market_breadth,
        "min_breadth_above_ma20": min_breadth_above_ma20,
        "market_liquidity": market_liquidity,
        "sector_flows": sector_flows,
        "sector_flow_top_n": sector_flow_top_n,
        "sector_flow_bonus": sector_flow_bonus,
        "sector_flow_candidate_count": len(strong_sector_ranked),
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
