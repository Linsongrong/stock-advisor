# -*- coding: utf-8 -*-
"""Market screening and Top N selection with two-pass sentiment."""

from datetime import date
from typing import Any, Dict

from config import FUNDAMENTAL_PREFETCH_POOL
from factors.fundamental import batch_fetch_fundamentals
from factors.sentiment import batch_fetch_sentiment
from scorer import score_stock


def screen_market(market_data: Dict[str, Any], top_n: int, fetch_sentiment: bool = True) -> Dict[str, Any]:
    """Two-pass scoring: first pass without sentiment, then sentiment for top candidates."""
    filtered = []
    failed = list(market_data.get("failed", []))
    stocks = market_data.get("stocks", [])

    if FUNDAMENTAL_PREFETCH_POOL <= 0:
        codes = [item.get("code", "") for item in stocks if item.get("code")]
        fund_map = batch_fetch_fundamentals(codes) if codes else {}
        pass1_results = []
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
            if result.get("filtered_out"):
                filtered.append(result)
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
            "top_stocks": final_ranked[:top_n],
            "ranked_stocks": final_ranked,
            "filtered_stocks": filtered,
            "failed_stocks": failed,
        }

    # Pass 0: lightweight ranking without fetched fundamentals or sentiment
    pass0_results = []
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
        if result.get("filtered_out"):
            filtered.append(result)
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
        "top_stocks": final_ranked[:top_n],
        "ranked_stocks": final_ranked,
        "filtered_stocks": filtered,
        "failed_stocks": failed,
    }
