# -*- coding: utf-8 -*-
"""Market screening and Top N selection with two-pass sentiment."""

from datetime import date
from typing import Any, Dict

from factors.fundamental import batch_fetch_fundamentals
from factors.sentiment import batch_fetch_sentiment
from scorer import score_stock


def screen_market(market_data: Dict[str, Any], top_n: int, fetch_sentiment: bool = True) -> Dict[str, Any]:
    """Two-pass scoring: first pass without sentiment, then sentiment for top candidates."""
    ranked = []
    filtered = []
    failed = list(market_data.get("failed", []))
    stocks = market_data.get("stocks", [])

    # Batch fetch fundamentals for all stocks
    codes = [s.get("code", "") for s in stocks if s.get("code")]
    fund_map = batch_fetch_fundamentals(codes) if codes else {}

    # Pass 1: Score all stocks without sentiment
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

    # Sort by pass1 score
    pass1_results.sort(key=lambda item: item["total_score"], reverse=True)

    # Pass 2: Fetch sentiment only for top 30 candidates
    sentiment_map = {}
    if fetch_sentiment:
        sentiment_codes = [s["code"] for s in pass1_results[:30]]
        if sentiment_codes:
            sentiment_map = batch_fetch_sentiment(sentiment_codes)

    # Re-score top 30 with sentiment, keep the rest as-is
    final_ranked = []
    for i, result in enumerate(pass1_results):
        code = result["code"]
        if code in sentiment_map:
            result = score_stock(
                {"quote": result["quote"], "kline": result["kline"], "code": code, "name": result["name"], "sector": result["sector"]},
                fund_data=fund_map.get(code),
                sentiment_data=sentiment_map.get(code),
            )
        final_ranked.append(result)

    # Final sort
    final_ranked.sort(key=lambda item: (item["total_score"], item["capital_score"], item["technical_score"]), reverse=True)
    filtered.sort(key=lambda item: item.get("technical_indicators", {}).get("rsi", 0.0), reverse=True)

    return {
        "report_date": date.today().isoformat(),
        "source_status": market_data.get("source_status", {}),
        "universe_size": len(stocks) + len(market_data.get("failed", [])),
        "fetched_count": len(stocks),
        "failed_count": len(failed),
        "filtered_count": len(filtered),
        "qualified_count": len(final_ranked),
        "top_stocks": final_ranked[:top_n],
        "ranked_stocks": final_ranked,
        "filtered_stocks": filtered,
        "failed_stocks": failed,
    }
