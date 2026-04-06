# -*- coding: utf-8 -*-
"""Sentiment factor calculations backed by Sina stock-specific news pages."""

import html
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from config import DATA_DIR, REQUEST_TIMEOUT, USER_AGENT


SENTIMENT_CACHE_PATH = DATA_DIR / "sentiment_cache.json"
SENTIMENT_CACHE_MAX_AGE_HOURS = 6
SENTIMENT_NEWS_MAX_AGE_DAYS = 3
MAX_WORKERS = 8

BULLISH_KEYWORDS = [
    "大涨",
    "涨停",
    "突破",
    "新高",
    "利好",
    "增长",
    "超预期",
    "复苏",
    "升级",
    "订单",
    "中标",
    "合作",
    "获批",
    "放量",
    "买入",
    "增持",
    "回购",
    "分红",
    "业绩预增",
    "扭亏",
    "向好",
    "上调",
    "看多",
    "强势",
    "龙头",
    "领涨",
    "反攻",
    "反弹",
    "爆发",
    "高增",
    "景气",
    "提价",
    "改善",
]

BEARISH_KEYWORDS = [
    "大跌",
    "跌停",
    "暴跌",
    "新低",
    "利空",
    "下滑",
    "不及预期",
    "衰退",
    "减持",
    "抛售",
    "套现",
    "亏损",
    "预警",
    "缩量",
    "卖出",
    "清仓",
    "召回",
    "处罚",
    "调查",
    "违规",
    "下跌",
    "杀跌",
    "看空",
    "弱势",
    "领跌",
    "崩盘",
    "闪崩",
    "暴雷",
    "风险",
    "压力",
    "下调",
    "波动",
]


def _score(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def _cache_is_fresh(saved_at: str, max_age: timedelta) -> bool:
    try:
        saved = datetime.fromisoformat(saved_at)
    except (TypeError, ValueError):
        return False
    return datetime.now() - saved <= max_age


def _load_cache() -> Dict[str, Dict[str, Any]]:
    if not SENTIMENT_CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(SENTIMENT_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    items = payload.get("items", {})
    return items if isinstance(items, dict) else {}


def _save_cache(items: Dict[str, Dict[str, Any]]) -> None:
    SENTIMENT_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "items": items,
    }
    SENTIMENT_CACHE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _get_cached_entry(
    cache_items: Dict[str, Dict[str, Any]],
    code: str,
    max_age: timedelta,
    allow_stale: bool = False,
) -> Optional[Dict[str, Any]]:
    entry = cache_items.get(code)
    if not isinstance(entry, dict):
        return None

    data = entry.get("data")
    if not isinstance(data, dict):
        return None

    if not allow_stale and not _cache_is_fresh(entry.get("saved_at", ""), max_age):
        return None

    cached = dict(data)
    cached["source"] = "cache_stale" if allow_stale else "cache"
    return cached


def _set_cached_entry(cache_items: Dict[str, Dict[str, Any]], code: str, data: Dict[str, Any]) -> None:
    cache_items[code] = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "data": {key: value for key, value in data.items() if key != "source"},
    }


def _normalize_text(raw_text: str) -> str:
    text = html.unescape(raw_text or "")
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _count_sentiment(text: str) -> Dict[str, int]:
    bullish = sum(1 for keyword in BULLISH_KEYWORDS if keyword in text)
    bearish = sum(1 for keyword in BEARISH_KEYWORDS if keyword in text)
    return {"bullish": bullish, "bearish": bearish}


def _sentiment_score(bullish: int, bearish: int) -> float:
    total = bullish + bearish
    if total == 0:
        return 50.0

    ratio = bullish / total
    confidence = min(1.0, total / 3.0)
    volume_bonus = min(total, 5) / 5.0
    base = ratio * 100.0
    score = 50.0 + (base - 50.0) * confidence * volume_bonus
    return max(0.0, min(100.0, score))


def _fetch_stock_news_sina(code: str, count: int = 8) -> List[Dict[str, str]]:
    """Parse the stock-specific Sina news list instead of generic site search."""
    symbol = f"sh{code}" if str(code).startswith("6") else f"sz{code}"
    url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllNewsStock/symbol/{symbol}.phtml"
    headers = {"User-Agent": USER_AGENT}

    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    response.encoding = "gb2312"

    block_match = re.search(r'<div class="datelist">\s*<ul>(.*?)</ul>', response.text, re.S)
    if not block_match:
        return []

    cutoff = datetime.now() - timedelta(days=SENTIMENT_NEWS_MAX_AGE_DAYS)
    pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2})&nbsp;(\d{2}:\d{2})&nbsp;&nbsp;"
        r"<a[^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
        re.S,
    )

    articles: List[Dict[str, str]] = []
    seen_titles = set()
    for date_str, time_str, url, title_html in pattern.findall(block_match.group(1)):
        try:
            published_at = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            continue

        if published_at < cutoff:
            continue

        title = _normalize_text(title_html)
        if len(title) < 4 or title in seen_titles:
            continue

        seen_titles.add(title)
        articles.append(
            {
                "title": title,
                "published_at": published_at.isoformat(timespec="minutes"),
                "url": html.unescape(url),
            }
        )
        if len(articles) >= count:
            break

    return articles


def _fetch_stock_sentiment_live(code: str) -> Dict[str, Any]:
    articles = _fetch_stock_news_sina(code)
    combined_text = " ".join(article["title"] for article in articles)
    counts = _count_sentiment(combined_text)
    score = _sentiment_score(counts["bullish"], counts["bearish"])

    return {
        "code": code,
        "sentiment_score": _score(score),
        "bullish_hits": counts["bullish"],
        "bearish_hits": counts["bearish"],
        "news_count": len(articles),
        "has_data": len(articles) > 0,
        "source": "live",
    }


def fetch_stock_sentiment(code: str) -> Dict[str, Any]:
    """Fetch and score sentiment for a single stock with cache fallback."""
    max_age = timedelta(hours=SENTIMENT_CACHE_MAX_AGE_HOURS)
    cache_items = _load_cache()

    cached = _get_cached_entry(cache_items, code, max_age=max_age, allow_stale=False)
    if cached:
        return cached

    stale = _get_cached_entry(cache_items, code, max_age=max_age, allow_stale=True)
    try:
        result = _fetch_stock_sentiment_live(code)
        _set_cached_entry(cache_items, code, result)
        _save_cache(cache_items)
        return result
    except Exception:
        if stale:
            return stale
        return {
            "code": code,
            "sentiment_score": 50.0,
            "bullish_hits": 0,
            "bearish_hits": 0,
            "news_count": 0,
            "has_data": False,
            "source": "unavailable",
        }


def batch_fetch_sentiment(codes: List[str], max_workers: int = MAX_WORKERS) -> Dict[str, Dict[str, Any]]:
    """Fetch sentiment for multiple stocks in parallel with cache fallback."""
    unique_codes = [code for code in dict.fromkeys(codes) if code]
    if not unique_codes:
        return {}

    max_age = timedelta(hours=SENTIMENT_CACHE_MAX_AGE_HOURS)
    cache_items = _load_cache()
    result_map: Dict[str, Dict[str, Any]] = {}
    stale_map: Dict[str, Dict[str, Any]] = {}
    pending_codes: List[str] = []

    for code in unique_codes:
        cached = _get_cached_entry(cache_items, code, max_age=max_age, allow_stale=False)
        if cached:
            result_map[code] = cached
            continue

        stale = _get_cached_entry(cache_items, code, max_age=max_age, allow_stale=True)
        if stale:
            stale_map[code] = stale
        pending_codes.append(code)

    cache_updated = False
    if pending_codes:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_fetch_stock_sentiment_live, code): code for code in pending_codes}
            for future in as_completed(future_map):
                code = future_map[future]
                try:
                    data = future.result()
                except Exception:
                    if code in stale_map:
                        result_map[code] = stale_map[code]
                    continue

                result_map[code] = data
                _set_cached_entry(cache_items, code, data)
                cache_updated = True

    if cache_updated:
        try:
            _save_cache(cache_items)
        except OSError:
            pass

    for code in unique_codes:
        if code in result_map:
            continue
        if code in stale_map:
            result_map[code] = stale_map[code]
        else:
            result_map[code] = {
                "code": code,
                "sentiment_score": 50.0,
                "bullish_hits": 0,
                "bearish_hits": 0,
                "news_count": 0,
                "has_data": False,
                "source": "unavailable",
            }

    return result_map


def calculate_sentiment_factors(
    quote: Dict[str, Any],
    kline: List[Dict[str, Any]],
    code: str = "",
    sentiment_data: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Calculate sentiment score for a stock."""
    if not quote and not kline:
        return {"factors": {}, "metrics": {}}

    sentiment_data = sentiment_data or {}
    sentiment_score = float(sentiment_data.get("sentiment_score", 50.0))
    bullish_hits = int(sentiment_data.get("bullish_hits", 0))
    bearish_hits = int(sentiment_data.get("bearish_hits", 0))
    news_count = int(sentiment_data.get("news_count", 0))
    has_data = bool(sentiment_data.get("has_data", False))

    if not has_data:
        sentiment_score = 50.0

    data_source = sentiment_data.get("source", "unavailable")
    if not has_data and data_source == "live":
        data_source = "live_empty"

    return {
        "factors": {
            "sentiment": _score(sentiment_score),
        },
        "metrics": {
            "sentiment_score": round(sentiment_score, 2),
            "bullish_hits": bullish_hits,
            "bearish_hits": bearish_hits,
            "news_count": news_count,
            "data_source": data_source,
        },
    }
