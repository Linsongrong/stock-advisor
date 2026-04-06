# -*- coding: utf-8 -*-
"""Fundamental factor calculations backed by Sina Finance jsvar data."""

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from config import DATA_DIR, REQUEST_TIMEOUT, USER_AGENT


FUNDAMENTAL_CACHE_PATH = DATA_DIR / "fundamental_cache.json"
FUNDAMENTAL_CACHE_MAX_AGE_HOURS = 24
JSVAR_URL_TEMPLATE = "https://finance.sina.com.cn/realstock/company/{symbol}/jsvar.js"


def _score(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if value in ("", None, "--", "-"):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _cache_is_fresh(saved_at: str, max_age: timedelta) -> bool:
    try:
        saved = datetime.fromisoformat(saved_at)
    except (TypeError, ValueError):
        return False
    return datetime.now() - saved <= max_age


def _load_cache() -> Dict[str, Dict[str, Any]]:
    if not FUNDAMENTAL_CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(FUNDAMENTAL_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    items = payload.get("items", {})
    return items if isinstance(items, dict) else {}


def _save_cache(items: Dict[str, Dict[str, Any]]) -> None:
    FUNDAMENTAL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "items": items,
    }
    FUNDAMENTAL_CACHE_PATH.write_text(
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


def _extract_jsvar_float(text: str, field_name: str) -> Optional[float]:
    match = re.search(rf"var\s+{re.escape(field_name)}\s*=\s*([^;]+);", text)
    if not match:
        return None
    raw_value = match.group(1).strip().strip("'").strip('"')
    return _safe_float(raw_value, default=None)


def _fetch_single_fundamental(code: str) -> Dict[str, Any]:
    """Fetch raw fundamental metrics from Sina Finance jsvar.js."""
    symbol = f"sh{code}" if str(code).startswith("6") else f"sz{code}"
    headers = {"User-Agent": USER_AGENT, "Referer": f"https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml"}
    result: Dict[str, Any] = {
        "code": code,
        "eps_ttm": None,
        "eps_last_year": None,
        "bvps": None,
        "roe": None,
        "source": "unavailable",
    }

    response = requests.get(
        JSVAR_URL_TEMPLATE.format(symbol=symbol),
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    response.encoding = "gb2312"

    eps_ttm = _extract_jsvar_float(response.text, "fourQ_mgsy")
    eps_last_year = _extract_jsvar_float(response.text, "lastyear_mgsy")
    bvps = _extract_jsvar_float(response.text, "mgjzc")

    roe = None
    if eps_ttm is not None and bvps and bvps > 0:
        roe = round(eps_ttm / bvps * 100, 2)

    result.update(
        {
            "eps_ttm": eps_ttm,
            "eps_last_year": eps_last_year,
            "bvps": bvps,
            "roe": roe,
            "source": "live" if any(value is not None for value in (eps_ttm, bvps, roe)) else "unavailable",
        }
    )
    return result


def batch_fetch_fundamentals(codes: List[str], max_workers: int = 12) -> Dict[str, Dict[str, Any]]:
    """Fetch fundamentals for multiple stocks in parallel with cache fallback."""
    unique_codes = [code for code in dict.fromkeys(codes) if code]
    if not unique_codes:
        return {}

    max_age = timedelta(hours=FUNDAMENTAL_CACHE_MAX_AGE_HOURS)
    cache_items = _load_cache()
    fund_map: Dict[str, Dict[str, Any]] = {}
    stale_map: Dict[str, Dict[str, Any]] = {}
    pending_codes: List[str] = []

    for code in unique_codes:
        cached = _get_cached_entry(cache_items, code, max_age=max_age, allow_stale=False)
        if cached:
            fund_map[code] = cached
            continue

        stale = _get_cached_entry(cache_items, code, max_age=max_age, allow_stale=True)
        if stale:
            stale_map[code] = stale
        pending_codes.append(code)

    cache_updated = False
    if pending_codes:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_fetch_single_fundamental, code): code for code in pending_codes}
            for future in as_completed(future_map):
                code = future_map[future]
                try:
                    result = future.result()
                except Exception:
                    if code in stale_map:
                        fund_map[code] = stale_map[code]
                    continue

                if any(result.get(field) is not None for field in ("eps_ttm", "bvps", "roe")):
                    fund_map[code] = result
                    _set_cached_entry(cache_items, code, result)
                    cache_updated = True
                elif code in stale_map:
                    fund_map[code] = stale_map[code]
                else:
                    fund_map[code] = result

    if cache_updated:
        try:
            _save_cache(cache_items)
        except OSError:
            pass

    return fund_map


def _derive_pe(price: float, eps_ttm: Optional[float]) -> Optional[float]:
    if price <= 0 or eps_ttm is None:
        return None
    if eps_ttm == 0:
        return None
    return round(price / eps_ttm, 2)


def _derive_pb(price: float, bvps: Optional[float]) -> Optional[float]:
    if price <= 0 or bvps is None or bvps <= 0:
        return None
    return round(price / bvps, 2)


def _score_growth(kline: List[Dict[str, Any]], eps_growth: Optional[float]) -> float:
    if eps_growth is not None:
        if eps_growth >= 50:
            return 95.0
        if eps_growth >= 20:
            return 85.0
        if eps_growth >= 10:
            return 70.0
        if eps_growth >= 0:
            return 60.0
        if eps_growth >= -10:
            return 40.0
        return 20.0

    if kline and len(kline) >= 20:
        closes = [float(item.get("close", 0) or 0) for item in kline]
        if closes[-1] > 0 and closes[-20] > 0:
            momentum = (closes[-1] / closes[-20] - 1) * 100
            if momentum > 10:
                return 90.0
            if momentum > 5:
                return 75.0
            if momentum > 0:
                return 60.0
            if momentum > -5:
                return 40.0
    return 20.0


def calculate_fundamental_factors(
    quote: Dict[str, Any],
    kline: List[Dict[str, Any]],
    code: str = "",
    fund_data: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Calculate PE, PB, ROE, and growth scores for a stock."""
    if not quote and not kline:
        return {"factors": {}, "metrics": {}}

    fund_data = fund_data or {}
    price = _safe_float(quote.get("price"), default=0.0) or 0.0
    eps_ttm = _safe_float(fund_data.get("eps_ttm"), default=None)
    eps_last_year = _safe_float(fund_data.get("eps_last_year"), default=None)
    bvps = _safe_float(fund_data.get("bvps"), default=None)
    roe = _safe_float(fund_data.get("roe"), default=None)

    pe = _derive_pe(price, eps_ttm)
    pb = _derive_pb(price, bvps)

    eps_growth = None
    if eps_ttm is not None and eps_last_year is not None:
        if eps_last_year > 0:
            eps_growth = round((eps_ttm / eps_last_year - 1.0) * 100, 2)
        elif eps_last_year < 0 and eps_ttm > 0:
            eps_growth = 100.0

    if pe is not None and pe > 0:
        if pe <= 15:
            pe_score = 100.0
        elif pe <= 25:
            pe_score = 70.0
        elif pe <= 40:
            pe_score = 40.0
        else:
            pe_score = 20.0
    elif pe is not None and pe < 0:
        pe_score = 0.0
    else:
        pe_score = 50.0

    if pb is not None and pb > 0:
        if pb <= 1:
            pb_score = 90.0
        elif pb <= 3:
            pb_score = 70.0
        elif pb <= 5:
            pb_score = 40.0
        else:
            pb_score = 20.0
    else:
        pb_score = 50.0

    if roe is not None:
        if roe >= 20:
            roe_score = 100.0
        elif roe >= 15:
            roe_score = 80.0
        elif roe >= 10:
            roe_score = 60.0
        elif roe >= 5:
            roe_score = 40.0
        else:
            roe_score = 20.0
    else:
        roe_score = 50.0

    growth_score = _score_growth(kline, eps_growth)
    data_source = fund_data.get("source", "unavailable")
    if data_source == "unavailable" and any(value is not None for value in (pe, pb, roe)):
        data_source = "live"

    return {
        "factors": {
            "pe_score": _score(pe_score),
            "pb_score": _score(pb_score),
            "roe_score": _score(roe_score),
            "growth_score": _score(growth_score),
        },
        "metrics": {
            "pe": pe,
            "pb": pb,
            "roe": roe,
            "eps_ttm": eps_ttm,
            "eps_last_year": eps_last_year,
            "eps_growth_pct": eps_growth,
            "bvps": bvps,
            "data_source": data_source,
        },
    }
