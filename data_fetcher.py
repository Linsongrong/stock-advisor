# -*- coding: utf-8 -*-
"""Unified market data fetching layer using Tencent and THS endpoints."""

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    CONNECTIVITY_TIMEOUT,
    KLINE_CACHE_DIR,
    KLINE_CACHE_MAX_AGE_HOURS,
    MAX_WORKERS,
    MIN_KLINE_DAYS,
    PROBE_STOCK_CODE,
    QUOTE_CACHE_DIR,
    QUOTE_CACHE_MAX_AGE_MINUTES,
    REQUEST_TIMEOUT,
    SCREENING_KLINE_LOOKBACK_DAYS,
    SNAPSHOT_PREFILTER_MAX_TARGETS,
    SNAPSHOT_PREFILTER_MIN_UNIVERSE,
    SNAPSHOT_PREFILTER_TOP_AMOUNT,
    SNAPSHOT_PREFILTER_TOP_CHANGE,
    SNAPSHOT_PREFILTER_TOP_VOLUME,
    THS_REFERER,
    USER_AGENT,
    ensure_runtime_directories,
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert a value to float safely."""
    try:
        if value in ("", None, "--"):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """Convert a value to int safely."""
    try:
        if value in ("", None, "--"):
            return default
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return default


def _parse_date_label(raw_value: str) -> str:
    """Normalize raw date strings into YYYY-MM-DD when possible."""
    digits = re.sub(r"[^0-9]", "", str(raw_value or ""))
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return str(raw_value or "")


def _has_intraday_timestamp(raw_value: Any) -> bool:
    return len(re.sub(r"[^0-9]", "", str(raw_value or ""))) >= 12


def _cache_is_fresh(saved_at: str, max_age: timedelta) -> bool:
    """Return True when a cache timestamp is within max_age."""
    try:
        saved = datetime.fromisoformat(saved_at)
    except (TypeError, ValueError):
        return False
    return datetime.now() - saved <= max_age


class MarketDataFetcher:
    """Fetch quotes and K-line data with cache fallback."""

    def __init__(self) -> None:
        ensure_runtime_directories()
        self.quote_headers = {
            "User-Agent": USER_AGENT,
            "Referer": "https://gu.qq.com/",
        }
        self.ths_headers = {
            "User-Agent": USER_AGENT,
            "Referer": THS_REFERER,
        }
        self.session = requests.Session()
        retry = Retry(
            total=1,
            connect=1,
            read=1,
            status=1,
            backoff_factor=0.2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=max(16, MAX_WORKERS),
            pool_maxsize=max(16, MAX_WORKERS * 2),
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @staticmethod
    def _get_market_symbol(code: str) -> str:
        return f"sh{code}" if str(code).startswith("6") else f"sz{code}"

    @staticmethod
    def _read_cache(cache_path: Path) -> Optional[Dict[str, Any]]:
        if not cache_path.exists():
            return None
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    @staticmethod
    def _write_cache(cache_path: Path, data: Dict[str, Any]) -> None:
        payload = {
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "data": data,
        }
        cache_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _normalize_quote_payload(
        quote: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(quote, dict):
            return None

        normalized = dict(quote)
        stored_unit = str(normalized.get("volume_unit") or "").lower()
        stored_volume_shares = _safe_int(normalized.get("volume_shares"))
        if stored_unit == "shares" and stored_volume_shares > 0:
            volume_shares = stored_volume_shares
            input_volume = _safe_int(normalized.get("volume_input"), default=stored_volume_shares)
            input_unit = str(normalized.get("volume_input_unit") or "shares").lower()
        else:
            input_volume = _safe_int(normalized.get("volume_input"), default=_safe_int(normalized.get("volume")))
            input_unit = str(normalized.get("volume_input_unit") or normalized.get("volume_unit") or "").lower()
            if input_unit not in {"shares", "lots"}:
                if _has_intraday_timestamp(normalized.get("updated_at")):
                    input_unit = "lots"
                else:
                    input_unit = "shares"
            volume_shares = input_volume * 100 if input_unit == "lots" else input_volume

        normalized["volume"] = volume_shares
        normalized["volume_shares"] = volume_shares
        normalized["volume_input"] = input_volume
        normalized["volume_input_unit"] = input_unit
        normalized["volume_unit"] = "shares"
        return normalized

    @staticmethod
    def _build_cache_meta(cache_payload: Dict[str, Any], max_age: timedelta) -> Dict[str, Any]:
        saved_at = cache_payload.get("saved_at", "")
        is_fresh = _cache_is_fresh(saved_at, max_age)
        return {
            "cache_saved_at": saved_at,
            "cache_is_fresh": is_fresh,
            "cache_source": "cache_fresh" if is_fresh else "cache_stale",
        }

    def _get_quote_cache_entry(self, code: str, fresh_only: bool = False) -> Optional[Dict[str, Any]]:
        cache_payload = self._read_cache(QUOTE_CACHE_DIR / f"{code}.json")
        if not cache_payload:
            return None
        meta = self._build_cache_meta(
            cache_payload,
            timedelta(minutes=QUOTE_CACHE_MAX_AGE_MINUTES),
        )
        if fresh_only and not meta["cache_is_fresh"]:
            return None
        return {
            "meta": meta,
            "data": cache_payload.get("data"),
        }

    def _get_quote_cache(self, code: str, fresh_only: bool = False) -> Optional[Dict[str, Any]]:
        cache_entry = self._get_quote_cache_entry(code, fresh_only=fresh_only)
        if not cache_entry:
            return None
        data = cache_entry.get("data")
        meta = cache_entry.get("meta", {})
        if isinstance(data, dict):
            result = self._normalize_quote_payload(data)
            if result is None:
                return None
            result["cache_origin_source"] = data.get("source", "")
            result["source"] = meta.get("cache_source", "cache")
            result["cache_saved_at"] = meta.get("cache_saved_at", "")
            result["cache_is_fresh"] = meta.get("cache_is_fresh", False)
            return result
        return None

    def _get_kline_cache_entry(self, code: str, fresh_only: bool = False) -> Optional[Dict[str, Any]]:
        cache_payload = self._read_cache(KLINE_CACHE_DIR / f"{code}.json")
        if not cache_payload:
            return None
        meta = self._build_cache_meta(
            cache_payload,
            timedelta(hours=KLINE_CACHE_MAX_AGE_HOURS),
        )
        if fresh_only and not meta["cache_is_fresh"]:
            return None
        return {
            "meta": meta,
            "data": cache_payload.get("data"),
        }

    def _get_kline_cache(self, code: str, fresh_only: bool = False) -> Optional[List[Dict[str, Any]]]:
        cache_entry = self._get_kline_cache_entry(code, fresh_only=fresh_only)
        if not cache_entry:
            return None
        data = cache_entry.get("data")
        if isinstance(data, list):
            return data
        return None

    def fetch_quote_live(self, code: str, timeout: int = REQUEST_TIMEOUT) -> Optional[Dict[str, Any]]:
        """Fetch real-time quote data from Tencent Finance."""
        symbol = self._get_market_symbol(code)
        url = f"https://qt.gtimg.cn/q={symbol}"

        response = self.session.get(url, headers=self.quote_headers, timeout=timeout)
        response.raise_for_status()
        response.encoding = "gbk"  # Force gbk; apparent_encoding misdetects as shift_jis

        match = re.search(r'"([^"]+)"', response.text)
        if not match:
            return None

        parts = match.group(1).split("~")
        if len(parts) <= 32:
            return None

        price = _safe_float(parts[3])
        prev_close = _safe_float(parts[4])
        open_price = _safe_float(parts[5])
        if price <= 0 or prev_close < 0:
            return None

        quote = {
            "code": parts[2] or code,
            "name": parts[1] or "",
            "price": price,
            "prev_close": prev_close,
            "open": open_price,
            "volume": _safe_int(parts[6]),
            "amount": _safe_float(parts[37]) * 10000 if len(parts) > 37 else 0.0,
            "change_amount": _safe_float(parts[31]),
            "change_pct": _safe_float(parts[32]),
            "updated_at": parts[30] if len(parts) > 30 else "",
            "volume_input_unit": "lots",
            "source": "live",
        }
        return self._normalize_quote_payload(quote)

    @staticmethod
    def _extract_json_payload(text: str) -> Optional[Dict[str, Any]]:
        match = re.search(r"\((\{.*\})\)\s*;?\s*$", text, re.S)
        if not match:
            return None
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _select_kline_container(payload: Dict[str, Any], code: str) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None

        stock_key = f"hs_{code}"
        if stock_key in payload and isinstance(payload[stock_key], dict):
            return payload[stock_key]
        if "data" in payload:
            return payload
        for value in payload.values():
            if isinstance(value, dict) and "data" in value:
                return value
        return None

    @staticmethod
    def _parse_kline_ohlc(parts: List[str]) -> Optional[Dict[str, float]]:
        if len(parts) < 5:
            return None

        p_open = _safe_float(parts[1])
        p2 = _safe_float(parts[2])
        p3 = _safe_float(parts[3])
        p4 = _safe_float(parts[4])
        if p_open <= 0:
            return None

        candidate_a = {"open": p_open, "high": p2, "low": p3, "close": p4}
        candidate_b = {"open": p_open, "close": p2, "high": p3, "low": p4}

        def is_valid(ohlc: Dict[str, float]) -> bool:
            return (
                ohlc["high"] >= max(ohlc["open"], ohlc["close"], ohlc["low"])
                and ohlc["low"] <= min(ohlc["open"], ohlc["close"], ohlc["high"])
            )

        if is_valid(candidate_a) and not is_valid(candidate_b):
            return candidate_a
        if is_valid(candidate_b) and not is_valid(candidate_a):
            return candidate_b
        if is_valid(candidate_a):
            return candidate_a
        if is_valid(candidate_b):
            return candidate_b
        return None

    def fetch_kline_live(
        self,
        code: str,
        timeout: int = REQUEST_TIMEOUT,
        prefer_long_history: bool = False,
        min_rows: int = MIN_KLINE_DAYS,
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch daily K-line data from THS JSONP endpoints."""
        urls = [
            f"https://d.10jqka.com.cn/v6/line/hs_{code}/01/last.js",
            f"https://d.10jqka.com.cn/v6/line/hs_{code}/01/last36000.js",
        ]
        best_rows: List[Dict[str, Any]] = []

        for url in urls:
            try:
                response = self.session.get(url, headers=self.ths_headers, timeout=timeout)
            except requests.RequestException:
                continue

            if response.status_code != 200:
                continue

            response.encoding = response.apparent_encoding or response.encoding or "gbk"
            payload = self._extract_json_payload(response.text)
            if not payload:
                continue

            container = self._select_kline_container(payload, code)
            if not container:
                continue

            raw_rows = container.get("data", [])
            rows: List[Dict[str, Any]] = []

            if isinstance(raw_rows, str):
                data_rows = [row for row in raw_rows.strip().strip(";").split(";") if row.strip()]
            elif isinstance(raw_rows, list):
                data_rows = raw_rows
            else:
                data_rows = []

            for raw_row in data_rows:
                parts = raw_row.split(",") if isinstance(raw_row, str) else [str(item) for item in raw_row]
                if len(parts) < 7:
                    continue
                ohlc = self._parse_kline_ohlc(parts)
                if not ohlc:
                    continue
                rows.append(
                    {
                        "date": _parse_date_label(parts[0]),
                        "open": ohlc["open"],
                        "close": ohlc["close"],
                        "high": ohlc["high"],
                        "low": ohlc["low"],
                        "volume": _safe_int(parts[5]),
                        "amount": _safe_float(parts[6]),
                    }
                )

            if rows:
                rows.sort(key=lambda item: item["date"])
                if not prefer_long_history and len(rows) >= max(1, int(min_rows)):
                    return rows
                if len(rows) > len(best_rows):
                    best_rows = rows
        return best_rows or None

    def refresh_kline_cache_for_universe(
        self,
        universe: List[Dict[str, str]],
        max_workers: int = MAX_WORKERS,
        max_stocks: int = 0,
    ) -> Dict[str, Any]:
        """Refresh local K-line cache using the longest live history available."""
        targets = [stock for stock in universe if stock.get("code")]
        if max_stocks > 0:
            targets = targets[:max_stocks]

        updated_codes: List[str] = []
        failed_codes: List[str] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(self.fetch_kline_live, stock["code"], REQUEST_TIMEOUT, True): stock["code"]
                for stock in targets
            }
            for future in as_completed(future_map):
                code = future_map[future]
                try:
                    rows = future.result()
                except Exception:
                    rows = None

                if not rows:
                    failed_codes.append(code)
                    continue

                try:
                    self._write_cache(KLINE_CACHE_DIR / f"{code}.json", rows)
                    updated_codes.append(code)
                except OSError:
                    failed_codes.append(code)

        updated_codes.sort()
        failed_codes.sort()
        return {
            "requested_count": len(targets),
            "updated_count": len(updated_codes),
            "failed_count": len(failed_codes),
            "updated_codes": updated_codes,
            "failed_codes": failed_codes,
        }

    @staticmethod
    def _source_meta(source: str, cache_saved_at: str = "", cache_is_fresh: bool = False) -> Dict[str, Any]:
        return {
            "source": source,
            "cache_saved_at": cache_saved_at,
            "cache_is_fresh": cache_is_fresh,
        }

    def get_quote_with_meta(
        self,
        code: str,
        allow_live: bool = True,
        prefer_cache: bool = False,
    ) -> tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        """Get quote data plus source metadata."""
        fresh_cache = self._get_quote_cache(code, fresh_only=True)
        if prefer_cache and fresh_cache is not None:
            return fresh_cache, self._source_meta(
                fresh_cache.get("source", "cache_fresh"),
                cache_saved_at=fresh_cache.get("cache_saved_at", ""),
                cache_is_fresh=bool(fresh_cache.get("cache_is_fresh", False)),
            )

        if allow_live:
            try:
                live_data = self.fetch_quote_live(code)
                if live_data:
                    self._write_cache(QUOTE_CACHE_DIR / f"{code}.json", live_data)
                    return live_data, self._source_meta("live", cache_is_fresh=True)
            except requests.RequestException:
                pass
            except OSError:
                pass

        if fresh_cache is not None:
            return fresh_cache, self._source_meta(
                fresh_cache.get("source", "cache_fresh"),
                cache_saved_at=fresh_cache.get("cache_saved_at", ""),
                cache_is_fresh=bool(fresh_cache.get("cache_is_fresh", False)),
            )

        stale_cache = self._get_quote_cache(code, fresh_only=False)
        if stale_cache is not None:
            return stale_cache, self._source_meta(
                stale_cache.get("source", "cache_stale"),
                cache_saved_at=stale_cache.get("cache_saved_at", ""),
                cache_is_fresh=bool(stale_cache.get("cache_is_fresh", False)),
            )

        return None, self._source_meta("unavailable")

    def get_quote(self, code: str, allow_live: bool = True) -> Optional[Dict[str, Any]]:
        """Get quote data using live request first, then cache fallback."""
        quote, _ = self.get_quote_with_meta(code, allow_live=allow_live, prefer_cache=False)
        return quote

    def get_kline_with_meta(
        self,
        code: str,
        allow_live: bool = True,
        prefer_cache: bool = False,
        prefer_long_history: bool = False,
    ) -> tuple[Optional[List[Dict[str, Any]]], Dict[str, Any]]:
        """Get daily K-line data plus source metadata."""
        fresh_cache_entry = self._get_kline_cache_entry(code, fresh_only=True)
        if prefer_cache and fresh_cache_entry and isinstance(fresh_cache_entry.get("data"), list):
            meta = fresh_cache_entry.get("meta", {})
            return fresh_cache_entry["data"], self._source_meta(
                meta.get("cache_source", "cache_fresh"),
                cache_saved_at=meta.get("cache_saved_at", ""),
                cache_is_fresh=bool(meta.get("cache_is_fresh", False)),
            )

        if allow_live:
            try:
                live_data = self.fetch_kline_live(code, prefer_long_history=prefer_long_history)
                if live_data:
                    if prefer_long_history:
                        self._write_cache(KLINE_CACHE_DIR / f"{code}.json", live_data)
                    return live_data, self._source_meta("live", cache_is_fresh=True)
            except requests.RequestException:
                pass
            except OSError:
                pass

        if fresh_cache_entry and isinstance(fresh_cache_entry.get("data"), list):
            meta = fresh_cache_entry.get("meta", {})
            return fresh_cache_entry["data"], self._source_meta(
                meta.get("cache_source", "cache_fresh"),
                cache_saved_at=meta.get("cache_saved_at", ""),
                cache_is_fresh=bool(meta.get("cache_is_fresh", False)),
            )

        stale_cache_entry = self._get_kline_cache_entry(code, fresh_only=False)
        if stale_cache_entry and isinstance(stale_cache_entry.get("data"), list):
            meta = stale_cache_entry.get("meta", {})
            return stale_cache_entry["data"], self._source_meta(
                meta.get("cache_source", "cache_stale"),
                cache_saved_at=meta.get("cache_saved_at", ""),
                cache_is_fresh=bool(meta.get("cache_is_fresh", False)),
            )

        return None, self._source_meta("unavailable")

    def get_kline(
        self,
        code: str,
        allow_live: bool = True,
        prefer_long_history: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        """Get daily K-line data using live request first, then cache fallback."""
        kline, _ = self.get_kline_with_meta(
            code,
            allow_live=allow_live,
            prefer_cache=False,
            prefer_long_history=prefer_long_history,
        )
        return kline

    @staticmethod
    def _build_quote_from_kline(
        code: str,
        fallback_name: str,
        kline: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if len(kline) < 2:
            return None
        latest = kline[-1]
        previous = kline[-2]
        prev_close = _safe_float(previous.get("close"))
        price = _safe_float(latest.get("close"))
        if price <= 0:
            return None

        change_pct = ((price / prev_close) - 1.0) * 100 if prev_close > 0 else 0.0
        return {
            "code": code,
            "name": fallback_name,
            "price": price,
            "prev_close": prev_close,
            "open": _safe_float(latest.get("open")),
            "volume": _safe_int(latest.get("volume")),
            "volume_shares": _safe_int(latest.get("volume")),
            "volume_input": _safe_int(latest.get("volume")),
            "volume_input_unit": "shares",
            "volume_unit": "shares",
            "change_amount": price - prev_close,
            "change_pct": round(change_pct, 2),
            "updated_at": latest.get("date", ""),
            "source": "derived_kline",
        }

    def _get_prefetched_quote(self, stock: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        snapshot = stock.get("quote_snapshot")
        snapshot_saved_at = str(stock.get("quote_snapshot_saved_at", "")).strip()
        if not isinstance(snapshot, dict):
            return None
        if snapshot_saved_at and not _cache_is_fresh(snapshot_saved_at, timedelta(minutes=QUOTE_CACHE_MAX_AGE_MINUTES)):
            return None

        quote = self._normalize_quote_payload(snapshot)
        if quote is None:
            return None
        return quote

    @staticmethod
    def _trim_kline_for_screening(
        rows: Optional[List[Dict[str, Any]]],
        keep_rows: int = SCREENING_KLINE_LOOKBACK_DAYS,
    ) -> Optional[List[Dict[str, Any]]]:
        if rows is None:
            return None
        if keep_rows <= 0 or len(rows) <= keep_rows:
            return rows
        return rows[-keep_rows:]

    def _get_prefilter_seed(self, stock: Dict[str, Any]) -> tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        prefetched = self._get_prefetched_quote(stock)
        if prefetched is not None:
            return prefetched, stock

        cached_quote = self._get_quote_cache(stock.get("code", ""), fresh_only=True)
        if cached_quote is None:
            return None, stock

        enriched = dict(stock)
        enriched["quote_snapshot"] = {
            key: value
            for key, value in cached_quote.items()
            if key not in {"cache_origin_source", "cache_saved_at", "cache_is_fresh"}
        }
        enriched["quote_snapshot_saved_at"] = str(cached_quote.get("cache_saved_at", "")).strip()
        return cached_quote, enriched

    def probe_sources(self, code: str = PROBE_STOCK_CODE) -> Dict[str, bool]:
        """Probe the upstream APIs to avoid long waits in offline environments."""
        status = {"quote": False, "kline": False}
        try:
            status["quote"] = self.fetch_quote_live(code, timeout=CONNECTIVITY_TIMEOUT) is not None
        except requests.RequestException:
            status["quote"] = False
        try:
            status["kline"] = self.fetch_kline_live(code, timeout=CONNECTIVITY_TIMEOUT) is not None
        except requests.RequestException:
            status["kline"] = False
        return status

    @staticmethod
    def _empty_usage_summary() -> Dict[str, int]:
        return {
            "live": 0,
            "universe_snapshot": 0,
            "cache_fresh": 0,
            "cache_stale": 0,
            "derived_kline": 0,
            "unavailable": 0,
        }

    @staticmethod
    def _record_source_usage(summary: Dict[str, int], source: str) -> None:
        key = source if source in summary else "unavailable"
        summary[key] = summary.get(key, 0) + 1

    def _select_prefilter_targets(self, universe: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        if len(universe) < SNAPSHOT_PREFILTER_MIN_UNIVERSE:
            return universe, {
                "enabled": False,
                "reason": "universe_below_threshold",
                "input_count": len(universe),
                "seed_quote_count": 0,
                "selected_count": len(universe),
            }

        snapshot_pairs: List[tuple[Dict[str, Any], Dict[str, Any]]] = []
        for stock in universe:
            quote, target_stock = self._get_prefilter_seed(stock)
            if quote is None:
                continue
            if float(quote.get("price", 0.0) or 0.0) <= 0:
                continue
            snapshot_pairs.append((target_stock, quote))

        if len(snapshot_pairs) < SNAPSHOT_PREFILTER_MIN_UNIVERSE:
            return universe, {
                "enabled": False,
                "reason": "insufficient_snapshots",
                "input_count": len(universe),
                "seed_quote_count": len(snapshot_pairs),
                "selected_count": len(universe),
            }

        score_map: Dict[str, float] = {}
        stock_map: Dict[str, Dict[str, Any]] = {}

        def _apply_rank(
            pairs: List[tuple[Dict[str, Any], Dict[str, Any]]],
            limit: int,
            weight: float,
        ) -> None:
            if limit <= 0:
                return
            capped = pairs[:limit]
            size = len(capped)
            if size == 0:
                return
            for index, (stock, _) in enumerate(capped):
                code = stock["code"]
                stock_map[code] = stock
                score_map[code] = score_map.get(code, 0.0) + weight * (size - index) / size

        by_amount = sorted(
            snapshot_pairs,
            key=lambda item: (
                float(item[1].get("amount", 0.0) or (float(item[1].get("price", 0.0) or 0.0) * float(item[1].get("volume", 0.0) or 0.0))),
                float(item[1].get("change_pct", 0.0) or 0.0),
            ),
            reverse=True,
        )
        by_change = sorted(
            snapshot_pairs,
            key=lambda item: (
                float(item[1].get("change_pct", 0.0) or 0.0),
                float(item[1].get("amount", 0.0) or 0.0),
            ),
            reverse=True,
        )
        by_volume = sorted(
            snapshot_pairs,
            key=lambda item: (
                float(item[1].get("volume", 0.0) or 0.0),
                float(item[1].get("amount", 0.0) or 0.0),
            ),
            reverse=True,
        )

        _apply_rank(by_amount, SNAPSHOT_PREFILTER_TOP_AMOUNT, 1.0)
        _apply_rank(by_change, SNAPSHOT_PREFILTER_TOP_CHANGE, 0.8)
        _apply_rank(by_volume, SNAPSHOT_PREFILTER_TOP_VOLUME, 0.6)

        ranked = sorted(
            stock_map.values(),
            key=lambda stock: (-score_map.get(stock["code"], 0.0), stock["code"]),
        )
        if SNAPSHOT_PREFILTER_MAX_TARGETS > 0:
            ranked = ranked[:SNAPSHOT_PREFILTER_MAX_TARGETS]

        return ranked, {
            "enabled": len(ranked) < len(universe),
            "reason": "quote_prefilter",
            "input_count": len(universe),
            "seed_quote_count": len(snapshot_pairs),
            "selected_count": len(ranked),
        }

    def fetch_stock_bundle(
        self,
        stock: Dict[str, str],
        source_status: Optional[Dict[str, bool]] = None,
    ) -> Dict[str, Any]:
        """Fetch both quote and K-line data for a single stock."""
        source_status = source_status or {"quote": True, "kline": True}
        code = stock["code"]
        fallback_name = stock.get("name", "")

        kline, kline_meta = self.get_kline_with_meta(
            code,
            allow_live=True,
            prefer_cache=True,
            prefer_long_history=False,
        )
        kline = self._trim_kline_for_screening(kline)
        quote = self._get_prefetched_quote(stock)
        if quote is not None:
            try:
                self._write_cache(QUOTE_CACHE_DIR / f"{code}.json", quote)
            except OSError:
                pass
            quote_meta = self._source_meta(
                "universe_snapshot",
                cache_saved_at=str(stock.get("quote_snapshot_saved_at", "")),
                cache_is_fresh=True,
            )
        else:
            quote, quote_meta = self.get_quote_with_meta(
                code,
                allow_live=True,
                prefer_cache=True,
            )

        if quote is None and kline:
            quote = self._build_quote_from_kline(code, fallback_name, kline)
            quote_meta = self._source_meta(
                "derived_kline",
                cache_saved_at=kline_meta.get("cache_saved_at", ""),
                cache_is_fresh=bool(kline_meta.get("cache_is_fresh", False)),
            )
        elif quote is not None:
            quote = self._normalize_quote_payload(quote)

        if kline and quote:
            return {
                "code": code,
                "name": quote.get("name") or fallback_name,
                "sector": stock.get("sector", ""),
                "quote": quote,
                "kline": kline,
                "data_sources": {
                    "quote": quote_meta,
                    "kline": kline_meta,
                },
            }

        error = []
        if not quote:
            error.append("quote_unavailable")
        if not kline:
            error.append("kline_unavailable")
        return {
            "code": code,
            "name": fallback_name,
            "sector": stock.get("sector", ""),
            "error": ",".join(error) if error else "unknown_error",
            "data_sources": {
                "quote": quote_meta,
                "kline": kline_meta,
            },
        }

    def fetch_universe_data(
        self,
        universe: List[Dict[str, str]],
        max_workers: int = MAX_WORKERS,
    ) -> Dict[str, Any]:
        """Fetch market data for the hardcoded stock universe."""
        source_status = self.probe_sources()
        fetch_targets, prefilter = self._select_prefilter_targets(list(universe))
        source_usage = {
            "quote": self._empty_usage_summary(),
            "kline": self._empty_usage_summary(),
        }
        stocks: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(self.fetch_stock_bundle, stock, source_status): stock
                for stock in fetch_targets
            }
            for future in as_completed(future_map):
                stock = future_map[future]
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover - defensive runtime guard
                    failed.append(
                        {
                            "code": stock["code"],
                            "name": stock.get("name", ""),
                            "sector": stock.get("sector", ""),
                            "error": str(exc),
                        }
                    )
                    continue

                data_sources = result.get("data_sources", {})
                self._record_source_usage(
                    source_usage["quote"],
                    str(data_sources.get("quote", {}).get("source", "unavailable")),
                )
                self._record_source_usage(
                    source_usage["kline"],
                    str(data_sources.get("kline", {}).get("source", "unavailable")),
                )

                if "error" in result:
                    failed.append(result)
                else:
                    stocks.append(result)

        stocks.sort(key=lambda item: item["code"])
        failed.sort(key=lambda item: item["code"])
        return {
            "total_universe_size": len(universe),
            "source_status": source_status,
            "source_usage": source_usage,
            "prefilter": prefilter,
            "stocks": stocks,
            "failed": failed,
        }
