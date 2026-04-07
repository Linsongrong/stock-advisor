# -*- coding: utf-8 -*-
"""Unified market data fetching layer using Tencent and THS endpoints."""

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from config import (
    CONNECTIVITY_TIMEOUT,
    KLINE_CACHE_DIR,
    KLINE_CACHE_MAX_AGE_HOURS,
    MAX_WORKERS,
    PROBE_STOCK_CODE,
    QUOTE_CACHE_DIR,
    QUOTE_CACHE_MAX_AGE_MINUTES,
    REQUEST_TIMEOUT,
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

    def _get_quote_cache(self, code: str, fresh_only: bool = False) -> Optional[Dict[str, Any]]:
        cache_payload = self._read_cache(QUOTE_CACHE_DIR / f"{code}.json")
        if not cache_payload:
            return None
        is_fresh = _cache_is_fresh(
            cache_payload.get("saved_at", ""),
            timedelta(minutes=QUOTE_CACHE_MAX_AGE_MINUTES),
        )
        if fresh_only and not is_fresh:
            return None
        data = cache_payload.get("data")
        if isinstance(data, dict):
            result = dict(data)
            result["source"] = "cache"
            return result
        return None

    def _get_kline_cache(self, code: str, fresh_only: bool = False) -> Optional[List[Dict[str, Any]]]:
        cache_payload = self._read_cache(KLINE_CACHE_DIR / f"{code}.json")
        if not cache_payload:
            return None
        is_fresh = _cache_is_fresh(
            cache_payload.get("saved_at", ""),
            timedelta(hours=KLINE_CACHE_MAX_AGE_HOURS),
        )
        if fresh_only and not is_fresh:
            return None
        data = cache_payload.get("data")
        if isinstance(data, list):
            return data
        return None

    def fetch_quote_live(self, code: str, timeout: int = REQUEST_TIMEOUT) -> Optional[Dict[str, Any]]:
        """Fetch real-time quote data from Tencent Finance."""
        symbol = self._get_market_symbol(code)
        url = f"https://qt.gtimg.cn/q={symbol}"

        response = requests.get(url, headers=self.quote_headers, timeout=timeout)
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
            "change_amount": _safe_float(parts[31]),
            "change_pct": _safe_float(parts[32]),
            "updated_at": parts[30] if len(parts) > 30 else "",
            "source": "live",
        }
        return quote

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

    def fetch_kline_live(self, code: str, timeout: int = REQUEST_TIMEOUT) -> Optional[List[Dict[str, Any]]]:
        """Fetch daily K-line data from THS JSONP endpoints."""
        urls = [
            f"https://d.10jqka.com.cn/v6/line/hs_{code}/01/last.js",
            f"https://d.10jqka.com.cn/v6/line/hs_{code}/01/last36000.js",
        ]
        best_rows: List[Dict[str, Any]] = []

        for url in urls:
            try:
                response = requests.get(url, headers=self.ths_headers, timeout=timeout)
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
                executor.submit(self.fetch_kline_live, stock["code"]): stock["code"]
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

    def get_quote(self, code: str, allow_live: bool = True) -> Optional[Dict[str, Any]]:
        """Get quote data using live request first, then cache fallback."""
        if allow_live:
            try:
                live_data = self.fetch_quote_live(code)
                if live_data:
                    self._write_cache(QUOTE_CACHE_DIR / f"{code}.json", live_data)
                    return live_data
            except requests.RequestException:
                pass
            except OSError:
                pass

        return self._get_quote_cache(code, fresh_only=False)

    def get_kline(self, code: str, allow_live: bool = True) -> Optional[List[Dict[str, Any]]]:
        """Get daily K-line data using live request first, then cache fallback."""
        if allow_live:
            try:
                live_data = self.fetch_kline_live(code)
                if live_data:
                    self._write_cache(KLINE_CACHE_DIR / f"{code}.json", live_data)
                    return live_data
            except requests.RequestException:
                pass
            except OSError:
                pass

        return self._get_kline_cache(code, fresh_only=False)

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
            "change_amount": price - prev_close,
            "change_pct": round(change_pct, 2),
            "updated_at": latest.get("date", ""),
            "source": "derived_kline",
        }

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

    def fetch_stock_bundle(
        self,
        stock: Dict[str, str],
        source_status: Optional[Dict[str, bool]] = None,
    ) -> Dict[str, Any]:
        """Fetch both quote and K-line data for a single stock."""
        source_status = source_status or {"quote": True, "kline": True}
        code = stock["code"]
        fallback_name = stock.get("name", "")

        kline = self.get_kline(code, allow_live=source_status.get("kline", True))
        quote = self.get_quote(code, allow_live=source_status.get("quote", True))

        if quote is None and kline:
            quote = self._build_quote_from_kline(code, fallback_name, kline)

        if kline and quote:
            return {
                "code": code,
                "name": quote.get("name") or fallback_name,
                "sector": stock.get("sector", ""),
                "quote": quote,
                "kline": kline,
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
        }

    def fetch_universe_data(
        self,
        universe: List[Dict[str, str]],
        max_workers: int = MAX_WORKERS,
    ) -> Dict[str, Any]:
        """Fetch market data for the hardcoded stock universe."""
        source_status = self.probe_sources()
        stocks: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(self.fetch_stock_bundle, stock, source_status): stock
                for stock in universe
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

                if "error" in result:
                    failed.append(result)
                else:
                    stocks.append(result)

        stocks.sort(key=lambda item: item["code"])
        failed.sort(key=lambda item: item["code"])
        return {
            "source_status": source_status,
            "stocks": stocks,
            "failed": failed,
        }
