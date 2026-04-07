# -*- coding: utf-8 -*-
"""Global configuration for the A-share stock advisor MVP."""

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
QUOTE_CACHE_DIR = DATA_DIR / "quotes"
KLINE_CACHE_DIR = DATA_DIR / "kline"
UNIVERSE_CACHE_PATH = DATA_DIR / "universe_cache.json"

REQUEST_TIMEOUT = 5
CONNECTIVITY_TIMEOUT = 3
QUOTE_CACHE_MAX_AGE_MINUTES = 30
KLINE_CACHE_MAX_AGE_HOURS = 24
UNIVERSE_CACHE_MAX_AGE_HOURS = 24
DEFAULT_TOP_N = 10
MAX_CHART_STOCKS = 3
MIN_KLINE_DAYS = 60
MAX_WORKERS = 12
PROBE_STOCK_CODE = "600519"

THS_REFERER = "https://stockpage.10jqka.com.cn/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def _normalize_factor_weights(weight_groups: dict) -> dict:
    """Normalize factor weights so the full model sums to exactly 1.0."""
    total = sum(sum(group.values()) for group in weight_groups.values())
    if total <= 0:
        raise ValueError("factor weights must sum to a positive number")

    return {
        group_name: {
            factor_name: weight / total
            for factor_name, weight in group.items()
        }
        for group_name, group in weight_groups.items()
    }


RAW_FACTOR_WEIGHTS = {
    "capital": {
        "net_inflow_rate": 0.22,
        "volume_ratio": 0.13,
        "turnover_anomaly": 0.08,
        "volume_breakout": 0.07,
    },
    "technical": {
        "macd": 0.09,
        "rsi": 0.09,
        "ma_alignment": 0.08,
        "breakout": 0.04,
        "trend_strength": 0.05,
    },
    "fundamental": {
        "pe_score": 0.04,
        "pb_score": 0.03,
        "roe_score": 0.03,
        "growth_score": 0.02,
    },
    "sentiment": {
        "sentiment": 0.07,
    },
}

RAW_TOTAL_WEIGHT = sum(sum(group.values()) for group in RAW_FACTOR_WEIGHTS.values())
FACTOR_WEIGHTS = _normalize_factor_weights(RAW_FACTOR_WEIGHTS)
CAPITAL_WEIGHT_TOTAL = sum(FACTOR_WEIGHTS["capital"].values())
TECHNICAL_WEIGHT_TOTAL = sum(FACTOR_WEIGHTS["technical"].values())
FUNDAMENTAL_WEIGHT_TOTAL = sum(FACTOR_WEIGHTS["fundamental"].values())
SENTIMENT_WEIGHT_TOTAL = sum(FACTOR_WEIGHTS["sentiment"].values())
TOTAL_WEIGHT = round(
    CAPITAL_WEIGHT_TOTAL + TECHNICAL_WEIGHT_TOTAL + FUNDAMENTAL_WEIGHT_TOTAL + SENTIMENT_WEIGHT_TOTAL,
    10,
)

RSI_FILTER_MAX = 80.0
CHART_LOOKBACK_DAYS = 30
BREAKOUT_LOOKBACK_DAYS = 20
VOLUME_AVG_DAYS = 5
SNAPSHOT_PREFILTER_MIN_UNIVERSE = 1000
SNAPSHOT_PREFILTER_TOP_AMOUNT = 1200
SNAPSHOT_PREFILTER_TOP_CHANGE = 1200
SNAPSHOT_PREFILTER_TOP_VOLUME = 1000
SNAPSHOT_PREFILTER_MAX_TARGETS = 1800
FUNDAMENTAL_PREFETCH_POOL = 0
SCREENING_KLINE_LOOKBACK_DAYS = 180


def ensure_runtime_directories() -> None:
    """Create data and report directories used by the application."""
    for directory in (DATA_DIR, REPORTS_DIR, QUOTE_CACHE_DIR, KLINE_CACHE_DIR):
        directory.mkdir(parents=True, exist_ok=True)
