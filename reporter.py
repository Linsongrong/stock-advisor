# -*- coding: utf-8 -*-
"""Markdown report generation and K-line chart rendering."""

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Fix CJK font on Windows
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
import numpy as np
from matplotlib.patches import Rectangle

from config import CHART_LOOKBACK_DAYS, MAX_CHART_STOCKS, REPORTS_DIR, ensure_runtime_directories


def _moving_average(values: List[float], period: int) -> List[float]:
    result: List[float] = []
    for index in range(len(values)):
        start = max(0, index - period + 1)
        window = values[start : index + 1]
        result.append(sum(window) / len(window))
    return result


def _prepare_chart_rows(stock: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = [dict(item) for item in stock.get("kline", [])]
    if not rows:
        return rows

    quote = stock.get("quote", {})
    last_row = rows[-1]
    live_price = float(quote.get("price") or 0)
    live_open = float(quote.get("open") or 0)
    live_volume = int(quote.get("volume") or 0)

    if live_price > 0:
        last_row["close"] = live_price
        last_row["high"] = max(float(last_row.get("high", live_price)), live_price, live_open or live_price)
        last_row["low"] = min(float(last_row.get("low", live_price)), live_price, live_open or live_price)
    if live_open > 0:
        last_row["open"] = live_open
    if live_volume > 0:
        last_row["volume"] = live_volume
    return rows


def render_kline_chart(stock: Dict[str, Any], rank: int, report_date: str) -> str:
    """Render a 30-day candlestick chart with MA5/10/20 and volume bars."""
    ensure_runtime_directories()
    chart_rows = _prepare_chart_rows(stock)
    if not chart_rows:
        return ""

    rows = chart_rows[-CHART_LOOKBACK_DAYS:]
    closes = [float(item.get("close", 0) or 0) for item in chart_rows]
    ma5 = _moving_average(closes, 5)[-len(rows):]
    ma10 = _moving_average(closes, 10)[-len(rows):]
    ma20 = _moving_average(closes, 20)[-len(rows):]

    x_values = np.arange(len(rows))
    fig, (ax_price, ax_volume) = plt.subplots(
        2,
        1,
        figsize=(12, 7),
        gridspec_kw={"height_ratios": [3, 1]},
        sharex=True,
    )

    for idx, row in enumerate(rows):
        open_price = float(row.get("open", 0) or 0)
        close_price = float(row.get("close", 0) or 0)
        high_price = float(row.get("high", 0) or 0)
        low_price = float(row.get("low", 0) or 0)
        volume = float(row.get("volume", 0) or 0)

        color = "#c62828" if close_price >= open_price else "#2e7d32"
        ax_price.vlines(idx, low_price, high_price, color=color, linewidth=1.0)
        body_low = min(open_price, close_price)
        body_height = max(abs(close_price - open_price), 0.01)
        rect = Rectangle(
            (idx - 0.3, body_low),
            0.6,
            body_height,
            facecolor=color,
            edgecolor=color,
            alpha=0.75,
        )
        ax_price.add_patch(rect)
        ax_volume.bar(idx, volume, color=color, width=0.6, alpha=0.45)

    ax_price.plot(x_values, ma5, color="#1565c0", linewidth=1.2, label="MA5")
    ax_price.plot(x_values, ma10, color="#ef6c00", linewidth=1.2, label="MA10")
    ax_price.plot(x_values, ma20, color="#6a1b9a", linewidth=1.2, label="MA20")
    ax_price.set_title(f"{stock['name']} ({stock['code']}) - Last 30 Trading Days")
    ax_price.set_ylabel("Price")
    ax_price.grid(alpha=0.2)
    ax_price.legend(loc="upper left")

    ax_volume.set_ylabel("Volume")
    ax_volume.grid(alpha=0.2)
    label_step = max(1, len(rows) // 6)
    ax_volume.set_xticks(x_values[::label_step])
    ax_volume.set_xticklabels([row["date"][5:] for row in rows][::label_step], rotation=30)

    fig.tight_layout()
    chart_name = f"{report_date}_{rank:02d}_{stock['code']}.png"
    chart_path = REPORTS_DIR / chart_name
    fig.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return chart_name


def _build_table_rows(stocks: List[Dict[str, Any]]) -> List[str]:
    header = "| Rank | Code | Name | Price | Change% | Cap | Tech | Fund | Sent | Total |"
    separator = "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    lines = [header, separator]
    for index, stock in enumerate(stocks, start=1):
        row = (
            "| {rank} | {code} | {name} | {price:.2f} | {change:+.2f}% "
            "| {capital:.1f} | {technical:.1f} | {fundamental:.1f} "
            "| {sentiment:.1f} | {total:.1f} |"
        ).format(
            rank=index,
            code=stock["code"],
            name=stock["name"],
            price=stock["price"],
            change=stock["change_pct"],
            capital=stock["capital_score"],
            technical=stock["technical_score"],
            fundamental=stock.get("fundamental_score", 0),
            sentiment=stock.get("sentiment_score", 0),
            total=stock["total_score"],
        )
        lines.append(row)
    return lines


def _format_source_usage(source_usage: Dict[str, Dict[str, int]], source_name: str) -> str:
    usage = source_usage.get(source_name, {}) if isinstance(source_usage, dict) else {}
    return (
        "{name}: live={live}, snapshot={snapshot}, cache_fresh={fresh}, cache_stale={stale}, derived={derived}, unavailable={unavailable}".format(
            name=source_name,
            live=int(usage.get("live", 0)),
            snapshot=int(usage.get("universe_snapshot", 0)),
            fresh=int(usage.get("cache_fresh", 0)),
            stale=int(usage.get("cache_stale", 0)),
            derived=int(usage.get("derived_kline", 0)),
            unavailable=int(usage.get("unavailable", 0)),
        )
    )


def generate_report(screened_data: Dict[str, Any], top_n: int) -> Path:
    """Generate the markdown report and save charts for the Top 3 picks."""
    ensure_runtime_directories()
    report_date = screened_data.get("report_date") or date.today().isoformat()
    report_path = REPORTS_DIR / f"{report_date}.md"
    top_stocks = screened_data.get("top_stocks", [])[:top_n]
    source_usage = screened_data.get("source_usage", {})
    prefilter = screened_data.get("prefilter", {})
    fundamental_pool = screened_data.get("fundamental_pool", {})

    chart_names: Dict[str, str] = {}
    for rank, stock in enumerate(top_stocks[:MAX_CHART_STOCKS], start=1):
        try:
            chart_name = render_kline_chart(stock, rank, report_date)
        except Exception:
            chart_name = ""
        chart_names[stock["code"]] = chart_name

    lines = [
        f"# A-share Stock Recommendation Report - {report_date}",
        "",
        "## Summary",
        f"- Universe size: {screened_data.get('universe_size', 0)}",
        f"- Universe source: {screened_data.get('universe_source') or 'unknown'}",
        "- Quote prefilter: {status} ({selected}/{input_count}, seed_quotes={seed_quotes})".format(
            status="on" if prefilter.get("enabled") else "off",
            selected=int(prefilter.get("selected_count", screened_data.get("universe_size", 0)) or 0),
            input_count=int(prefilter.get("input_count", screened_data.get("universe_size", 0)) or 0),
            seed_quotes=int(prefilter.get("seed_quote_count", 0) or 0),
        ),
        "- Fundamental pool: {selected}/{input_count}".format(
            selected=int(fundamental_pool.get("selected_count", screened_data.get("fetched_count", 0)) or 0),
            input_count=int(fundamental_pool.get("input_count", screened_data.get("fetched_count", 0)) or 0),
        ),
        f"- Successfully fetched: {screened_data.get('fetched_count', 0)}",
        f"- Qualified after filters: {screened_data.get('qualified_count', 0)}",
        f"- Filtered out by RSI > 80: {screened_data.get('filtered_count', 0)}",
        f"- Failed fetch or scoring: {screened_data.get('failed_count', 0)}",
        "- Source reachability: quote={quote}, kline={kline}".format(
            quote="up" if screened_data.get("source_status", {}).get("quote") else "down",
            kline="up" if screened_data.get("source_status", {}).get("kline") else "down",
        ),
        f"- Source usage: {_format_source_usage(source_usage, 'quote')}",
        f"- Source usage: {_format_source_usage(source_usage, 'kline')}",
        "",
        f"## Top {top_n}",
        "",
    ]

    if top_stocks:
        lines.extend(_build_table_rows(top_stocks))
    else:
        lines.append("No qualified stocks were produced in this run.")

    if top_stocks:
        lines.extend(["", "## Top 3 Charts", ""])
        for rank, stock in enumerate(top_stocks[:MAX_CHART_STOCKS], start=1):
            indicators = stock.get("technical_indicators", {})
            lines.append(f"### {rank}. {stock['name']} ({stock['code']})")
            lines.append(
                "- Sector: {sector} | RSI: {rsi:.2f} | MA20: {ma20:.2f} | Breakout Level: {breakout:.2f}".format(
                    sector=stock.get("sector", "N/A") or "N/A",
                    rsi=float(indicators.get("rsi", 0.0)),
                    ma20=float(indicators.get("ma20", 0.0)),
                    breakout=float(indicators.get("breakout_level", 0.0)),
                )
            )
            chart_name = chart_names.get(stock["code"], "")
            if chart_name:
                lines.append(f"![{stock['code']}]({chart_name})")
            else:
                lines.append("Chart generation failed for this stock.")
            lines.append("")

    report_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return report_path
