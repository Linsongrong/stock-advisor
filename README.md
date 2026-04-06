# A-share Stock Advisor MVP

This project is a lightweight A-share stock screening and reporting tool built with:

- `requests`
- `numpy`
- `matplotlib`

It fetches market data, scores stocks with a multi-factor model, ranks candidates, and writes a daily Markdown report with chart images.

## Data Sources

- Stock universe:
  - Eastmoney list API for automatic universe refresh
  - Hardcoded fallback universe grouped by sector
- Real-time quote:
  - Tencent Finance `https://qt.gtimg.cn/q=sh{code}` / `sz{code}`
- Daily K-line:
  - THS / 10jqka `https://d.10jqka.com.cn/v6/line/hs_{code}/01/last.js`
- Fundamental metrics:
  - Sina Finance `jsvar.js`
  - Parsed fields include TTM EPS, last-year EPS, and book value per share
  - PE, PB, and ROE are derived locally from those fields
- Sentiment:
  - Sina stock-specific news page `vCB_AllNewsStock`
  - The parser extracts recent stock news titles and applies keyword-based sentiment scoring

## Project Structure

- `config.py`: paths, thresholds, raw weights, and normalized runtime weights
- `stock_universe.py`: stock universe fetch, cache, and hardcoded fallback pools
- `data_fetcher.py`: quote and K-line fetch with cache fallback
- `factors/capital.py`: capital-flow proxy factors
- `factors/technical.py`: technical indicators and scores
- `factors/fundamental.py`: fundamental fetch, cache, and scoring
- `factors/sentiment.py`: news parsing, sentiment cache, and scoring
- `scorer.py`: weighted scoring engine
- `screener.py`: market-wide scoring, filtering, ranking, and two-pass sentiment fetch
- `reporter.py`: Markdown report and candlestick chart generation
- `run.py`: CLI entrypoint
- `tests/test_regressions.py`: parser, cache, weight, and score regression tests

## Scoring Pipeline

1. Build the stock universe.
2. Fetch quote and K-line data for the full universe.
3. Fetch fundamental data in batch.
4. Run pass 1 scoring without sentiment for all qualified stocks.
5. Fetch sentiment only for the top 30 pass 1 candidates.
6. Re-score those candidates with sentiment, sort again, and keep top `N`.
7. Write `reports/YYYY-MM-DD.md` and up to 3 chart images.

## Factor Model

Each factor returns a score in the `0-100` range. The final score is the weighted sum of all factor groups.

Raw weights are defined in `config.py`, then normalized at runtime so the total model weight is exactly `1.00`.

Normalized group weights:

- Capital: about `48.1%`
- Technical: about `33.7%`
- Fundamental: about `11.5%`
- Sentiment: about `6.7%`

Raw factor weights before normalization:

- Capital:
  - Net inflow rate proxy `0.22`
  - Volume ratio `0.13`
  - Turnover anomaly `0.08`
  - Volume breakout `0.07`
- Technical:
  - MACD `0.09`
  - RSI `0.09`
  - MA alignment `0.08`
  - Breakout `0.04`
  - Trend strength `0.05`
- Fundamental:
  - PE score `0.04`
  - PB score `0.03`
  - ROE score `0.03`
  - Growth score `0.02`
- Sentiment:
  - Sentiment score `0.07`

Rules:

- Stocks with `RSI > 80` are filtered out.
- Failed fetches or scoring errors are skipped instead of aborting the run.
- Missing fundamental or sentiment data falls back to neutral behavior where possible.

## Cache Files

Generated cache files include:

- `data/quotes/*.json`
- `data/kline/*.json`
- `data/universe_cache.json`
- `data/fundamental_cache.json`
- `data/sentiment_cache.json`

Cache behavior:

- Live sources are preferred first.
- Fresh cache is used when available.
- Stale cache may still be used as a fallback for fundamentals and sentiment.
- If live quote or K-line sources are unavailable, the system falls back to cache and still tries to produce a report.

## Usage

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the screener:

```bash
python run.py
python run.py --top 5
```

Run regression tests:

```bash
python -m unittest discover -s tests -v
```

## Output

- Report Markdown: `reports/YYYY-MM-DD.md`
- Top chart images: `reports/YYYY-MM-DD_01_CODE.png`, `reports/YYYY-MM-DD_02_CODE.png`, `reports/YYYY-MM-DD_03_CODE.png`

The report includes:

- Run summary
- Top `N` ranked stocks
- Capital, technical, fundamental, sentiment, and total scores
- Up to 3 chart sections with RSI, MA20, and breakout reference levels

## Runtime Notes

- The full market run can take noticeable time because quote, K-line, fundamentals, and second-pass sentiment are all fetched remotely.
- Sentiment is intentionally delayed until the top 30 pass 1 candidates to reduce total runtime.
- The project is designed to degrade gracefully: partial failures should not stop report generation.
