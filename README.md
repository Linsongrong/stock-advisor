# A-share Stock Advisor MVP

## 中文说明

这是一个轻量级的 A 股选股与报告生成工具，依赖很少，只使用：

- `requests`
- `numpy`
- `matplotlib`

它会抓取行情数据，按多因子模型为股票打分、排序，并生成每日 Markdown 报告和 K 线图。

### 数据来源

- 股票池：
  - 优先使用东方财富列表接口自动刷新股票池
  - 失败时退回到代码内置的板块股票池
- 实时行情：
  - 腾讯财经 `https://qt.gtimg.cn/q=sh{code}` / `sz{code}`
- 日线 K 线：
  - 同花顺 / 10jqka `https://d.10jqka.com.cn/v6/line/hs_{code}/01/last.js`
- 基本面数据：
  - 新浪财经 `jsvar.js`
  - 解析字段包括 `TTM EPS`、`上年 EPS`、`每股净资产`
  - `PE / PB / ROE` 在本地计算得到
- 情绪数据：
  - 新浪个股资讯页 `vCB_AllNewsStock`
  - 抽取最近资讯标题，并基于关键词做情绪打分

### 项目结构

- `config.py`：路径、阈值、原始权重、归一化后的运行时权重
- `stock_universe.py`：股票池抓取、缓存和硬编码兜底池
- `data_fetcher.py`：行情和 K 线抓取，支持缓存回退
- `factors/capital.py`：资金流代理因子
- `factors/technical.py`：技术指标和技术面打分
- `factors/fundamental.py`：基本面抓取、缓存和打分
- `factors/sentiment.py`：资讯解析、情绪缓存和打分
- `backtester.py`：基于本地 K 线缓存的历史回测框架
- `backtest_scan.py`：参数扫描与分时期回测对比
- `scorer.py`：统一加权评分引擎
- `screener.py`：全市场筛选、排序、过滤和二阶段情绪补分
- `reporter.py`：Markdown 报告和 K 线图生成
- `run.py`：CLI 入口
- `tests/test_regressions.py`：解析器、缓存、权重和评分回归测试
- `tests/test_backtester.py`：回测构造和小样本回测测试
- `tests/test_backtest_scan.py`：参数扫描工具测试

### 评分流程

1. 构建股票池。
2. 抓取全股票池的实时行情和日线 K 线。
3. 批量抓取基本面数据。
4. 第一轮先不带情绪因子，对全部股票打分。
5. 只对第一轮前 30 名候选股抓取情绪数据。
6. 用情绪数据对这些股票重新打分，再做最终排序并保留 Top `N`。
7. 输出 `reports/YYYY-MM-DD.md` 和最多 3 张图表。

### 因子模型

每个因子都返回 `0-100` 分，最终总分是所有因子组的加权和。

`config.py` 中维护的是原始权重，运行时会自动归一化，保证总权重严格等于 `1.00`。

归一化后的组权重大致为：

- 资金面：约 `48.1%`
- 技术面：约 `33.7%`
- 基本面：约 `11.5%`
- 情绪面：约 `6.7%`

归一化前的原始权重为：

- 资金面：
  - 净流入代理 `0.22`
  - 量比 `0.13`
  - 换手异常 `0.08`
  - 放量突破 `0.07`
- 技术面：
  - MACD `0.09`
  - RSI `0.09`
  - 均线排列 `0.08`
  - 突破 `0.04`
  - 趋势强度 `0.05`
- 基本面：
  - PE 分数 `0.04`
  - PB 分数 `0.03`
  - ROE 分数 `0.03`
  - 成长分数 `0.02`
- 情绪面：
  - 情绪分数 `0.07`

规则说明：

- `RSI > 80` 的股票会被过滤掉。
- 默认会过滤价格低于 `5` 元的股票，尽量避开过于脆弱的低价票；可通过 CLI 关闭或调整。
- 最终输出列表默认启用行业分散，同一行业最多保留 `2` 只；可通过 CLI 关闭或调整。
- 默认会根据市场温度自动降仓：如果“站上 MA20 的股票占比”低于 `55%`，则建议把仓位降到 `60%`。
- 默认会先识别强资金板块，最终只从强资金板块里挑选个股；市场流动性只作为风险提示。
- 单只股票抓取失败或打分失败不会中断整次运行。
- 基本面或情绪数据缺失时，会尽量退回到中性行为而不是直接报错退出。
- 默认启用数据质量闸门：如果股票池规模、预筛覆盖率、抓取完成率或源状态不达标，当天会直接停止出推荐，并写出质量报告。
- 通过数据质量闸门的日筛结果会自动写入模拟跟踪记录，方便后续做 2-4 周连续观察。

### 缓存文件

运行过程中会生成这些缓存：

- `data/quotes/*.json`
- `data/kline/*.json`
- `data/universe_cache.json`
- `data/fundamental_cache.json`
- `data/sentiment_cache.json`

缓存策略：

- 默认优先使用实时数据。
- 如果本地有新鲜缓存，会优先复用缓存。
- 基本面和情绪数据在必要时允许回退到过期缓存。
- 即使实时行情或 K 线源不可用，只要缓存存在，系统仍会尽量完成报告生成。

### 使用方式

安装依赖：

```bash
pip install -r requirements.txt
```

运行筛选：

```bash
python run.py
python run.py --top 5
python run.py --allow-partial-data
python run.py --top 5 --max-per-sector 1
python run.py --top 5 --min-price 8
python run.py --top 5 --min-breadth-ma20 0.55 --low-exposure 0.6
python run.py --top 5 --min-breadth-ma20 0.55 --low-exposure 0
python run.py --top 5 --min-market-liquidity-ratio 1.05 --sector-flow-top 5 --sector-flow-bonus 8
```

运行历史回测：

```bash
python run.py --backtest
python run.py --backtest --refresh-history
python run.py --backtest --top 5 --rebalance-days 5 --hold-days 5
python run.py --backtest --top 5 --start-date 2025-12-01 --end-date 2026-03-31
python run.py --backtest --commission-rate 0.0003 --slippage-rate 0.0005 --sell-tax-rate 0.0005
python run.py --backtest --top 5 --keep-rank 10
python run.py --backtest --top 3 --hold-days 10 --rebalance-days 10 --keep-rank 18 --max-per-sector 2 --min-price 5.5
python run.py --backtest --top 3 --hold-days 10 --rebalance-days 10 --keep-rank 18 --max-per-sector 2 --min-price 5.5 --min-breadth-ma20 0.55 --low-exposure 0.6
python run.py --backtest --top 3 --hold-days 10 --rebalance-days 10 --keep-rank 20 --max-per-sector 2 --min-price 5 --min-breadth-ma20 0.55 --low-exposure 0
python run.py --backtest --max-forward-return-pct 50
python run.py --scan-backtests
python run.py --scan-backtests --scan-regimes
python run.py --scan-backtests --scan-start-dates 2020-01-01 --scan-hold-options 5,10,20 --scan-rebalance-options 5,10,20
python run.py --paper-trade-review
python run.py --paper-trade-review --paper-hold-days 5
```

运行回归测试：

```bash
python -m unittest discover -s tests -v
```

### 输出内容

- 报告：`reports/YYYY-MM-DD.md`
- 质量检查：`reports/quality/YYYY-MM-DD.md`
- 模拟跟踪：`reports/paper_trades/YYYY-MM-DD.json`
- 模拟回看：`reports/paper_trades/reviews/*.md`
- 图表：`reports/YYYY-MM-DD_01_CODE.png`、`reports/YYYY-MM-DD_02_CODE.png`、`reports/YYYY-MM-DD_03_CODE.png`
- 回测报告：`reports/backtests/*.md`
- 扫描报告：`reports/backtests/sweeps/*.md`

报告中包括：

- 本次运行摘要
- Top `N` 股票列表
- 资金面、技术面、基本面、情绪面和总分
- 最多 3 只股票的图表区块，附带 RSI、MA20、突破参考位等指标
- 回测结果会额外包含换手率、成本拖累、毛收益、净收益和基准对比
- `keep-rank` 可以保留还在较高排名区间内的旧持仓，用来降低无效换手
- `max-per-sector` 可以限制最终入选组合的行业集中度，默认每个行业最多 `2` 只
- `min-price` 可以过滤过低价的股票，默认价格底线为 `5.5`
- `min-breadth-ma20` 可以在弱市时触发降仓，例如 `0.55` 表示只有当至少 55% 的样本股票站上 MA20 才维持满仓
- `low-exposure` 用来定义弱市时保留多少仓位，例如 `0.6` 表示降到六成仓，`0` 表示直接空仓
- `min-market-liquidity-ratio` 只用于标记市场流动性好坏，不再决定是否启用强板块选股
- `sector-flow-top` 控制可获得加分的强资金板块数量，`sector-flow-bonus` 控制板块加分上限
- `--refresh-history` 会在回测前主动刷新本地 K 线缓存，并优先保存更长历史版本
- `--max-forward-return-pct` 会过滤掉异常大的前瞻收益样本，降低复权口径或极端事件对长期回测的污染
- `--scan-backtests` 会批量比较不同参数组合，并输出分窗口报告和跨窗口总榜
- `--scan-regimes` 会使用内置的市场阶段窗口做扫描，例如 `2020-2022`、`2023-至今`
- `--paper-trade-review` 会回看已经记录的模拟组合，并按持有天数统计表现

### 运行说明

- 全市场运行可能需要一定时间，因为行情、K 线、基本面和二阶段情绪都依赖远程请求。
- 情绪面只对第一轮 Top 30 候选股抓取，这是为了控制总耗时。
- 系统默认会先过数据质量闸门，只有闸门通过才会产出日筛报告；如需强行继续，可手动加 `--allow-partial-data`。
- 回测当前默认使用本地 K 线缓存，并默认启用一组更平衡的参数：`Top 3`、`10/10` 持有与换仓节奏、`keep-rank=18`、`max-per-sector=2`、`min-price=5.5`、`min-breadth-ma20=0.55`、`low-exposure=0.6`。
- 如果更重视回撤而不是收益峰值，可以把 `--low-exposure 0` 作为更保守的模式。
- 参数扫描默认聚焦 `2020` 之后，并会复用历史打分缓存，比逐组完全重算更快。

---

## English

This project is a lightweight A-share stock screening and reporting tool built with:

- `requests`
- `numpy`
- `matplotlib`

It fetches market data, scores stocks with a multi-factor model, ranks candidates, and writes a daily Markdown report with chart images.

### Data Sources

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

### Project Structure

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
- `backtester.py`: historical backtesting framework using local K-line cache
- `backtest_scan.py`: parameter scan and multi-window backtest comparison
- `tests/test_backtester.py`: backtest helper and small-fixture backtest tests
- `tests/test_backtest_scan.py`: parameter scan tool tests

### Scoring Pipeline

1. Build the stock universe.
2. Fetch quote and K-line data for the full universe.
3. Fetch fundamental data in batch.
4. Run pass 1 scoring without sentiment for all qualified stocks.
5. Fetch sentiment only for the top 30 pass 1 candidates.
6. Re-score those candidates with sentiment, sort again, and keep top `N`.
7. Write `reports/YYYY-MM-DD.md` and up to 3 chart images.

### Factor Model

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
- Stocks below `5.5` are filtered by default to avoid fragile low-priced names.
- The final list applies a sector cap by default: at most `2` picks per sector.
- The default setup reduces exposure to `60%` when the share of stocks above `MA20` drops below `55%`.
- The final picks are restricted to the strongest money-flow sectors; market turnover is reported as a risk signal.
- Failed fetches or scoring errors are skipped instead of aborting the run.
- Missing fundamental or sentiment data falls back to neutral behavior where possible.

### Cache Files

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

### Usage

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the screener:

```bash
python run.py
python run.py --top 5
python run.py --allow-partial-data
python run.py --top 5 --max-per-sector 1
python run.py --top 5 --min-price 8
python run.py --top 5 --min-breadth-ma20 0.55 --low-exposure 0.6
python run.py --top 5 --min-breadth-ma20 0.55 --low-exposure 0
python run.py --top 5 --min-market-liquidity-ratio 1.05 --sector-flow-top 5 --sector-flow-bonus 8
```

Run a historical backtest:

```bash
python run.py --backtest
python run.py --backtest --refresh-history
python run.py --backtest --top 5 --rebalance-days 5 --hold-days 5
python run.py --backtest --top 5 --start-date 2025-12-01 --end-date 2026-03-31
python run.py --backtest --commission-rate 0.0003 --slippage-rate 0.0005 --sell-tax-rate 0.0005
python run.py --backtest --top 5 --keep-rank 10
python run.py --backtest --top 3 --hold-days 10 --rebalance-days 10 --keep-rank 18 --max-per-sector 2 --min-price 5.5
python run.py --backtest --top 3 --hold-days 10 --rebalance-days 10 --keep-rank 18 --max-per-sector 2 --min-price 5.5 --min-breadth-ma20 0.55 --low-exposure 0.6
python run.py --backtest --top 3 --hold-days 10 --rebalance-days 10 --keep-rank 20 --max-per-sector 2 --min-price 5 --min-breadth-ma20 0.55 --low-exposure 0
python run.py --backtest --max-forward-return-pct 50
python run.py --scan-backtests
python run.py --scan-backtests --scan-regimes
python run.py --scan-backtests --scan-start-dates 2020-01-01 --scan-hold-options 5,10,20 --scan-rebalance-options 5,10,20
python run.py --paper-trade-review
python run.py --paper-trade-review --paper-hold-days 5
```

Run regression tests:

```bash
python -m unittest discover -s tests -v
```

### Output

- Report Markdown: `reports/YYYY-MM-DD.md`
- Quality report: `reports/quality/YYYY-MM-DD.md`
- Paper-trade snapshot: `reports/paper_trades/YYYY-MM-DD.json`
- Paper-trade review: `reports/paper_trades/reviews/*.md`
- Top chart images: `reports/YYYY-MM-DD_01_CODE.png`, `reports/YYYY-MM-DD_02_CODE.png`, `reports/YYYY-MM-DD_03_CODE.png`
- Backtest reports: `reports/backtests/*.md`
- Scan reports: `reports/backtests/sweeps/*.md`

The report includes:

- Run summary
- Top `N` ranked stocks
- Capital, technical, fundamental, sentiment, and total scores
- Up to 3 chart sections with RSI, MA20, and breakout reference levels
- Backtest reports additionally include turnover, cost drag, gross returns, net returns, and benchmark comparison
- `keep-rank` can retain prior holdings that still rank well enough, reducing unnecessary turnover
- `max-per-sector` limits concentration in the final list, defaulting to `2` picks per sector
- `min-price` filters out low-priced stocks, defaulting to `5.5`
- `min-breadth-ma20` triggers lower exposure in weak markets
- `low-exposure` defines how much capital remains deployed in weak regimes
- `min-market-liquidity-ratio` only labels market liquidity strength; strong-sector selection remains active
- `sector-flow-top` and `sector-flow-bonus` control how many money-flow sectors can receive a ranking bonus and how large that bonus can be
- `--refresh-history` refreshes local K-line cache before backtesting and prefers the longer history endpoint
- `--max-forward-return-pct` filters abnormally large forward-return samples that can distort long-horizon backtests
- `--scan-backtests` runs a parameter scan and writes both per-window tables and a cross-window ranking
- `--scan-regimes` uses built-in windows such as `2020-2022` and `2023-now`
- `--paper-trade-review` reviews saved paper-trade snapshots using cached K-line data

### Runtime Notes

- The full market run can take noticeable time because quote, K-line, fundamentals, and second-pass sentiment are all fetched remotely.
- Sentiment is intentionally delayed until the top 30 pass 1 candidates to reduce total runtime.
- A strict data-quality gate now blocks the daily report when the universe size, prefilter coverage, fetch completion, or source health fall below the configured minimums. Use `--allow-partial-data` only when you explicitly want to inspect a compromised run.
- Backtests currently use local K-line cache and a more balanced default setup: `Top 3`, `10/10` rebalance cadence, `keep-rank=18`, `max-per-sector=2`, `min-price=5.5`, `min-breadth-ma20=0.55`, and `low-exposure=0.6`.
- Parameter scans now focus on `2020+` by default and reuse cached scores to reduce repeated work.
