# 策略优化计划 (2026-04-20)

> 状态：待验证。代码已回退，需跑对比回测确认后再实施。

## 问题诊断

回测显示三大核心问题：
1. **换手率~98%**：keep_rank 默认关闭(0)，每期几乎全部换仓，交易成本吞噬50-62%累计收益
2. **因子权重失衡**：资金流占48%，本质是追涨动量信号，熊市/震荡市持续亏损
3. **回撤过大**：集中持仓无行业约束，最大回撤64%

## 拟议修改

### 修改1：启用 keep_rank 降低换手（确定合理）

`backtester.py`: `DEFAULT_KEEP_RANK` 0 → 30

效果：排名仍在前30的持仓保留不换，预计换手率从~98%降至~40-60%

### 修改2：行业分散约束（确定合理）

`backtester.py`: 新增 `_apply_sector_limit()` 函数
`config.py`: 新增 `DEFAULT_MAX_PER_SECTOR = 2`
`run.py`: 新增 `--max-per-sector` CLI参数

效果：同一行业最多选2只，防止单一板块集中暴露

### 修改3：重新平衡因子权重（需验证）

`config.py` 中 `RAW_FACTOR_WEIGHTS` 调整：

| 因子组 | 旧占比 | 新占比 |
|--------|--------|--------|
| 资金流 | 48% | 33% |
| 技术面 | 35% | 36% |
| 基本面 | 12% | 23% |
| 情绪面 | 7% | 8% |

具体数值：
```python
RAW_FACTOR_WEIGHTS = {
    "capital": {
        "net_inflow_rate": 0.12,    # was 0.22
        "volume_ratio": 0.08,       # was 0.13
        "turnover_anomaly": 0.05,   # was 0.08
        "volume_breakout": 0.05,    # was 0.07
    },
    "technical": {
        "macd": 0.08,               # was 0.09
        "rsi": 0.08,                # was 0.09
        "ma_alignment": 0.07,       # was 0.08
        "breakout": 0.04,           # unchanged
        "trend_strength": 0.05,     # unchanged
    },
    "fundamental": {
        "pe_score": 0.06,           # was 0.04
        "pb_score": 0.05,           # was 0.03
        "roe_score": 0.06,          # was 0.03
        "growth_score": 0.04,       # was 0.02
    },
    "sentiment": {
        "sentiment": 0.07,          # unchanged
    },
}
```

### 修改4：调整默认持仓/换仓周期（需验证）

`run.py`: `--hold-days` 5→10, `--rebalance-days` 5→10
`backtest_scan.py`: 扫描默认值更新

### 注意：回测不应启用基本面因子

回测模式下 `fund_data=None`，基本面因子对所有股票返回中性分50，纯噪声。
`DEFAULT_ENABLED_GROUPS` 应保持 `("capital", "technical")`。

## 验证方法

跑以下两条命令对比（约各30-40分钟，303只股票）：

```bash
# 基线（当前参数）
python run.py --backtest --start-date 2020-01-01 --hold-days 5 --rebalance-days 5 --keep-rank 0 --max-per-sector 0 --max-stocks 303 --groups capital technical

# 优化后（新参数）
python run.py --backtest --start-date 2020-01-01 --hold-days 10 --rebalance-days 10 --keep-rank 30 --max-per-sector 2 --max-stocks 303 --groups capital technical
```

重点对比：**累计净收益、Sharpe、最大回撤、换手率**

- 如果优化后 Sharpe 提升且回撤下降 → 全部实施
- 如果收益大幅下降 → 只保留 keep_rank 和 sector_limit，回退权重改动
