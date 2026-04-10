# Batch Backtest Pipeline Design

**Created:** 2026-04-06
**Updated:** 2026-04-07 — redesigned to reuse live infrastructure
**Related:** Task 7.4, Section 9 (Backtest Pipeline)
**Scope:** US pre-market momentum strategy backtest

## Two Backtest Modes

### Mode A: Replay (existing)
Full system replay with global clock, real-time orchestration.
Purpose: integration testing, system validation.

### Mode B: Batch Backtest (new)
Reuses the same BarBuilder + FactorEngine + SignalEngine as live,
but feeds data from memory in one pass instead of via WebSocket/Redis.
Purpose: strategy research, rule optimization.

**Key principle: backtest and live share the same computation path.**
Any new indicator added to FactorRegistry automatically works in backtest.

## Context

The system targets US pre-market momentum trading driven by market snapshots.
Strategy: stocks that **newly enter** the pre-market top 20 with sufficient price gain.
Backtest evaluates this specific strategy across historical dates.

## Architecture: Reuse Live Pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│                    Batch Backtest Pipeline                        │
│                                                                  │
│  Step 1: Pre-filter Candidates                                   │
│  ┌─────────────────────────────────────────┐                     │
│  │ ClickHouse market_snapshot               │                     │
│  │   true entry detection → candidates[]   │                     │
│  └──────────────────┬──────────────────────┘                     │
│                     ↓                                            │
│  Step 2: Load Raw Data                                           │
│  ┌─────────────────────────────────────────┐                     │
│  │ Rust: load_trades_from_parquet           │                     │
│  │ Rust: load_quotes_from_parquet           │                     │
│  │ → TickerData(symbol, trades, quotes)     │                     │
│  └──────────────────┬──────────────────────┘                     │
│                     ↓                                            │
│  Step 3: Feed through Live Pipeline (batch mode)                 │
│  ┌─────────────────────────────────────────┐                     │
│  │ BarBuilder.ingest_trade() → drain_bars()│  ← 复用 Rust        │
│  │           ↓                           │                     │
│  │ FactorEngine._on_bar(bar)              │  ← 复用 indicators   │
│  │ FactorEngine._on_tick(trade)           │  ← 复用 tick ind.    │
│  │ FactorEngine._on_quote(quote)          │  ← 复用 quote ind.   │
│  │ → FactorTimeseries (in-memory output)   │                     │
│  └──────────────────┬──────────────────────┘                     │
│                     ↓                                            │
│  Step 4: Evaluate Signals                                        │
│  ┌─────────────────────────────────────────┐                     │
│  │ SignalEngine.evaluate(rules, factors)   │  ← 复用 DSL eval    │
│  │ → list[SignalEvent]                      │                     │
│  └──────────────────┬──────────────────────┘                     │
│                     ↓                                            │
│  Step 5: Compute Returns + Metrics                               │
│  ┌─────────────────────────────────────────┐                     │
│  │ Slippage, returns, MFE/MAE              │  ← 新增             │
│  │ → list[SignalResult]                     │                     │
│  └──────────────────┬──────────────────────┘                     │
│                     ↓                                            │
│  Step 6: Output                                                  │
│  ┌─────────────────────────────────────────┐                     │
│  │ Console table + ClickHouse persistence  │                     │
│  └─────────────────────────────────────────┘                     │
└──────────────────────────────────────────────────────────────────┘
```

## Core Design: FactorEngine Batch Mode

FactorEngine 目前依赖 Redis pub/sub + WebSocket + BootstrapCoordinator。
Backtest 需要一个 "headless" 模式：

### Live vs Batch 对比

| | Live | Batch |
|---|---|---|
| 数据来源 | WebSocket / Replayer | Parquet (Rust loader) |
| BarBuilder 输入 | `ingest_trade()` per tick | `ingest_trades_batch()` 一次性 |
| FactorEngine 输入 | Redis pub/sub `_on_bar()` | 直接调用 `_on_bar()` |
| Tick indicators | `_on_tick()` per tick | `_process_bootstrap_trades()` batch |
| Quote indicators | `_on_quote()` per quote | 直接调用 `_on_quote()` |
| 输出 | Redis publish + ClickHouse write | 内存 `dict[int, dict[str, float]]` |
| Bootstrap | 需要 wait coordinator | 不需要，直接喂数据 |

### 实现方式：`FactorEngineBatchAdapter`

```python
class FactorEngineBatchAdapter:
    """Wraps live FactorEngine for batch backtest.

    Reuses the same indicator instances (from FactorRegistry)
    but feeds data from memory and collects output in-memory.
    """

    def __init__(self, factor_registry: FactorRegistry):
        self._registry = factor_registry

    def compute(self, symbol: str, bars: list[Bar],
                trades: list, quotes: list) -> FactorTimeseries:
        """One-shot factor computation for a ticker.

        Steps:
        1. Create fresh indicator instances from registry
        2. Walk bars chronologically → update bar indicators
        3. Feed trades → tick indicator warmup (Rust batch)
        4. Feed quotes → quote indicators
        5. Return merged FactorTimeseries
        """
```

关键：**indicator 实例来自 FactorRegistry**，和 live 完全一样。
FactorRegistry 新增的任何 indicator 自动在 backtest 里生效。

### 指标的三种计算路径

| Indicator Type | Live 路径 | Batch 路径 | 复用代码 |
|---|---|---|---|
| Bar (EMA, etc.) | `_on_bar()` → `ind.update(bar)` | 相同循环 | ✅ |
| Tick (TradeRate) | `_on_tick()` per tick → `_process_bootstrap_trades()` batch | 直接用 `bootstrap_trade_rate()` | ✅ |
| Quote (spread) | `_on_quote()` → `ind.on_quote()` | 相同循环 | ✅ |

## Data Sources

| Source | Format | Loader |
|--------|--------|--------|
| trades_v1 | Parquet | Rust `load_trades_from_parquet` |
| quotes_v1 | Parquet | Rust `load_quotes_from_parquet` |
| market_snapshot | ClickHouse | Python `PreFilter` |

**不再需要从 ClickHouse 加载 bars** — BarBuilder 从 trades 直接构建。
如果已有 bars 也可以跳过 BarBuilder 直接加载（后续优化）。

## Pre-filter Design

See [backtest-prefilter-findings.md](./backtest-prefilter-findings.md) for test results.

Key decisions:
- Only select stocks that **newly entered** top 20 during the session
- mode = `live`/`replay`, NOT `premarket`/`regular`
- CH client config passed via `BacktestConfig`, works across machines
- `get_common_stocks(backtest_date)` for ETF/delisted filtering

## Slippage Model

Entry price = `ask_at_trigger * (1 + buffer_pct)`

- Uses actual bid/ask spread from quote data
- Configurable buffer (default 0.1%) for market impact
- Fallback: `trigger_price * (1 + default_slippage_pct)` when no quote available

## Metrics

| Metric | Description |
|--------|-------------|
| Return horizons | 30s, 1m, 2m, 5m, 10m, 15m, 30m, 60m, EOD |
| MFE | Maximum Favorable Excursion — max unrealized gain |
| MAE | Maximum Adverse Excursion — max unrealized loss |
| Time to peak | When max return occurs |
| Win rate | % of signals with positive return at each horizon |
| Profit factor | sum(wins) / sum(losses) |
| Slippage stats | avg spread, buffer impact |

## Output

- Console: summary table with per-rule stats
- ClickHouse: `backtest_results` table for historical comparison

```sql
CREATE TABLE backtest_results (
    run_id String,
    date Date,
    rule_id String,
    ticker String,
    trigger_time_ns Int64,
    entry_price Float64,
    trigger_price Float64,
    slippage_pct Float64,
    factors String,          -- JSON
    return_30s Nullable(Float64),
    return_1m Nullable(Float64),
    return_5m Nullable(Float64),
    return_15m Nullable(Float64),
    mfe Nullable(Float64),
    mae Nullable(Float64),
    time_to_peak_ms Nullable(Int64),
    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, rule_id, ticker, trigger_time_ns);
```

## File Structure

```
python/src/jerry_trader/domain/backtest/
├── __init__.py
└── types.py            # Candidate, SignalResult, BacktestResult (pure value objects)

python/src/jerry_trader/services/backtest/
├── __init__.py
├── cli.py              # CLI entry point
├── runner.py           # BacktestRunner orchestrator
├── pre_filter.py       # Candidate pre-filter from market_snapshot (9.2 ✅)
├── data_loader.py      # Load trades/quotes from Parquet via Rust
├── batch_engine.py     # FactorEngineBatchAdapter — reuses live indicators
├── evaluator.py        # Signal evaluation (reuse SignalEngine)
├── metrics.py          # Return, slippage, MFE/MAE computation
└── config.py           # BacktestConfig, PreFilterConfig, TickerData (9.1 ✅)

python/tests/core/test_backtest/
├── test_pre_filter.py   # ✅ 15 tests
├── test_batch_engine.py # Tests for batch adapter
├── test_evaluator.py
└── test_metrics.py
```

**删除:** `factor_batch.py` — 被 `batch_engine.py` (FactorEngineBatchAdapter) 替代
**删除:** `FactorComputer` + `BarContext` 对 backtest 的依赖 — 不再需要

## Implementation Order

1. ~~9.1~~ ✅ domain/backtest/types.py + services/backtest/config.py
2. ~~9.2~~ ✅ pre_filter.py — true entry detection
3. ~~9.3~~ data_loader.py — Rust loaders (已实现，保留)
4. **9.3b** batch_engine.py — FactorEngineBatchAdapter (替代 factor_batch.py)
5. **9.4** evaluator.py — signal evaluation via SignalEngine
6. **9.5** metrics.py — return/slippage/MFE/MAE
7. **9.6** output.py — console + ClickHouse
8. **9.7** runner.py + cli.py — orchestrator + entry point
9. `sql/clickhouse_backtest_results.sql` — schema
10. Tests for each component

## What Changed vs Previous Design

| Before (旧) | After (新) |
|---|---|
| `factor_batch.py` 独立 factor 计算 | `batch_engine.py` 复用 FactorEngine indicators |
| `FactorComputer` + `BarContext` 单独计算 | 直接用 FactorRegistry 创建的 indicator instances |
| Rust `compute_quote_factors` | 用 QuoteIndicator instances (和 live 一样) |
| ClickHouse 加载 bars | BarBuilder 从 trades 构建 bars (和 live 一样) |
| 两套 factor 定义可能 diverge | 单一来源: FactorRegistry |

## CLI Usage

```bash
# Run backtest for a specific date (console + ClickHouse)
poetry run python -m jerry_trader.services.backtest.cli \
    --date 2026-03-13 \
    --rules config/rules/ \
    --gain-threshold 2.0 \
    --slippage 0.001

# Console only (no ClickHouse write)
poetry run python -m jerry_trader.services.backtest.cli \
    --date 2026-03-13 --no-ch

# Run for multiple dates
poetry run python -m jerry_trader.services.backtest.cli \
    --date-range 2026-03-01 2026-03-10 \
    --rules config/rules/
```

## Verification

1. Run unit tests: `poetry run pytest python/tests/core/test_backtest/ -v`
2. Run single-date backtest: `poetry run python -m jerry_trader.services.backtest.cli --date 2026-03-13`
3. **Consistency check:** compare batch factors with live factors at same timestamp — should be identical
4. Verify slippage is applied correctly (entry_price != trigger_price)
5. Verify MFE/MAE calculations against manual spot-checks
