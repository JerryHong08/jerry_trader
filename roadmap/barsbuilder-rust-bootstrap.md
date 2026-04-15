# BarsBuilder Bootstrap Rust Optimization

## Problem

Current `trades_backfill()` flow has unnecessary FFI overhead:

```
Python trades_backfill()
  → Rust load_trades() returns Vec<(ts, price, size)> to Python  [FFI copy]
  → Python receives trades list (potentially 400K+ items for BHAT)
  → Python converts UTC→ET timestamps (loop overhead)
  → Python passes trades to Rust BarBuilder.ingest_trades_batch()  [FFI copy]
  → Rust builds bars, returns Vec<dict> to Python  [FFI copy]
```

For BHAT with 431,999 trades:
- 3 FFI crossings
- 2 large Vec copies (trades in, trades out)
- Python loop for timestamp conversion

## Proposed Solution

Consolidate into single Rust function:

```rust
pub fn bootstrap_build_bars(
    config: BootstrapConfig,  // CH config, lake_dir, date, symbol
    timeframes: Vec<&str>,
    start_ts_ms: i64,
    end_ts_ms: i64,
    late_arrival_ms: i64,
    idle_close_ms: i64,
) -> Result<Vec<BarDict>>
```

Internal flow (all in Rust):
1. `load_trades_from_clickhouse_sync()` or `load_trades_from_parquet_sync()`
2. Timestamp conversion (ns→ms, UTC→ET) — no copy
3. `BarBuilder.ingest_trades_batch()` — direct ingest
4. `BarBuilder.advance()` + `BarBuilder.flush()` — close bars
5. Return only completed bars (smaller output)

Benefits:
- Single FFI crossing (only bars returned)
- No large Vec copies between Rust/Python
- Timestamp conversion in Rust (batch operation)
- ~10x faster for large tickers

## Implementation

### Rust side (`rust/src/lib.rs`)

```rust
#[pyfunction]
#[pyo3(name = "bootstrap_build_bars")]
fn py_bootstrap_build_bars(
    lake_data_dir: &str,
    symbol: &str,
    date_yyyymmdd: &str,
    timeframes: Vec<&str>,
    start_ts_ms: i64,
    end_ts_ms: i64,
    late_arrival_ms: i64,
    idle_close_ms: i64,
    clickhouse_url: Option<&str>,
    clickhouse_user: Option<&str>,
    clickhouse_password: Option<&str>,
    clickhouse_database: Option<&str>,
) -> PyResult<Vec<HashMap<String, Value>>> {
    // 1. Load trades (CH → Parquet fallback)
    // 2. Convert timestamps
    // 3. Create BarBuilder with config
    // 4. ingest_trades_batch
    // 5. advance + flush
    // 6. Return bars as Vec<dict>
}
```

### Python side (`bars_builder_service.py`)

```python
def trades_backfill(self, symbol: str, ...):
    # Simplified - just call Rust
    bars = bootstrap_build_bars(
        lake_data_dir,
        symbol,
        self.db_date,
        bootstrap_tfs,
        from_ms or 0,
        clock_mod.now_ms(),
        self._bootstrap_late_arrival_ms,
        self._bootstrap_idle_close_ms,
        ch_url, ch_user, ch_password, ch_database,
    )
    # Store bars to ClickHouse
    self._enqueue_bars(bars, source="trades_backfill")
```

## Considerations

1. **Bar dict format**: Need to match current `Bar.from_rust_dict()` schema
2. **Meeting bar logic**: Current `detect_meeting_bars()` happens in Python after bars returned — may need to move to Rust
3. **Factor warmup trades**: Current code stores trades for FactorEngine warmup — need to decide if Rust returns them too or FactorEngine loads separately

## Alternative: Keep trades in Rust memory

Even simpler — don't return trades at all:
- BarsBuilder keeps trades in Rust memory
- FactorEngine calls its own `load_trades_sync()` for warmup
- Python only receives bars

This decouples bar building from factor warmup completely.

## Status

- [x] Design approved
- [ ] ~~Rust `bootstrap_build_bars()` implementation~~ **撤回** (2026-04-15)
- [ ] ~~Python `trades_backfill()` simplification~~ **撤回** (2026-04-15)
- [ ] Integration testing with BHAT 20260305

## 撤回原因

2026-04-15 撤回此实现，因为：
1. FactorEngine 无法获取 trades 数据进行 warmup
2. Live 和 Replay 模式代码不统一
3. 更好的方案是统一 Rust Compute Box 架构（见 `roadmap/rust-compute-box-architecture.md`）

此任务将在 Rust Compute Box 架构实施后重新考虑，届时 trades 会在 Rust 内部流转，不再需要返回 Python。
