# Factor Trait Design (Option C - Hybrid Architecture)

**日期**: 2026-04-14
**任务**: 12.1 — Design Rust unified Factor trait interface
**状态**: 设计完成，选定 Option C
**决策**: 混合架构 — Python FactorEngine shell + Rust 状态

---

## 选定方案：Option C 混合架构

**核心设计**：

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Option C 混合架构                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Python FactorEngine (shell)                                        │
│  ├── Redis pub/sub 集成                                             │
│  ├── EventBus 集成                                                  │
│  ├── BootstrapCoordinator 集成                                      │
│  └── 调用 Rust PyFactorEngine                                       │
│                                                                     │
│  Rust PyFactorEngine                                                │
│  ├── FactorRegistry (factory map)                                   │
│  ├── ticker_factors: HashMap<String, HashMap<String, Arc<dyn Factor>>>│
│  └── trait.update() / trait.compute_batch()                         │
│                                                                     │
│  Python Indicator 类 → 删除                                         │
│  状态全部在 Rust                                                     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**好处**：
- FactorEngine Python shell 保留 → BootstrapCoordinator 不改动
- 状态在 Rust → 性能提升，减少跨语言数据复制
- 改动可控，风险中等

---

## 详细设计

### 1. Rust Trait 定义

```rust
// rust/src/factor/trait.rs

/// Factor type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactorType {
    Bar,
    Tick,
    Quote,
}

/// Base trait for all factors.
pub trait Factor: Send + Sync {
    fn name(&self) -> &str;
    fn factor_type(&self) -> FactorType;
    fn reset(&mut self);
    fn is_ready(&self) -> bool;
}

/// Bar-based factors.
pub trait BarFactor: Factor {
    /// Batch computation for backtest/warmup.
    fn compute_batch(&self, bars: &[Bar]) -> Vec<Option<f64>>;

    /// Incremental update for live.
    fn update(&mut self, bar: &Bar) -> Option<f64>;
}

/// Tick-based factors.
pub trait TickFactor: Factor {
    /// Batch computation for backtest/warmup.
    fn compute_batch(&self, trades: &[Trade], compute_ts: &[i64]) -> Vec<Option<f64>>;

    /// Incremental update for live.
    fn on_tick(&mut self, tick: &Trade) -> Option<f64>;
}

/// Quote-based factors.
pub trait QuoteFactor: Factor {
    fn compute_batch(&self, quotes: &[Quote]) -> Vec<Option<f64>>;
    fn on_quote(&mut self, quote: &Quote) -> Option<f64>;
}
```

### 2. Data Structures

```rust
// rust/src/factor/data.rs

#[derive(Debug, Clone, Copy)]
pub struct Bar {
    pub ts_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct Trade {
    pub ts_ms: i64,
    pub price: f64,
    pub size: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct Quote {
    pub ts_ms: i64,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: i64,
    pub ask_size: i64,
}
```

### 3. Factor Registry

```rust
// rust/src/factor/registry.rs

use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating factor instances.
pub trait FactorFactory: Send + Sync {
    fn name(&self) -> &str;
    fn factor_type(&self) -> FactorType;
    fn create_bar_factor(&self, params: &HashMap<String, f64>) -> Arc<dyn BarFactor>;
    fn create_tick_factor(&self, params: &HashMap<String, f64>) -> Arc<dyn TickFactor>;
}

/// Registry holding all factor factories.
pub struct FactorRegistry {
    factories: HashMap<String, Arc<dyn FactorFactory>>,
}

impl FactorRegistry {
    pub fn new() -> Self {
        let mut factories = HashMap::new();

        // Register factors via macro (single registration point)
        register_factors!(factories, EMAFactory, RelativeVolumeFactory, TradeRateFactory);

        Self { factories }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn FactorFactory>> {
        self.factories.get(name).cloned()
    }
}

// Registration macro
macro_rules! register_factors {
    ($map:expr, $($factory:ty),*) => {
        $(
            let f = Arc::new($factory::default());
            $map.insert(f.name().to_string(), f);
        )*
    };
}
```

### 4. PyO3 Wrapper

```rust
// rust/src/factor/py_engine.rs

use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// PyO3 Bar wrapper (converts from Python Bar)
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyBar {
    pub ts_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
}

impl From<PyBar> for Bar {
    fn from(py: PyBar) -> Bar {
        Bar {
            ts_ms: py.ts_ms,
            open: py.open,
            high: py.high,
            low: py.low,
            close: py.close,
            volume: py.volume,
        }
    }
}

/// PyO3 FactorEngine wrapper.
#[pyclass]
pub struct PyFactorEngine {
    registry: FactorRegistry,
    // ticker → factor_name → timeframe → factor instance
    // For simplicity, first iteration: ticker → factor_name → factor (single timeframe)
    // TODO: Add timeframe dimension later
    bar_factors: Mutex<HashMap<String, HashMap<String, Arc<Mutex<dyn BarFactor>>>>>,
    tick_factors: Mutex<HashMap<String, HashMap<String, Arc<Mutex<dyn TickFactor>>>>>,
}

#[pymethods]
impl PyFactorEngine {
    #[new]
    fn new() -> Self {
        Self {
            registry: FactorRegistry::new(),
            bar_factors: Mutex::new(HashMap::new()),
            tick_factors: Mutex::new(HashMap::new()),
        }
    }

    /// Create a bar factor for a ticker.
    #[pyo3(signature = (ticker, factor_name, params=None))]
    fn create_bar_factor(
        &mut self,
        ticker: String,
        factor_name: String,
        params: Option<HashMap<String, f64>>,
    ) -> PyResult<()> {
        let factory = self.registry.get(&factor_name)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>(factor_name))?;

        let params = params.unwrap_or_default();
        let factor = factory.create_bar_factor(&params);

        let mut factors = self.bar_factors.lock().unwrap();
        factors
            .entry(ticker)
            .or_insert_with(HashMap::new)
            .insert(factor_name, factor);

        Ok(())
    }

    /// Update bar factor with new bar (incremental).
    fn update_bar(&self, ticker: String, factor_name: String, bar: PyBar) -> Option<f64> {
        let factors = self.bar_factors.lock().unwrap();
        let ticker_factors = factors.get(&ticker)?;
        let factor = ticker_factors.get(&factor_name)?;
        let mut factor = factor.lock().unwrap();
        factor.update(&bar.into())
    }

    /// Batch compute bar factor values (backtest/warmup).
    fn compute_batch_bar(&self, factor_name: String, bars: Vec<PyBar>) -> Vec<Option<f64>> {
        let factory = self.registry.get(&factor_name).unwrap();
        let params = HashMap::new(); // Default params for batch
        let factor = factory.create_bar_factor(&params);
        let rust_bars: Vec<Bar> = bars.into_iter().map(|b| b.into()).collect();
        factor.compute_batch(&rust_bars)
    }

    /// Reset factor state.
    fn reset_factor(&self, ticker: String, factor_name: String) -> PyResult<()> {
        let factors = self.bar_factors.lock().unwrap();
        let ticker_factors = factors.get(&ticker)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>(ticker))?;
        let factor = ticker_factors.get(&factor_name)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>(factor_name))?;
        factor.lock().unwrap().reset();
        Ok(())
    }

    /// Check if factor is ready (warmup complete).
    fn is_ready(&self, ticker: String, factor_name: String) -> bool {
        let factors = self.bar_factors.lock().unwrap();
        if let Some(ticker_factors) = factors.get(&ticker) {
            if let Some(factor) = ticker_factors.get(&factor_name) {
                return factor.lock().unwrap().is_ready();
            }
        }
        false
    }
}
```

### 5. Python FactorEngine 改动

```python
# services/factor/factor_engine.py (改动后)
from jerry_trader._rust import PyFactorEngine, PyBar

class FactorEngine:
    """Factor computation engine (Python shell + Rust state)."""

    def __init__(self, session_id, redis_config, ...):
        # Rust engine for factor state
        self._rust_engine = PyFactorEngine()

        # Redis/EventBus integration (keep)
        self.redis_client = redis.Redis(...)
        self.storage = FactorStorage(...)

        # BootstrapCoordinator integration (keep)
        self._coordinator = None

    def add_ticker(self, symbol: str, timeframes: list[str]) -> bool:
        # Create factors in Rust
        for tf in timeframes:
            self._rust_engine.create_bar_factor(symbol, "ema_20", {"period": 20.0})

        # BootstrapCoordinator registration (keep)
        if self._coordinator:
            self._coordinator.register_consumer(symbol, "bar_warmup:1m", "factor_engine")

        return True

    def _on_event_bar_closed(self, event) -> None:
        """Handle bar closed from EventBus."""
        symbol = event.symbol
        bar_data = event.data

        # Convert to PyBar
        bar = PyBar(
            ts_ms=bar_data["ts_ms"],
            open=bar_data["open"],
            high=bar_data["high"],
            low=bar_data["low"],
            close=bar_data["close"],
            volume=bar_data["volume"],
        )

        # Call Rust update
        value = self._rust_engine.update_bar(symbol, "ema_20", bar)

        if value is not None:
            # Publish to Redis (keep)
            self._publish_factor(symbol, "ema_20", value, bar.ts_ms)

    def _bootstrap_bars_for_timeframe(self, symbol, timeframe, tf_state) -> None:
        """Warmup bars from ClickHouse."""
        bars = self._query_historical_bars(symbol, timeframe)

        # Convert to PyBar list
        py_bars = [PyBar(...) for bar in bars]

        # Batch compute in Rust
        values = self._rust_engine.compute_batch_bar("ema_20", py_bars)

        # Check if ready
        if values and values[-1] is not None:
            logger.info(f"{symbol}/{timeframe}: warmup complete")
```

---

## 实施计划

### Phase 1: Rust Trait Infrastructure

**文件**：
- `rust/src/factor/mod.rs` — 导出
- `rust/src/factor/trait.rs` — Factor, BarFactor traits
- `rust/src/factor/data.rs` — Bar, Trade, Quote
- `rust/src/factor/registry.rs` — FactorRegistry + macro
- `rust/src/factor/py_engine.rs` — PyFactorEngine, PyBar
- `rust/src/lib.rs` — 添加 `mod factor;` 和 export

### Phase 2: 实现 RelativeVolume (12.2)

**文件**：
- `rust/src/factor/relative_volume.rs`

### Phase 3: 迁移 EMA

**文件**：
- `rust/src/factor/ema.rs`

### Phase 4: 迁移 TradeRate

**文件**：
- `rust/src/factor/trade_rate.rs`

### Phase 5: Python FactorEngine 改动

**文件**：
- `services/factor/factor_engine.py` — 改调用 Rust
- 删除 `services/factor/indicators/*.py`

---

## 不改动组件

| 组件 | 原因 |
|------|------|
| BootstrapCoordinator | Python shell 保留，调用 Rust |
| ChartBFF | 只订阅 Redis，不改 |
| BarsBuilderService | 已是 Rust，不改 |
| Redis pub/sub | 解耦机制保留 |

---

## 改动范围评估

| 组件 | 新增 | 改动 | 删除 |
|------|------|------|------|
| Rust factor module | ~800 lines | - | - |
| lib.rs | ~20 lines | - | - |
| FactorEngine.py | - | ~150 lines | ~200 lines |
| indicators/*.py | - | - | ~270 lines |
| **总计** | ~820 lines | ~150 lines | ~470 lines |

---

## 时间预估

| Phase | 内容 | 时间 |
|-------|------|------|
| Phase 1 | Rust trait + registry + py_engine | 3-4小时 |
| Phase 2 | RelativeVolume 实现 + 测试 | 2小时 |
| Phase 3 | EMA 迁移 | 1小时 |
| Phase 4 | TradeRate 迁移 | 1小时 |
| Phase 5 | Python FactorEngine 改动 | 2小时 |
| **总计** | - | **~10小时 (1.5周)** |

---

## 下一步

开始 **12.2: 实现 RelativeVolume 统一接口** — 验证 Option C 设计。
