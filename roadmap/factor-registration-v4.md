# Factor Registration Architecture V4 - Auto-Registration Design

## Problem Statement

Current architecture (V3) has friction points when adding new factors:

1. **Manual registration** - Must add to `INDICATOR_CLASSES` dict in Python
2. **Hardcoded fallbacks** - FactorEngine bypasses registry with `EMA(period=20)` defaults
3. **No auto-registration** - `__init_subclass__` pattern documented but not implemented
4. **YAML + Python split** - Params in YAML, class name mapping in Python code

Example friction flow:
```
Add relative_volume factor:
1. Write indicators/relative_volume.py (implement class)
2. Add "RelativeVolume": RelativeVolume to INDICATOR_CLASSES dict ← MANUAL STEP
3. Add relative_volume entry to factors.yaml
4. If step 2 missed → runtime error "class not found"
```

## Design Goals

| Goal | Current State | Target State |
|------|---------------|--------------|
| Add new factor | Edit 2+ files | Write class only (YAML auto-sync optional) |
| Class discovery | Manual dict | `__init_subclass__` auto-registration |
| Fallback behavior | Hardcoded defaults | Registry-driven or explicit config |
| Batch + incremental | Separate paths | Unified trait/interface |

---

## V4 Architecture: Auto-Registration

### 1. Indicator Base Classes with Auto-Registration

```python
# indicators/base.py

from abc import ABC, abstractmethod
from typing import ClassVar, dict, Type

class Factor(ABC):
    """Unified factor interface supporting batch + incremental computation.

    All factors (bar, tick, quote) inherit from this base.
    Auto-registration via __init_subclass__ eliminates manual INDICATOR_CLASSES dict.
    """

    # Auto-populated registry (class name → class)
    _registry: ClassVar[dict[str, Type["Factor"]]] = {}

    # Factor metadata (set in subclass)
    name: ClassVar[str]           # "relative_volume"
    factor_type: ClassVar[str]    # "bar", "tick", "quote"
    default_params: ClassVar[dict] = {}  # {"lookback": 20}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Auto-register by class name
        if cls.__name__ != "Factor":  # Skip base class
            Factor._registry[cls.__name__] = cls
            # Also register by factor.name if set
            if hasattr(cls, 'name') and cls.name:
                Factor._registry[cls.name] = cls

    @classmethod
    def get_registry(cls) -> dict[str, Type["Factor"]]:
        """Get all registered factor classes."""
        return cls._registry.copy()

    # ─────────────────────────────────────────────────────────────────────
    # Unified interface: batch + incremental
    # ─────────────────────────────────────────────────────────────────────

    @abstractmethod
    def reset(self) -> None:
        """Reset internal state."""
        pass

    @property
    @abstractmethod
    def ready(self) -> bool:
        """Whether factor has enough data to produce values."""
        pass

    # Batch mode (backtest)
    @abstractmethod
    def compute_batch(self, data: list) -> list[float | None]:
        """Compute factor values for entire history (backtest mode).

        Args:
            data: List of Bar or Tick objects

        Returns:
            List of factor values (None for warmup period)
        """
        pass

    # Incremental mode (real-time)
    @abstractmethod
    def update(self, datum) -> float | None:
        """Incremental update with new bar/tick/quote.

        Args:
            datum: Bar, Tick, or Quote object

        Returns:
            Current factor value if ready, None if warming up
        """
        pass


# Convenience: type-specific mixins preserve familiar interface

class BarFactor(Factor):
    """Mixin for bar-based factors. Adds Bar type hint."""
    factor_type: ClassVar[str] = "bar"

    @abstractmethod
    def update(self, bar: Bar) -> float | None:
        pass

    @abstractmethod
    def compute_batch(self, bars: list[Bar]) -> list[float | None]:
        pass


class TickFactor(Factor):
    """Mixin for tick-based factors."""
    factor_type: ClassVar[str] = "tick"

    @abstractmethod
    def on_tick(self, ts_ms: int, price: float, size: int) -> None:
        """Ingest tick (incremental)."""
        pass

    @abstractmethod
    def compute(self, ts_ms: int) -> float | None:
        """Compute current value on demand."""
        pass

    def update(self, datum) -> float | None:
        """Unified update for tick (calls on_tick + compute)."""
        self.on_tick(datum.ts_ms, datum.price, datum.size)
        return self.compute(datum.ts_ms)

    def compute_batch(self, ticks: list) -> list[float | None]:
        """Batch compute for ticks (call on_tick for each, then compute at intervals)."""
        # Default implementation: compute every 1s
        results = []
        last_compute_ms = 0
        for tick in ticks:
            self.on_tick(tick.ts_ms, tick.price, tick.size)
            if tick.ts_ms - last_compute_ms >= 1000:
                results.append(self.compute(tick.ts_ms))
                last_compute_ms = tick.ts_ms
        return results


class QuoteFactor(Factor):
    """Mixin for quote-based factors."""
    factor_type: ClassVar[str] = "quote"
```

### 2. Simplified FactorRegistry (No Manual Dict)

```python
# factor_registry.py

class FactorRegistry:
    """Central registry using auto-discovered classes."""

    def __init__(self, config_path: Path | None = None):
        self.config_path = config_path
        self._specs: dict[str, FactorSpec] = {}
        self._loaded = False

    def load(self) -> None:
        """Load factor specs from YAML. Classes auto-discovered via Factor._registry."""
        if self._loaded:
            return

        # Import all indicator modules to trigger __init_subclass__ registration
        from jerry_trader.services.factor.indicators import (
            ema, trade_rate, relative_volume  # Add new imports here
        )
        # Or use dynamic import:
        # import importlib
        # for module_name in discover_indicator_modules():
        #     importlib.import_module(module_name)

        # Now Factor._registry contains all classes
        logger.info(f"Auto-discovered {len(Factor._registry)} factor classes")

        # Load YAML config (params, display, timeframes)
        with open(self.config_path) as f:
            config = yaml.safe_load(f)

        for factor_id, factor_config in config.get("factors", {}).items():
            class_name = factor_config.get("class")
            # Look up class from auto-registry (not manual dict!)
            factor_class = Factor._registry.get(class_name)
            if not factor_class:
                logger.warning(f"Factor class '{class_name}' not found in auto-registry")
                continue

            spec = FactorSpec(
                id=factor_id,
                factor_class=factor_class,  # Store class directly
                type=factor_class.factor_type,  # From class metadata
                params=factor_config.get("params", {}),
                display=parse_display(factor_config.get("display")),
                timeframes=factor_config.get("timeframes", []),
            )
            self._specs[factor_id] = spec

        self._loaded = True

    def create_indicator(self, factor_id: str) -> Factor | None:
        """Create factor instance from spec."""
        spec = self._specs.get(factor_id)
        if not spec:
            return None

        # Instantiate with params from YAML
        return spec.factor_class(**spec.params)
```

### 3. Factor Implementation Example

```python
# indicators/relative_volume.py

from dataclasses import dataclass, field
from collections import deque
from jerry_trader.services.factor.indicators.base import BarFactor
from jerry_trader.domain.market import Bar

@dataclass
class RelativeVolume(BarFactor):
    """Volume relative to N-period average.

    Auto-registered as "RelativeVolume" via __init_subclass__.
    """

    # Class metadata (auto-registration uses these)
    name: ClassVar[str] = "relative_volume"
    factor_type: ClassVar[str] = "bar"
    default_params: ClassVar[dict] = {"lookback": 20}

    # Instance params
    lookback: int = 20

    # State
    volumes: deque[float] = field(default_factory=lambda: deque(maxlen=20))
    _ready: bool = False

    def reset(self) -> None:
        self.volumes.clear()
        self._ready = False

    @property
    def ready(self) -> bool:
        return len(self.volumes) >= self.lookback

    def update(self, bar: Bar) -> float | None:
        """Incremental update."""
        self.volumes.append(bar.volume)
        if not self.ready:
            return None
        avg = sum(self.volumes) / len(self.volumes)
        return bar.volume / avg if avg > 0 else 0.0

    def compute_batch(self, bars: list[Bar]) -> list[float | None]:
        """Batch compute for backtest."""
        results = []
        volumes = []
        for bar in bars:
            volumes.append(bar.volume)
            if len(volumes) < self.lookback:
                results.append(None)
            else:
                avg = sum(volumes[-self.lookback:]) / self.lookback
                results.append(bar.volume / avg if avg > 0 else 0.0)
        return results
```

### 4. No More Hardcoded Fallbacks

```python
# factor_engine.py (V4)

class FactorEngine:
    def add_ticker(self, symbol: str, timeframes: list[str] | None = None):
        registry = get_factor_registry()

        # NO MORE HARDCODED FALLBACKS!
        # If registry is empty, that's a config error, not a runtime fallback.

        for tf in timeframes:
            # Get factors configured for this timeframe
            specs = registry.get_factors_for_timeframe(tf)
            if not specs:
                logger.warning(f"No factors configured for timeframe {tf}")
                continue  # Skip, don't fallback

            bar_factors = [registry.create_indicator(s.id) for s in specs if s.type == "bar"]

            state.timeframe_states[tf] = FactorTimeframeState(
                timeframe=tf,
                bar_indicators=bar_factors,
            )
```

---

## Comparison: V3 vs V4

| Aspect | V3 (Current) | V4 (Proposed) |
|--------|--------------|---------------|
| Add new factor | Write class + edit dict + edit YAML | Write class + edit YAML |
| Class discovery | Manual `INDICATOR_CLASSES` dict | Auto `__init_subclass__` |
| Fallback behavior | Hardcoded `[EMA(period=20)]` | Registry-driven or explicit error |
| Batch + incremental | Separate implementations | Unified interface |
| Type hierarchy | 3 separate ABCs | 1 base + 3 mixins |
| FactorEngine imports | `from indicators import EMA, TradeRate` | `import indicators` (all modules) |

---

## Implementation Steps

### Phase 1: Auto-Registration (ROADMAP 6.17)

1. Modify `base.py` to add `Factor` base with `__init_subclass__`
2. Create `BarFactor`, `TickFactor`, `QuoteFactor` mixins
3. Remove `INDICATOR_CLASSES` manual dict
4. Update `FactorRegistry` to use `Factor._registry`
5. Migrate `EMA` and `TradeRate` to new base classes

### Phase 2: Unified Batch + Incremental (ROADMAP 6.18)

1. Add `compute_batch()` to all factor classes
2. Update `BatchEngine` to use `compute_batch()` instead of loop
3. Verify backtest results match (factor values identical)

### Phase 3: Remove Hardcoded Fallbacks (ROADMAP 6.19)

1. Remove fallback code from `FactorEngine`
2. Add config validation: error if no factors for required timeframe
3. Update tests to use explicit factor config

---

## Edge Cases

### What if same class name in multiple modules?

Python's `__init_subclass__` uses `cls.__name__`, so duplicate class names would collide.
Solution: Use fully qualified name or explicit `name: ClassVar[str]` override.

```python
class RelativeVolume(BarFactor):
    name: ClassVar[str] = "relative_volume"  # Explicit, not class name
```

Registry keyed by both `__name__` and `name` (if set).

### What about Rust factors?

Rust implementation mirrors Python interface:

```rust
pub trait Factor: Send + Sync {
    fn name(&self) -> &str;
    fn compute_batch(&self, data: &[Bar]) -> Vec<Option<f64>>;
    fn update(&mut self, bar: &Bar) -> Option<f64>;
    fn reset(&mut self);
}

// inventory::submit! for auto-registration (Rust equivalent)
```

Python registry would include both Python and Rust factors (via PyO3 bridge).

---

## Questions for Discussion

1. Should YAML be optional? (Pure Python config via class attributes)
2. How to handle factor variants (EMA_20, EMA_50) - same class, different params?
3. Should `compute_batch` return timestamps too? (For ClickHouse writes)
4. Validation: what to do if timeframe has no configured factors?
