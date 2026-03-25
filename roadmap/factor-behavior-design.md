# Factor Behavior Design Discussion

**Status:** ✅ Task 3.3 Completed - Multi-timeframe FactorEngine implemented

## Implementation Summary

- [x] Multi-timeframe FactorEngine with per-timeframe state management
- [x] Dynamic timeframe registration via `add_ticker(timeframes=[...])`
- [x] Timeframe-specific Redis pub/sub channels (`factors:AAPL:10s`, `factors:AAPL:1m`)
- [x] Tick-based factors (TradeRate) stored with timeframe='tick'
- [x] Bar-based factors (EMA20) stored per timeframe

## Current Situation

- **TradeRate**: Updates every 1s (tick-based), timeframe-agnostic
- **EMA20**: Updates on bar close (bar-based), currently hardcoded to '1m'
- **Problem**: Frontend displays 5m, but backend computes on 1m - mismatch

## Design Questions & Recommendations

### 1. Should EMA(20) Update Every 1s Like TradeRate?

**No.** EMA is inherently a bar-based indicator:

```
EMA(today) = Price(today) × k + EMA(yesterday) × (1 - k)
```

It needs a **sampling period** (bar close) to be meaningful.

**But:** Users want to see "current EMA value" intrabar. Options:

| Approach | Update Frequency | Use Case |
|----------|------------------|----------|
| **Bar EMA** (current) | On bar close | Final signal generation |
| **Running EMA** | Every tick/1s | Visual feedback during bar |

**Recommendation:**
- Keep bar-based EMA for signals/backtesting
- Optional: Add "intrabar projection" - estimate EMA if bar closed now
  - Formula: `projected_ema = (current_price × k) + (last_ema × (1 - k))`
  - Updates every tick, but marked as "estimated"

---

### 2. How Should TradeRate Behave About Timeframe?

**TradeRate is timeframe-agnostic by nature.** It's:
```
TradeRate = (# trades in last 20s) / 20s
```

Always computed from raw tick stream.

**But for display/analysis, it needs timeframe alignment:**

| Mode | Behavior |
|------|----------|
| **Real-time** | Updates every 1s, continuous |
| **Historical per bar** | "What was avg TradeRate during this 5m bar?" |

**Recommendation:**
- Real-time: Continuous 1s updates (current)
- Historical: Store "average TradeRate per bar" for backtesting
- Frontend displays current TradeRate regardless of chart timeframe

---

### 3. What About More Factors?

**Factor Categories:**

```
┌─────────────────┬──────────────────┬──────────────────┐
│ Tick-Based      │ Bar-Based        │ Multi-Bar        │
│ (High Freq)     │ (Per Bar)        │ (Lookback)       │
├─────────────────┼──────────────────┼──────────────────┤
│ TradeRate       │ EMA              │ RSI(14)          │
│ VolumePerSecond │ VWAP             │ MACD             │
│ BidAskSpread    │ Open,High,Low,Close│ BollingerBands │
│ TradeSizeAvg    │ Range (H-L)      │ ATR              │
└─────────────────┴──────────────────┴──────────────────┘
```

**Timeframe Implications:**

| Factor Type | Timeframe-Specific? | Notes |
|-------------|---------------------|-------|
| Tick-based | No | Same computation, regardless of bar size |
| Bar-based | Yes | EMA(20) on 1m ≠ EMA(20) on 5m |
| Multi-bar | Yes | RSI(14) needs 14 bars of specific timeframe |

---

## Proposed Architecture

### Multi-Timeframe FactorEngine

```python
class MultiTimeframeFactorEngine:
    """Computes factors for all active timeframes."""

    def __init__(self):
        # Per-ticker, per-timeframe state
        self.ticker_tf_state: Dict[(str, str), TickerTimeframeState]

    def add_ticker_timeframe(self, symbol: str, timeframe: str):
        """Activate factor computation for symbol+timeframe."""
        # Creates BarBuilder listener for this timeframe
        # Creates indicator state (EMA20, etc.)

    def on_bar_close(self, symbol: str, timeframe: str, bar: Bar):
        """Update bar-based factors for this timeframe."""
        # Update EMA20(symbol, timeframe)
        # Update RSI(symbol, timeframe)
        # Publish to factors:{symbol}:{timeframe}

    def on_tick(self, symbol: str, tick: Tick):
        """Update tick-based factors (timeframe-agnostic)."""
        # Update TradeRate(symbol) - no timeframe
        # Publish to factors:{symbol}:tick
```

### Frontend Subscription

```typescript
// Factor chart at 5m timeframe
subscribeFactors('AAPL', '5m')  // → listens to factors:AAPL:5m

// Different factor chart at 1m
subscribeFactors('AAPL', '1m')  // → listens to factors:AAPL:1m

// Can have both simultaneously
```

### Redis Channels

```
factors:AAPL:10s    # EMA20 on 10s bars
factors:AAPL:1m     # EMA20 on 1m bars
factors:AAPL:5m     # EMA20 on 5m bars
factors:AAPL:tick   # TradeRate (timeframe-agnostic)
```

---

## Immediate Actions Needed

1. **Task 3.3**: Multi-timeframe FactorEngine
   - Remove hardcoded '1m' timeframe
   - Support dynamic timeframe registration
   - Publish to timeframe-specific channels

2. **Frontend**: Subscribe with timeframe
   - `subscribeFactors(symbol, timeframe)` instead of just symbol

3. **TradeRate Decision:**
   - Keep as "tick" channel (timeframe-agnostic)
   - OR duplicate to all timeframe channels as "auxiliary data"

---

## Open Questions

1. **Memory/CPU tradeoff:** Computing EMA for 10 timeframes × 100 tickers = 1000 EMA instances. Acceptable?

Ans: Typical compuation amount will be [10s, 1m, 5m, ...] x 10 tickers(at most)

2. **TradeRate storage:** Store per-bar average for backtesting, or only real-time?

Ans: store per-bar for bar based factor, store real-time for tick-based

3. **Factor priority:** Which factors to implement next?
   - VWAP (bar-based, anchored)
   - RSI (multi-bar)
   - Volume Profile (tick-based, session)

Ans: You decide.
