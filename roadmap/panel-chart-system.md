# TradingView-style Panel Chart System

## Context

Current architecture separates ChartModule (OHLCV bars) and FactorChartModule (factors). This leads to:
- Duplicate symbol/timeframe management
- Timeframe misalignment between charts
- Redundant subscriptions
- Non-intuitive UX (factors in separate window)

TradingView's approach is more elegant: single chart with multiple collapsible panels, all sharing the same timeframe.

## Analysis

### Current State
- `ChartModule.tsx` - OHLCV bars only
- `FactorChartModule.tsx` - Factors with dual Y-axis
- Both manage their own symbol/timeframe state
- Factor data fetched separately from bar data

### Similar Patterns
- TradingView: Panels for price, volume, indicators (RSI, MACD)
- Each panel can collapse/expand
- All share same symbol/timeframe
- Overlays (MA, Bollinger) render on price chart

### Design Decisions

**Panel Types:**
| Type | Description | Example |
|------|-------------|---------|
| `price` | OHLCV bars + overlays | Main chart |
| `panel` | Separate Y-axis, collapsible | TradeRate, RSI, MACD |

**Factor Display Modes:**
| Mode | Behavior | Example |
|------|----------|---------|
| `overlay` | Render on price chart | EMA, VWAP |
| `panel` | Separate panel below | TradeRate, RSI |

**Panel Operations:**
- `[−]` Collapse - show only header
- `[×]` Close - remove panel entirely
- `+ Add Panel` - dropdown with available panels

## Decision

Merge FactorChartModule into ChartModule with a panel system:

1. Single module handles all chart types
2. Panel state stored in module settings
3. Factor Registry extended with `display.mode`
4. All panels share same symbol/timeframe (synced)

## Rejected

**Option A: Keep separate modules**
- Requires user to sync timeframes manually
- Duplicate subscriptions
- More complex state management

**Option B: Tabs for different charts**
- Not TradingView-like
- Hard to compare factors side-by-side

## Plan

### Phase 1: Factor Registry Enhancement
- [x] Add `display.mode` field to FactorSpec
- [x] Update factors.yaml with mode for each factor
- [x] Update frontend FactorConfig type

### Phase 2: Panel State Management
- [x] Define ChartPanel interface
- [x] Add panel state to ChartModule settings
- [x] Implement panel add/remove/collapse logic

### Phase 3: UI Implementation
- [x] Panel header component with collapse/close buttons
- [x] Add Panel dropdown with checkboxes
- [x] Create ChartPanelSystem component with panels
- [x] Implement overlay rendering on price chart
- [x] Integrate factor panels

### Phase 4: Cleanup & Bug Fixes
- [x] Create ChartPanelSystem component
- [x] Update moduleRegistry to use ChartPanelSystem
- [x] Fix real-time price chart updates (trade tick processing)
- [x] Fix TradeRate canvas repositioning (only fitContent once)
- [x] Fix EMA20 overlay rendering (chartReady state timing)
- [ ] Remove FactorChartModule (optional - kept for backward compatibility)
- [ ] Test with existing factors

## Technical Details

### State Structure

```typescript
interface ChartPanel {
  id: string;           // 'price', 'trade_rate', 'rsi_14'
  type: 'price' | 'panel';
  collapsed: boolean;
  visible: boolean;     // false = closed/removed
  factors: string[];    // for price panel: overlays ['ema_20', 'vwap']
  height?: number;      // optional custom height ratio
}

interface ChartModuleSettings {
  symbol: string;
  timeframe: ChartTimeframe;
  panels: ChartPanel[];
}
```

### Default Configuration

```typescript
const defaultPanels: ChartPanel[] = [
  { id: 'price', type: 'price', collapsed: false, visible: true, factors: ['ema_20'] },
  { id: 'trade_rate', type: 'panel', collapsed: false, visible: true, factors: [] },
];
```

### Factor Registry Schema

```yaml
ema_20:
  type: bar
  display:
    mode: overlay    # NEW: overlay on price chart
    name: EMA(20)
    color: "#3b82f6"

trade_rate:
  type: trade
  display:
    mode: panel      # NEW: separate panel
    name: TradeRate
    color: "#f97316"
```

### Files to Modify

| File | Changes |
|------|---------|
| `FactorSpec` dataclass | Add `display.mode` field |
| `factors.yaml` | Add mode to each factor |
| `FactorChartModule.tsx` | DELETE (merge into ChartModule) |
| `ChartModule.tsx` | Add panel system, overlay rendering |
| `types.ts` | Add ChartPanel interface |
| `moduleRegistry.ts` | Remove FactorChartModule |
