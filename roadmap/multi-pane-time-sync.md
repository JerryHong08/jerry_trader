# Multi-pane Chart Time Axis Sync

## Context

BacktestChartModule has a multi-pane chart layout:
- Price panel (OHLCV bars)
- Volume panel (optional)
- Factor panels (trade_rate, rel_vol_20, etc.)

Each panel is a separate `IChartApi` instance. Users expect all panels to share a synchronized time axis - when they pan/zoom on one panel, others should follow.

## Problem

**Lightweight Charts cannot properly sync time scales across panels with different data granularity.**

Attempts:

1. **`Range<Time>` (time-based)**: `setVisibleRange()` adjusts to nearest data point on each chart. Factor data has different timestamps than bar data, causing pixel offset.

2. **`LogicalRange` (index-based)**: `setVisibleLogicalRange()` ensures same screen position, but index 5 on price chart ≠ index 5 on factor chart (different data point counts).

3. **Forward-fill padding**: Tried padding factor data to match bar timestamps. Still has issues because:
   - Factor data may start/end at different times
   - Padding with forward-fill creates visual artifacts
   - Library still handles time->index mapping differently

## Root Cause

Lightweight Charts is designed for single-chart scenarios. Each `IChartApi` has its own time scale that maps time to logical index based on its data. When data differs, the mapping differs.

Official docs state:
- `setVisibleRange()` "cannot extrapolate time"
- `setVisibleLogicalRange()` uses logical indices specific to each chart's data

## Possible Solutions

### Option A: Single chart with multiple panes
Create one chart with multiple series/panes instead of separate chart instances. This naturally shares time scale.

**Pros**: Native sync, no hacky workaround
**Cons**: Requires significant refactor, loses panel independence

### Option B: Backend data alignment
Ensure factor API returns data with timestamps exactly matching bar timestamps.

**Pros**: LogicalRange would work
**Cons**: Backend change, may lose factor resolution

### Option C: Accept current state
Keep panels independent, no sync. Each panel has its own time scale controls.

**Pros**: Simple, no bugs
**Cons**: UX trade-off

## Decision

**Option C for now** - accept current state without sync.

Reasons:
- Sync attempts caused bugs worse than no sync
- Users can still manually adjust each panel
- Future: explore Option A when time permits

## Status

- [x] Attempted `Range<Time>` sync - failed
- [x] Attempted `LogicalRange` sync - failed
- [x] Attempted forward-fill padding - failed
- [x] Removed broken sync code
- [ ] Future: Consider single-chart refactor
