# Factor Subscription Scenarios Analysis

**Created:** 2026-04-05
**Status:** Investigating
**Component:** ChartPanelSystem + ChartBFF + FactorEngine

## Current Frontend Layout

```
┌─────────────────────────────────────┐
│ Chart 1: timeframe=10s              │
│ - Price Channel                      │
│   - overlay: EMA (bar-based factor)  │
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│ Chart 2: timeframe=1m               │
│ - Price Channel                      │
│   - overlay: EMA (bar-based factor)  │
│ - Trade Rate Channel                 │
│   - trade_rate (tick-based factor)   │
└─────────────────────────────────────┘
```

## Problem Scenarios

### Scenario A: Timeframe Switch on 10s Chart (First Subscription)

**Steps:**
1. Frontend has no prior subscription for PRSO
2. User adds PRSO ticker on Chart 1 (10s timeframe)
3. First bootstrap succeeds, real-time updates work
4. User switches Chart 1 timeframe from 10s → 5s (or any other timeframe)

**Expected Behavior:**
- Unsubscribe PRSO/10s factors
- Subscribe PRSO/5s factors
- Bootstrap PRSO/5s factors
- Real-time updates for PRSO/5s

**Current Problem:**
- ?

---

### Scenario B: Timeframe Switch on 1m Chart (After First Subscription)

**Steps:**
1. User has PRSO subscribed on Chart 1 (10s timeframe)
2. User adds PRSO ticker on Chart 2 (1m timeframe)
3. Both charts receive updates correctly
4. User switches Chart 2 timeframe from 1m → 5m

**Expected Behavior:**
- Unsubscribe PRSO/1m factors
- Subscribe PRSO/5m factors
- Bootstrap PRSO/5m factors (if not already subscribed)
- Chart 1 (10s) should continue receiving updates unaffected

**Current Problem:**
- ?

---

### Scenario C: Unsubscribe and Re-subscribe After Delay

**Steps:**
1. User has PRSO subscribed on both charts
2. User removes PRSO ticker from all charts (unsubscribe)
3. Wait 30 seconds (or 2 minutes)
4. User adds PRSO ticker again on Chart 1 (10s timeframe)

**Expected Behavior:**
- Clean unsubscribe when ticker removed
- On re-subscribe:
  - Bootstrap fetches historical data (gap fill from last data point?)
  - Or bootstrap fetches full window (last N bars)
  - Real-time updates resume

**Current Problem:**
- Behavior unstable - what happens?

---

### Scenario D: Multiple Charts Same Ticker Different Timeframes

**Steps:**
1. Chart 1 subscribes PRSO/10s
2. Chart 2 subscribes PRSO/1m (same ticker, different timeframe)
3. Both charts should receive updates independently

**Expected Behavior:**
- Backend maintains separate factor streams per timeframe
- Each chart receives only its timeframe's updates
- No interference between timeframes

**Current Problem:**
- ?

---

### Scenario E: Same Ticker Same Timeframe on Multiple Charts

**Steps:**
1. Both Chart 1 and Chart 2 set to 10s timeframe
2. Both subscribe PRSO/10s
3. One chart unsubscribes PRSO

**Expected Behavior:**
- Other chart continues receiving updates (reference counting?)
- Or backend stops publishing when any subscriber unsubscribes?

**Current Problem:**
- ?

---

## Backend Behavior Questions

### Factor Subscription Lifecycle

| Event | Expected Backend Action | Current Behavior |
|-------|------------------------|------------------|
| First subscribe_factors | Bootstrap + start publishing | Works |
| Subscribe new timeframe | Bootstrap new timeframe + add to publishing | ? |
| Unsubscribe timeframe | Stop publishing that timeframe | ? |
| Unsubscribe all timeframes | Cleanup ticker state entirely | ? |
| Re-subscribe after gap | Bootstrap with gap fill | ? |

### Gap Fill Logic

| Scenario | Expected Gap Fill | Current Behavior |
|----------|------------------|------------------|
| Re-subscribe immediately | No gap needed (last data point valid) | ? |
| Re-subscribe after 30s | Fetch missing bars from ClickHouse | ? |
| Re-subscribe after 2min | Fetch missing bars from ClickHouse | ? |

---

## Investigation Plan

1. **Test each scenario** with logging enabled
2. **Compare expected vs actual behavior**
3. **Identify root cause** for each deviation
4. **Fix systematically** - prioritize by user impact

---

## Notes

- Need to check `tickDataStore.ts` `subscribeFactors` / `unsubscribeFactors` implementation
- Need to check `ChartBFF` WebSocket handler for subscription management
- Need to check `FactorEngine` for multi-timeframe state management
- Need to check `Coordinator` for ticker lifecycle management
