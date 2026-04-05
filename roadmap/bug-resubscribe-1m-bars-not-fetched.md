# Bug: Re-subscribe 1m Bars Not Re-fetched

**Created:** 2026-04-05
**Status:** Fixed
**Severity:** Medium
**Component:** Frontend — ChartPanelSystem.tsx

## Symptoms

When a user unsubscribes a ticker and re-subscribes from a 10s timeframe panel:
- 10s price chart works correctly (bars re-fetched via REST + WS)
- 1m price chart shows **stale bars** from the first subscription
- 1m overlays (EMA) are stale or missing
- trade_rate on the 1m panel may show stale or no data

## Root Cause

`ChartPanelSystem.tsx` has a `useEffect` that fetches bars on subscription. The effect's dependency array is `[symbol, timeframe, moduleId]` — it only fetches bars for the **current panel's timeframe**.

On re-subscribe from 10s:
1. Frontend clears all bar data via `clearBarsForTicker()` (cheat method)
2. Only the 10s panel's `useEffect` fires (since that's the active panel)
3. The 1m panel's bars were cleared but never re-fetched
4. The 1m panel either shows nothing or holds stale data if `clearBarsForTicker` wasn't called

Even with the cheat method cleanup, the 1m panel's `useEffect` won't re-fire because `symbol` didn't change from its perspective — the ticker was added back with the same symbol name.

## Affected Files

- `frontend/src/components/ChartPanelSystem.tsx` — useEffect dependency logic
- `frontend/src/stores/chartDataStore.ts` — bar fetch and storage
- `frontend/src/stores/tickDataStore.ts` — subscribe/unsubscribe orchestration

## Fix Options

### Option A: Fetch All Timeframes on Subscribe (Recommended)

When `subscribeFactors` is called for a ticker, fetch bars for ALL timeframes that have active panels, not just the triggering one.

- Modify `subscribeFactors` or add a post-subscribe step
- After WS subscribe succeeds, issue REST bar requests for all active timeframes

### Option B: Fetch Counter / Version

Add a `fetchVersion` counter to `chartDataStore` that increments on every subscribe. The `useEffect` includes `fetchVersion` in its deps, forcing re-fetch for all timeframes.

```typescript
// chartDataStore.ts
fetchVersion: number;
incrementFetchVersion: (ticker: string) => void;

// ChartPanelSystem.tsx useEffect deps
}, [symbol, timeframe, moduleId, fetchVersion])
```

### Option C: Symbol-specific Version

Track version per ticker. Only the affected ticker's panels re-fetch.

## Related

- `roadmap/re-subscribe-gap-fill.md` — overall re-subscribe design
- `roadmap/factor-subscription-scenarios.md` — scenario analysis
