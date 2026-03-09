/**
 * Chart Data Zustand Store
 *
 * Manages OHLCV bar data for the ChartModule (candlestick chart).
 *
 * Responsibilities:
 *   - Fetch historical bars from BFF REST API (bootstrap)
 *   - Maintain per-symbol bar state with timeframe tracking
 *   - Update the current (last) bar from real-time trade ticks
 *   - Handle bar boundary transitions (create new bar when trade crosses boundary)
 *
 * Data Flow:
 *   Bootstrap:  fetchBars() → GET /api/chart/bars/{ticker} → store bars
 *   Real-time:  tickDataStore trade → updateFromTrade() → update current bar
 *
 * The store is intentionally separated from tickDataStore to maintain
 * single-responsibility: tickDataStore handles raw tick data subscription,
 * while this store handles OHLCV bar aggregation and state.
 */

import { create } from 'zustand';
import type { ChartTimeframe } from '../types';

// ============================================================================
// Types
// ============================================================================

export interface OHLCVBar {
  time: number; // Unix timestamp in seconds (UTC)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ChartBarsResponse {
  ticker: string;
  timeframe: string;
  bars: OHLCVBar[];
  barCount: number;
  barDurationSec: number;
  source: string;
  from: string;
  to: string;
  error?: string;
  requestId?: string;
}

/** Per-symbol chart state */
export interface SymbolChartState {
  bars: OHLCVBar[];
  timeframe: ChartTimeframe;
  barDurationSec: number;
  loading: boolean;
  error: string | null;
  source: string | null;
  lastFetchTime: number | null; // When bars were last fetched (ms)
  requestId: string | null;     // Echoed from BFF to detect stale responses
}

/** Build the composite store key: "moduleId::TICKER" */
export function chartStoreKey(moduleId: string, ticker: string): string {
  return `${moduleId}::${ticker.toUpperCase()}`;
}

type ChartDataState = {
  // Per-instance + symbol bar data (keyed by "moduleId::TICKER")
  symbolBars: Record<string, SymbolChartState>;

  // Actions — moduleId scopes state per chart instance
  fetchBars: (moduleId: string, ticker: string, timeframe: ChartTimeframe) => Promise<void>;
  updateFromTrade: (
    moduleId: string,
    symbol: string,
    price: number,
    size: number,
    timestampMs: number,
  ) => void;
  applyBarUpdate: (moduleId: string, ticker: string, timeframe: string, bar: OHLCVBar) => void;
  /** Broadcast a bar update to ALL chart instances showing this ticker+timeframe */
  broadcastBarUpdate: (ticker: string, timeframe: string, bar: OHLCVBar) => void;
  clearSymbol: (moduleId: string, symbol: string) => void;
  reset: () => void;
};

// ============================================================================
// Config
// ============================================================================

function getChartBffBaseUrl(): string {
  const defaultHost =
    typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  const url =
    typeof import.meta !== 'undefined' && import.meta.env?.VITE_CHART_BFF_URL
      ? (import.meta.env.VITE_CHART_BFF_URL as string)
      : `http://${defaultHost}:5002`;
  return url || `http://${defaultHost}:5002`;
}

/** Bar duration in seconds for each timeframe */
const TIMEFRAME_DURATION_SEC: Record<ChartTimeframe, number> = {
  '10s': 10,
  '1m': 60,
  '5m': 300,
  '15m': 900,
  '30m': 1800,
  '1h': 3600,
  '4h': 14400,
  '1D': 86400,
  '1W': 604800,
  '1M': 2592000,
};

/** Minimum refetch interval per timeframe (ms) — prevents hammering API */
const MIN_REFETCH_INTERVAL: Record<ChartTimeframe, number> = {
  '10s': 10_000, // 10s
  '1m': 30_000, // 30s
  '5m': 60_000,
  '15m': 120_000,
  '30m': 300_000,
  '1h': 300_000,
  '4h': 600_000,
  '1D': 600_000,
  '1W': 600_000,
  '1M': 600_000,
};

// ============================================================================
// Store
// ============================================================================

export const useChartDataStore = create<ChartDataState>()((set, get) => ({
  symbolBars: {},

  // ========================================================================
  // Fetch historical bars
  // ========================================================================
  fetchBars: async (moduleId: string, ticker: string, timeframe: ChartTimeframe) => {
    const tickerUpper = ticker.toUpperCase();
    const key = chartStoreKey(moduleId, tickerUpper);
    const existing = get().symbolBars[key];

    // Skip if already loading
    if (existing?.loading) return;

    // Skip if recently fetched for the same timeframe
    if (
      existing &&
      existing.timeframe === timeframe &&
      existing.lastFetchTime &&
      Date.now() - existing.lastFetchTime < MIN_REFETCH_INTERVAL[timeframe]
    ) {
      return;
    }

    // Set loading state
    // Generate a unique request ID so we can discard stale responses
    const requestId = `${tickerUpper}-${timeframe}-${Date.now()}`;

    set((s) => ({
      symbolBars: {
        ...s.symbolBars,
        [key]: {
          bars: existing?.timeframe === timeframe ? existing.bars : [],
          timeframe,
          barDurationSec: TIMEFRAME_DURATION_SEC[timeframe],
          loading: true,
          error: null,
          source: existing?.source ?? null,
          lastFetchTime: existing?.lastFetchTime ?? null,
          requestId,
        },
      },
    }));

    try {
      const baseUrl = getChartBffBaseUrl();
      const params = new URLSearchParams({ timeframe, request_id: requestId });
      const url = `${baseUrl}/api/chart/bars/${tickerUpper}?${params}`;

      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }

      const data: ChartBarsResponse = await res.json();

      // Race condition guard: if another fetch for this ticker was started
      // while we were waiting, our requestId will no longer match — discard.
      const current = get().symbolBars[key];
      if (current?.requestId !== requestId) {
        return; // superseded by a newer fetch
      }

      if (data.error) {
        set((s) => ({
          symbolBars: {
            ...s.symbolBars,
            [key]: {
              ...s.symbolBars[key],
              loading: false,
              error: data.error ?? 'No data',
              lastFetchTime: Date.now(),
              requestId,
            },
          },
        }));
        return;
      }

      set((s) => ({
        symbolBars: {
          ...s.symbolBars,
          [key]: {
            bars: data.bars,
            timeframe,
            barDurationSec: data.barDurationSec || TIMEFRAME_DURATION_SEC[timeframe],
            loading: false,
            error: null,
            source: data.source,
            lastFetchTime: Date.now(),
            requestId,
          },
        },
      }));

      console.log(
        `📊 Chart bars loaded: ${tickerUpper} ${timeframe} — ${data.bars.length} bars` +
        ` (${data.source})` +
        (data.bars.length > 0
          ? ` | first=${new Date(data.bars[0].time * 1000).toISOString()}` +
            ` | last=${new Date(data.bars[data.bars.length - 1].time * 1000).toISOString()}` +
            ` | range=${data.from}..${data.to}`
          : ''),
      );

    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      console.error(`❌ Chart bars fetch failed: ${tickerUpper}`, errMsg);
      set((s) => ({
        symbolBars: {
          ...s.symbolBars,
          [key]: {
            ...s.symbolBars[key],
            loading: false,
            error: errMsg,
            lastFetchTime: Date.now(),
          },
        },
      }));
    }
  },

  // ========================================================================
  // Real-time bar update from trade tick
  // ========================================================================
  updateFromTrade: (
    moduleId: string,
    symbol: string,
    price: number,
    size: number,
    timestampMs: number,
  ) => {
    const tickerUpper = symbol.toUpperCase();
    const key = chartStoreKey(moduleId, tickerUpper);
    const state = get().symbolBars[key];
    if (!state || state.bars.length === 0 || state.loading) return;

    const bars = [...state.bars];
    const lastBar = bars[bars.length - 1];
    const barDuration = state.barDurationSec;
    const tradeSec = Math.floor(timestampMs / 1000);

    // Compute the bar boundary this trade belongs to.
    // For daily+ bars (>= 86400s), Polygon bar timestamps are NOT exact
    // multiples of barDuration (they use market-open or date-based offsets).
    // Use range comparison instead of floor alignment for those.
    let tradeBarTime: number;
    if (barDuration >= 86400) {
      if (tradeSec >= lastBar.time && tradeSec < lastBar.time + barDuration) {
        // Trade falls within the current bar's expected range
        tradeBarTime = lastBar.time;
      } else if (tradeSec >= lastBar.time + barDuration) {
        // Trade is past the current bar — new bar
        tradeBarTime = lastBar.time + barDuration;
      } else {
        // Trade is before the current bar — stale, ignore
        return;
      }
    } else {
      // Intraday: floor alignment works perfectly
      tradeBarTime = Math.floor(tradeSec / barDuration) * barDuration;
    }

    if (tradeBarTime === lastBar.time) {
      // Trade belongs to current bar — update OHLC in place
      const updatedBar: OHLCVBar = {
        ...lastBar,
        high: Math.max(lastBar.high, price),
        low: Math.min(lastBar.low, price),
        close: price,
        volume: lastBar.volume + size,
      };
      bars[bars.length - 1] = updatedBar;
    } else if (tradeBarTime > lastBar.time) {
      // Trade crosses into a new bar — create new bar
      const newBar: OHLCVBar = {
        time: tradeBarTime,
        open: price,
        high: price,
        low: price,
        close: price,
        volume: size,
      };
      bars.push(newBar);

      // Cap total bars to prevent memory growth
      if (bars.length > 2000) {
        bars.splice(0, bars.length - 2000);
      }
    }
    // else: trade is for an older bar — ignore (out of order)

    set((s) => ({
      symbolBars: {
        ...s.symbolBars,
        [key]: {
          ...state,
          bars,
        },
      },
    }));
  },

  // ========================================================================
  // Server-pushed completed bar (from BarsBuilder via BFF WebSocket)
  // ========================================================================
  applyBarUpdate: (moduleId: string, ticker: string, timeframe: string, bar: OHLCVBar) => {
    const tickerUpper = ticker.toUpperCase();
    const key = chartStoreKey(moduleId, tickerUpper);
    const state = get().symbolBars[key];
    if (!state || state.loading) return;

    // Map BarBuilder timeframe names (lowercase) to frontend convention
    const tfMap: Record<string, string> = {
      '10s': '10s', '1m': '1m', '5m': '5m', '15m': '15m',
      '1h': '1h', '4h': '4h', '1d': '1D', '1w': '1W',
    };
    const frontendTf = tfMap[timeframe] ?? timeframe;

    // Only apply if this update matches the currently displayed timeframe
    if (state.timeframe !== frontendTf) return;

    const bars = [...state.bars];
    const lastBar = bars[bars.length - 1];

    if (lastBar && bar.time === lastBar.time) {
      // Update existing bar in place (late tick → re-emit)
      bars[bars.length - 1] = bar;
    } else if (!lastBar || bar.time > lastBar.time) {
      // New completed bar — append
      bars.push(bar);
      if (bars.length > 2000) {
        bars.splice(0, bars.length - 2000);
      }
    }
    // else: stale bar — ignore

    set((s) => ({
      symbolBars: {
        ...s.symbolBars,
        [key]: {
          ...state,
          bars,
        },
      },
    }));
  },

  // ========================================================================
  // Broadcast bar update to ALL instances showing this ticker+timeframe
  // (called from WebSocket handler which doesn't know about moduleIds)
  // ========================================================================
  broadcastBarUpdate: (ticker: string, timeframe: string, bar: OHLCVBar) => {
    const tickerUpper = ticker.toUpperCase();
    const suffix = `::${tickerUpper}`;

    // Map BarBuilder timeframe names to frontend convention
    const tfMap: Record<string, string> = {
      '10s': '10s', '1m': '1m', '5m': '5m', '15m': '15m',
      '1h': '1h', '4h': '4h', '1d': '1D', '1w': '1W',
    };
    const frontendTf = tfMap[timeframe] ?? timeframe;

    const allBars = get().symbolBars;
    const updates: Record<string, SymbolChartState> = {};

    for (const [key, state] of Object.entries(allBars)) {
      if (!key.endsWith(suffix)) continue;
      if (state.loading || state.timeframe !== frontendTf) continue;

      const bars = [...state.bars];
      const lastBar = bars[bars.length - 1];

      if (lastBar && bar.time === lastBar.time) {
        bars[bars.length - 1] = bar;
      } else if (!lastBar || bar.time > lastBar.time) {
        bars.push(bar);
        if (bars.length > 2000) bars.splice(0, bars.length - 2000);
      } else {
        continue; // stale
      }

      updates[key] = { ...state, bars };
    }

    if (Object.keys(updates).length > 0) {
      set((s) => ({
        symbolBars: { ...s.symbolBars, ...updates },
      }));
    }
  },

  // ========================================================================
  // Cleanup
  // ========================================================================
  clearSymbol: (moduleId: string, symbol: string) => {
    set((s) => {
      const next = { ...s.symbolBars };
      delete next[chartStoreKey(moduleId, symbol)];
      return { symbolBars: next };
    });
  },

  reset: () => {
    set({ symbolBars: {} });
  },
}));
