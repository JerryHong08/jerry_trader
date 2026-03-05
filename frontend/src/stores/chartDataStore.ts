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
}

type ChartDataState = {
  // Per-symbol bar data
  symbolBars: Record<string, SymbolChartState>;

  // Actions
  fetchBars: (ticker: string, timeframe: ChartTimeframe) => Promise<void>;
  updateFromTrade: (
    symbol: string,
    price: number,
    size: number,
    timestampMs: number,
  ) => void;
  clearSymbol: (symbol: string) => void;
  reset: () => void;
};

// ============================================================================
// Config
// ============================================================================

function getBffBaseUrl(): string {
  const defaultHost =
    typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  const url =
    typeof import.meta !== 'undefined' && import.meta.env?.VITE_BFF_URL
      ? (import.meta.env.VITE_BFF_URL as string)
      : `http://${defaultHost}:5001`;
  return url || `http://${defaultHost}:5001`;
}

/** Bar duration in seconds for each timeframe */
const TIMEFRAME_DURATION_SEC: Record<ChartTimeframe, number> = {
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
  fetchBars: async (ticker: string, timeframe: ChartTimeframe) => {
    const tickerUpper = ticker.toUpperCase();
    const existing = get().symbolBars[tickerUpper];

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
    set((s) => ({
      symbolBars: {
        ...s.symbolBars,
        [tickerUpper]: {
          bars: existing?.timeframe === timeframe ? existing.bars : [],
          timeframe,
          barDurationSec: TIMEFRAME_DURATION_SEC[timeframe],
          loading: true,
          error: null,
          source: existing?.source ?? null,
          lastFetchTime: existing?.lastFetchTime ?? null,
        },
      },
    }));

    try {
      const baseUrl = getBffBaseUrl();
      const params = new URLSearchParams({ timeframe });
      const url = `${baseUrl}/api/chart/bars/${tickerUpper}?${params}`;

      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }

      const data: ChartBarsResponse = await res.json();

      if (data.error) {
        set((s) => ({
          symbolBars: {
            ...s.symbolBars,
            [tickerUpper]: {
              ...s.symbolBars[tickerUpper],
              loading: false,
              error: data.error ?? 'No data',
              lastFetchTime: Date.now(),
            },
          },
        }));
        return;
      }

      // Deduplicate and sort bars by time
      const barMap = new Map<number, OHLCVBar>();
      for (const bar of data.bars) {
        barMap.set(bar.time, bar);
      }
      const sortedBars = Array.from(barMap.values()).sort(
        (a, b) => a.time - b.time,
      );

      set((s) => ({
        symbolBars: {
          ...s.symbolBars,
          [tickerUpper]: {
            bars: sortedBars,
            timeframe,
            barDurationSec: data.barDurationSec || TIMEFRAME_DURATION_SEC[timeframe],
            loading: false,
            error: null,
            source: data.source,
            lastFetchTime: Date.now(),
          },
        },
      }));

      console.log(
        `📊 Chart bars loaded: ${tickerUpper} ${timeframe} — ${sortedBars.length} bars (${data.source})`,
      );
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      console.error(`❌ Chart bars fetch failed: ${tickerUpper}`, errMsg);
      set((s) => ({
        symbolBars: {
          ...s.symbolBars,
          [tickerUpper]: {
            ...s.symbolBars[tickerUpper],
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
    symbol: string,
    price: number,
    size: number,
    timestampMs: number,
  ) => {
    const tickerUpper = symbol.toUpperCase();
    const state = get().symbolBars[tickerUpper];
    if (!state || state.bars.length === 0 || state.loading) return;

    const bars = [...state.bars];
    const lastBar = bars[bars.length - 1];
    const barDuration = state.barDurationSec;
    const tradeSec = Math.floor(timestampMs / 1000);

    // Compute the bar boundary this trade belongs to
    const tradeBarTime = Math.floor(tradeSec / barDuration) * barDuration;

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
        [tickerUpper]: {
          ...state,
          bars,
        },
      },
    }));
  },

  // ========================================================================
  // Cleanup
  // ========================================================================
  clearSymbol: (symbol: string) => {
    set((s) => {
      const next = { ...s.symbolBars };
      delete next[symbol.toUpperCase()];
      return { symbolBars: next };
    });
  },

  reset: () => {
    set({ symbolBars: {} });
  },
}));
