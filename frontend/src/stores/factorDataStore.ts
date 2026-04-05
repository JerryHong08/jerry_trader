/**
 * Factor Data Zustand Store
 *
 * Manages factor data (EMA, TradeRate, etc.) for the FactorChartModule.
 *
 * Responsibilities:
 *   - Fetch historical factors from ChartBFF REST API (bootstrap)
 *   - Maintain per-symbol factor state
 *   - Update factors from real-time WebSocket messages
 *
 * Data Flow:
 *   Bootstrap:  fetchFactors() → GET /api/factors/{ticker} → store factors
 *   Real-time:  WebSocket factor_update → updateFactor() → append to series
 */

import { create } from 'zustand';

// ============================================================================
// Types
// ============================================================================

export interface FactorPoint {
  time: number; // Unix timestamp in seconds (UTC)
  value: number;
}

export interface FactorData {
  [factorName: string]: FactorPoint[];
}

export interface FactorResponse {
  ticker: string;
  from_ms: number;
  to_ms: number;
  factors: FactorData;
  count: number;
  error?: string;
}

/** Per-symbol factor state */
export interface SymbolFactorState {
  factors: FactorData; // {ema_20: [{time, value}, ...], trade_rate: [...]}
  loading: boolean;
  error: string | null;
  lastFetchTime: number | null; // When factors were last fetched (ms)
  timeframe: string; // 'tick' for tick-based, or bar timeframe like '1m', '5m'
}

/** Build the composite store key: "moduleId::TICKER::TIMEFRAME" */
export function factorStoreKey(moduleId: string, ticker: string, timeframe: string = 'trade'): string {
  return `${moduleId}::${ticker.toUpperCase()}::${timeframe}`;
}

type FactorDataState = {
  // Per-instance + symbol factor data (keyed by "moduleId::TICKER")
  symbolFactors: Record<string, SymbolFactorState>;

  // Actions — moduleId scopes state per chart instance
  fetchFactors: (
    moduleId: string,
    ticker: string,
    fromMs?: number,
    toMs?: number,
    factorNames?: string[],
    timeframe?: string
  ) => Promise<void>;

  updateFactor: (
    moduleId: string,
    ticker: string,
    factorName: string,
    point: FactorPoint
  ) => void;

  updateFactors: (
    moduleId: string,
    ticker: string,
    timestamp_ns: number,
    factors: Record<string, number>,
    timeframe?: string
  ) => void;

  clearFactors: (moduleId: string, ticker: string) => void;
  clearFactorsForTicker: (ticker: string) => void; // Clear all factor data for a ticker (all modules, all timeframes)
  clearAll: () => void;
};

// ============================================================================
// Store Implementation
// ============================================================================

// Use ChartBFF URL (same as tickDataStore)
// Default to port 8000 (ChartBFF), not 5001 (main BFF)
function getChartBFFUrl(): string {
  const defaultHost =
    typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  const base =
    (import.meta.env.VITE_TICKDATA_URL as string | undefined) ||
    `http://${defaultHost}:8000`;
  return base;
}

const CHART_BFF_URL = getChartBFFUrl();

export const useFactorDataStore = create<FactorDataState>((set, get) => ({
  symbolFactors: {},

  fetchFactors: async (
    moduleId: string,
    ticker: string,
    fromMs?: number,
    toMs?: number,
    factorNames?: string[],
    timeframe: string = 'trade'
  ) => {
    const key = factorStoreKey(moduleId, ticker, timeframe);
    const tickerUpper = ticker.toUpperCase();

    // Set loading state
    set((state) => ({
      symbolFactors: {
        ...state.symbolFactors,
        [key]: {
          factors: state.symbolFactors[key]?.factors || {},
          loading: true,
          error: null,
          lastFetchTime: null,
          timeframe,
        },
      },
    }));

    try {
      // Build query params
      const params = new URLSearchParams();
      if (fromMs) params.append('from_ms', fromMs.toString());
      if (toMs) params.append('to_ms', toMs.toString());
      if (factorNames && factorNames.length > 0) {
        params.append('factors', factorNames.join(','));
      }
      // Add timeframe parameter to query correct factor data
      params.append('timeframe', timeframe);

      const url = `${CHART_BFF_URL}/api/factors/${tickerUpper}?${params.toString()}`;
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data: FactorResponse = await response.json();

      if (data.error) {
        throw new Error(data.error);
      }

      // Update store with fetched factors
      set((state) => ({
        symbolFactors: {
          ...state.symbolFactors,
          [key]: {
            factors: data.factors,
            loading: false,
            error: null,
            lastFetchTime: Date.now(),
            timeframe,
          },
        },
      }));

      console.log(`[FactorStore] Fetched ${data.count} factor points for ${tickerUpper}/${timeframe}`);
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      console.error(`[FactorStore] fetchFactors error for ${tickerUpper}:`, errorMsg);

      set((state) => ({
        symbolFactors: {
          ...state.symbolFactors,
          [key]: {
            factors: state.symbolFactors[key]?.factors || {},
            loading: false,
            error: errorMsg,
            lastFetchTime: null,
            timeframe,
          },
        },
      }));
    }
  },

  updateFactor: (moduleId: string, ticker: string, factorName: string, point: FactorPoint, timeframe: string = 'trade') => {
    const key = factorStoreKey(moduleId, ticker, timeframe);

    set((state) => {
      const current = state.symbolFactors[key];
      if (!current) return state;

      const existingPoints = current.factors[factorName] || [];

      // Check if point already exists (avoid duplicates)
      // Optimization: only check last point since new points should have higher timestamps
      const lastPoint = existingPoints[existingPoints.length - 1];
      if (lastPoint && point.time <= lastPoint.time) {
        // Skip if not strictly newer (avoids sorting)
        return state;
      }

      // Append new point (no need to sort since it's always newer)
      // This is O(1) instead of O(n log n)
      const cappedPoints = existingPoints.length >= 10000
        ? [...existingPoints.slice(1), point]  // Drop oldest, add newest
        : [...existingPoints, point];

      return {
        symbolFactors: {
          ...state.symbolFactors,
          [key]: {
            ...current,
            factors: {
              ...current.factors,
              [factorName]: cappedPoints,
            },
          },
        },
      };
    });
  },

  updateFactors: (
    moduleId: string,
    ticker: string,
    timestamp_ns: number,
    factors: Record<string, number>,
    timeframe: string = 'trade'
  ) => {
    const time = Math.floor(timestamp_ns / 1_000_000_000); // ns to seconds
    const key = factorStoreKey(moduleId, ticker, timeframe);

    set((state) => {
      const current = state.symbolFactors[key];
      if (!current) return state;

      const updatedFactors = { ...current.factors };

      for (const [factorName, value] of Object.entries(factors)) {
        const existingPoints = updatedFactors[factorName] || [];
        const lastPoint = existingPoints[existingPoints.length - 1];
        if (lastPoint && time <= lastPoint.time) continue; // skip older/duplicate

        const point: FactorPoint = { time, value };
        updatedFactors[factorName] = existingPoints.length >= 10000
          ? [...existingPoints.slice(1), point]
          : [...existingPoints, point];
      }

      return {
        symbolFactors: {
          ...state.symbolFactors,
          [key]: { ...current, factors: updatedFactors },
        },
      };
    });
  },

  clearFactors: (moduleId: string, ticker: string, timeframe: string = 'trade') => {
    const key = factorStoreKey(moduleId, ticker, timeframe);
    set((state) => {
      const { [key]: _, ...rest } = state.symbolFactors;
      return { symbolFactors: rest };
    });
  },

  clearFactorsForTicker: (ticker: string) => {
    const tickerUpper = ticker.toUpperCase();
    set((state) => {
      // Find all keys that contain this ticker (format: "moduleId::TICKER::timeframe")
      const newFactors = { ...state.symbolFactors };
      for (const key of Object.keys(newFactors)) {
        if (key.includes(`::${tickerUpper}::`)) {
          delete newFactors[key];
        }
      }
      return { symbolFactors: newFactors };
    });
  },

  clearAll: () => {
    set({ symbolFactors: {} });
  },
}));
