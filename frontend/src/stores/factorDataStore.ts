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
export function factorStoreKey(moduleId: string, ticker: string, timeframe: string = 'tick'): string {
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
    factorNames?: string[]
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
    timeframe: string = 'tick'
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

  updateFactor: (moduleId: string, ticker: string, factorName: string, point: FactorPoint, timeframe: string = 'tick') => {
    const key = factorStoreKey(moduleId, ticker, timeframe);

    set((state) => {
      const current = state.symbolFactors[key];
      if (!current) return state;

      const existingPoints = current.factors[factorName] || [];

      // Check if point already exists (avoid duplicates)
      const exists = existingPoints.some((p) => p.time === point.time);
      if (exists) return state;

      // Append new point and sort by time
      const updatedPoints = [...existingPoints, point].sort((a, b) => a.time - b.time);

      // Cap at 10,000 points to prevent memory growth
      const cappedPoints = updatedPoints.length > 10000
        ? updatedPoints.slice(-10000)
        : updatedPoints;

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
    timeframe: string = 'tick'
  ) => {
    const time = Math.floor(timestamp_ns / 1_000_000_000); // ns to seconds

    for (const [factorName, value] of Object.entries(factors)) {
      get().updateFactor(moduleId, ticker, factorName, { time, value }, timeframe);
    }
  },

  clearFactors: (moduleId: string, ticker: string, timeframe: string = 'tick') => {
    const key = factorStoreKey(moduleId, ticker, timeframe);
    set((state) => {
      const { [key]: _, ...rest } = state.symbolFactors;
      return { symbolFactors: rest };
    });
  },

  clearAll: () => {
    set({ symbolFactors: {} });
  },
}));
