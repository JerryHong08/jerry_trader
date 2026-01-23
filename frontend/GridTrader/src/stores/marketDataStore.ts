/**
 * Market Data Store - Zustand State Management
 *
 * Manages all market data state including:
 * - Rank entity map (symbol → entity)
 * - Chart series data
 * - Visible tickers (pure UI state)
 * - WebSocket connection status
 *
 * Key Design:
 * - Snapshot updates patch only dynamic fields (price, change, volume, etc.)
 * - State updates patch only state and stateReason fields
 * - This preserves state consistency across different update sources
 * - Visibility is pure frontend UI state - not synced with backend
 */

import { create } from 'zustand';
import { subscribeWithSelector } from 'zustand/middleware';
import type { RankItem, TickerState } from '../types';

// Fields that come from snapshot stream (patched on rank_list_update)
const SNAPSHOT_FIELDS = [
  'symbol',
  'rank',
  'price',
  'change',
  'changePercent',
  'volume',
  'relativeVolume5min',
  'relativeVolumeDaily',
] as const;

// Entity type with optional state until state event arrives
export type RankEntity = Omit<RankItem, 'state'> & {
  state?: TickerState;
  rank?: number;
};

// Lightweight Charts data format from backend
export interface LWSeriesData {
  data: { time: number; value: number }[]; // time in seconds
  states: { time: number; state: TickerState }[];
}

// Connection status
export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error';

interface MarketDataState {
  // Entity map for rank data (symbol → entity)
  entities: Map<string, RankEntity>;
  rankTimestamp: string | null;

  // Chart series data
  seriesData: Record<string, LWSeriesData>;
  chartTimestamp: string | null;

  // Visible tickers - which tickers to display in overview chart (pure UI state)
  visibleTickers: Set<string>;

  // Hidden tickers - tickers user explicitly hid (used to persist visibility state)
  hiddenTickers: Set<string>;

  // WebSocket connection status
  connectionStatus: ConnectionStatus;

  // ============================================================================
  // Derived Data (Selectors)
  // ============================================================================

  /**
   * Get rank data as a sorted array for rendering.
   * Returns all entities with state defaulting to 'OnWatch' if not set.
   */
  getRankDataArray: () => RankItem[];

  // ============================================================================
  // Actions
  // ============================================================================

  /**
   * Patch snapshot data (rank_list_update, overview_chart_update).
   * Only updates snapshot fields, preserves state and stateReason.
   */
  patchSnapshotData: (items: Partial<RankItem & { rank?: number }>[], timestamp?: string) => void;

  /**
   * Patch state change from state_change event.
   * Only updates state and stateReason fields.
   */
  patchStateChange: (symbol: string, state: TickerState, stateReason?: string) => void;

  /**
   * Set chart series data from overview_chart_update.
   */
  setChartData: (seriesData: Record<string, LWSeriesData>, timestamp: string) => void;

  /**
   * Set WebSocket connection status.
   */
  setConnectionStatus: (status: ConnectionStatus) => void;

  /**
   * Update a single ticker visibility (UI state only).
   */
  updateTickerVisibility: (ticker: string, visible: boolean) => void;

  /**
   * Set all visible tickers at once (UI state only).
   */
  setVisibleTickers: (tickers: string[]) => void;

  /**
   * Sync visibility with new chart data: new tickers become visible, explicitly hidden stay hidden.
   */
  syncVisibility: (allTickers: string[]) => void;

  /**
   * Reset all state (for disconnect/cleanup).
   */
  reset: () => void;
}

export const useMarketDataStore = create<MarketDataState>()(
  subscribeWithSelector((set, get) => ({
    // ============================================================================
    // Initial State
    // ============================================================================
    entities: new Map(),
    rankTimestamp: null,
    seriesData: {},
    chartTimestamp: null,
    visibleTickers: new Set(),
    hiddenTickers: new Set(),
    connectionStatus: 'disconnected',

    // ============================================================================
    // Derived Data
    // ============================================================================
    getRankDataArray: () => {
      const entities = Array.from(get().entities.values());
      // Sort by rank if available, otherwise by changePercent descending
      return entities
        .map((entity) => ({
          ...entity,
          state: entity.state ?? 'OnWatch', // Default to OnWatch if no state yet
        }))
        .sort((a, b) => {
          // Sort by rank if available
          if (a.rank !== undefined && b.rank !== undefined) {
            return a.rank - b.rank;
          }
          // Fallback to changePercent descending
          return (b.changePercent ?? 0) - (a.changePercent ?? 0);
        }) as RankItem[];
    },

    // ============================================================================
    // Actions
    // ============================================================================
    patchSnapshotData: (items, timestamp) => {
      set((state) => {
        const newEntities = new Map(state.entities);

        items.forEach((item) => {
          if (!item.symbol) return;

          const existing = newEntities.get(item.symbol);
          if (existing) {
            // Merge only snapshot fields onto existing entity, preserving state
            const patched: RankEntity = { ...existing };
            SNAPSHOT_FIELDS.forEach((field) => {
              if (field in item) {
                (patched as any)[field] = (item as any)[field];
              }
            });
            newEntities.set(item.symbol, patched);
          } else {
            // New entity - initialize with snapshot fields, state will be undefined
            const newEntity: RankEntity = {
              symbol: item.symbol,
              price: item.price ?? 0,
              change: item.change ?? 0,
              changePercent: item.changePercent ?? 0,
              volume: item.volume ?? 0,
              marketCap: 0, // Static field, will be set by other source
              vwap: item.vwap ?? 0, // Snapshotdata field
              float: 0, // Static field
              relativeVolumeDaily: item.relativeVolumeDaily ?? 0,
              relativeVolume5min: item.relativeVolume5min ?? 0,
              rank: item.rank,
              // state is intentionally undefined until state event arrives
            };
            newEntities.set(item.symbol, newEntity);
          }
        });

        return {
          entities: newEntities,
          rankTimestamp: timestamp ?? state.rankTimestamp,
        };
      });
    },

    patchStateChange: (symbol, state, stateReason) => {
      set((prevState) => {
        const newEntities = new Map(prevState.entities);
        const existing = newEntities.get(symbol);

        if (existing) {
          // Patch state fields only
          newEntities.set(symbol, {
            ...existing,
            state,
            stateReason: stateReason ?? '',
          });
        } else {
          // Create minimal entity with state (snapshot data will fill in later)
          newEntities.set(symbol, {
            symbol,
            price: 0,
            change: 0,
            changePercent: 0,
            volume: 0,
            marketCap: 0,
            vwap: 0,
            float: 0,
            relativeVolumeDaily: 0,
            relativeVolume5min: 0,
            state,
            stateReason: stateReason ?? '',
          });
        }

        return { entities: newEntities };
      });
    },

    setChartData: (seriesData, timestamp) => {
      set({ seriesData, chartTimestamp: timestamp });
    },

    setConnectionStatus: (connectionStatus) => {
      set({ connectionStatus });
    },

    updateTickerVisibility: (ticker, visible) => {
      set((state) => {
        const newVisibleSet = new Set(state.visibleTickers);
        const newHiddenSet = new Set(state.hiddenTickers);
        if (visible) {
          newVisibleSet.add(ticker);
          newHiddenSet.delete(ticker); // Remove from explicitly hidden
        } else {
          newVisibleSet.delete(ticker);
          newHiddenSet.add(ticker); // Mark as explicitly hidden
        }
        return { visibleTickers: newVisibleSet, hiddenTickers: newHiddenSet };
      });
    },

    setVisibleTickers: (tickers) => {
      set({ visibleTickers: new Set(tickers) });
    },

    syncVisibility: (allTickers) => {
      set((state) => {
        const allTickersSet = new Set(allTickers);
        const newVisibleSet = new Set<string>();

        // For each ticker in the new data:
        // - If not explicitly hidden, make it visible
        allTickers.forEach((ticker) => {
          if (!state.hiddenTickers.has(ticker)) {
            newVisibleSet.add(ticker);
          }
        });

        // Also clean up hiddenTickers: remove tickers that are no longer in data
        // This prevents the hiddenTickers set from growing indefinitely
        const newHiddenSet = new Set<string>();
        state.hiddenTickers.forEach((ticker) => {
          if (allTickersSet.has(ticker)) {
            newHiddenSet.add(ticker);
          }
        });

        return { visibleTickers: newVisibleSet, hiddenTickers: newHiddenSet };
      });
    },

    reset: () => {
      set({
        entities: new Map(),
        rankTimestamp: null,
        seriesData: {},
        chartTimestamp: null,
        visibleTickers: new Set(),
        hiddenTickers: new Set(),
        connectionStatus: 'disconnected',
      });
    },
  }))
);

// ============================================================================
// Convenience Selectors (for use outside React components)
// ============================================================================

/**
 * Get rank data array from store (non-reactive).
 */
export function getRankDataArray(): RankItem[] {
  return useMarketDataStore.getState().getRankDataArray();
}

/**
 * Get visible tickers from store (non-reactive).
 */
export function getVisibleTickers(): Set<string> {
  return new Set(useMarketDataStore.getState().visibleTickers);
}
