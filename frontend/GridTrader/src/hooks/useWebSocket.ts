/**
 * WebSocket Hook for GridTrader Backend Communication
 *
 * Connects to the FastAPI BFF via native WebSocket and provides real-time data updates
 * for RankList and OverviewChart components.
 *
 * Uses Zustand store for state management to ensure:
 * - Snapshot updates patch only dynamic fields (price, change, volume, etc.)
 * - State updates patch only state and stateReason fields
 * - State consistency across different update sources
 */

import { useEffect, useCallback, useState, useMemo } from 'react';
import type { RankItem, TickerState } from '../types';
import {
  useMarketDataStore,
  type LWSeriesData,
  type ConnectionStatus,
} from '../stores/marketDataStore';

// Configuration - use Vite env variable or default
const BFF_HTTP_URL =
  typeof import.meta !== 'undefined' && import.meta.env?.VITE_BFF_URL
    ? (import.meta.env.VITE_BFF_URL as string)
    : 'http://localhost:5001';

// Convert HTTP URL to WebSocket URL
const BFF_WS_URL = BFF_HTTP_URL.replace(/^http/, 'ws');

// Generate unique client ID
const CLIENT_ID = `gridtrader_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

// ============================================================================
// Re-export types from store for backwards compatibility
// ============================================================================

export type { LWSeriesData, ConnectionStatus };

export interface StateHistoryPoint {
  timestamp: number;
  state: TickerState;
}

export interface TickerDataWithHistory extends RankItem {
  stateHistory: StateHistoryPoint[];
}

export interface OverviewChartData {
  seriesData: Record<string, LWSeriesData>;
  rankData: RankItem[];
  timestamp: string | null;
}

export interface RankListData {
  data: RankItem[];
  timestamp: string;
}

export interface StateChangeEvent {
  symbol: string;
  from: TickerState;
  to: TickerState;
  stateReason: string;
  timestamp: string;
}

// ============================================================================
// WebSocket Singleton
// ============================================================================

// WebSocket message types
interface WebSocketMessage {
  type: string;
  [key: string]: any;
}

// Singleton WebSocket instance
let wsInstance: WebSocket | null = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY = 1000;
let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;

// Message queue for messages sent while connecting
let messageQueue: WebSocketMessage[] = [];

// Stock detail message handlers map
const stockDetailHandlers = new Map<
  string,
  {
    onDetail: (data: any) => void;
    onError: (data: any) => void;
  }
>();

function registerStockDetailHandler(
  ticker: string,
  handlers: { onDetail: (data: any) => void; onError: (data: any) => void }
) {
  stockDetailHandlers.set(ticker, handlers);
}

function unregisterStockDetailHandler(ticker: string) {
  stockDetailHandlers.delete(ticker);
}

/**
 * Send a message via WebSocket. Queues if not connected.
 */
function sendMessage(message: WebSocketMessage) {
  if (wsInstance && wsInstance.readyState === WebSocket.OPEN) {
    wsInstance.send(JSON.stringify(message));
  } else {
    // Queue message for later
    messageQueue.push(message);
    // Ensure connection is being established
    getWebSocket();
  }
}

/**
 * Handle incoming WebSocket messages.
 * Updates Zustand store with partial patches.
 */
function handleMessage(message: WebSocketMessage) {
  const store = useMarketDataStore.getState();

  switch (message.type) {
    case 'rank_list_update':
      // Patch snapshot data (preserves state)
      store.patchSnapshotData(message.data || [], message.timestamp);
      break;

    case 'overview_chart_update':
      // Patch rank data if included
      if (message.rankData) {
        store.patchSnapshotData(message.rankData, message.timestamp);
      }
      // Update chart series data
      if (message.seriesData) {
        // Debug: Check the data format from backend
        const firstSymbol = Object.keys(message.seriesData)[0];
        if (firstSymbol && message.seriesData[firstSymbol]?.data?.length > 0) {
          const samplePoint = message.seriesData[firstSymbol].data[0];
          if (typeof samplePoint.time !== 'number') {
            console.warn('[WebSocket] Invalid time format in seriesData:',
              'type:', typeof samplePoint.time,
              'value:', samplePoint.time
            );
          }
        }
        store.setChartData(message.seriesData, message.timestamp);

        // Sync visibility: new tickers become visible, explicitly hidden stay hidden
        const allTickers = Object.keys(message.seriesData);
        store.syncVisibility(allTickers);
      }
      break;

    case 'state_change':
      // Patch state only (preserves snapshot data)
      if (message.symbol) {
        store.patchStateChange(
          message.symbol,
          message.to as TickerState,
          message.stateReason
        );
      }
      break;

    case 'stock_detail':
      // Stock detail responses are handled by registered handlers
      const handler = stockDetailHandlers.get(message.ticker);
      if (handler) {
        handler.onDetail(message);
      }
      break;

    case 'error':
      console.error('[WebSocket] Server error:', message.message);
      // Try to find a matching stock detail handler for this error
      stockDetailHandlers.forEach((handler, ticker) => {
        if (message.message?.includes(ticker)) {
          handler.onError(message);
        }
      });
      break;

    default:
      // Silently ignore unknown message types
      break;
  }
}

/**
 * Get or create WebSocket connection.
 */
function getWebSocket(): WebSocket {
  if (!wsInstance || wsInstance.readyState === WebSocket.CLOSED) {
    const store = useMarketDataStore.getState();
    store.setConnectionStatus('connecting');

    const wsUrl = `${BFF_WS_URL}/ws/${CLIENT_ID}`;
    console.log('[WebSocket] Connecting to:', wsUrl);

    wsInstance = new WebSocket(wsUrl);

    wsInstance.onopen = () => {
      console.log('[WebSocket] Connected to BFF');
      useMarketDataStore.getState().setConnectionStatus('connected');
      reconnectAttempts = 0;

      // Subscribe to market snapshot updates
      sendMessage({ type: 'subscribe_market_snapshot', payload: {} });

      // Flush message queue
      while (messageQueue.length > 0) {
        const msg = messageQueue.shift();
        if (msg) sendMessage(msg);
      }
    };

    wsInstance.onclose = (event) => {
      console.log('[WebSocket] Disconnected:', event.code, event.reason);
      useMarketDataStore.getState().setConnectionStatus('disconnected');
      wsInstance = null;

      // Auto-reconnect
      if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttempts++;
        console.log(
          `[WebSocket] Reconnecting in ${RECONNECT_DELAY}ms (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`
        );
        reconnectTimeout = setTimeout(() => {
          getWebSocket();
        }, RECONNECT_DELAY * reconnectAttempts);
      }
    };

    wsInstance.onerror = (error) => {
      console.error('[WebSocket] Connection error:', error);
      useMarketDataStore.getState().setConnectionStatus('error');
    };

    wsInstance.onmessage = (event) => {
      try {
        const message: WebSocketMessage = JSON.parse(event.data);
        handleMessage(message);
      } catch (e) {
        console.error('[WebSocket] Failed to parse message:', e);
      }
    };
  }

  return wsInstance;
}

// ============================================================================
// Public API - Chart Subscriptions
// ============================================================================

/**
 * Update ticker visibility in overview chart (pure UI state - no backend sync)
 */
export function updateTickerVisibility(ticker: string, visible: boolean) {
  const store = useMarketDataStore.getState();
  store.updateTickerVisibility(ticker, visible);
}

/**
 * Get current visible tickers
 */
export function getVisibleTickers(): Set<string> {
  return new Set(useMarketDataStore.getState().visibleTickers);
}

/**
 * Set all visible tickers at once (pure UI state - no backend sync)
 */
export function setVisibleTickers(tickers: string[]) {
  useMarketDataStore.getState().setVisibleTickers(tickers);
}

/**
 * Set top N tickers to request from backend (persists in BFF)
 */
export function setTopN(topN: number) {
  sendMessage({ type: 'set_top_n', payload: { top_n: topN } });
}

// ============================================================================
// React Hooks
// ============================================================================

/**
 * Hook for WebSocket connection status
 */
export function useWebSocketConnection(): ConnectionStatus {
  const status = useMarketDataStore((s) => s.connectionStatus);

  useEffect(() => {
    // Initialize WebSocket connection
    getWebSocket();
  }, []);

  return status;
}

/**
 * Hook for Rank List data from backend.
 * Derives sorted array from entity map for rendering.
 * Uses useMemo to avoid creating new array on every render.
 */
export function useRankListData(): {
  data: RankItem[];
  timestamp: string | null;
  isConnected: boolean;
  refresh: () => void;
} {
  // Subscribe to entities Map - this triggers re-render when entities change
  const entities = useMarketDataStore((s) => s.entities);
  const rankTimestamp = useMarketDataStore((s) => s.rankTimestamp);
  const isConnected = useMarketDataStore((s) => s.connectionStatus === 'connected');

  // Memoize the sorted array to avoid creating new reference on every render
  const data = useMemo(() => {
    const arr = Array.from(entities.values());
    return arr
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
  }, [entities]);

  useEffect(() => {
    getWebSocket();
  }, []);

  const refresh = useCallback(() => {
    sendMessage({ type: 'refresh_rank_list', payload: {} });
  }, []);

  return {
    data,
    timestamp: rankTimestamp,
    isConnected,
    refresh,
  };
}

/**
 * Hook for ticker visibility state (pure UI state - no backend sync)
 */
export function useTickerVisibility(): {
  visibleTickers: Set<string>;
  toggleVisibility: (ticker: string) => void;
  isVisible: (ticker: string) => boolean;
  showAll: (tickers: string[]) => void;
  hideAll: () => void;
} {
  const visibleTickers = useMarketDataStore((s) => s.visibleTickers);

  const toggleVisibility = useCallback((ticker: string) => {
    const isCurrentlyVisible = useMarketDataStore
      .getState()
      .visibleTickers.has(ticker);
    updateTickerVisibility(ticker, !isCurrentlyVisible);
  }, []);

  const isVisible = useCallback((ticker: string) => {
    return useMarketDataStore.getState().visibleTickers.has(ticker);
  }, []);

  const showAll = useCallback((tickers: string[]) => {
    setVisibleTickers(tickers);
  }, []);

  const hideAll = useCallback(() => {
    setVisibleTickers([]);
  }, []);

  return {
    visibleTickers,
    toggleVisibility,
    isVisible,
    showAll,
    hideAll,
  };
}

/**
 * Hook for Overview Chart data from backend.
 * Returns data in Lightweight Charts ready format.
 * Derives rank data from entity map to ensure state consistency.
 *
 * @param topN - Number of top tickers to request from backend (default: 20)
 */
export function useOverviewChartData(topN: number = 20): {
  seriesData: Record<string, LWSeriesData>;
  rankData: TickerDataWithHistory[];
  timestamp: string | null;
  isConnected: boolean;
  refresh: () => void;
} {
  const seriesData = useMarketDataStore((s) => s.seriesData);
  const entities = useMarketDataStore((s) => s.entities);
  const chartTimestamp = useMarketDataStore((s) => s.chartTimestamp);
  const isConnected = useMarketDataStore((s) => s.connectionStatus === 'connected');

  useEffect(() => {
    getWebSocket();
  }, []);

  const refresh = useCallback(() => {
    sendMessage({ type: 'refresh_chart', payload: { top_n: topN } });
  }, [topN]);

  // Derive rank data with stateHistory from entity map - memoized to avoid new array each render
  const rankData: TickerDataWithHistory[] = useMemo(() => {
    const arr = Array.from(entities.values());
    return arr
      .map((entity) => ({
        ...entity,
        state: entity.state ?? 'OnWatch',
        stateHistory: [], // State history is in seriesData.states
      }))
      .sort((a, b) => {
        if (a.rank !== undefined && b.rank !== undefined) {
          return a.rank - b.rank;
        }
        return (b.changePercent ?? 0) - (a.changePercent ?? 0);
      }) as TickerDataWithHistory[];
  }, [entities]);

  return {
    seriesData,
    rankData,
    timestamp: chartTimestamp,
    isConnected,
    refresh,
  };
}

/**
 * Hook for requesting stock detail data
 */
export function useStockDetail(ticker: string | null): {
  history: { timestamp: string; changePercent: number; price: number }[];
  stateHistory: { timestamp: string; state: TickerState }[];
  isLoading: boolean;
  error: string | null;
} {
  const [history, setHistory] = useState<
    { timestamp: string; changePercent: number; price: number }[]
  >([]);
  const [stateHistory, setStateHistory] = useState<
    { timestamp: string; state: TickerState }[]
  >([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!ticker) {
      setHistory([]);
      setStateHistory([]);
      return;
    }

    getWebSocket();

    setIsLoading(true);
    setError(null);

    // Register handlers for this ticker
    registerStockDetailHandler(ticker, {
      onDetail: (data) => {
        if (data.ticker === ticker) {
          setHistory(data.history || []);
          setStateHistory(data.stateHistory || []);
          setIsLoading(false);
        }
      },
      onError: (data) => {
        if (data.message?.includes(ticker)) {
          setError(data.message);
          setIsLoading(false);
        }
      },
    });

    // Request stock detail
    sendMessage({ type: 'request_stock_detail', payload: { ticker } });

    return () => {
      unregisterStockDetailHandler(ticker);
    };
  }, [ticker]);

  return { history, stateHistory, isLoading, error };
}

/**
 * Utility to disconnect WebSocket (for cleanup)
 */
export function disconnectSocket() {
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
    reconnectTimeout = null;
  }
  if (wsInstance) {
    wsInstance.close();
    wsInstance = null;
  }
  useMarketDataStore.getState().reset();
  messageQueue = [];
}
