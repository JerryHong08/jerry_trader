/**
 * WebSocket Hook for GridTrader Backend Communication
 *
 * Connects to the FastAPI BFF via native WebSocket and provides real-time data updates
 * for RankList and OverviewChart components.
 */

import { useEffect, useRef, useCallback, useState } from 'react';
import type { RankItem, TickerState } from '../types';

// Configuration - use Vite env variable or default
const BFF_HTTP_URL = (typeof import.meta !== 'undefined' && import.meta.env?.VITE_BFF_URL)
  ? import.meta.env.VITE_BFF_URL as string
  : 'http://localhost:5001';

// Convert HTTP URL to WebSocket URL
const BFF_WS_URL = BFF_HTTP_URL.replace(/^http/, 'ws');

// Generate unique client ID
const CLIENT_ID = `gridtrader_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

// Types for backend data
export interface StateHistoryPoint {
  timestamp: number;
  state: TickerState;
}

export interface TickerDataWithHistory extends RankItem {
  stateHistory: StateHistoryPoint[];
}

export interface ChartSegment {
  key: string;
  color: string;
  startIdx: number;
  endIdx: number;
}

export interface OverviewChartData {
  data: Record<string, any>[];
  segmentInfo: Record<string, ChartSegment[]>;
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
  timestamp: string;
}

// WebSocket message types
interface WebSocketMessage {
  type: string;
  [key: string]: any;
}

// Connection status
export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error';

// Singleton WebSocket instance
let wsInstance: WebSocket | null = null;
let connectionStatus: ConnectionStatus = 'disconnected';
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY = 1000;
let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;

const listeners = new Set<() => void>();

// Cached data
let cachedRankData: RankItem[] = [];
let cachedChartData: OverviewChartData | null = null;
let lastRankTimestamp: string | null = null;
let lastChartTimestamp: string | null = null;

// Chart subscription state - which tickers to display in overview chart
let chartSubscribedTickers: Set<string> = new Set();

// Message queue for messages sent while connecting
let messageQueue: WebSocketMessage[] = [];

function getWebSocket(): WebSocket {
  if (!wsInstance || wsInstance.readyState === WebSocket.CLOSED) {
    connectionStatus = 'connecting';
    notifyListeners();

    const wsUrl = `${BFF_WS_URL}/ws/${CLIENT_ID}`;
    console.log('[WebSocket] Connecting to:', wsUrl);

    wsInstance = new WebSocket(wsUrl);

    wsInstance.onopen = () => {
      console.log('[WebSocket] Connected to BFF');
      connectionStatus = 'connected';
      reconnectAttempts = 0;

      // Subscribe to market snapshot updates
      sendMessage({ type: 'subscribe_market_snapshot', payload: {} });

      // Flush message queue
      while (messageQueue.length > 0) {
        const msg = messageQueue.shift();
        if (msg) sendMessage(msg);
      }

      notifyListeners();
    };

    wsInstance.onclose = (event) => {
      console.log('[WebSocket] Disconnected:', event.code, event.reason);
      connectionStatus = 'disconnected';
      wsInstance = null;
      notifyListeners();

      // Auto-reconnect
      if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttempts++;
        console.log(`[WebSocket] Reconnecting in ${RECONNECT_DELAY}ms (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
        reconnectTimeout = setTimeout(() => {
          getWebSocket();
        }, RECONNECT_DELAY * reconnectAttempts);
      }
    };

    wsInstance.onerror = (error) => {
      console.error('[WebSocket] Connection error:', error);
      connectionStatus = 'error';
      notifyListeners();
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

// Stock detail message handlers map (needs to be before handleMessage)
const stockDetailHandlers = new Map<string, {
  onDetail: (data: any) => void;
  onError: (data: any) => void;
}>();

function registerStockDetailHandler(ticker: string, handlers: { onDetail: (data: any) => void; onError: (data: any) => void }) {
  stockDetailHandlers.set(ticker, handlers);
}

function unregisterStockDetailHandler(ticker: string) {
  stockDetailHandlers.delete(ticker);
}

function handleMessage(message: WebSocketMessage) {
  switch (message.type) {
    case 'rank_list_update':
      cachedRankData = message.data || [];
      lastRankTimestamp = message.timestamp;
      notifyListeners();
      break;

    case 'overview_chart_update':
      cachedChartData = {
        data: message.data || [],
        segmentInfo: message.segmentInfo || {},
        rankData: message.rankData || [],
        timestamp: message.timestamp,
      };
      lastChartTimestamp = message.timestamp;
      notifyListeners();
      break;

    case 'state_change':
      // Update cached rank data with new state
      cachedRankData = cachedRankData.map(item =>
        item.symbol === message.symbol ? { ...item, state: message.to as TickerState } : item
      );
      notifyListeners();
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
      // Silently ignore unknown message types to reduce logging
      break;
  }
}

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

// Batched notify to avoid excessive re-renders
let notifyPending = false;
function notifyListeners() {
  if (notifyPending) return;
  notifyPending = true;
  // Use requestAnimationFrame to batch notifications
  requestAnimationFrame(() => {
    notifyPending = false;
    listeners.forEach(listener => listener());
  });
}

/**
 * Update which tickers are subscribed for overview chart display
 */
export function updateChartSubscription(ticker: string, subscribed: boolean) {
  if (subscribed) {
    chartSubscribedTickers.add(ticker);
  } else {
    chartSubscribedTickers.delete(ticker);
  }
  // Notify backend of subscription change
  sendMessage({
    type: 'update_chart_subscription',
    payload: {
      ticker,
      subscribed,
      allSubscribed: Array.from(chartSubscribedTickers)
    }
  });
  notifyListeners();
}

/**
 * Get current chart subscribed tickers
 */
export function getChartSubscribedTickers(): Set<string> {
  return new Set(chartSubscribedTickers);
}

/**
 * Set all chart subscriptions at once
 */
export function setChartSubscriptions(tickers: string[]) {
  chartSubscribedTickers = new Set(tickers);
  sendMessage({
    type: 'set_chart_subscriptions',
    payload: { tickers }
  });
  notifyListeners();
}

/**
 * Hook for WebSocket connection status
 */
export function useWebSocketConnection(): ConnectionStatus {
  const [status, setStatus] = useState<ConnectionStatus>(connectionStatus);

  useEffect(() => {
    // Initialize WebSocket connection
    getWebSocket();

    const listener = () => {
      setStatus(connectionStatus);
    };

    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  }, []);

  return status;
}

/**
 * Hook for Rank List data from backend
 */
export function useRankListData(): {
  data: RankItem[];
  timestamp: string | null;
  isConnected: boolean;
  refresh: () => void;
} {
  const [data, setData] = useState<RankItem[]>(cachedRankData);
  const [timestamp, setTimestamp] = useState<string | null>(lastRankTimestamp);
  const [isConnected, setIsConnected] = useState(connectionStatus === 'connected');

  useEffect(() => {
    getWebSocket();

    const listener = () => {
      setData([...cachedRankData]);
      setTimestamp(lastRankTimestamp);
      setIsConnected(connectionStatus === 'connected');
    };

    listeners.add(listener);

    // If already connected and have cached data, use it
    if (cachedRankData.length > 0) {
      setData([...cachedRankData]);
      setTimestamp(lastRankTimestamp);
    }

    return () => {
      listeners.delete(listener);
    };
  }, []);

  const refresh = useCallback(() => {
    sendMessage({ type: 'refresh_rank_list', payload: {} });
  }, []);

  return { data, timestamp, isConnected, refresh };
}

/**
 * Hook for chart subscriptions state
 */
export function useChartSubscriptions(): {
  subscribedTickers: Set<string>;
  toggleSubscription: (ticker: string) => void;
  isSubscribed: (ticker: string) => boolean;
  subscribeAll: (tickers: string[]) => void;
  unsubscribeAll: () => void;
} {
  const [subscribedTickers, setSubscribedTickers] = useState<Set<string>>(new Set(chartSubscribedTickers));

  useEffect(() => {
    const listener = () => {
      setSubscribedTickers(new Set(chartSubscribedTickers));
    };

    listeners.add(listener);
    return () => {
      listeners.delete(listener);
    };
  }, []);

  const toggleSubscription = useCallback((ticker: string) => {
    const isCurrentlySubscribed = chartSubscribedTickers.has(ticker);
    updateChartSubscription(ticker, !isCurrentlySubscribed);
  }, []);

  const isSubscribed = useCallback((ticker: string) => {
    return chartSubscribedTickers.has(ticker);
  }, []);

  const subscribeAll = useCallback((tickers: string[]) => {
    setChartSubscriptions(tickers);
  }, []);

  const unsubscribeAll = useCallback(() => {
    setChartSubscriptions([]);
  }, []);

  return { subscribedTickers, toggleSubscription, isSubscribed, subscribeAll, unsubscribeAll };
}

/**
 * Hook for Overview Chart data from backend
 */
export function useOverviewChartData(): {
  chartData: Record<string, any>[];
  segmentInfo: Record<string, ChartSegment[]>;
  rankData: TickerDataWithHistory[];
  timestamp: string | null;
  isConnected: boolean;
  refresh: () => void;
} {
  const [chartData, setChartData] = useState<Record<string, any>[]>([]);
  const [segmentInfo, setSegmentInfo] = useState<Record<string, ChartSegment[]>>({});
  const [rankData, setRankData] = useState<TickerDataWithHistory[]>([]);
  const [timestamp, setTimestamp] = useState<string | null>(null);
  const [isConnected, setIsConnected] = useState(connectionStatus === 'connected');

  useEffect(() => {
    getWebSocket();

    const listener = () => {
      if (cachedChartData) {
        setChartData(cachedChartData.data || []);
        setSegmentInfo(cachedChartData.segmentInfo || {});
        // Convert rankData to TickerDataWithHistory (add empty stateHistory if needed)
        const convertedRankData: TickerDataWithHistory[] = (cachedChartData.rankData || []).map(item => ({
          ...item,
          stateHistory: [], // State history is in segmentInfo
        }));
        setRankData(convertedRankData);
        setTimestamp(cachedChartData.timestamp);
      }
      setIsConnected(connectionStatus === 'connected');
    };

    listeners.add(listener);

    // If already have cached data, use it
    if (cachedChartData) {
      setChartData(cachedChartData.data || []);
      setSegmentInfo(cachedChartData.segmentInfo || {});
      const convertedRankData: TickerDataWithHistory[] = (cachedChartData.rankData || []).map(item => ({
        ...item,
        stateHistory: [],
      }));
      setRankData(convertedRankData);
      setTimestamp(cachedChartData.timestamp);
    }

    return () => {
      listeners.delete(listener);
    };
  }, []);

  const refresh = useCallback(() => {
    sendMessage({ type: 'refresh_chart', payload: {} });
  }, []);

  return { chartData, segmentInfo, rankData, timestamp, isConnected, refresh };
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
  const [history, setHistory] = useState<{ timestamp: string; changePercent: number; price: number }[]>([]);
  const [stateHistory, setStateHistory] = useState<{ timestamp: string; state: TickerState }[]>([]);
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
    connectionStatus = 'disconnected';
    cachedRankData = [];
    cachedChartData = null;
    messageQueue = [];
    notifyListeners();
  }
}
