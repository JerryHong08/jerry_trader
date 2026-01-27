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
import type { RankItem, TickerState, NewsArticle } from '../types';
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

// LocalStorage keys for cache persistence
const PROFILE_CACHE_KEY = 'gridtrader_profile_cache';
const NEWS_CACHE_KEY = 'gridtrader_news_cache';
const DATA_STATUS_KEY = 'gridtrader_data_status';
const VERSION_CACHE_KEY = 'gridtrader_version_cache';

// Version tracking type: symbol -> domain -> version (summary/profile)
export type VersionCache = Record<string, Record<string, number>>;

// Helper functions for localStorage persistence
function loadMapFromStorage<T>(key: string): Map<string, T> {
  try {
    const stored = localStorage.getItem(key);
    if (stored) {
      const parsed = JSON.parse(stored);
      return new Map(Object.entries(parsed));
    }
  } catch (e) {
    console.warn(`Failed to load ${key} from localStorage:`, e);
  }
  return new Map();
}

function saveMapToStorage<T>(key: string, map: Map<string, T>) {
  try {
    const obj = Object.fromEntries(map);
    localStorage.setItem(key, JSON.stringify(obj));
  } catch (e) {
    console.warn(`Failed to save ${key} to localStorage:`, e);
  }
}

function loadVersionCacheFromStorage(): VersionCache {
  try {
    const stored = localStorage.getItem(VERSION_CACHE_KEY);
    if (stored) {
      return JSON.parse(stored);
    }
  } catch (e) {
    console.warn('Failed to load version cache from localStorage:', e);
  }
  return {};
}

function saveVersionCacheToStorage(cache: VersionCache) {
  try {
    localStorage.setItem(VERSION_CACHE_KEY, JSON.stringify(cache));
  } catch (e) {
    console.warn('Failed to save version cache to localStorage:', e);
  }
}

// Static profile cache - populated by static_update messages from BFF
// Used by StockDetail to avoid re-fetching profile data
// Persisted to localStorage across page refreshes
const staticProfileCache = loadMapFromStorage<Record<string, any>>(PROFILE_CACHE_KEY);

// Static news cache - populated by news_article messages from BFF
// Persisted to localStorage across page refreshes
const staticNewsCache = loadMapFromStorage<any[]>(NEWS_CACHE_KEY);

// Data status tracking - which symbols have been loaded
// 'pending' = queued for fetch, 'loading' = being fetched, 'ready' = data available
export type DataStatus = 'pending' | 'loading' | 'ready' | 'none';
const dataStatusMap = loadMapFromStorage<{ profile: DataStatus; news: DataStatus }>(DATA_STATUS_KEY);

// Version cache - tracks version per (symbol, domain) for stale update detection
let versionCache: VersionCache = loadVersionCacheFromStorage();

/**
 * Get cached profile data for a symbol (from static_update stream or API fetch)
 */
export function getCachedProfile(symbol: string): Record<string, any> | undefined {
  return staticProfileCache.get(symbol);
}

/**
 * Set cached profile data for a symbol (from API fetch)
 * Also persists to localStorage
 */
export function setCachedProfile(symbol: string, profile: Record<string, any>) {
  staticProfileCache.set(symbol, profile);
  saveMapToStorage(PROFILE_CACHE_KEY, staticProfileCache);
  setDataStatus(symbol, 'profile', 'ready');
}

/**
 * Get cached news data for a symbol (from static_update stream or API fetch)
 */
export function getCachedNews(symbol: string): any[] | undefined {
  return staticNewsCache.get(symbol);
}

/**
 * Set cached news data for a symbol (from API fetch)
 * Also persists to localStorage
 */
export function setCachedNews(symbol: string, news: any[]) {
  staticNewsCache.set(symbol, news);
  saveMapToStorage(NEWS_CACHE_KEY, staticNewsCache);
  setDataStatus(symbol, 'news', 'ready');
}

/**
 * Check if profile is cached for a symbol
 */
export function hasProfileCache(symbol: string): boolean {
  return staticProfileCache.has(symbol);
}

/**
 * Check if news is cached for a symbol
 */
export function hasNewsCache(symbol: string): boolean {
  return staticNewsCache.has(symbol);
}

/**
 * Get data status for a symbol
 */
export function getDataStatus(symbol: string): { profile: DataStatus; news: DataStatus } {
  return dataStatusMap.get(symbol) || { profile: 'none', news: 'none' };
}

/**
 * Update data status for a symbol
 */
export function setDataStatus(symbol: string, type: 'profile' | 'news', status: DataStatus) {
  const current = dataStatusMap.get(symbol) || { profile: 'none', news: 'none' };
  current[type] = status;
  dataStatusMap.set(symbol, current);
  saveMapToStorage(DATA_STATUS_KEY, dataStatusMap);
}

/**
 * Get cached version for a (symbol, domain) pair
 */
export function getCachedVersion(symbol: string, domain: string): number {
  return versionCache[symbol]?.[domain] ?? 0;
}

/**
 * Check if an incoming version is newer than cached version
 * Returns true if incoming version is newer and should be applied
 */
export function isVersionNewer(symbol: string, domain: string, incomingVersion: number): boolean {
  const cachedVersion = getCachedVersion(symbol, domain);
  return incomingVersion > cachedVersion;
}

/**
 * Update cached version for a (symbol, domain) pair
 * Only updates if incoming version is actually newer
 */
export function updateCachedVersion(symbol: string, domain: string, version: number): boolean {
  if (!isVersionNewer(symbol, domain, version)) {
    return false; // Stale update, don't apply
  }
  if (!versionCache[symbol]) {
    versionCache[symbol] = {};
  }
  versionCache[symbol][domain] = version;
  saveVersionCacheToStorage(versionCache);
  return true;
}

/**
 * Get all cached versions for a symbol
 */
export function getSymbolVersions(symbol: string): Record<string, number> {
  return versionCache[symbol] ?? {};
}

// Flag to track if caches have been reloaded to store
let cacheReloadedToStore = false;

/**
 * Clear all cached data (profile, news, status, versions)
 * Call this from settings to manually reset caches
 */
export function clearAllCaches() {
  staticProfileCache.clear();
  staticNewsCache.clear();
  dataStatusMap.clear();
  versionCache = {};
  localStorage.removeItem(PROFILE_CACHE_KEY);
  localStorage.removeItem(NEWS_CACHE_KEY);
  localStorage.removeItem(DATA_STATUS_KEY);
  localStorage.removeItem(VERSION_CACHE_KEY);
  // Reset the reload flag so cache can be reloaded on next page load
  cacheReloadedToStore = false;
  console.log('[WebSocket] All caches cleared');
}

/**
 * Get cache statistics for display in settings
 */
export function getCacheStats(): { profiles: number; news: number; versions: number; total: number } {
  const versionCount = Object.keys(versionCache).length;
  return {
    profiles: staticProfileCache.size,
    news: staticNewsCache.size,
    versions: versionCount,
    total: staticProfileCache.size + staticNewsCache.size,
  };
}

/**
 * Reload cached static data into the marketDataStore on startup.
 * This ensures RankList columns (float, marketCap, hasNews) show cached values after page refresh.
 */
export function reloadCachedStaticDataToStore() {
  const store = useMarketDataStore.getState();
  let patchedCount = 0;

  // Iterate over cached profiles and patch static data into store
  staticProfileCache.forEach((profile, symbol) => {
    const staticData: Record<string, any> = {};

    // Extract static fields from profile
    if (profile.float !== undefined) {
      staticData.float = typeof profile.float === 'number' ? profile.float : parseFloat(profile.float) || null;
    }
    if (profile.marketCap !== undefined) {
      staticData.marketCap = typeof profile.marketCap === 'number' ? profile.marketCap : parseFloat(profile.marketCap) || null;
    }
    if (profile.country !== undefined) {
      staticData.country = profile.country;
    }
    if (profile.sector !== undefined) {
      staticData.sector = profile.sector;
    }

    if (Object.keys(staticData).length > 0) {
      store.patchStaticData(symbol, staticData);
      patchedCount++;
    }
  });

  // Also patch hasNews from news cache
  staticNewsCache.forEach((news, symbol) => {
    store.patchStaticData(symbol, { hasNews: news.length > 0 });
  });

  if (patchedCount > 0 || staticNewsCache.size > 0) {
    console.log(`[WebSocket] Reloaded ${patchedCount} profiles and ${staticNewsCache.size} news caches into store`);
  }
}

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

// News update subscribers
type NewsUpdatePayload = { symbol: string; article: NewsArticle; articles: NewsArticle[] };
const newsUpdateHandlers = new Set<(payload: NewsUpdatePayload) => void>();

export function subscribeNewsUpdates(handler: (payload: NewsUpdatePayload) => void) {
  newsUpdateHandlers.add(handler);
  return () => newsUpdateHandlers.delete(handler);
}

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

    case 'static_update':
      // Patch static data only (preserves snapshot and state data)
      // Versioned schema (v2):
      // - message.domains: ['summary', 'profile'] - which domains are included
      // - message.version: { summary: 3, profile: 2 } - version per domain
      // - message.summary: { marketCap, float, hasNews, country, sector } - for RankList
      // - message.profile: full profile data - for StockDetail cache
      // Note: News updates are now delivered via 'news_article' messages.
      if (message.symbol) {
        const symbol = message.symbol;
        const domains: string[] = message.domains || [];
        const versions: Record<string, number> = message.version || {};

        // Process summary domain with version check
        if (domains.includes('summary') && message.summary) {
          const summaryVersion = versions.summary ?? 0;
          if (updateCachedVersion(symbol, 'summary', summaryVersion)) {
            store.patchStaticData(symbol, message.summary, summaryVersion);
          } else {
            console.debug(`[WebSocket] Skipping stale summary update for ${symbol}: v${summaryVersion}`);
          }
        }

        // Process profile domain with version check
        if (domains.includes('profile') && message.profile && Object.keys(message.profile).length > 0) {
          const profileVersion = versions.profile ?? 0;
          if (updateCachedVersion(symbol, 'profile', profileVersion)) {
            staticProfileCache.set(symbol, { ...message.profile, _version: profileVersion });
            saveMapToStorage(PROFILE_CACHE_KEY, staticProfileCache);
            setDataStatus(symbol, 'profile', 'ready');
          } else {
            console.debug(`[WebSocket] Skipping stale profile update for ${symbol}: v${profileVersion}`);
          }
        }

        // Fallback for legacy format (no domains array)
        if (domains.length === 0) {
          if (message.summary) {
            store.patchStaticData(symbol, message.summary);
          }
          if (message.profile && Object.keys(message.profile).length > 0) {
            staticProfileCache.set(symbol, message.profile);
            saveMapToStorage(PROFILE_CACHE_KEY, staticProfileCache);
            setDataStatus(symbol, 'profile', 'ready');
          }
        }
      }
      break;

    case 'news_article':
      if (message.symbol && message.article) {
        const symbol = message.symbol as string;
        const incoming = message.article as Record<string, any>;

        const article: NewsArticle = {
          id: incoming.id || `${symbol}-news-${Date.now()}`,
          title: incoming.title || '',
          source: incoming.source || incoming.sources || '',
          publishedAt: incoming.publishedAt || incoming.published_time || '',
          url: incoming.url || '',
          summary: incoming.summary || incoming.text || '',
          isNew: true,
        };

        const existing = (staticNewsCache.get(symbol) || []) as NewsArticle[];
        const hasDuplicate = existing.some(
          (item) => item.id === article.id || (article.url && item.url === article.url)
        );

        if (!hasDuplicate) {
          const normalizedExisting = existing.map((item) => ({ ...item, isNew: false }));
          const updated = [article, ...normalizedExisting].slice(0, 50);
          staticNewsCache.set(symbol, updated);
          saveMapToStorage(NEWS_CACHE_KEY, staticNewsCache);
          setDataStatus(symbol, 'news', 'ready');
          store.patchStaticData(symbol, { hasNews: true });

          newsUpdateHandlers.forEach((handler) => {
            handler({ symbol, article, articles: updated });
          });
        }
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
    // Reload cached static data into store on first mount (before WebSocket connects)
    // This ensures RankList shows cached float/marketCap/hasNews immediately
    if (!cacheReloadedToStore) {
      cacheReloadedToStore = true;
      reloadCachedStaticDataToStore();
    }

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
