/**
 * TickData Zustand Store
 *
 * Manages the ChartBFF WebSocket (wsl2 machine, default port 8000).
 * Provides real-time quote (Q) and trade (T) data for:
 *   - OrderBook / quote display
 *   - ChartModule (real-time line chart)
 *
 * The WebSocket protocol matches OrderTrader's existing implementation:
 *   subscribe:   { subscriptions: [{ symbol, events: ["Q","T"] }] }
 *   unsubscribe: { action: "unsubscribe", symbol, events: ["Q","T"] }
 *   messages:    { event_type: "Q"|"T", symbol, ...fields }
 */

import { create } from 'zustand';
import { IS_DEMO } from '../data/mockData';
import { useFactorDataStore } from './factorDataStore';
import { useChartDataStore } from './chartDataStore';

// ============================================================================
// Types
// ============================================================================

/**
 * Parse timeframe string to seconds
 * e.g., '10s' -> 10, '1m' -> 60, '5m' -> 300, '1h' -> 3600
 */
function parseTimeframeToSeconds(tf: string): number {
  const match = tf.match(/^(\d+)(s|m|h)$/);
  if (!match) return 0;
  const value = parseInt(match[1], 10);
  const unit = match[2];
  switch (unit) {
    case 's': return value;
    case 'm': return value * 60;
    case 'h': return value * 3600;
    default: return 0;
  }
}

export interface Quote {
  symbol: string;
  bid: number;
  ask: number;
  bid_size: number;
  ask_size: number;
  timestamp: number;
}

export interface Trade {
  symbol: string;
  price: number;
  size: number;
  timestamp: number;
}

export type SymbolData = Record<
  string,
  Partial<Record<'Q' | 'T', Quote | Trade>>
>;

type TickDataState = {
  // Data
  symbols: string[];
  symbolData: SymbolData;
  perSymbolEvents: Record<string, string[]>;
  factorSubscriptions: Record<string, string[]>; // {symbol: [timeframes]}

  // Connection
  connected: boolean;

  // Derived
  getLatestTrade: (symbol: string) => Trade | null;
  getLatestQuote: (symbol: string) => Quote | null;

  // Actions
  init: () => void;
  dispose: () => void;
  reconnect: () => void;
  addSymbols: (newSyms: string[], events: string[]) => void;
  removeSymbol: (symbol: string) => void;
};

// ============================================================================
// Config
// ============================================================================

function getTickDataWsUrl(): string {
  const defaultHost =
    typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  const base =
    (import.meta.env.VITE_TICKDATA_URL as string | undefined) ||
    `http://${defaultHost}:8000`;
  const url = new URL(base);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  url.pathname = '/ws/tickdata';
  url.search = '';
  url.hash = '';
  return url.toString();
}

// ============================================================================
// Persistent state helpers (localStorage)
// ============================================================================

function loadSymbols(): string[] {
  try {
    const saved = localStorage.getItem('tickdata-symbols');
    return saved ? JSON.parse(saved) : [];
  } catch {
    return [];
  }
}

function loadPerSymbolEvents(): Record<string, string[]> {
  try {
    const saved = localStorage.getItem('tickdata-perSymbolEvents');
    return saved ? JSON.parse(saved) : {};
  } catch {
    return {};
  }
}

function loadFactorSubscriptions(): Record<string, string[]> {
  // Key: symbol, Value: list of timeframes subscribed
  // e.g., {"AAPL": ["trade", "1m"], "TSLA": ["trade"]}
  try {
    const saved = localStorage.getItem('tickdata-factorSubscriptions');
    return saved ? JSON.parse(saved) : {};
  } catch {
    return {};
  }
}

function saveSymbols(symbols: string[]) {
  localStorage.setItem('tickdata-symbols', JSON.stringify(symbols));
}

function savePerSymbolEvents(events: Record<string, string[]>) {
  localStorage.setItem('tickdata-perSymbolEvents', JSON.stringify(events));
}

function saveFactorSubscriptions(subs: Record<string, string[]>) {
  localStorage.setItem('tickdata-factorSubscriptions', JSON.stringify(subs));
}

// ============================================================================
// Store
// ============================================================================

let ws: WebSocket | null = null;

export const useTickDataStore = create<TickDataState>()((set, get) => ({
  symbols: loadSymbols(),
  symbolData: {},
  perSymbolEvents: loadPerSymbolEvents(),
  factorSubscriptions: loadFactorSubscriptions(),
  connected: false,

  // Derived
  getLatestTrade: (symbol: string) => {
    const d = get().symbolData[symbol]?.T;
    return d ? (d as Trade) : null;
  },

  getLatestQuote: (symbol: string) => {
    const d = get().symbolData[symbol]?.Q;
    return d ? (d as Quote) : null;
  },

  // ========================================================================
  // Lifecycle
  // ========================================================================

  init: () => {
    if (IS_DEMO) {
      set({ connected: true });
      return;
    }

    ws?.close();

    const wsUrl = getTickDataWsUrl();
    let socket: WebSocket;
    try {
      socket = new WebSocket(wsUrl);
    } catch (e) {
      console.warn('[TickData] Failed to connect (mixed content or invalid URL):', e);
      set({ connected: false });
      return;
    }
    ws = socket;

    socket.onopen = () => {
      console.log('✅ TickData WS connected');
      set({ connected: true });

      // Re-subscribe saved symbols
      const { symbols, perSymbolEvents } = get();
      if (symbols.length > 0) {
        const subs = symbols.map((s) => ({
          symbol: s,
          events: perSymbolEvents[s] || ['Q', 'T'],
        }));
        socket.send(JSON.stringify({ subscriptions: subs }));
      }

      // Factor subscriptions are NOT re-subscribed here on purpose:
      // React components call subscribeFactors() via their useEffect when
      // they mount, handling both fresh loads and reconnects.  Re-subscribing
      // from localStorage would trigger stale backend bootstraps for tickers
      // the user may not be actively watching.
    };

    socket.onmessage = (event) => {
      const data = JSON.parse(event.data);

      // Handle factor updates - broadcast to all modules with this symbol and timeframe
      if (data.type === 'factor_update') {
        const factorStore = useFactorDataStore.getState();
        const { symbol, timestamp_ns, factors, timeframe } = data.data || {};
        const clock_now_ns = data.clock_now_ns;  // Replay clock time from backend

        if (symbol && timestamp_ns && factors) {
          // timeframe from backend (e.g., 'trade', '1m', '5m')
          const tf = timeframe || 'trade';

          // Calculate delay using replay clock (not wall-clock time)
          if (clock_now_ns) {
            let wsDelayMs: number;

            if (tf === 'trade') {
              // Tick-based: delay = clock_now - timestamp
              wsDelayMs = Math.floor((clock_now_ns - timestamp_ns) / 1_000_000);
            } else {
              // Bar-based: delay = clock_now - (bar_start + bar_duration)
              // Parse timeframe to get bar duration in seconds (e.g., '10s' -> 10, '1m' -> 60, '5m' -> 300)
              const barDurationSec = parseTimeframeToSeconds(tf);
              const barCloseNs = timestamp_ns + barDurationSec * 1_000_000_000;
              wsDelayMs = Math.floor((clock_now_ns - barCloseNs) / 1_000_000);
            }

            // Only log if delay is significant (> 500ms in replay time)
            if (wsDelayMs > 500) {
              const factorTime = new Date(timestamp_ns / 1_000_000);
              console.log(
                `[FactorUpdate] ${symbol}/${tf} ts=${factorTime.toISOString()} ` +
                `delay=${wsDelayMs}ms factors=${Object.keys(factors).join(',')}`
              );
            }
          }

          // Find all moduleIds that have factor data for this symbol AND timeframe
          // Keys are formatted as "{moduleId}::{ticker}::{timeframe}"
          const keySuffix = `::${symbol.toUpperCase()}::${tf}`;
          const matchingKeys = Object.keys(factorStore.symbolFactors).filter((key) =>
            key.endsWith(keySuffix)
          );

          if (matchingKeys.length === 0) {
            // No active factor charts for this symbol+timeframe yet - update will be lost
            // This is expected if user hasn't opened a factor chart for this timeframe
            return;
          }

          // Broadcast to all matching modules synchronously
          for (const key of matchingKeys) {
            const moduleId = key.split('::')[0];
            factorStore.updateFactors(moduleId, symbol, timestamp_ns, factors, tf);
          }
        }
        return;
      }

      // Handle tick data (Q/T events)
      const ev = data.event_type as 'Q' | 'T';
      const sym = data.symbol as string;

      if (!ev || !sym) return;

      set((s) => {
        const prevSym = s.symbolData[sym] || {};
        return {
          symbolData: {
            ...s.symbolData,
            [sym]: { ...prevSym, [ev]: data },
          },
        };
      });
    };

    socket.onclose = () => {
      console.log('❌ TickData WS disconnected');
      set({ connected: false });
    };

    socket.onerror = () => {
      set({ connected: false });
    };
  },

  dispose: () => {
    ws?.close();
    ws = null;
    set({ connected: false });
  },

  reconnect: () => {
    console.log('[TickData] Manual reconnect requested');
    ws?.close();
    ws = null;
    get().init();
  },

  addSymbols: (newSyms, events) => {
    const { symbols, perSymbolEvents } = get();
    const merged = Array.from(new Set([...symbols, ...newSyms]));

    const newPer = { ...perSymbolEvents };
    newSyms.forEach((s) => {
      newPer[s] = events;
    });

    set({ symbols: merged, perSymbolEvents: newPer });
    saveSymbols(merged);
    savePerSymbolEvents(newPer);

    // Increment fetch triggers so all modules showing re-subscribed tickers re-fetch data
    const chartStore = useChartDataStore.getState();
    const newTriggers = { ...chartStore.fetchTriggers };
    for (const s of newSyms) {
      newTriggers[s] = (newTriggers[s] || 0) + 1;
    }
    useChartDataStore.setState({ fetchTriggers: newTriggers });

    // Send subscribe
    const subs = newSyms.map((s) => ({ symbol: s, events }));
    if (subs.length > 0 && ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ subscriptions: subs }));
    }
  },

  removeSymbol: (symbol) => {
    const { symbols, perSymbolEvents, symbolData } = get();
    const updated = symbols.filter((s) => s !== symbol);
    const events = perSymbolEvents[symbol] || ['Q'];

    // Send unsubscribe
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ action: 'unsubscribe', symbol, events }));
    }

    const newPer = { ...perSymbolEvents };
    delete newPer[symbol];

    const newData = { ...symbolData };
    delete newData[symbol];

    // Also clean up factor subscriptions for this symbol
    const { factorSubscriptions } = get();
    const newFactorSubs = { ...factorSubscriptions };
    delete newFactorSubs[symbol];
    saveFactorSubscriptions(newFactorSubs);

    // Clear all factor data for this symbol (cheat method)
    useFactorDataStore.getState().clearFactorsForTicker(symbol);

    // Clear all chart bars for this symbol (cheat method)
    useChartDataStore.getState().clearBarsForTicker(symbol);

    set({
      symbols: updated,
      perSymbolEvents: newPer,
      symbolData: newData,
      factorSubscriptions: newFactorSubs,
    });
    saveSymbols(updated);
    savePerSymbolEvents(newPer);
  },

  // ========================================================================
  // Factor Subscriptions (for FactorChartModule)
  // ========================================================================

  subscribeFactors: (symbols: string | string[], timeframe: string = 'trade') => {
    const symbolArray = Array.isArray(symbols) ? symbols : [symbols];
    const symbolUpper = symbolArray.map(s => s.toUpperCase());

    // Persist to localStorage for re-subscribe on reconnect
    const { factorSubscriptions } = get();
    const updatedSubs: Record<string, string[]> = {};
    for (const sym of symbolUpper) {
      const existing = factorSubscriptions[sym] || [];
      if (!existing.includes(timeframe)) {
        updatedSubs[sym] = [...existing, timeframe];
      } else {
        updatedSubs[sym] = existing; // Already subscribed
      }
    }

    // Merge with existing subscriptions
    const mergedSubs = { ...factorSubscriptions, ...updatedSubs };
    set({ factorSubscriptions: mergedSubs });
    saveFactorSubscriptions(mergedSubs);

    // Send WebSocket message if connected, otherwise it will be sent on reconnect
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        action: 'subscribe_factors',
        symbols: symbolUpper,
        timeframe,
      }));
      console.log('[TickData] Subscribed to factors:', symbolUpper, `timeframe=${timeframe}`);
    } else {
      console.warn('[TickData] WebSocket not connected, factor subscription queued for reconnect:', symbolUpper, `timeframe=${timeframe}`);
    }
  },

  unsubscribeFactors: (symbols: string | string[], timeframe: string = 'trade') => {
    const symbolArray = Array.isArray(symbols) ? symbols : [symbols];
    const symbolUpper = symbolArray.map(s => s.toUpperCase());

    // Remove from persisted subscriptions
    const { factorSubscriptions } = get();
    const updatedSubs: Record<string, string[]> = {};
    for (const sym of symbolUpper) {
      const existing = factorSubscriptions[sym] || [];
      updatedSubs[sym] = existing.filter(tf => tf !== timeframe);
      if (updatedSubs[sym].length === 0) {
        delete updatedSubs[sym];
      }
    }

    // Merge with existing
    const mergedSubs = { ...factorSubscriptions };
    for (const sym of symbolUpper) {
      if (mergedSubs[sym]) {
        mergedSubs[sym] = mergedSubs[sym].filter(tf => tf !== timeframe);
        if (mergedSubs[sym].length === 0) {
          delete mergedSubs[sym];
        }
      }
    }
    set({ factorSubscriptions: mergedSubs });
    saveFactorSubscriptions(mergedSubs);

    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        action: 'unsubscribe_factors',
        symbols: symbolUpper,
        timeframe,
      }));
      console.log('[TickData] Unsubscribed from factors:', symbolUpper, `timeframe=${timeframe}`);
    }
  },
}));
