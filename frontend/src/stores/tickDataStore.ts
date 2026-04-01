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

// ============================================================================
// Types
// ============================================================================

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

function saveSymbols(symbols: string[]) {
  localStorage.setItem('tickdata-symbols', JSON.stringify(symbols));
}

function savePerSymbolEvents(events: Record<string, string[]>) {
  localStorage.setItem('tickdata-perSymbolEvents', JSON.stringify(events));
}

// ============================================================================
// Store
// ============================================================================

let ws: WebSocket | null = null;

export const useTickDataStore = create<TickDataState>()((set, get) => ({
  symbols: loadSymbols(),
  symbolData: {},
  perSymbolEvents: loadPerSymbolEvents(),
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
    };

    socket.onmessage = (event) => {
      const data = JSON.parse(event.data);

      // Handle factor updates - broadcast to all modules with this symbol and timeframe
      if (data.type === 'factor_update') {
        const factorStore = useFactorDataStore.getState();
        const { symbol, timestamp_ns, factors, timeframe } = data.data || {};

        if (symbol && timestamp_ns && factors) {
          // timeframe from backend (e.g., 'trade', '1m', '5m')
          const tf = timeframe || 'trade';

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

    set({
      symbols: updated,
      perSymbolEvents: newPer,
      symbolData: newData,
    });
    saveSymbols(updated);
    savePerSymbolEvents(newPer);
  },

  // ========================================================================
  // Factor Subscriptions (for FactorChartModule)
  // ========================================================================

  subscribeFactors: (symbols: string | string[], timeframe: string = 'trade') => {
    const symbolArray = Array.isArray(symbols) ? symbols : [symbols];
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        action: 'subscribe_factors',
        symbols: symbolArray.map(s => s.toUpperCase()),
        timeframe,
      }));
      console.log('[TickData] Subscribed to factors:', symbolArray, `timeframe=${timeframe}`);
    } else {
      console.warn('[TickData] Cannot subscribe to factors - WebSocket not connected');
    }
  },

  unsubscribeFactors: (symbols: string | string[], timeframe: string = 'trade') => {
    const symbolArray = Array.isArray(symbols) ? symbols : [symbols];
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        action: 'unsubscribe_factors',
        symbols: symbolArray.map(s => s.toUpperCase()),
        timeframe,
      }));
      console.log('[TickData] Unsubscribed from factors:', symbolArray, `timeframe=${timeframe}`);
    }
  },
}));
