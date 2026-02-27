/**
 * IBBot Zustand Store
 *
 * Centralized state for the IB OrderManagement backend:
 * - Orders (by ID, streamed via WS + REST refresh)
 * - Positions (by symbol)
 * - Account info (tag/value patches from WS)
 * - Portfolio summary (REST snapshot)
 * - WebSocket connection lifecycle
 */

import { create } from 'zustand';
import type {
  OrderStatusEventData,
  PositionUpdatedEventData,
  PortfolioSummaryResponse,
  PlaceOrderRequest,
} from '../types/ibbot';
import {
  getIbbotBaseUrl,
  getIbbotWsUrl,
  listOrders,
  placeOrder as apiPlaceOrder,
  cancelOrder as apiCancelOrder,
  refreshPortfolio as apiRefreshPortfolio,
  getPortfolioSummary,
} from '../services/ibbotApi';
import { connectIbbotWs, type IbbotWsClient } from '../services/ibbotWs';

// ============================================================================
// Types
// ============================================================================

export type PositionRow = {
  symbol: string;
  position?: number;
  average_cost?: number;
  market_price?: number;
  market_value?: number;
  unrealized_pnl?: number;
  realized_pnl?: number;
};

type WsStatus = 'connected' | 'disconnected' | 'error';

interface IbbotState {
  // Connection
  restBaseUrl: string;
  wsUrl: string;
  wsStatus: WsStatus;
  wsInfo: string;

  // Data
  ordersById: Record<number, OrderStatusEventData>;
  positionsBySymbol: Record<string, PositionRow>;
  account: Record<string, unknown>;
  portfolioSummary: PortfolioSummaryResponse | null;

  // UI
  loading: boolean;
  error: string;

  // Derived
  ordersList: () => OrderStatusEventData[];
  positionsList: () => PositionRow[];
  buyingPower: () => number | null;

  // Actions
  init: () => void;
  dispose: () => void;
  refreshOrders: () => Promise<void>;
  loadInitial: () => Promise<void>;
  doRefreshPortfolio: () => Promise<void>;
  submitOrder: (form: PlaceOrderRequest, orderMode: 'quantity' | 'pct') => Promise<void>;
  doCancelOrder: (orderId: number, cancelReason: string) => Promise<void>;
}

// ============================================================================
// Helpers
// ============================================================================

function mergeOrder(
  prev: OrderStatusEventData | undefined,
  next: OrderStatusEventData,
): OrderStatusEventData {
  if (!prev) return next;
  return {
    ...prev,
    ...next,
    symbol: next.symbol ?? prev.symbol ?? null,
    action: next.action ?? prev.action ?? null,
    commission: next.commission ?? prev.commission ?? null,
  };
}

// ============================================================================
// Store
// ============================================================================

let wsClient: IbbotWsClient | null = null;

export const useIbbotStore = create<IbbotState>()((set, get) => {
  const restBaseUrl = getIbbotBaseUrl();
  const wsUrl = getIbbotWsUrl();

  return {
    restBaseUrl,
    wsUrl,
    wsStatus: 'disconnected',
    wsInfo: '',

    ordersById: {},
    positionsBySymbol: {},
    account: {},
    portfolioSummary: null,

    loading: false,
    error: '',

    // Derived
    ordersList: () =>
      Object.values(get().ordersById).sort((a, b) => b.order_id - a.order_id),

    positionsList: () =>
      Object.values(get().positionsBySymbol).sort((a, b) =>
        a.symbol.localeCompare(b.symbol),
      ),

    buyingPower: () => {
      const bp = get().portfolioSummary?.account?.['BuyingPower'];
      return typeof bp === 'number' ? bp : null;
    },

    // ========================================================================
    // Lifecycle
    // ========================================================================

    init: () => {
      const state = get();
      state.loadInitial();

      wsClient?.close();
      wsClient = connectIbbotWs(state.wsUrl, {
        onConnection: (status, info) => {
          set({ wsStatus: status, wsInfo: info ?? '' });
          if (status === 'connected') {
            get().refreshOrders().catch(() => undefined);
            window.setTimeout(() => {
              get().refreshOrders().catch(() => undefined);
            }, 1200);
          }
        },
        onOrderStatus: (data) => {
          set((s) => ({
            ordersById: {
              ...s.ordersById,
              [data.order_id]: mergeOrder(s.ordersById[data.order_id], data),
            },
          }));
        },
        onPositionUpdated: (data: PositionUpdatedEventData) => {
          set((s) => ({
            positionsBySymbol: {
              ...s.positionsBySymbol,
              [data.symbol]: {
                symbol: data.symbol,
                position: data.position,
                average_cost: data.average_cost,
                market_price: data.market_price,
                market_value: data.market_value,
                unrealized_pnl: data.unrealized_pnl,
                realized_pnl: data.realized_pnl,
              },
            },
          }));
        },
        onAccountUpdated: (data) => {
          set((s) => ({
            account: { ...s.account, [data.tag]: data.value },
          }));
        },
      });
    },

    dispose: () => {
      wsClient?.close();
      wsClient = null;
    },

    // ========================================================================
    // Data actions
    // ========================================================================

    refreshOrders: async () => {
      try {
        const orders = await listOrders(get().restBaseUrl);
        const map: Record<number, OrderStatusEventData> = {};
        for (const o of orders) map[o.order_id] = o;
        set({ ordersById: map });
      } catch {
        // silently ignore refresh failures
      }
    },

    loadInitial: async () => {
      set({ error: '', loading: true });
      try {
        const [orders, summary] = await Promise.all([
          listOrders(restBaseUrl),
          getPortfolioSummary(restBaseUrl),
        ]);

        const ordersMap: Record<number, OrderStatusEventData> = {};
        for (const o of orders) ordersMap[o.order_id] = o;

        const posMap: Record<string, PositionRow> = {};
        for (const p of summary.positions ?? []) {
          posMap[p.symbol] = {
            symbol: p.symbol,
            position: p.quantity,
            average_cost: p.average_cost,
            market_price: p.market_price,
            market_value: p.market_value,
            unrealized_pnl: p.unrealized_pnl,
            realized_pnl: p.realized_pnl,
          };
        }

        set({
          ordersById: ordersMap,
          portfolioSummary: summary,
          positionsBySymbol: posMap,
          account: summary.account ?? {},
        });
      } catch (e) {
        set({ error: e instanceof Error ? e.message : String(e) });
      } finally {
        set({ loading: false });
      }
    },

    doRefreshPortfolio: async () => {
      set({ error: '', loading: true });
      try {
        await apiRefreshPortfolio(restBaseUrl);
        const summary = await getPortfolioSummary(restBaseUrl);

        const posMap: Record<string, PositionRow> = {};
        for (const p of summary.positions ?? []) {
          posMap[p.symbol] = {
            symbol: p.symbol,
            position: p.quantity,
            average_cost: p.average_cost,
            market_price: p.market_price,
            market_value: p.market_value,
            unrealized_pnl: p.unrealized_pnl,
            realized_pnl: p.realized_pnl,
          };
        }

        set({
          portfolioSummary: summary,
          positionsBySymbol: posMap,
          account: summary.account ?? {},
        });
      } catch (e) {
        set({ error: e instanceof Error ? e.message : String(e) });
      } finally {
        set({ loading: false });
      }
    },

    submitOrder: async (form, orderMode) => {
      set({ error: '', loading: true });
      try {
        const req: PlaceOrderRequest = {
          ...form,
          symbol: form.symbol.trim().toUpperCase(),
          limit_price:
            form.order_type === 'LMT' ? Number(form.limit_price ?? 0) : null,
          reason: (form.reason ?? '').trim() || null,
        };

        if (orderMode === 'pct') {
          if (!form.pct || form.pct <= 0 || form.pct > 100)
            throw new Error('Percentage must be between 1 and 100');
          const priceForCalc = form.price ?? form.limit_price;
          if (!priceForCalc || priceForCalc <= 0)
            throw new Error('Price is required for percentage-based orders');
          req.quantity = null;
          req.pct = form.pct;
          req.price = priceForCalc;
        } else {
          req.quantity = Number(form.quantity);
          req.pct = null;
          req.price = null;
          if (!req.quantity || req.quantity <= 0)
            throw new Error('Quantity must be greater than 0');
        }

        if (
          req.order_type === 'LMT' &&
          (!req.limit_price || Number.isNaN(req.limit_price))
        )
          throw new Error('Limit order requires a valid limit_price');

        await apiPlaceOrder(restBaseUrl, req);
        await get().refreshOrders();
        window.setTimeout(() => {
          get().refreshOrders().catch(() => undefined);
        }, 1200);
      } catch (e) {
        set({ error: e instanceof Error ? e.message : String(e) });
      } finally {
        set({ loading: false });
      }
    },

    doCancelOrder: async (orderId, cancelReason) => {
      // Optimistic local update — mark as PendingCancel immediately.
      // The real Cancelled status will arrive via the WebSocket onOrderStatus handler.
      // We intentionally do NOT set global loading or trigger refreshOrders()
      // to avoid a full-list re-render / glitch.
      set((s) => {
        const existing = s.ordersById[orderId];
        if (!existing) return s;
        return {
          ordersById: {
            ...s.ordersById,
            [orderId]: { ...existing, status: 'PendingCancel' },
          },
        };
      });

      try {
        await apiCancelOrder(restBaseUrl, orderId, cancelReason);
      } catch (e) {
        // Revert optimistic update on failure
        set((s) => {
          const existing = s.ordersById[orderId];
          if (!existing || existing.status !== 'PendingCancel') return s;
          return {
            error: e instanceof Error ? e.message : String(e),
            ordersById: {
              ...s.ordersById,
              [orderId]: { ...existing, status: 'Submitted' },
            },
          };
        });
      }
    },
  };
});
