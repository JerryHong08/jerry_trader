/**
 * Shared types for IBBot (OrderManagement) WebSocket & REST communication.
 *
 * Ported from OrderTrader — canonical source for the wsl2 IB backend.
 */

// ============================================================================
// WebSocket message envelope
// ============================================================================

export type WsConnectionMessage = {
  type: 'connection';
  data: {
    status: 'connected' | string;
    message?: string;
  };
};

export type WsEventMessage<TName extends string = string, TData = unknown> = {
  type: 'event';
  event_name: TName;
  data: TData;
};

export type WsMessage = WsConnectionMessage | WsEventMessage;

// ============================================================================
// Event payloads
// ============================================================================

export type OrderStatusEventData = {
  order_id: number;
  status: string;
  filled: number;
  remaining: number;
  avg_fill_price?: number;
  commission?: number | null;
  symbol?: string | null;
  action?: string | null;
  quantity?: number | null;
  order_type?: string | null;
  limit_price?: number | null;
  stop_price?: number | null;
  tif?: string | null;
  outsideRth?: boolean | null;
  sec_type?: string | null;
  exchange?: string | null;
  currency?: string | null;
};

export type PositionUpdatedEventData = {
  symbol: string;
  position: number;
  average_cost: number;
  market_price: number;
  market_value: number;
  unrealized_pnl: number;
  realized_pnl: number;
};

export type AccountUpdatedEventData = {
  tag: string;
  value: unknown;
  currency: string;
};

// ============================================================================
// REST types
// ============================================================================

export type PlaceOrderRequest = {
  symbol: string;
  action: 'BUY' | 'SELL';
  quantity?: number | null;
  order_type: 'MKT' | 'LMT';
  limit_price?: number | null;
  tif?: string;
  OutsideRth?: boolean;
  sec_type?: string;
  reason?: string | null;
  pct?: number | null;
  price?: number | null;
};

export type PlaceOrderResponse = {
  status: 'ok' | string;
  order_id: number;
  quantity?: number;
  message?: string;
};

export type PortfolioSummaryResponse = {
  account: Record<string, unknown>;
  positions: Array<{
    symbol: string;
    quantity: number;
    average_cost: number;
    market_value: number;
    market_price?: number;
    unrealized_pnl?: number;
    realized_pnl?: number;
  }>;
  total_market_value: number;
  position_count: number;
};

export type CancelOrderResponse = {
  status: 'ok' | string;
  order_id: number;
  message?: string;
};
