// Shared message types for IBBot WebSocket.
//
// Server messages are designed to be backward-compatible:
// - `type` is always present
// - `event_name` is present for `type: 'event'`

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
