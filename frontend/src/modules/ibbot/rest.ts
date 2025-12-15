import type { OrderStatusEventData } from '../../types/ibbot';

export type PlaceOrderRequest = {
  symbol: string;
  action: 'BUY' | 'SELL';
  quantity: number;
  order_type: 'MKT' | 'LMT';
  limit_price?: number | null;
  tif?: string;
  OutsideRth?: boolean;
  sec_type?: string;
  reason?: string | null;
};

export type PlaceOrderResponse = {
  status: 'ok' | string;
  order_id: number;
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

async function httpJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text || res.statusText}`);
  }

  return (await res.json()) as T;
}

export function listOrders(restBaseUrl: string): Promise<OrderStatusEventData[]> {
  return httpJson<OrderStatusEventData[]>(`${restBaseUrl}/orders/list`);
}

export function placeOrder(
  restBaseUrl: string,
  req: PlaceOrderRequest,
): Promise<PlaceOrderResponse> {
  return httpJson<PlaceOrderResponse>(`${restBaseUrl}/orders/place`, {
    method: 'POST',
    body: JSON.stringify(req),
  });
}

export function cancelOrder(
  restBaseUrl: string,
  orderId: number,
  reason?: string | null,
): Promise<CancelOrderResponse> {
  const body = (reason ?? '').trim();
  return httpJson<CancelOrderResponse>(`${restBaseUrl}/orders/cancel/${orderId}`, {
    method: 'POST',
    body: body ? JSON.stringify({ reason: body }) : undefined,
  });
}

export function refreshPortfolio(restBaseUrl: string): Promise<{ status: string } | unknown> {
  return httpJson(`${restBaseUrl}/portfolio/refresh`, { method: 'POST' });
}

export function getPortfolioSummary(
  restBaseUrl: string,
): Promise<PortfolioSummaryResponse> {
  return httpJson<PortfolioSummaryResponse>(`${restBaseUrl}/portfolio/summary`);
}
