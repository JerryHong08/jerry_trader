/**
 * IBBot REST API client
 *
 * Talks to the OrderManagement FastAPI backend (default: http://localhost:8888).
 * Ported from OrderTrader/src/modules/ibbot/rest.ts
 */

import type {
  OrderStatusEventData,
  PlaceOrderRequest,
  PlaceOrderResponse,
  PortfolioSummaryResponse,
  CancelOrderResponse,
} from '../types/ibbot';

// ============================================================================
// Config helper
// ============================================================================

export function getIbbotBaseUrl(): string {
  const defaultHost =
    typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  return (
    (import.meta.env.VITE_IBBOT_URL as string | undefined) ||
    `http://${defaultHost}:8888`
  );
}

export function getIbbotWsUrl(): string {
  const base = getIbbotBaseUrl();
  const url = new URL(base);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  url.pathname = '/ws';
  url.search = '';
  url.hash = '';
  return url.toString();
}

// ============================================================================
// HTTP helper
// ============================================================================

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

// ============================================================================
// API functions
// ============================================================================

export function listOrders(baseUrl: string): Promise<OrderStatusEventData[]> {
  return httpJson<OrderStatusEventData[]>(`${baseUrl}/orders/list`);
}

export function placeOrder(
  baseUrl: string,
  req: PlaceOrderRequest,
): Promise<PlaceOrderResponse> {
  return httpJson<PlaceOrderResponse>(`${baseUrl}/orders/place`, {
    method: 'POST',
    body: JSON.stringify(req),
  });
}

export function cancelOrder(
  baseUrl: string,
  orderId: number,
  reason?: string | null,
): Promise<CancelOrderResponse> {
  const body = (reason ?? '').trim();
  return httpJson<CancelOrderResponse>(`${baseUrl}/orders/cancel/${orderId}`, {
    method: 'POST',
    body: body ? JSON.stringify({ reason: body }) : undefined,
  });
}

export function refreshPortfolio(
  baseUrl: string,
): Promise<{ status: string } | unknown> {
  return httpJson(`${baseUrl}/portfolio/refresh`, { method: 'POST' });
}

export function getPortfolioSummary(
  baseUrl: string,
): Promise<PortfolioSummaryResponse> {
  return httpJson<PortfolioSummaryResponse>(`${baseUrl}/portfolio/summary`);
}
