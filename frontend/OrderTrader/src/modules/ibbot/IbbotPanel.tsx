import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type {
  OrderStatusEventData,
} from '../../types/ibbot';

import { getIbbotConfig } from './config';
import {
  cancelOrder,
  getPortfolioSummary,
  listOrders,
  placeOrder,
  refreshPortfolio,
  type PlaceOrderRequest,
  type PortfolioSummaryResponse,
} from './rest';
import { connectIbbotWs, type IbbotWsClient } from './ws';

type PositionRow = {
  symbol: string;
  position?: number;
  average_cost?: number;
  market_price?: number;
  market_value?: number;
  unrealized_pnl?: number;
  realized_pnl?: number;
};

interface IbbotPanelProps {
  chartSymbol?: string | null;
  lastTradePrice?: number | null;
}

function canCancelOrder(status: string): boolean {
  const s = (status || '').toLowerCase();
  // terminal-ish states
  if (['filled', 'cancelled', 'apicancelled', 'inactive', 'error', 'pendingcancel'].includes(s)) return false;
  return true;
}

function mergeOrder(prev: OrderStatusEventData | undefined, next: OrderStatusEventData): OrderStatusEventData {
  if (!prev) return next;
  return {
    ...prev,
    ...next,
    // keep some stable fields if next is missing
    symbol: next.symbol ?? prev.symbol ?? null,
    action: next.action ?? prev.action ?? null,
    commission: next.commission ?? prev.commission ?? null,
  };
}

// Scrollable price input component
function PriceInput({
  value,
  onChange,
  disabled,
}: {
  value: number | null;
  onChange: (v: number | null) => void;
  disabled?: boolean;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [isHovering, setIsHovering] = useState(false);

  const currentPrice = value ?? 0;

  const handleWheel = (e: React.WheelEvent) => {
    if (disabled) return;
    e.preventDefault();
    const step = currentPrice * 0.001; // 0.1% per scroll tick
    const delta = e.deltaY < 0 ? step : -step;
    const newPrice = Math.max(0.01, currentPrice + delta);
    onChange(Math.round(newPrice * 100) / 100);
  };

  const adjustByPercent = (percent: number) => {
    if (disabled) return;
    const newPrice = currentPrice * (1 + percent / 100);
    onChange(Math.max(0.01, Math.round(newPrice * 100) / 100));
  };

  return (
    <div
      onMouseEnter={() => setIsHovering(true)}
      onMouseLeave={() => setIsHovering(false)}
      style={{ display: 'flex', flexDirection: 'column', gap: 4 }}
    >
      <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
        <button
          type="button"
          disabled={disabled}
          onClick={() => adjustByPercent(-5)}
          style={{ padding: '2px 6px', fontSize: 11 }}
          title="-5%"
        >
          -5%
        </button>
        <button
          type="button"
          disabled={disabled}
          onClick={() => adjustByPercent(-1)}
          style={{ padding: '2px 6px', fontSize: 11 }}
          title="-1%"
        >
          -1%
        </button>
        <input
          ref={inputRef}
          style={{
            width: '100%',
            cursor: isHovering && !disabled ? 'ns-resize' : 'text',
          }}
          type="number"
          step="0.01"
          value={value ?? ''}
          onChange={(e) => onChange(e.target.value === '' ? null : Number(e.target.value))}
          onWheel={handleWheel}
          disabled={disabled}
        />
        <button
          type="button"
          disabled={disabled}
          onClick={() => adjustByPercent(1)}
          style={{ padding: '2px 6px', fontSize: 11 }}
          title="+1%"
        >
          +1%
        </button>
        <button
          type="button"
          disabled={disabled}
          onClick={() => adjustByPercent(5)}
          style={{ padding: '2px 6px', fontSize: 11 }}
          title="+5%"
        >
          +5%
        </button>
      </div>
      {isHovering && !disabled && (
        <div style={{ fontSize: 10, opacity: 0.6, textAlign: 'center' }}>
          Scroll to adjust price (0.1% per tick)
        </div>
      )}
    </div>
  );
}

export default function IbbotPanel({ chartSymbol, lastTradePrice }: IbbotPanelProps) {
  const cfg = useMemo(() => getIbbotConfig(), []);

  const [wsStatus, setWsStatus] = useState<'connected' | 'disconnected' | 'error'>('disconnected');
  const [wsInfo, setWsInfo] = useState<string>('');

  const [ordersById, setOrdersById] = useState<Record<number, OrderStatusEventData>>({});
  const [positionsBySymbol, setPositionsBySymbol] = useState<Record<string, PositionRow>>({});
  const [account, setAccount] = useState<Record<string, unknown>>({});
  const [portfolioSummary, setPortfolioSummary] = useState<PortfolioSummaryResponse | null>(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>('');
  const [showPortfolio, setShowPortfolio] = useState(false);

  const wsClientRef = useRef<IbbotWsClient | null>(null);

  const refreshOrders = useCallback(async () => {
    const orders = await listOrders(cfg.restBaseUrl);
    setOrdersById(() => {
      const map: Record<number, OrderStatusEventData> = {};
      for (const o of orders) map[o.order_id] = o;
      return map;
    });
  }, [cfg.restBaseUrl]);

  // Order mode: 'quantity' or 'pct'
  const [orderMode, setOrderMode] = useState<'quantity' | 'pct'>('pct');

  const [form, setForm] = useState<PlaceOrderRequest>({
    symbol: chartSymbol || 'AAPL',
    action: 'BUY',
    quantity: 100,
    order_type: 'LMT',  // Default to LMT for pre-market trading (MKT conflicts with OutsideRth)
    limit_price: lastTradePrice ?? null,
    tif: 'DAY',
    OutsideRth: true,
    sec_type: 'STK',
    reason: '',
    pct: 60,
    price: lastTradePrice ?? null,  // Price for pct calculation
  });

  // Track if price has been manually set (to avoid overwriting user input)
  const priceInitializedRef = useRef(false);

  // Sync symbol from chart when it changes
  useEffect(() => {
    if (chartSymbol && chartSymbol !== form.symbol) {
      setForm((p) => ({ ...p, symbol: chartSymbol }));
      // Reset price initialization when symbol changes
      priceInitializedRef.current = false;
    }
  }, [chartSymbol, form.symbol]);

  // Sync limit price and pct price from last trade price (only on first load or after refresh)
  useEffect(() => {
    if (lastTradePrice && !priceInitializedRef.current && form.order_type === 'LMT') {
      const roundedPrice = Math.round(lastTradePrice * 100) / 100;
      setForm((p) => ({
        ...p,
        limit_price: roundedPrice,
        price: roundedPrice,  // Also sync the price for pct calculation
      }));
      priceInitializedRef.current = true;
    }
  }, [lastTradePrice, form.order_type]);

  // Function to refresh limit price from latest trade
  const syncPriceFromTrade = () => {
    if (lastTradePrice) {
      const roundedPrice = Math.round(lastTradePrice * 100) / 100;
      setForm((p) => ({
        ...p,
        limit_price: roundedPrice,
        price: roundedPrice,
      }));
    }
  };

  // Get buying power from portfolio summary for display
  const buyingPower = useMemo(() => {
    const bp = portfolioSummary?.account?.['BuyingPower'];
    return typeof bp === 'number' ? bp : null;
  }, [portfolioSummary]);

  const [cancelReason, setCancelReason] = useState<string>('');

  const ordersList = useMemo(() => {
    return Object.values(ordersById).sort((a, b) => b.order_id - a.order_id);
  }, [ordersById]);

  const positionsList = useMemo(() => {
    return Object.values(positionsBySymbol).sort((a, b) => a.symbol.localeCompare(b.symbol));
  }, [positionsBySymbol]);

  const loadInitial = useCallback(async () => {
    setError('');
    setLoading(true);
    try {
      const [orders, summary] = await Promise.all([
        listOrders(cfg.restBaseUrl),
        getPortfolioSummary(cfg.restBaseUrl),
      ]);

      setOrdersById(() => {
        const map: Record<number, OrderStatusEventData> = {};
        for (const o of orders) map[o.order_id] = o;
        return map;
      });

      setPortfolioSummary(summary);

      setPositionsBySymbol(() => {
        const map: Record<string, PositionRow> = {};
        for (const p of summary.positions ?? []) {
          map[p.symbol] = {
            symbol: p.symbol,
            position: p.quantity,
            average_cost: p.average_cost,
            market_price: p.market_price,
            market_value: p.market_value,
            unrealized_pnl: p.unrealized_pnl,
            realized_pnl: p.realized_pnl,
          };
        }
        return map;
      });

      setAccount(summary.account ?? {});
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [cfg.restBaseUrl]);

  async function doRefreshPortfolio() {
    setError('');
    setLoading(true);
    try {
      await refreshPortfolio(cfg.restBaseUrl);
      const summary = await getPortfolioSummary(cfg.restBaseUrl);
      setPortfolioSummary(summary);

      setPositionsBySymbol(() => {
        const map: Record<string, PositionRow> = {};
        for (const p of summary.positions ?? []) {
          map[p.symbol] = {
            symbol: p.symbol,
            position: p.quantity,
            average_cost: p.average_cost,
            market_price: p.market_price,
            market_value: p.market_value,
            unrealized_pnl: p.unrealized_pnl,
            realized_pnl: p.realized_pnl,
          };
        }
        return map;
      });

      setAccount(summary.account ?? {});
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  async function doCancelOrder(orderId: number) {
    setError('');
    setLoading(true);
    try {
      await cancelOrder(cfg.restBaseUrl, orderId, cancelReason);

      // Optimistic local update to prevent double-click while we wait for IB callback.
      setOrdersById((prev) => {
        const existing = prev[orderId];
        if (!existing) return prev;
        return {
          ...prev,
          [orderId]: {
            ...existing,
            status: 'PendingCancel',
          },
        };
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      // Always refresh to avoid stale UI (e.g., order filled before cancel)
      try {
        await refreshOrders();
        window.setTimeout(() => {
          refreshOrders().catch(() => undefined);
        }, 1200);
      } catch {
        // ignore
      }
      setLoading(false);
    }
  }

  async function submitOrder() {
    setError('');
    setLoading(true);
    try {
      const req: PlaceOrderRequest = {
        ...form,
        symbol: form.symbol.trim().toUpperCase(),
        limit_price: form.order_type === 'LMT' ? Number(form.limit_price ?? 0) : null,
        reason: (form.reason ?? '').trim() || null,
      };

      // Handle quantity vs pct mode
      if (orderMode === 'pct') {
        // Use pct mode: send pct and price, backend calculates quantity
        if (!form.pct || form.pct <= 0 || form.pct > 100) {
          throw new Error('Percentage must be between 1 and 100');
        }
        const priceForCalc = form.price ?? form.limit_price;
        if (!priceForCalc || priceForCalc <= 0) {
          throw new Error('Price is required for percentage-based orders');
        }
        req.quantity = null;
        req.pct = form.pct;
        req.price = priceForCalc;
      } else {
        // Use quantity mode
        req.quantity = Number(form.quantity);
        req.pct = null;
        req.price = null;
        if (!req.quantity || req.quantity <= 0) {
          throw new Error('Quantity must be greater than 0');
        }
      }

      if (req.order_type === 'LMT' && (!req.limit_price || Number.isNaN(req.limit_price))) {
        throw new Error('Limit order requires a valid limit_price');
      }

      await placeOrder(cfg.restBaseUrl, req);
      // Server *should* push updates via WS, but do two best-effort refreshes
      // to handle fast fills or missed WS updates.
      await refreshOrders();
      window.setTimeout(() => {
        refreshOrders().catch(() => undefined);
      }, 1200);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadInitial();

    wsClientRef.current?.close();
    wsClientRef.current = connectIbbotWs(cfg.wsUrl, {
      onConnection: (status, info) => {
        setWsStatus(status);
        setWsInfo(info ?? '');

        if (status === 'connected') {
          // Resync on (re)connect to avoid stale UI if any WS messages were missed.
          refreshOrders().catch(() => undefined);
          window.setTimeout(() => {
            refreshOrders().catch(() => undefined);
          }, 1200);
        }
      },
      onOrderStatus: (data) => {
        setOrdersById((prev) => ({
          ...prev,
          [data.order_id]: mergeOrder(prev[data.order_id], data),
        }));
      },
      onPositionUpdated: (data) => {
        setPositionsBySymbol((prev) => ({
          ...prev,
          [data.symbol]: {
            symbol: data.symbol,
            position: data.position,
            average_cost: data.average_cost,
            market_price: data.market_price,
            market_value: data.market_value,
            unrealized_pnl: data.unrealized_pnl,
            realized_pnl: data.realized_pnl,
          },
        }));
      },
      onAccountUpdated: (data) => {
        // unify account view: treat stream tag updates as patches over snapshot
        setAccount((prev) => ({
          ...prev,
          [data.tag]: data.value,
        }));
      },
    });

    return () => {
      wsClientRef.current?.close();
      wsClientRef.current = null;
    };
  }, [cfg.restBaseUrl, cfg.wsUrl, loadInitial, refreshOrders]);

  const wsStatusText = wsStatus === 'connected' ? 'Connected' : wsStatus === 'error' ? 'Error' : 'Disconnected';

  return (
    <div style={{ textAlign: 'left' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, marginBottom: 8 }}>
        <div>
          <div style={{ fontSize: 14 }}>
            <strong>REST</strong>: {cfg.restBaseUrl}
          </div>
          <div style={{ fontSize: 14 }}>
            <strong>WS</strong>: {cfg.wsUrl}
          </div>
        </div>

        <div style={{ fontSize: 14 }}>
          <strong>WebSocket</strong>: {wsStatusText}{wsInfo ? ` (${wsInfo})` : ''}
        </div>
      </div>

      {error ? (
        <div style={{ padding: 10, border: '1px solid', borderRadius: 8, marginBottom: 12 }}>
          <strong>Error:</strong> {error}
        </div>
      ) : null}

      <div style={{ display: 'grid', gap: 16, gridTemplateColumns: '1fr', marginBottom: 16 }}>
        <section style={{ padding: 12, border: '1px solid', borderRadius: 10 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <h3 style={{ marginTop: 0, marginBottom: 0 }}>Place Order</h3>
            {buyingPower !== null && (
              <span style={{ fontSize: 12, opacity: 0.8 }}>
                Buying Power: {showPortfolio
                  ? `$${buyingPower.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
                  : '****'}
              </span>
            )}
          </div>

          <div style={{ display: 'grid', gap: 10, gridTemplateColumns: 'repeat(6, minmax(0, 1fr))' }}>
            <label style={{ gridColumn: 'span 2' }}>
              Symbol
              <input
                style={{ width: '100%' }}
                value={form.symbol}
                onChange={(e) => setForm((p) => ({ ...p, symbol: e.target.value }))}
              />
            </label>

            <label style={{ gridColumn: 'span 1' }}>
              Action
              <select
                style={{ width: '100%' }}
                value={form.action}
                onChange={(e) => setForm((p) => ({ ...p, action: e.target.value as 'BUY' | 'SELL' }))}
              >
                <option value="BUY">BUY</option>
                <option value="SELL">SELL</option>
              </select>
            </label>

            {/* Mode selector: Quantity vs Percentage */}
            <label style={{ gridColumn: 'span 1' }}>
              Mode
              <select
                style={{ width: '100%' }}
                value={orderMode}
                onChange={(e) => setOrderMode(e.target.value as 'quantity' | 'pct')}
              >
                <option value="quantity">Qty</option>
                <option value="pct">Pct %</option>
              </select>
            </label>

            {orderMode === 'quantity' ? (
              <label style={{ gridColumn: 'span 1' }}>
                Qty
                <input
                  style={{ width: '100%' }}
                  type="number"
                  min={1}
                  value={form.quantity ?? ''}
                  onChange={(e) => setForm((p) => ({ ...p, quantity: e.target.value === '' ? null : Number(e.target.value) }))}
                />
              </label>
            ) : (
              <label style={{ gridColumn: 'span 1' }}>
                Pct %
                <input
                  style={{ width: '100%' }}
                  type="number"
                  min={1}
                  max={100}
                  step={1}
                  value={form.pct ?? ''}
                  onChange={(e) => setForm((p) => ({ ...p, pct: e.target.value === '' ? null : Number(e.target.value) }))}
                  placeholder="1-100"
                />
              </label>
            )}

            <label style={{ gridColumn: 'span 1' }}>
              Type
              <select
                style={{ width: '100%' }}
                value={form.order_type}
                onChange={(e) =>
                  setForm((p) => ({
                    ...p,
                    order_type: e.target.value as 'MKT' | 'LMT',
                    limit_price: e.target.value === 'LMT' ? p.limit_price : null,
                  }))
                }
              >
                <option value="MKT">MKT</option>
                <option value="LMT">LMT</option>
              </select>
            </label>

            <label style={{ gridColumn: 'span 1' }}>
              TIF
              <select
                style={{ width: '100%' }}
                value={form.tif}
                onChange={(e) => setForm((p) => ({ ...p, tif: e.target.value }))}
              >
                <option value="DAY">DAY</option>
                <option value="GTC">GTC</option>
              </select>
            </label>

            {form.order_type === 'LMT' ? (
              <div style={{ gridColumn: 'span 4' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                  <span>Limit Price</span>
                  <button
                    type="button"
                    disabled={loading || !lastTradePrice}
                    onClick={syncPriceFromTrade}
                    style={{ padding: '2px 8px', fontSize: 11 }}
                    title="Sync price from last trade"
                  >
                    ↻ Sync
                  </button>
                  {lastTradePrice && (
                    <span style={{ fontSize: 11, opacity: 0.7 }}>
                      (Last: ${lastTradePrice.toFixed(2)})
                    </span>
                  )}
                </div>
                <PriceInput
                  value={form.limit_price ?? null}
                  onChange={(v) => setForm((p) => ({ ...p, limit_price: v }))}
                  disabled={loading}
                />
              </div>
            ) : null}

            <label style={{ gridColumn: form.order_type === 'LMT' ? 'span 2' : 'span 2', display: 'flex', alignItems: 'center', gap: 8, marginTop: 22 }}>
              <input
                type="checkbox"
                checked={Boolean(form.OutsideRth)}
                onChange={(e) => setForm((p) => ({ ...p, OutsideRth: e.target.checked }))}
              />
              Outside RTH
            </label>

            <label style={{ gridColumn: 'span 6' }}>
              Reason (optional)
              <input
                style={{ width: '100%' }}
                value={String(form.reason ?? '')}
                onChange={(e) => setForm((p) => ({ ...p, reason: e.target.value }))}
                placeholder="e.g. Rebalance / manual test / strategy-xyz"
              />
            </label>
          </div>

          <div style={{ display: 'flex', gap: 10, marginTop: 12 }}>
            <button disabled={loading} onClick={submitOrder}>
              {loading ? 'Working...' : 'Submit'}
            </button>
            <button disabled={loading} onClick={loadInitial}>
              Reload Orders/Portfolio
            </button>
          </div>
        </section>

        <section style={{ padding: 12, border: '1px solid', borderRadius: 10 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <h3 style={{ marginTop: 0 }}>Orders (stream)</h3>
            <div style={{ fontSize: 12, opacity: 0.8 }}>Count: {ordersList.length}</div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <label style={{ display: 'block' }}>
              Cancel reason (optional)
              <input
                style={{ width: '100%' }}
                value={cancelReason}
                onChange={(e) => setCancelReason(e.target.value)}
                placeholder="Used when clicking Cancel"
              />
            </label>
          </div>

          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr>
                  {['ID', 'Symbol', 'Action', 'Type', 'LmtPx', 'Status', 'Filled', 'Remaining', 'AvgPx', 'Commission', 'Actions'].map((h) => (
                    <th key={h} style={{ textAlign: 'left', borderBottom: '1px solid', padding: '6px 8px' }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {ordersList.map((o) => (
                  <tr key={o.order_id}>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.order_id}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.symbol ?? '—'}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.action ?? '—'}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.order_type ?? '—'}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>
                      {o.order_type === 'LMT' && o.limit_price != null
                        ? `$${Number(o.limit_price).toFixed(2)}`
                        : '—'}
                    </td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.status}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.filled}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.remaining}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.avg_fill_price ?? '—'}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{o.commission ?? '—'}</td>
                    <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>
                      <button
                        disabled={loading || !canCancelOrder(o.status)}
                        onClick={() => doCancelOrder(o.order_id)}
                      >
                        Cancel
                      </button>
                    </td>
                  </tr>
                ))}
                {ordersList.length === 0 ? (
                  <tr>
                    <td colSpan={11} style={{ padding: 10, opacity: 0.7 }}>
                      No orders yet
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>

        <section style={{ padding: 12, border: '1px solid', borderRadius: 10 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <h3 style={{ marginTop: 0, marginBottom: 0 }}>Portfolio</h3>
              <button
                onClick={() => setShowPortfolio((v) => !v)}
                title={showPortfolio ? 'Hide portfolio' : 'Show portfolio'}
                style={{
                  background: 'none',
                  border: 'none',
                  cursor: 'pointer',
                  fontSize: 16,
                  padding: '2px 4px',
                  opacity: 0.7,
                }}
              >
                {showPortfolio ? '👁️' : '👁️‍🗨️'}
              </button>
            </div>
            <div style={{ display: 'flex', gap: 10 }}>
              <button disabled={loading} onClick={doRefreshPortfolio}>
                Refresh
              </button>
            </div>
          </div>

          {showPortfolio && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
            <div>
              <h4 style={{ marginTop: 0 }}>Account (snapshot)</h4>
              <div style={{ fontSize: 12, opacity: 0.85, marginBottom: 8 }}>
                Total market value: {portfolioSummary?.total_market_value ?? '—'} | positions: {portfolioSummary?.position_count ?? '—'}
              </div>
              <pre style={{ whiteSpace: 'pre-wrap', padding: 10, borderRadius: 8, maxHeight: 220, overflow: 'auto', border: '1px solid' }}>
                {Object.keys(account).length > 0 ? JSON.stringify(account, null, 2) : '—'}
              </pre>
            </div>

            <div>
              <h4 style={{ marginTop: 0 }}>Positions (stream)</h4>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                  <thead>
                    <tr>
                      {['Symbol', 'Pos', 'AvgCost', 'MktPx', 'MktVal', 'UPnL', 'RPnL'].map((h) => (
                        <th key={h} style={{ textAlign: 'left', borderBottom: '1px solid', padding: '6px 8px' }}>
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {positionsList.map((p) => (
                      <tr key={p.symbol}>
                        <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{p.symbol}</td>
                        <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{p.position ?? '—'}</td>
                        <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{p.average_cost ?? '—'}</td>
                        <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{p.market_price ?? '—'}</td>
                        <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{p.market_value ?? '—'}</td>
                        <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{p.unrealized_pnl ?? '—'}</td>
                        <td style={{ padding: '6px 8px', borderBottom: '1px solid' }}>{p.realized_pnl ?? '—'}</td>
                      </tr>
                    ))}
                    {positionsList.length === 0 ? (
                      <tr>
                        <td colSpan={7} style={{ padding: 10, opacity: 0.7 }}>
                          No positions
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
          )}
        </section>
      </div>
    </div>
  );
}
