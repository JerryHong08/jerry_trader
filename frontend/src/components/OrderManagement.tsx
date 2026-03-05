/**
 * OrderManagement – Real IB order placement & order history
 *
 * Connected to the OrderManagement backend (port 8888) via ibbotStore.
 * Features ported from OrderTrader IbbotPanel:
 *  - Place Order form with pct / qty mode
 *  - Price sync from latest trade (TickDataStore)
 *  - Scrollable price input with ±1%/±5% buttons
 *  - Order reason field
 *  - LMT/MKT, TIF, OutsideRTH
 *  - Live order table with cancel (with reason)
 *  - WS connection status indicator
 *  - Buying power display
 */

import React, { useState, useEffect, useRef } from 'react';
import { RefreshCw, Wifi, WifiOff, Eye, EyeOff, X } from 'lucide-react';
import type { ModuleProps, OrderManagementView } from '../types';
import type { PlaceOrderRequest, OrderStatusEventData } from '../types/ibbot';
import { useIbbotStore } from '../stores/ibbotStore';
import { useTickDataStore, type Trade } from '../stores/tickDataStore';
import { usePrivacyStore } from '../stores/privacyStore';

// ============================================================================
// Helpers
// ============================================================================

function canCancelOrder(status: string): boolean {
  const s = (status || '').toLowerCase();
  if (['filled', 'cancelled', 'apicancelled', 'inactive', 'error', 'pendingcancel'].includes(s)) return false;
  return true;
}

// ============================================================================
// PriceInput — scrollable with ±1%/±5% quick-adjust
// ============================================================================

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
  const currentPrice = value ?? 0;

  // Attach a native wheel listener with { passive: false } so we can
  // preventDefault + stopPropagation without React's synthetic-event
  // limitations. This prevents the parent grid from scrolling while
  // the user adjusts the price.
  useEffect(() => {
    const el = inputRef.current;
    if (!el || disabled) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      e.stopPropagation();
      const step = (value ?? 0) * 0.001;
      const delta = e.deltaY < 0 ? step : -step;
      onChange(Math.max(0.01, Math.round(((value ?? 0) + delta) * 100) / 100));
    };
    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  });

  const adjustByPercent = (percent: number) => {
    if (disabled) return;
    onChange(Math.max(0.01, Math.round(currentPrice * (1 + percent / 100) * 100) / 100));
  };

  return (
    <div className="flex gap-1 items-center">
      {[-5, -1].map((p) => (
        <button
          key={p}
          type="button"
          disabled={disabled}
          onClick={() => adjustByPercent(p)}
          className="px-1.5 py-0.5 text-[10px] bg-zinc-800 hover:bg-zinc-700 disabled:opacity-40"
        >
          {p}%
        </button>
      ))}
      <input
        ref={inputRef}
        className="flex-1 bg-black border border-zinc-700 px-2 py-1 text-sm focus:outline-none focus:border-zinc-500 cursor-ns-resize"
        type="number"
        step="0.01"
        title="Scroll to adjust price"
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value === '' ? null : Number(e.target.value))}
        disabled={disabled}
      />
      {[1, 5].map((p) => (
        <button
          key={p}
          type="button"
          disabled={disabled}
          onClick={() => adjustByPercent(p)}
          className="px-1.5 py-0.5 text-[10px] bg-zinc-800 hover:bg-zinc-700 disabled:opacity-40"
        >
          +{p}%
        </button>
      ))}
    </div>
  );
}

// ============================================================================
// Main Component
// ============================================================================

export function OrderManagement({
  onRemove,
  selectedSymbol,
  settings,
  onSettingsChange,
}: ModuleProps) {
  // ── Store ──────────────────────────────────────────────────────────────
  const wsStatus = useIbbotStore((s) => s.wsStatus);
  const wsInfo = useIbbotStore((s) => s.wsInfo);
  const loading = useIbbotStore((s) => s.loading);
  const error = useIbbotStore((s) => s.error);
  const ordersById = useIbbotStore((s) => s.ordersById);
  const buyingPower = useIbbotStore((s) => s.buyingPower);
  const submitOrder = useIbbotStore((s) => s.submitOrder);
  const doCancelOrder = useIbbotStore((s) => s.doCancelOrder);
  const loadInitial = useIbbotStore((s) => s.loadInitial);
  const restBaseUrl = useIbbotStore((s) => s.restBaseUrl);

  // TickData for price sync
  const symbolData = useTickDataStore((s) => s.symbolData);

  // Privacy
  const privacyMode = usePrivacyStore((s) => s.privacyMode);
  const togglePrivacy = usePrivacyStore((s) => s.toggle);

  // ── Local form state ───────────────────────────────────────────────────
  const [activeView, setActiveView] = useState<OrderManagementView>(
    settings?.orderManagement?.view || 'placement',
  );

  const [orderMode, setOrderMode] = useState<'quantity' | 'pct'>('pct');
  const [cancelReason, setCancelReason] = useState('');
  const [cancellingIds, setCancellingIds] = useState<Set<number>>(new Set());
  const [showBuyingPower, setShowBuyingPower] = useState(false);

  const [form, setForm] = useState<PlaceOrderRequest>({
    symbol: selectedSymbol || 'AAPL',
    action: 'BUY',
    quantity: 100,
    order_type: 'LMT',
    limit_price: null,
    tif: 'DAY',
    OutsideRth: true,
    sec_type: 'STK',
    reason: '',
    pct: 60,
    price: null,
  });

  const priceInitializedRef = useRef(false);

  // ── Sync symbol from external selection ────────────────────────────────
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== form.symbol) {
      setForm((p) => ({ ...p, symbol: selectedSymbol }));
      priceInitializedRef.current = false;
    }
  }, [selectedSymbol]);

  // ── Sync view from settings ────────────────────────────────────────────
  useEffect(() => {
    if (settings?.orderManagement?.view) {
      setActiveView(settings.orderManagement.view);
    }
  }, [settings?.orderManagement?.view]);

  // ── Price from latest trade ────────────────────────────────────────────
  const latestTrade = form.symbol
    ? (symbolData[form.symbol]?.T as Trade | undefined) ?? null
    : null;
  const lastTradePrice = latestTrade?.price ?? null;

  // Auto-sync price on first load
  useEffect(() => {
    if (lastTradePrice && !priceInitializedRef.current && form.order_type === 'LMT') {
      const rounded = Math.round(lastTradePrice * 100) / 100;
      setForm((p) => ({ ...p, limit_price: rounded, price: rounded }));
      priceInitializedRef.current = true;
    }
  }, [lastTradePrice, form.order_type]);

  const syncPriceFromTrade = () => {
    if (lastTradePrice) {
      const rounded = Math.round(lastTradePrice * 100) / 100;
      setForm((p) => ({ ...p, limit_price: rounded, price: rounded }));
    }
  };

  // ── Handlers ───────────────────────────────────────────────────────────
  const handleViewChange = (view: OrderManagementView) => {
    setActiveView(view);
    onSettingsChange?.({ orderManagement: { view } });
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    await submitOrder(form, orderMode);
  };

  const handleCancel = async (orderId: number) => {
    setCancellingIds((prev) => new Set(prev).add(orderId));
    try {
      await doCancelOrder(orderId, cancelReason);
    } finally {
      setCancellingIds((prev) => {
        const next = new Set(prev);
        next.delete(orderId);
        return next;
      });
    }
  };

  // ── Derived ────────────────────────────────────────────────────────────
  const orders = Object.values(ordersById).sort((a, b) => b.order_id - a.order_id);
  const bp = buyingPower();
  const wsConnected = wsStatus === 'connected';

  // ── Render ─────────────────────────────────────────────────────────────
  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Connection header */}
      <div className="px-3 py-1.5 border-b border-zinc-800 flex items-center justify-between text-xs text-gray-400">
        <div className="flex items-center gap-2">
          {wsConnected ? (
            <Wifi className="w-3 h-3 text-green-500" />
          ) : (
            <WifiOff className="w-3 h-3 text-red-500" />
          )}
          <span>{restBaseUrl}</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={togglePrivacy}
            className="p-0.5 hover:bg-zinc-700 rounded transition-colors"
            title={privacyMode ? 'Show numbers' : 'Hide numbers'}
          >
            {privacyMode ? <EyeOff className="w-3.5 h-3.5 text-yellow-500" /> : <Eye className="w-3.5 h-3.5" />}
          </button>
          <span>
            {wsStatus === 'connected' ? 'Connected' : wsStatus === 'error' ? 'Error' : 'Disconnected'}
            {wsInfo ? ` (${wsInfo})` : ''}
          </span>
        </div>
      </div>

      {/* Error banner */}
      {error && (
        <div className="px-3 py-1.5 bg-red-900/30 border-b border-red-800 text-xs text-red-300">
          {error}
        </div>
      )}

      {/* Content */}
      <div className="flex-1 overflow-auto p-3">
        {activeView === 'placement' && (
          <form onSubmit={handleSubmit} className="space-y-3">
            {/* Buying power */}
            {bp !== null && (
              <div className="text-xs text-gray-400 flex items-center gap-2">
                Buying Power:{' '}
                <span className="cursor-pointer" onClick={() => setShowBuyingPower((v) => !v)}>
                  {privacyMode
                    ? '••••'
                    : showBuyingPower
                      ? `$${bp.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
                      : '****'}
                </span>
              </div>
            )}

            {/* Row 1: Symbol + Action */}
            <div className="grid grid-cols-3 gap-2">
              <div className="col-span-2">
                <label className="block text-xs text-gray-400 mb-0.5">Symbol</label>
                <input
                  className="w-full bg-black border border-zinc-700 px-2 py-1.5 text-sm focus:outline-none focus:border-white"
                  value={form.symbol}
                  onChange={(e) => setForm((p) => ({ ...p, symbol: e.target.value }))}
                />
              </div>
              <div>
                <label className="block text-xs text-gray-400 mb-0.5">Action</label>
                <div className="grid grid-cols-2 gap-1">
                  <button
                    type="button"
                    onClick={() => { setOrderMode('pct'); setForm((p) => ({ ...p, action: 'BUY', pct: 60 })); }}
                    className={`py-1.5 text-xs ${form.action === 'BUY' ? 'bg-green-600' : 'bg-zinc-800 hover:bg-zinc-700 text-gray-400'}`}
                  >
                    BUY
                  </button>
                  <button
                    type="button"
                    onClick={() => { setOrderMode('pct'); setForm((p) => ({ ...p, action: 'SELL', pct: 100 })); }}
                    className={`py-1.5 text-xs ${form.action === 'SELL' ? 'bg-red-600' : 'bg-zinc-800 hover:bg-zinc-700 text-gray-400'}`}
                  >
                    SELL
                  </button>
                </div>
              </div>
            </div>

            {/* Row 2: Mode + Qty/Pct + Type + TIF */}
            <div className="grid grid-cols-4 gap-2">
              <div>
                <label className="block text-xs text-gray-400 mb-0.5">Mode</label>
                <select
                  className="w-full bg-black border border-zinc-700 px-2 py-1.5 text-sm focus:outline-none"
                  value={orderMode}
                  onChange={(e) => setOrderMode(e.target.value as 'quantity' | 'pct')}
                >
                  <option value="quantity">Qty</option>
                  <option value="pct">Pct %</option>
                </select>
              </div>

              <div>
                <label className="block text-xs text-gray-400 mb-0.5">
                  {orderMode === 'quantity' ? 'Qty' : 'Pct %'}
                </label>
                {orderMode === 'quantity' ? (
                  <input
                    className="w-full bg-black border border-zinc-700 px-2 py-1.5 text-sm focus:outline-none"
                    type={privacyMode ? 'password' : 'number'}
                    min={1}
                    value={form.quantity ?? ''}
                    onChange={(e) => setForm((p) => ({ ...p, quantity: e.target.value === '' ? null : Number(e.target.value) }))}
                  />
                ) : (
                  <input
                    className="w-full bg-black border border-zinc-700 px-2 py-1.5 text-sm focus:outline-none"
                    type="number"
                    min={1}
                    max={100}
                    step={1}
                    value={form.pct ?? ''}
                    onChange={(e) => setForm((p) => ({ ...p, pct: e.target.value === '' ? null : Number(e.target.value) }))}
                    placeholder="1-100"
                  />
                )}
              </div>

              <div>
                <label className="block text-xs text-gray-400 mb-0.5">Type</label>
                <select
                  className="w-full bg-black border border-zinc-700 px-2 py-1.5 text-sm focus:outline-none"
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
              </div>

              <div>
                <label className="block text-xs text-gray-400 mb-0.5">TIF</label>
                <select
                  className="w-full bg-black border border-zinc-700 px-2 py-1.5 text-sm focus:outline-none"
                  value={form.tif}
                  onChange={(e) => setForm((p) => ({ ...p, tif: e.target.value }))}
                >
                  <option value="DAY">DAY</option>
                  <option value="GTC">GTC</option>
                </select>
              </div>
            </div>

            {/* Limit price (only for LMT) */}
            {form.order_type === 'LMT' && (
              <div>
                <div className="flex items-center gap-2 mb-1">
                  <label className="text-xs text-gray-400">Limit Price</label>
                  <button
                    type="button"
                    disabled={loading || !lastTradePrice}
                    onClick={syncPriceFromTrade}
                    className="px-2 py-0.5 text-[10px] bg-zinc-800 hover:bg-zinc-700 disabled:opacity-40"
                  >
                    ↻ Sync
                  </button>
                  {lastTradePrice && (
                    <span className="text-[10px] text-gray-500">
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
            )}

            {/* Outside RTH */}
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={Boolean(form.OutsideRth)}
                onChange={(e) => setForm((p) => ({ ...p, OutsideRth: e.target.checked }))}
                className="w-3.5 h-3.5"
              />
              <span className="text-xs text-gray-400">Outside Regular Trading Hours</span>
            </label>

            {/* Reason */}
            <div>
              <label className="block text-xs text-gray-400 mb-0.5">Reason (optional)</label>
              <input
                className="w-full bg-black border border-zinc-700 px-2 py-1.5 text-sm focus:outline-none focus:border-zinc-500"
                value={String(form.reason ?? '')}
                onChange={(e) => setForm((p) => ({ ...p, reason: e.target.value }))}
                placeholder="e.g. Rebalance / manual test / strategy-xyz"
              />
            </div>

            {/* Submit */}
            <div className="flex gap-2">
              <button
                type="submit"
                disabled={loading}
                className={`flex-1 py-2 text-sm transition-colors ${
                  form.action === 'BUY'
                    ? 'bg-green-600 hover:bg-green-700'
                    : 'bg-red-600 hover:bg-red-700'
                } disabled:opacity-50`}
              >
                {loading ? 'Working...' : `Place ${form.action} Order`}
              </button>
              <button
                type="button"
                disabled={loading}
                onClick={() => loadInitial()}
                className="px-3 py-2 text-sm bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50"
              >
                <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              </button>
            </div>
          </form>
        )}

        {activeView === 'orders' && (
          <div>
            {/* Cancel reason input */}
            <div className="mb-3">
              <label className="block text-xs text-gray-400 mb-0.5">Cancel reason (optional)</label>
              <input
                className="w-full bg-black border border-zinc-700 px-2 py-1.5 text-sm focus:outline-none focus:border-zinc-500"
                value={cancelReason}
                onChange={(e) => setCancelReason(e.target.value)}
                placeholder="Used when clicking Cancel"
              />
            </div>

            {/* Refresh button */}
            <div className="flex justify-between items-center mb-2">
              <span className="text-xs text-gray-500">Orders: {orders.length}</span>
              <button
                onClick={() => loadInitial()}
                disabled={loading}
                className="flex items-center gap-1 px-3 py-1 text-xs bg-blue-600 hover:bg-blue-700 disabled:opacity-50"
              >
                <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
                Refresh
              </button>
            </div>

            {/* Orders table */}
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-zinc-800 border-b border-zinc-700">
                  <tr>
                    {['ID', 'Symbol', 'Action', 'Type', 'LmtPx', 'Status', 'Filled', 'Rem', 'AvgPx', 'Comm', ''].map(
                      (h) => (
                        <th key={h} className="text-left p-1.5 text-gray-400 whitespace-nowrap">
                          {h}
                        </th>
                      ),
                    )}
                  </tr>
                </thead>
                <tbody>
                  {orders.map((o) => {
                    const cancelable = canCancelOrder(o.status);
                    return (
                      <tr key={o.order_id} className="border-b border-zinc-800 hover:bg-zinc-800/50">
                        <td className="p-1.5">{o.order_id}</td>
                        <td className="p-1.5">{o.symbol ?? '—'}</td>
                        <td className="p-1.5">
                          <span className={o.action === 'BUY' ? 'text-green-500' : 'text-red-500'}>
                            {o.action ?? '—'}
                          </span>
                        </td>
                        <td className="p-1.5 text-gray-400">{o.order_type ?? '—'}</td>
                        <td className="p-1.5">
                          {privacyMode
                            ? '••••'
                            : o.order_type === 'LMT' && o.limit_price != null
                              ? `$${Number(o.limit_price).toFixed(2)}`
                              : '—'}
                        </td>
                        <td className="p-1.5">{o.status}</td>
                        <td className="p-1.5">{privacyMode ? '••••' : o.filled}</td>
                        <td className="p-1.5">{privacyMode ? '••••' : o.remaining}</td>
                        <td className="p-1.5">{privacyMode ? '••••' : (o.avg_fill_price ?? '—')}</td>
                        <td className="p-1.5">{privacyMode ? '••••' : (o.commission ?? '—')}</td>
                        <td className="p-1.5">
                          {(() => {
                            const isCancelling = cancellingIds.has(o.order_id);
                            const isPending = o.status?.toLowerCase() === 'pendingcancel';
                            if (isCancelling || isPending) {
                              return (
                                <span className="inline-flex items-center gap-1 px-2.5 py-1 text-xs text-gray-500">
                                  <RefreshCw className="w-3 h-3 animate-spin" />
                                  Cancelling…
                                </span>
                              );
                            }
                            if (!cancelable) return null;
                            return (
                              <button
                                onClick={() => handleCancel(o.order_id)}
                                className="flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded
                                  bg-red-600 hover:bg-red-500 active:bg-red-700
                                  border-none outline-none focus:outline-none
                                  text-white shadow-sm shadow-red-900/50
                                  transition-all duration-100"
                              >
                                <X className="w-3 h-3" />
                                Cancel
                              </button>
                            );
                          })()}
                        </td>
                      </tr>
                    );
                  })}
                  {orders.length === 0 && (
                    <tr>
                      <td colSpan={11} className="text-center text-gray-500 py-6">
                        No orders yet
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
