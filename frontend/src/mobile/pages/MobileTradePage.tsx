import { useState, useEffect, useMemo } from 'react';
import { Send, Minus, Plus, ChevronDown, ChevronUp } from 'lucide-react';
import { useIbbotStore } from '../../stores/ibbotStore';
import { useTickDataStore } from '../../stores/tickDataStore';
import { usePrivacyStore } from '../../stores/privacyStore';
import type { Trade } from '../../stores/tickDataStore';

interface MobileTradePageProps {
  symbol: string | null;
}

export function MobileTradePage({ symbol }: MobileTradePageProps) {
  const loading = useIbbotStore((s) => s.loading);
  const error = useIbbotStore((s) => s.error);
  // Subscribe to raw data, NOT the method — ordersList() returns new array each call
  const ordersById = useIbbotStore((s) => s.ordersById);
  const orders = useMemo(
    () => Object.values(ordersById).sort((a, b) => b.order_id - a.order_id),
    [ordersById],
  );
  const submitOrder = useIbbotStore((s) => s.submitOrder);
  const doCancelOrder = useIbbotStore((s) => s.doCancelOrder);
  const mask = usePrivacyStore((s) => s.mask);

  const [formSymbol, setFormSymbol] = useState(symbol || '');
  const [side, setSide] = useState<'BUY' | 'SELL'>('BUY');
  const [orderType, setOrderType] = useState<'MKT' | 'LMT'>('MKT');
  const [orderMode, setOrderMode] = useState<'quantity' | 'pct'>('quantity');
  const [quantity, setQuantity] = useState('');
  const [pct, setPct] = useState('');
  const [limitPrice, setLimitPrice] = useState('');
  const [tif, setTif] = useState<'DAY' | 'GTC'>('DAY');
  const [outsideRth, setOutsideRth] = useState(false);
  const [reason, setReason] = useState('');
  const [showOrders, setShowOrders] = useState(false);

  useEffect(() => {
    if (symbol) setFormSymbol(symbol);
  }, [symbol]);

  const latestTrade = useTickDataStore((s) =>
    formSymbol ? (s.symbolData[formSymbol]?.T as Trade | undefined) ?? null : null
  );
  const tradePrice = latestTrade?.price ?? null;

  const syncPrice = () => {
    if (tradePrice !== null && orderType === 'LMT') {
      setLimitPrice(tradePrice.toFixed(2));
    }
  };

  const adjustPrice = (delta: number) => {
    const current = parseFloat(limitPrice) || tradePrice || 0;
    const step = current * 0.001;
    setLimitPrice(Math.max(0.01, current + step * delta).toFixed(2));
  };

  const handleSubmit = () => {
    if (!formSymbol.trim()) return;
    submitOrder(
      {
        symbol: formSymbol.trim().toUpperCase(),
        action: side,
        order_type: orderType,
        quantity: orderMode === 'quantity' ? Number(quantity) : 0,
        pct: orderMode === 'pct' ? Number(pct) : undefined,
        limit_price: orderType === 'LMT' ? Number(limitPrice) : undefined,
        price: tradePrice ?? undefined,
        reason: reason.trim() || undefined,
        OutsideRth: outsideRth,
        tif,
      },
      orderMode
    );
  };

  const activeOrders = orders.filter(
    (o) => !['Cancelled', 'Filled', 'Inactive'].includes(o.status ?? '')
  );

  return (
    <div className="h-full flex flex-col">
      <div className="flex-1 overflow-y-auto overscroll-contain px-3 py-3 space-y-4">
        {/* Symbol */}
        <div>
          <label className="text-xs text-zinc-500 mb-1 block">Symbol</label>
          <input
            type="text"
            value={formSymbol}
            onChange={(e) => setFormSymbol(e.target.value.toUpperCase())}
            placeholder="AAPL"
            className="w-full px-3 py-2 text-sm bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:border-zinc-500"
          />
          {tradePrice !== null && (
            <div className="text-2xs text-zinc-500 mt-1">
              Last: ${tradePrice.toFixed(2)}
            </div>
          )}
        </div>

        {/* Buy/Sell */}
        <div className="grid grid-cols-2 gap-2">
          <button
            onClick={() => setSide('BUY')}
            className={`py-2.5 text-sm font-semibold rounded transition-colors ${
              side === 'BUY' ? 'bg-emerald-600 text-white' : 'bg-zinc-800 text-zinc-400'
            }`}
          >
            BUY
          </button>
          <button
            onClick={() => setSide('SELL')}
            className={`py-2.5 text-sm font-semibold rounded transition-colors ${
              side === 'SELL' ? 'bg-red-600 text-white' : 'bg-zinc-800 text-zinc-400'
            }`}
          >
            SELL
          </button>
        </div>

        {/* Order Type */}
        <div className="grid grid-cols-2 gap-2">
          <button
            onClick={() => setOrderType('MKT')}
            className={`py-2 text-sm rounded ${orderType === 'MKT' ? 'bg-white text-black' : 'bg-zinc-800'}`}
          >
            Market
          </button>
          <button
            onClick={() => setOrderType('LMT')}
            className={`py-2 text-sm rounded ${orderType === 'LMT' ? 'bg-white text-black' : 'bg-zinc-800'}`}
          >
            Limit
          </button>
        </div>

        {/* TIF */}
        <div className="grid grid-cols-2 gap-2">
          <button
            onClick={() => setTif('DAY')}
            className={`py-2 text-sm rounded ${tif === 'DAY' ? 'bg-white text-black' : 'bg-zinc-800'}`}
          >
            DAY
          </button>
          <button
            onClick={() => setTif('GTC')}
            className={`py-2 text-sm rounded ${tif === 'GTC' ? 'bg-white text-black' : 'bg-zinc-800'}`}
          >
            GTC
          </button>
        </div>

        {/* Mode: Qty / Pct */}
        <div className="grid grid-cols-2 gap-2">
          <button
            onClick={() => setOrderMode('quantity')}
            className={`py-2 text-sm rounded ${orderMode === 'quantity' ? 'bg-white text-black' : 'bg-zinc-800'}`}
          >
            Quantity
          </button>
          <button
            onClick={() => setOrderMode('pct')}
            className={`py-2 text-sm rounded ${orderMode === 'pct' ? 'bg-white text-black' : 'bg-zinc-800'}`}
          >
            Pct %
          </button>
        </div>

        {/* Qty or Pct input */}
        {orderMode === 'quantity' ? (
          <div>
            <label className="text-xs text-zinc-500 mb-1 block">Quantity</label>
            <input
              type="number"
              value={quantity}
              onChange={(e) => setQuantity(e.target.value)}
              placeholder="100"
              className="w-full px-3 py-2 text-sm bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:border-zinc-500"
            />
          </div>
        ) : (
          <div>
            <label className="text-xs text-zinc-500 mb-1 block">Percentage (%)</label>
            <input
              type="number"
              value={pct}
              onChange={(e) => setPct(e.target.value)}
              placeholder="50"
              min="1"
              max="100"
              className="w-full px-3 py-2 text-sm bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:border-zinc-500"
            />
          </div>
        )}

        {/* Limit price */}
        {orderType === 'LMT' && (
          <div>
            <label className="text-xs text-zinc-500 mb-1 block">Limit Price</label>
            <div className="flex items-center gap-2">
              <button
                onClick={() => adjustPrice(-1)}
                className="p-2 bg-zinc-800 rounded hover:bg-zinc-700 min-h-[44px] min-w-[44px] flex items-center justify-center"
              >
                <Minus className="w-4 h-4" />
              </button>
              <input
                type="number"
                value={limitPrice}
                onChange={(e) => setLimitPrice(e.target.value)}
                placeholder="0.00"
                step="0.01"
                className="flex-1 px-3 py-2 text-sm text-center bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:border-zinc-500"
              />
              <button
                onClick={() => adjustPrice(1)}
                className="p-2 bg-zinc-800 rounded hover:bg-zinc-700 min-h-[44px] min-w-[44px] flex items-center justify-center"
              >
                <Plus className="w-4 h-4" />
              </button>
            </div>
            <button
              onClick={syncPrice}
              disabled={tradePrice === null}
              className="text-xs text-blue-400 mt-1 disabled:opacity-50"
            >
              Sync last price
            </button>
          </div>
        )}

        {/* Outside RTH */}
        <label className="flex items-center gap-2 text-sm text-zinc-400">
          <input
            type="checkbox"
            checked={outsideRth}
            onChange={(e) => setOutsideRth(e.target.checked)}
            className="w-4 h-4"
          />
          Outside RTH
        </label>

        {/* Reason */}
        <div>
          <label className="text-xs text-zinc-500 mb-1 block">Reason (optional)</label>
          <input
            type="text"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="..."
            className="w-full px-3 py-2 text-sm bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:border-zinc-500"
          />
        </div>

        {/* Error */}
        {error && (
          <div className="text-xs text-red-400 bg-red-900/20 px-3 py-2 rounded">{error}</div>
        )}

        {/* Submit */}
        <button
          onClick={handleSubmit}
          disabled={loading || !formSymbol.trim()}
          className={`w-full py-3 text-sm font-semibold rounded transition-colors disabled:opacity-50 ${
            side === 'BUY' ? 'bg-emerald-600 text-white' : 'bg-red-600 text-white'
          }`}
        >
          <div className="flex items-center justify-center gap-2">
            <Send className="w-4 h-4" />
            {side === 'BUY' ? 'BUY' : 'SELL'} {formSymbol || '...'}
          </div>
        </button>

        {/* Orders section */}
        <div className="border-t border-zinc-800 pt-2">
          <button
            onClick={() => setShowOrders(!showOrders)}
            className="w-full flex items-center justify-between py-2 text-sm text-zinc-400 hover:text-white transition-colors"
          >
            <span>Orders ({activeOrders.length} active / {orders.length} total)</span>
            {showOrders ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          </button>

          {showOrders && (
            <div className="space-y-2 mt-2">
              {orders.length === 0 ? (
                <div className="text-center text-zinc-500 py-4 text-sm">No orders</div>
              ) : (
                orders.slice(0, 20).map((order) => (
                  <div key={order.order_id} className="bg-zinc-900 rounded p-2 border border-zinc-800 text-xs">
                    <div className="flex items-center justify-between mb-1">
                      <span className="font-semibold">{order.symbol ?? '—'}</span>
                      <span className={`px-1.5 py-0.5 rounded text-3xs ${
                        order.action === 'BUY' ? 'bg-emerald-900/50 text-emerald-400' : 'bg-red-900/50 text-red-400'
                      }`}>
                        {order.action}
                      </span>
                    </div>
                    <div className="grid grid-cols-3 gap-1 text-zinc-500">
                      <div>Status: <span className="text-zinc-300">{order.status}</span></div>
                      <div>
                        Qty: <span className="text-zinc-300 tabular-nums">
                          {mask(order.filled_quantity != null ? `${order.filled_quantity}/${order.quantity}` : order.quantity)}
                        </span>
                      </div>
                      <div>Type: <span className="text-zinc-300">{order.order_type}</span></div>
                    </div>
                    {order.status === 'Submitted' && (
                      <button
                        onClick={() => doCancelOrder(order.order_id, 'User cancelled')}
                        className="mt-1 text-xs text-red-400 hover:text-red-300"
                      >
                        Cancel
                      </button>
                    )}
                  </div>
                ))
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
