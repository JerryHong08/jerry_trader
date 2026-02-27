/**
 * ChartModule – Real-time line chart powered by lightweight-charts
 *
 * Consumes live trade data from the TickDataServer WebSocket via
 * useTickDataStore.  Replaces the previous recharts mock implementation.
 *
 * Features:
 *  - Symbol tabs with add / remove (shared subscription via store)
 *  - Sync-group symbol selection
 *  - Real-time trade line chart (same approach as OrderTrader ChartPanel)
 *  - Quote overlay (bid / ask from store)
 */

import React, { useState, useEffect, useRef } from 'react';
import {
  createChart,
  ColorType,
  LineSeries,
  CrosshairMode,
} from 'lightweight-charts';
import type {
  IChartApi,
  ISeriesApi,
  LineData,
  Time,
} from 'lightweight-charts';
import { Wifi, WifiOff } from 'lucide-react';
import type { ModuleProps } from '../types';
import { useTickDataStore, type Trade, type Quote } from '../stores/tickDataStore';

export function ChartModule({
  onRemove,
  selectedSymbol,
  onSymbolSelect,
  settings,
  onSettingsChange,
}: ModuleProps) {
  // ── Symbol & subscription ──────────────────────────────────────────────
  const [symbol, setSymbol] = useState(selectedSymbol || '');
  const [input, setInput] = useState('');

  const symbols = useTickDataStore((s) => s.symbols);
  const connected = useTickDataStore((s) => s.connected);
  const addSymbols = useTickDataStore((s) => s.addSymbols);
  const removeSymbolFromStore = useTickDataStore((s) => s.removeSymbol);
  const symbolData = useTickDataStore((s) => s.symbolData);

  // Sync from external sync group
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    }
  }, [selectedSymbol]);

  // ── Chart refs ─────────────────────────────────────────────────────────
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const dataRef = useRef<LineData<Time>[]>([]);
  const currentSymbolRef = useRef<string | null>(null);

  // Initialise lightweight-charts once
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      layout: {
        background: { type: ColorType.Solid, color: '#18181b' },
        textColor: '#a1a1aa',
      },
      grid: {
        vertLines: { color: '#27272a' },
        horzLines: { color: '#27272a' },
      },
      timeScale: {
        borderColor: '#3f3f46',
        timeVisible: true,
        secondsVisible: true,
      },
      rightPriceScale: { borderColor: '#3f3f46' },
      crosshair: { mode: CrosshairMode.Normal },
    });

    chartRef.current = chart;

    const series = chart.addSeries(LineSeries, {
      color: '#2962FF',
      lineWidth: 2,
      priceLineVisible: true,
      lastValueVisible: true,
      crosshairMarkerVisible: true,
      crosshairMarkerRadius: 4,
    });
    seriesRef.current = series;

    const ro = new ResizeObserver(() => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    });
    ro.observe(chartContainerRef.current);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      dataRef.current = [];
    };
  }, []);

  // Reset chart data when symbol changes
  useEffect(() => {
    if (!seriesRef.current) return;
    if (symbol !== currentSymbolRef.current) {
      dataRef.current = [];
      seriesRef.current.setData([]);
      currentSymbolRef.current = symbol;
    }
  }, [symbol]);

  // Consume latest trade from store
  const latestTrade = symbol
    ? (symbolData[symbol]?.T as Trade | undefined) ?? null
    : null;

  useEffect(() => {
    if (!seriesRef.current || !latestTrade || latestTrade.symbol !== symbol) return;
    if (typeof latestTrade.price !== 'number' || typeof latestTrade.timestamp !== 'number') return;

    const newTime = Math.floor(latestTrade.timestamp / 1000) as Time;
    const lastPoint = dataRef.current[dataRef.current.length - 1];

    // Skip exact duplicate
    if (lastPoint && lastPoint.time === newTime && lastPoint.value === latestTrade.price) return;

    // Same second, different price → update in place
    if (lastPoint && lastPoint.time === newTime) {
      lastPoint.value = latestTrade.price;
      try { seriesRef.current.update(lastPoint); } catch { /* ignore */ }
      return;
    }

    const newPoint: LineData<Time> = { time: newTime, value: latestTrade.price };

    try {
      seriesRef.current.update(newPoint);
      dataRef.current.push(newPoint);

      if (dataRef.current.length > 1000) {
        dataRef.current = dataRef.current.slice(-1000);
        seriesRef.current.setData(dataRef.current);
      }
    } catch {
      // Out-of-order data — deduplicate and rebuild
      dataRef.current.push(newPoint);
      const deduped = new Map<number, LineData<Time>>();
      for (const pt of dataRef.current) deduped.set(pt.time as number, pt);
      dataRef.current = Array.from(deduped.values()).sort(
        (a, b) => (a.time as number) - (b.time as number),
      );
      seriesRef.current!.setData(dataRef.current);
    }
  }, [latestTrade, symbol]);

  // ── Quote data ─────────────────────────────────────────────────────────
  const quote = symbol ? (symbolData[symbol]?.Q as Quote | undefined) : undefined;

  // ── Handlers ───────────────────────────────────────────────────────────
  const handleAdd = () => {
    const newSyms = input
      .split(',')
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
    if (newSyms.length > 0) {
      addSymbols(newSyms, ['Q', 'T']);
      const last = newSyms[newSyms.length - 1];
      setSymbol(last);
      onSymbolSelect?.(last);
    }
    setInput('');
  };

  const handleRemove = (sym: string) => {
    removeSymbolFromStore(sym);
    if (sym === symbol) {
      const remaining = symbols.filter((s) => s !== sym);
      const next = remaining.length > 0 ? remaining[remaining.length - 1] : '';
      setSymbol(next);
      if (next) onSymbolSelect?.(next);
    }
  };

  const handleSelectSymbol = (sym: string) => {
    setSymbol(sym);
    onSymbolSelect?.(sym);
  };

  // ── Render ─────────────────────────────────────────────────────────────
  return (
    <div className="h-full flex flex-col bg-zinc-900 relative">
      {/* Header bar */}
      <div className="p-2 border-b border-zinc-800 flex flex-col gap-1">
        {/* Connection + Symbol tabs */}
        <div className="flex items-center gap-2 flex-wrap">
          {connected ? (
            <Wifi className="w-3.5 h-3.5 text-green-500 flex-shrink-0" />
          ) : (
            <WifiOff className="w-3.5 h-3.5 text-red-500 flex-shrink-0" />
          )}

          {symbols.map((s) => (
            <button
              key={s}
              onClick={() => handleSelectSymbol(s)}
              className={`px-2 py-0.5 text-xs transition-colors ${
                s === symbol
                  ? 'bg-white text-black'
                  : 'bg-zinc-800 hover:bg-zinc-700 text-gray-300'
              }`}
            >
              {s}
              <span
                className="ml-1 text-red-400 hover:text-red-300 cursor-pointer"
                onClick={(e) => { e.stopPropagation(); handleRemove(s); }}
              >
                ✕
              </span>
            </button>
          ))}

          <input
            className="bg-black border border-zinc-700 px-2 py-0.5 text-xs w-28 focus:outline-none focus:border-zinc-500"
            placeholder="AAPL, NVDA"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
          />
          <button
            onClick={handleAdd}
            className="px-2 py-0.5 text-xs bg-blue-600 hover:bg-blue-700 transition-colors"
          >
            Add
          </button>
        </div>

        {/* Price info */}
        {symbol && latestTrade && (
          <div className="flex items-center gap-3 text-xs">
            <span className="text-gray-400">
              {symbol} — Last:{' '}
              <span className="text-white">${latestTrade.price.toFixed(2)}</span>
              <span className="text-gray-500 ml-1">({latestTrade.size} shares)</span>
            </span>
            {quote && (
              <span className="text-gray-500">
                Bid: <span className="text-green-400">{quote.bid}</span>
                {' '}Ask: <span className="text-red-400">{quote.ask}</span>
              </span>
            )}
          </div>
        )}
      </div>

      {/* Chart area */}
      <div className="flex-1 min-h-0" ref={chartContainerRef} />

      {!symbol && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <span className="text-gray-600 text-sm">Add a symbol to see real-time trades</span>
        </div>
      )}
    </div>
  );
}
