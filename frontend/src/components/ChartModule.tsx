/**
 * ChartModule – OHLCV candlestick chart powered by lightweight-charts
 *
 * Combines historical bar data (bootstrap from BFF REST API) with real-time
 * trade tick updates from TickDataServer WebSocket.
 *
 * Features:
 *   - Candlestick chart with timeframe selector (1m → 1M)
 *   - Historical bars bootstrap on symbol/timeframe change
 *   - Real-time current bar updates from trade ticks
 *   - Fallback to line chart when no OHLCV data available
 *   - Symbol tabs with add/remove (shared subscription via store)
 *   - Sync-group symbol selection
 *   - Quote overlay (bid/ask from store)
 *
 * Data Flow:
 *   Bootstrap:  chartDataStore.fetchBars() → BFF /api/chart/bars → candlestick render
 *   Real-time:  tickDataStore trade tick → chartDataStore.updateFromTrade() → update last bar
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  createChart,
  ColorType,
  CandlestickSeries,
  LineSeries,
  CrosshairMode,
  HistogramSeries,
} from 'lightweight-charts';
import type {
  IChartApi,
  ISeriesApi,
  CandlestickData,
  LineData,
  HistogramData,
  Time,
} from 'lightweight-charts';
import { Wifi, WifiOff, BarChart3, TrendingUp, Loader2 } from 'lucide-react';
import type { ModuleProps, ChartTimeframe } from '../types';
import { useTickDataStore, type Trade, type Quote } from '../stores/tickDataStore';
import { useChartDataStore, type OHLCVBar } from '../stores/chartDataStore';

// ── Constants ────────────────────────────────────────────────────────────

const TIMEFRAMES: ChartTimeframe[] = [
  '1m', '5m', '15m', '30m', '1h', '4h', '1D', '1W', '1M',
];

type ChartMode = 'candle' | 'line';

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
  const [chartMode, setChartMode] = useState<ChartMode>('candle');

  // Timeframe from settings, default '5m'
  const timeframe: ChartTimeframe = settings?.chart?.timeframe ?? '5m';

  const symbols = useTickDataStore((s) => s.symbols);
  const connected = useTickDataStore((s) => s.connected);
  const addSymbols = useTickDataStore((s) => s.addSymbols);
  const removeSymbolFromStore = useTickDataStore((s) => s.removeSymbol);
  const symbolData = useTickDataStore((s) => s.symbolData);

  // Chart data store (OHLCV bars)
  const fetchBars = useChartDataStore((s) => s.fetchBars);
  const updateFromTrade = useChartDataStore((s) => s.updateFromTrade);
  const symbolBars = useChartDataStore((s) => s.symbolBars);
  const chartState = symbol ? symbolBars[symbol.toUpperCase()] : undefined;

  // Ref to read latest chartState inside effects without adding it as a dependency
  const chartStateRef = useRef(chartState);
  chartStateRef.current = chartState;

  // Sync from external sync group
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    }
  }, [selectedSymbol]);

  // ── Chart refs ─────────────────────────────────────────────────────────
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  // Series refs — one of these is active at a time
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const lineSeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);

  // Track what's currently rendered to avoid unnecessary teardown
  const currentSymbolRef = useRef<string | null>(null);
  const currentTimeframeRef = useRef<ChartTimeframe | null>(null);
  const currentModeRef = useRef<ChartMode | null>(null);

  // Line chart fallback data (when no OHLCV bars available)
  const lineDataRef = useRef<LineData<Time>[]>([]);

  // ── Initialize lightweight-charts ──────────────────────────────────────
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
        secondsVisible: timeframe === '1m',
      },
      rightPriceScale: { borderColor: '#3f3f46' },
      crosshair: { mode: CrosshairMode.Normal },
    });

    chartRef.current = chart;

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
      candleSeriesRef.current = null;
      lineSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, []);

  // ── Create/switch series based on chart mode ───────────────────────────
  const ensureSeries = useCallback(
    (mode: ChartMode) => {
      const chart = chartRef.current;
      if (!chart) return;

      if (currentModeRef.current === mode) return;

      // Remove old series
      if (candleSeriesRef.current) {
        chart.removeSeries(candleSeriesRef.current);
        candleSeriesRef.current = null;
      }
      if (lineSeriesRef.current) {
        chart.removeSeries(lineSeriesRef.current);
        lineSeriesRef.current = null;
      }
      if (volumeSeriesRef.current) {
        chart.removeSeries(volumeSeriesRef.current);
        volumeSeriesRef.current = null;
      }

      if (mode === 'candle') {
        const candleSeries = chart.addSeries(CandlestickSeries, {
          upColor: '#22c55e',
          downColor: '#ef4444',
          borderDownColor: '#ef4444',
          borderUpColor: '#22c55e',
          wickDownColor: '#ef4444',
          wickUpColor: '#22c55e',
          priceLineVisible: true,
          lastValueVisible: true,
        });
        candleSeriesRef.current = candleSeries;

        // Volume histogram
        const volumeSeries = chart.addSeries(HistogramSeries, {
          priceFormat: { type: 'volume' },
          priceScaleId: 'volume',
        });
        chart.priceScale('volume').applyOptions({
          scaleMargins: { top: 0.8, bottom: 0 },
        });
        volumeSeriesRef.current = volumeSeries;
      } else {
        const lineSeries = chart.addSeries(LineSeries, {
          color: '#2962FF',
          lineWidth: 2,
          priceLineVisible: true,
          lastValueVisible: true,
          crosshairMarkerVisible: true,
          crosshairMarkerRadius: 4,
        });
        lineSeriesRef.current = lineSeries;
      }

      currentModeRef.current = mode;
    },
    [],
  );

  // ── Bootstrap: fetch bars on symbol/timeframe change ───────────────────
  useEffect(() => {
    if (!symbol) return;

    const tickerUpper = symbol.toUpperCase();
    fetchBars(tickerUpper, timeframe);

    // Reset line data for fallback mode
    lineDataRef.current = [];
    currentSymbolRef.current = tickerUpper;
    currentTimeframeRef.current = timeframe;
  }, [symbol, timeframe, fetchBars]);

  // ── Render bars when chartState changes ────────────────────────────────
  useEffect(() => {
    if (!chartRef.current || !symbol) return;
    if (!chartState || chartState.loading) return;

    const hasBars = chartState.bars.length > 0;

    if (hasBars && chartMode === 'candle') {
      ensureSeries('candle');

      // Set candlestick data
      const candleData: CandlestickData<Time>[] = chartState.bars.map((bar) => ({
        time: bar.time as Time,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      }));

      if (candleSeriesRef.current) {
        candleSeriesRef.current.setData(candleData);
      }

      // Set volume data
      if (volumeSeriesRef.current) {
        const volumeData: HistogramData<Time>[] = chartState.bars.map((bar) => ({
          time: bar.time as Time,
          value: bar.volume,
          color: bar.close >= bar.open ? '#22c55e40' : '#ef444440',
        }));
        volumeSeriesRef.current.setData(volumeData);
      }

      // Auto-fit visible range
      chartRef.current.timeScale().fitContent();
    } else if (hasBars && chartMode === 'line') {
      ensureSeries('line');

      const lineData: LineData<Time>[] = chartState.bars.map((bar) => ({
        time: bar.time as Time,
        value: bar.close,
      }));

      if (lineSeriesRef.current) {
        lineSeriesRef.current.setData(lineData);
      }

      chartRef.current.timeScale().fitContent();
    } else if (!hasBars) {
      // No bars — use line mode with live trade fallback
      ensureSeries('line');
    }

    // Update timeScale options based on timeframe
    chartRef.current.timeScale().applyOptions({
      timeVisible: ['1m', '5m', '15m', '30m', '1h', '4h'].includes(timeframe),
      secondsVisible: timeframe === '1m',
    });
  }, [chartState?.lastFetchTime, chartState?.loading, chartMode, symbol, ensureSeries, timeframe]);

  // ── Real-time trade tick → update current bar / line fallback ──────────
  const latestTrade = symbol
    ? (symbolData[symbol]?.T as Trade | undefined) ?? null
    : null;

  useEffect(() => {
    if (!latestTrade || latestTrade.symbol !== symbol) return;
    if (typeof latestTrade.price !== 'number' || typeof latestTrade.timestamp !== 'number') return;

    const cs = chartStateRef.current;
    const hasBars = cs && cs.bars.length > 0;

    if (hasBars) {
      // Update current bar in chartDataStore (mutates store)
      updateFromTrade(symbol, latestTrade.price, latestTrade.size, latestTrade.timestamp);

      // Read freshly updated bars from the store (avoids stale closure)
      const freshState = useChartDataStore.getState().symbolBars[symbol.toUpperCase()];
      if (!freshState || freshState.bars.length === 0) return;
      const lastBar = freshState.bars[freshState.bars.length - 1];

      if (chartMode === 'candle' && candleSeriesRef.current) {
        try {
          candleSeriesRef.current.update({
            time: lastBar.time as Time,
            open: lastBar.open,
            high: lastBar.high,
            low: lastBar.low,
            close: lastBar.close,
          });
        } catch { /* ignore out-of-order */ }

        if (volumeSeriesRef.current) {
          try {
            volumeSeriesRef.current.update({
              time: lastBar.time as Time,
              value: lastBar.volume,
              color: lastBar.close >= lastBar.open ? '#22c55e40' : '#ef444440',
            });
          } catch { /* ignore */ }
        }
      } else if (chartMode === 'line' && lineSeriesRef.current) {
        try {
          lineSeriesRef.current.update({
            time: lastBar.time as Time,
            value: lastBar.close,
          });
        } catch { /* ignore */ }
      }
    } else {
      // No OHLCV bars — fallback to raw trade line chart
      if (!lineSeriesRef.current) return;

      const newTime = Math.floor(latestTrade.timestamp / 1000) as Time;
      const lastPoint = lineDataRef.current[lineDataRef.current.length - 1];

      if (lastPoint && lastPoint.time === newTime && lastPoint.value === latestTrade.price) return;

      if (lastPoint && lastPoint.time === newTime) {
        lastPoint.value = latestTrade.price;
        try { lineSeriesRef.current.update(lastPoint); } catch { /* ignore */ }
        return;
      }

      const newPoint: LineData<Time> = { time: newTime, value: latestTrade.price };
      try {
        lineSeriesRef.current.update(newPoint);
        lineDataRef.current.push(newPoint);
        if (lineDataRef.current.length > 1000) {
          lineDataRef.current = lineDataRef.current.slice(-1000);
          lineSeriesRef.current.setData(lineDataRef.current);
        }
      } catch {
        lineDataRef.current.push(newPoint);
        const deduped = new Map<number, LineData<Time>>();
        for (const pt of lineDataRef.current) deduped.set(pt.time as number, pt);
        lineDataRef.current = Array.from(deduped.values()).sort(
          (a, b) => (a.time as number) - (b.time as number),
        );
        lineSeriesRef.current!.setData(lineDataRef.current);
      }
    }
    // NOTE: chartState intentionally excluded — we read via ref + getState()
    // to avoid infinite re-render loop (updateFromTrade mutates the store).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [latestTrade, symbol, chartMode, updateFromTrade]);

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

  const handleTimeframeChange = (tf: ChartTimeframe) => {
    onSettingsChange?.({ chart: { timeframe: tf } });
  };

  const toggleChartMode = () => {
    setChartMode((prev) => (prev === 'candle' ? 'line' : 'candle'));
  };

  // ── Render ─────────────────────────────────────────────────────────────
  return (
    <div className="h-full flex flex-col bg-zinc-900 relative">
      {/* Header bar */}
      <div className="p-2 border-b border-zinc-800 flex flex-col gap-1">
        {/* Row 1: Connection + Symbol tabs + Add input */}
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

        {/* Row 2: Timeframe selector + Chart mode toggle + Price info */}
        <div className="flex items-center gap-2 flex-wrap">
          {/* Timeframe buttons */}
          <div className="flex items-center gap-0.5">
            {TIMEFRAMES.map((tf) => (
              <button
                key={tf}
                onClick={() => handleTimeframeChange(tf)}
                className={`px-1.5 py-0.5 text-[10px] font-mono transition-colors ${
                  tf === timeframe
                    ? 'bg-blue-600 text-white'
                    : 'bg-zinc-800 hover:bg-zinc-700 text-gray-400'
                }`}
              >
                {tf}
              </button>
            ))}
          </div>

          {/* Chart mode toggle */}
          <button
            onClick={toggleChartMode}
            className="px-1.5 py-0.5 text-[10px] bg-zinc-800 hover:bg-zinc-700 text-gray-400 flex items-center gap-1"
            title={chartMode === 'candle' ? 'Switch to line' : 'Switch to candle'}
          >
            {chartMode === 'candle' ? (
              <BarChart3 className="w-3 h-3" />
            ) : (
              <TrendingUp className="w-3 h-3" />
            )}
          </button>

          {/* Loading indicator */}
          {chartState?.loading && (
            <Loader2 className="w-3 h-3 text-blue-400 animate-spin" />
          )}

          {/* Data source badge */}
          {chartState?.source && !chartState.loading && (
            <span className="text-[9px] text-gray-600 font-mono">
              {chartState.source}
            </span>
          )}

          {/* Price info */}
          {symbol && latestTrade && (
            <div className="flex items-center gap-3 text-xs ml-auto">
              <span className="text-gray-400">
                {symbol} — Last:{' '}
                <span className="text-white">${latestTrade.price.toFixed(2)}</span>
                <span className="text-gray-500 ml-1">({latestTrade.size})</span>
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
      </div>

      {/* Chart area */}
      <div className="flex-1 min-h-0" ref={chartContainerRef} />

      {/* Empty state */}
      {!symbol && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <span className="text-gray-600 text-sm">Add a symbol to see chart data</span>
        </div>
      )}

      {/* Error state */}
      {symbol && chartState?.error && !chartState.loading && chartState.bars.length === 0 && (
        <div className="absolute bottom-2 left-2 right-2 bg-red-900/50 border border-red-800 px-2 py-1 text-xs text-red-300">
          {chartState.error} — showing real-time trades only
        </div>
      )}
    </div>
  );
}
