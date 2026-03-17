/**
 * ChartModule – OHLCV candlestick chart powered by lightweight-charts
 *
 * Combines historical bar data (bootstrap from BFF REST API) with real-time
 * trade tick updates from ChartBFF WebSocket.
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
import { Wifi, WifiOff, BarChart3, TrendingUp, Loader2, RefreshCw } from 'lucide-react';
import type { ModuleProps, ChartTimeframe } from '../types';
import { useTickDataStore, type Trade, type Quote } from '../stores/tickDataStore';
import { useChartDataStore, chartStoreKey } from '../stores/chartDataStore';
import { subscribeBarUpdates, unsubscribeBarUpdates } from '../hooks/useWebSocket';

// ── Constants ──────────────────────────────────────────────────────────────────

const TIMEFRAMES: ChartTimeframe[] = [
  '10s', '1m', '5m', '15m', '30m', '1h', '4h', '1D', '1W', '1M',
];

type ChartMode = 'candle' | 'line';

export function ChartModule({
  moduleId,
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
  const reconnect = useTickDataStore((s) => s.reconnect);
  const symbolData = useTickDataStore((s) => s.symbolData);

  // Chart data store (OHLCV bars)
  const fetchBars = useChartDataStore((s) => s.fetchBars);
  const symbolBars = useChartDataStore((s) => s.symbolBars);
  const chartState = symbol ? symbolBars[chartStoreKey(moduleId, symbol)] : undefined;

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

  // Current bar OHLCV accumulator — tracks the in-progress bar for real-time updates.
  // lightweight-charts update() replaces the entire bar, so we must maintain OHLCV ourselves.
  const currentBarRef = useRef<{
    time: number; open: number; high: number; low: number; close: number; volume: number;
  } | null>(null);

  // Bar duration of the currently rendered series — decoupled from store loading state
  // so trade ticks keep flowing during a timeframe fetch.
  const activeBarDurationRef = useRef<number>(0);

  // Track whether we've ever rendered data — fitContent only on very first render.
  // After that, symbol switches keep X-axis (timeline) unchanged, Y-axis auto-scales.
  const hasEverRenderedRef = useRef(false);

  // Ref to read current timeframe inside effects without adding it as a dependency
  const timeframeRef = useRef(timeframe);
  timeframeRef.current = timeframe;

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

  // Track previous bar subscription so we can subscribe-first-then-unsubscribe,
  // preventing chart_bff from momentarily seeing zero subs during timeframe switches.
  const prevBarSubRef = useRef<{ ticker: string; timeframe: string } | null>(null);

  // ── Bootstrap: fetch bars on symbol/timeframe change ───────────────────
  // Only triggers the API fetch. The chart keeps showing current data and
  // trade ticks keep flowing until the response arrives and the render
  // effect swaps in the new bars.
  useEffect(() => {
    if (!symbol) return;

    const tickerUpper = symbol.toUpperCase();
    fetchBars(moduleId, tickerUpper, timeframe);

    // Subscribe NEW first — chart_bff sees the new sub before the old one
    // is removed, so it never drops to zero subs for this ticker.
    subscribeBarUpdates(tickerUpper, timeframe);

    // Then unsubscribe the previous (if it changed)
    const prev = prevBarSubRef.current;
    if (prev && (prev.ticker !== tickerUpper || prev.timeframe !== timeframe)) {
      unsubscribeBarUpdates(prev.ticker, prev.timeframe);
    }

    prevBarSubRef.current = { ticker: tickerUpper, timeframe };
    currentSymbolRef.current = tickerUpper;
    currentTimeframeRef.current = timeframe;

    return () => {
      // Component unmount — unsubscribe whatever is active
      if (prevBarSubRef.current) {
        unsubscribeBarUpdates(prevBarSubRef.current.ticker, prevBarSubRef.current.timeframe);
        prevBarSubRef.current = null;
      }
    };
  }, [symbol, timeframe, fetchBars]);

  // ── Retry fetch when initial response had an error ─────────────────────
  // When the backend returns "No data available" (e.g., trades_backfill
  // hasn't finished yet), schedule a retry so the chart fills in once
  // ClickHouse has data, without the user having to interact.
  useEffect(() => {
    if (!symbol || !chartState?.error || chartState.loading) return;

    const timer = setTimeout(() => {
      fetchBars(moduleId, symbol.toUpperCase(), timeframe);
    }, 3000);

    return () => clearTimeout(timer);
  }, [chartState?.error, chartState?.lastFetchTime, symbol, timeframe, moduleId, fetchBars]);

  // Reset refs AND clear chart series when the symbol changes — prevents stale
  // bars from the previous ticker lingering when the new ticker has no data
  // (e.g. 10s timeframe with no API/ClickHouse data yet).
  useEffect(() => {
    lineDataRef.current = [];
    currentBarRef.current = null;

    // Clear visible series so old ticker's bars don't remain on screen
    if (candleSeriesRef.current) {
      candleSeriesRef.current.setData([]);
    }
    if (volumeSeriesRef.current) {
      volumeSeriesRef.current.setData([]);
    }
    if (lineSeriesRef.current) {
      lineSeriesRef.current.setData([]);
    }
  }, [symbol]);

  // ── Render bars on bootstrap fetch ────────────────────────────────────
  useEffect(() => {
    if (!chartRef.current || !symbol) return;
    if (!chartState || chartState.loading || chartState.bars.length === 0) return;

    if (chartMode === 'candle') {
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

      // First render: fitContent to establish a sensible initial view.
      // Subsequent renders: only re-enable Y-axis autoScale so the price
      // range adapts to the new ticker without moving the X-axis.
      if (!hasEverRenderedRef.current) {
        chartRef.current.timeScale().fitContent();
        hasEverRenderedRef.current = true;
      }
      chartRef.current.priceScale('right').applyOptions({ autoScale: true });
      activeBarDurationRef.current = chartState.barDurationSec;

      // Seed currentBarRef from the last historical bar so that the first
      // incoming trade updates it in place rather than creating a brand-new
      // bar that would overwrite the last bar's real OHLCV.
      const lastHistBar = chartState.bars[chartState.bars.length - 1];
      currentBarRef.current = lastHistBar
        ? { time: lastHistBar.time, open: lastHistBar.open, high: lastHistBar.high, low: lastHistBar.low, close: lastHistBar.close, volume: lastHistBar.volume }
        : null;
    } else if (chartMode === 'line') {
      ensureSeries('line');

      const lineData: LineData<Time>[] = chartState.bars.map((bar) => ({
        time: bar.time as Time,
        value: bar.close,
      }));

      if (lineSeriesRef.current) {
        lineSeriesRef.current.setData(lineData);
      }

      if (!hasEverRenderedRef.current) {
        chartRef.current.timeScale().fitContent();
        hasEverRenderedRef.current = true;
      }
      chartRef.current.priceScale('right').applyOptions({ autoScale: true });
      activeBarDurationRef.current = chartState.barDurationSec;

      // Seed currentBarRef from the last bar (same reason as candle mode)
      const lastHistBarLine = chartState.bars[chartState.bars.length - 1];
      currentBarRef.current = lastHistBarLine
        ? { time: lastHistBarLine.time, open: lastHistBarLine.open, high: lastHistBarLine.high, low: lastHistBarLine.low, close: lastHistBarLine.close, volume: lastHistBarLine.volume }
        : null;
    }

    // Update timeScale options to match the rendered timeframe.
    // Uses ref so this only runs when new data arrives (lastFetchTime), not on button click.
    const tf = timeframeRef.current;
    chartRef.current.timeScale().applyOptions({
      timeVisible: ['10s', '1m', '5m', '15m', '30m', '1h', '4h'].includes(tf),
      secondsVisible: ['10s', '1m'].includes(tf),
    });

  }, [chartState?.lastFetchTime, chartMode, symbol, ensureSeries]);

  // ── Real-time trade tick → update current bar / line fallback ──────────
  const latestTrade = symbol
    ? (symbolData[symbol]?.T as Trade | undefined) ?? null
    : null;

  useEffect(() => {
    if (!latestTrade || latestTrade.symbol !== symbol) return;
    if (typeof latestTrade.price !== 'number' || typeof latestTrade.timestamp !== 'number') return;

    // Use the actively rendered bar duration, not the store's (which may be loading)
    const barDuration = activeBarDurationRef.current;

    // Compute bar time for this trade.
    // For bars >= 1h (3600s), timestamps are session-aligned (BarBuilder) or
    // market-open-based, NOT clean multiples of barDuration from epoch.
    // Use range comparison against currentBarRef instead of floor alignment.
    const tradeSec = Math.floor(latestTrade.timestamp / 1000);

    // If we have an active candlestick or line series, update via the ref directly
    const hasActiveSeries = candleSeriesRef.current || lineSeriesRef.current;
    if (hasActiveSeries && barDuration > 0) {
      // Accumulate OHLCV for the current bar
      const cur = currentBarRef.current;

      if (cur && barDuration >= 3600) {
        // Session-aligned timeframes (1h, 4h, 1D, 1W, 1M):
        // Use range comparison against the current bar's time.
        if (tradeSec >= cur.time && tradeSec < cur.time + barDuration) {
          // Trade falls within current bar — update
          cur.high = Math.max(cur.high, latestTrade.price);
          cur.low = Math.min(cur.low, latestTrade.price);
          cur.close = latestTrade.price;
          cur.volume += latestTrade.size;
        } else if (tradeSec >= cur.time + barDuration) {
          // Trade is past current bar — start new bar
          currentBarRef.current = {
            time: cur.time + barDuration,
            open: latestTrade.price,
            high: latestTrade.price,
            low: latestTrade.price,
            close: latestTrade.price,
            volume: latestTrade.size,
          };
        }
        // else: trade before current bar — stale, ignore
      } else {
        // Floor-aligned timeframes (10s, 1m, 5m, 15m, 30m)
        const barTime = barDuration > 0
          ? Math.floor(tradeSec / barDuration) * barDuration
          : tradeSec;

        if (cur && cur.time === barTime) {
          // Same bar — update high/low/close/volume
          cur.high = Math.max(cur.high, latestTrade.price);
          cur.low = Math.min(cur.low, latestTrade.price);
          cur.close = latestTrade.price;
          cur.volume += latestTrade.size;
        } else {
          // New bar boundary — start fresh
          currentBarRef.current = {
            time: barTime,
            open: latestTrade.price,
            high: latestTrade.price,
            low: latestTrade.price,
            close: latestTrade.price,
            volume: latestTrade.size,
          };
        }
      }

      const bar = currentBarRef.current!;

      if (chartMode === 'candle' && candleSeriesRef.current) {
        try {
          candleSeriesRef.current.update({
            time: bar.time as Time,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
          });
        } catch { /* ignore out-of-order */ }

        if (volumeSeriesRef.current) {
          try {
            volumeSeriesRef.current.update({
              time: bar.time as Time,
              value: bar.volume,
              color: bar.close >= bar.open ? '#22c55e40' : '#ef444440',
            });
          } catch { /* ignore */ }
        }
      } else if (chartMode === 'line' && lineSeriesRef.current) {
        try {
          lineSeriesRef.current.update({
            time: bar.time as Time,
            value: bar.close,
          });
        } catch { /* ignore */ }
      }
    } else {
      // No OHLCV bars — fallback to raw trade line chart
      if (!lineSeriesRef.current) return;

      const btAsTime = barTime as Time;
      const lastPoint = lineDataRef.current[lineDataRef.current.length - 1];

      if (lastPoint && lastPoint.time === btAsTime && lastPoint.value === latestTrade.price) return;

      if (lastPoint && lastPoint.time === btAsTime) {
        lastPoint.value = latestTrade.price;
        try { lineSeriesRef.current.update(lastPoint); } catch { /* ignore */ }
        return;
      }

      const newPoint: LineData<Time> = { time: btAsTime, value: latestTrade.price };
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
  }, [latestTrade, symbol, chartMode]);

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

          <button
            onClick={reconnect}
            className="p-0.5 hover:bg-zinc-800 transition-colors rounded"
            title="Reconnect WebSocket"
          >
            <RefreshCw className="w-3.5 h-3.5 text-gray-400" />
          </button>

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

          {/* Loading indicator — always in DOM to prevent reflow on toggle */}
          <Loader2
            className={`w-3 h-3 text-blue-400 ${
              chartState?.loading ? 'animate-spin' : 'invisible'
            }`}
          />

          {/* Data source badge — always in DOM to prevent reflow on toggle */}
          <span
            className={`text-[9px] font-mono ${
              chartState?.source && !chartState.loading
                ? 'text-gray-600'
                : 'invisible'
            }`}
          >
            {chartState?.source || ''}
          </span>

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

      {/* Error state
      {symbol && chartState?.error && !chartState.loading && chartState.bars.length === 0 && (
        <div className="absolute inset-0 flex items-center justify-center bg-red-900/50 border border-red-800 px-4 py-2 text-sm text-red-300">
          {chartState.error} — showing real-time trades only
        </div>
      )} */}
    </div>
  );
}
