/**
 * FactorChartModule - Real-time Factor Visualization
 *
 * Displays computed factors (EMA, TradeRate) using TradingView Lightweight Charts.
 * Fully synced with ChartModule via sync group (same symbol and timeframe).
 *
 * Features:
 * - Syncs symbol and timeframe from sync group (just like a second BarChart)
 * - Displays factors for the current symbol/timeframe instead of OHLCV bars
 * - Dual Y-axis for different factor scales (EMA ~price, TradeRate ~rate)
 * - Toggle visibility for each factor
 * - Auto-subscribe/unsubscribe on symbol/timeframe change
 */

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { createChart, IChartApi, ISeriesApi, LineSeries, ColorType, CrosshairMode } from 'lightweight-charts';
import { TrendingUp, Wifi, WifiOff, RefreshCw } from 'lucide-react';
import { useFactorDataStore, factorStoreKey } from '../stores/factorDataStore';
import { useTickDataStore } from '../stores/tickDataStore';
import type { ModuleProps, ChartTimeframe } from '../types';

// ============================================================================
// Constants
// ============================================================================

const TIMEFRAMES: ChartTimeframe[] = ['10s', '1m', '5m', '15m', '30m', '1h', '4h', '1D', '1W', '1M'];

const BFF_HTTP_URL =
  typeof import.meta !== 'undefined' && import.meta.env?.VITE_CHART_BFF_URL
    ? (import.meta.env.VITE_CHART_BFF_URL as string)
    : 'http://localhost:8000';

const BFF_URL_RESOLVED = BFF_HTTP_URL || 'http://localhost:8000';

interface FactorConfig {
  name: string;
  displayName: string;
  color: string;
  priceScaleId: 'left' | 'right';
  visible: boolean;
}

// ============================================================================
// Component
// ============================================================================

export function FactorChartModule({
  moduleId,
  onRemove,
  selectedSymbol,
  onSymbolSelect,
  selectedTimeframe,
  settings,
  onSettingsChange,
  zoom = 1,
}: ModuleProps) {
  // ── Symbol & Timeframe (synced initially, then independent like a 2nd BarChart) ──────────────
  const [symbol, setSymbol] = useState(selectedSymbol || '');
  const [timeframe, setTimeframe] = useState<ChartTimeframe>(selectedTimeframe ?? '10s');
  const [input, setInput] = useState('');

  // Track previous symbol to detect changes
  const prevSymbolRef = useRef(selectedSymbol);

  // Get symbol management from tickDataStore (shared with ChartModule)
  const symbols = useTickDataStore((s) => s.symbols);
  const connected = useTickDataStore((s) => s.connected);
  const addSymbols = useTickDataStore((s) => s.addSymbols);
  const removeSymbolFromStore = useTickDataStore((s) => s.removeSymbol);
  const reconnect = useTickDataStore((s) => s.reconnect);

  // Sync symbol from external sync group
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    }
  }, [selectedSymbol]);

  // When symbol changes, sync timeframe to follow the main chart (like 2nd BarChart behavior)
  useEffect(() => {
    if (selectedSymbol && prevSymbolRef.current !== selectedSymbol) {
      // Symbol changed - sync timeframe to main chart
      if (selectedTimeframe) {
        setTimeframe(selectedTimeframe);
      }
      prevSymbolRef.current = selectedSymbol;
    }
  }, [selectedSymbol, selectedTimeframe]);

  const handleTimeframeChange = (tf: ChartTimeframe) => {
    setTimeframe(tf);
    onSettingsChange?.({ chart: { timeframe: tf } });
  };

  // ── Symbol management (like ChartModule) ────────────────────────────────
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

  // ── Factor Configuration ───────────────────────────────────────────────
  const [factorConfigs, setFactorConfigs] = useState<Record<string, FactorConfig>>({
    ema_20: {
      name: 'ema_20',
      displayName: 'EMA(20)',
      color: '#3b82f6', // blue
      priceScaleId: 'left',   // Price-level indicator on left
      visible: true,
    },
    trade_rate: {
      name: 'trade_rate',
      displayName: 'TradeRate',
      color: '#f97316', // orange
      priceScaleId: 'right',  // Rate indicator on right (different scale)
      visible: true,
    },
  });

  // ── Chart refs ─────────────────────────────────────────────────────────
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRefsRef = useRef<Record<string, ISeriesApi<'Line'>>>({});

  // ── Factor data store ──────────────────────────────────────────────────
  const fetchFactors = useFactorDataStore((s) => s.fetchFactors);
  const symbolFactors = useFactorDataStore((s) => s.symbolFactors);
  // Get factor state for this module + symbol + timeframe
  // We need both tick-based (TradeRate) and bar-based (EMA) factors for the current timeframe
  const tickFactorKey = factorStoreKey(moduleId, symbol, 'tick');
  const barFactorKey = symbol ? factorStoreKey(moduleId, symbol, timeframe) : null;
  const tickFactorState = symbol ? symbolFactors[tickFactorKey] : undefined;
  const barFactorState = barFactorKey ? symbolFactors[barFactorKey] : undefined;

  // ── Initialize chart ───────────────────────────────────────────────────
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
        secondsVisible: false,
      },
      leftPriceScale: {
        borderColor: '#3f3f46',
        visible: true,
      },
      rightPriceScale: {
        borderColor: '#3f3f46',
        visible: true,
      },
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

    // Fix for browser zoom: redraw chart when devicePixelRatio changes
    const mq = window.matchMedia(`(resolution: ${window.devicePixelRatio}dppx)`);
    const handleDprChange = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    };
    mq.addEventListener('change', handleDprChange);

    return () => {
      ro.disconnect();
      mq.removeEventListener('change', handleDprChange);
      chart.remove();
      chartRef.current = null;
      seriesRefsRef.current = {};
    };
  }, []);

  // Keep track of previous timeframe to detect changes
  const prevTimeframeRef = useRef<ChartTimeframe>(timeframe);

  // ── Render factor series (initial setup only) ──────────────────────────
  // This effect runs once on mount to set up the series
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    // Create series for each factor config (empty initially)
    Object.values(factorConfigs).forEach((config) => {
      if (seriesRefsRef.current[config.name]) return; // Already exists

      try {
        const series = chart.addSeries(LineSeries, {
          color: config.color,
          lineWidth: 2,
          priceScaleId: config.priceScaleId,
          visible: config.visible,
          title: config.displayName,
        });

        seriesRefsRef.current[config.name] = series;
      } catch (e) {
        console.error(`[FactorChart] Failed to add series for ${config.name}:`, e);
      }
    });

    // Only fit content on initial mount
    chart.timeScale().fitContent();
  }, []); // Run once on mount

  // ── Update factor data (incremental, preserves zoom/pan) ───────────────
  // This effect updates data without recreating series or resetting view
  useEffect(() => {
    const chart = chartRef.current;
    const isLoading = tickFactorState?.loading || barFactorState?.loading;
    if (!chart || isLoading) return;

    // Check if timeframe changed - if so, don't clear tick-based data
    const timeframeChanged = prevTimeframeRef.current !== timeframe;
    prevTimeframeRef.current = timeframe;

    // Merge factors from both tick-based and bar-based states
    const mergedFactors: Record<string, { time: number; value: number }[]> = {};
    if (tickFactorState?.factors) {
      Object.entries(tickFactorState.factors).forEach(([name, points]) => {
        mergedFactors[name] = points;
      });
    }
    if (barFactorState?.factors) {
      Object.entries(barFactorState.factors).forEach(([name, points]) => {
        mergedFactors[name] = points;
      });
    }

    // Update each series with new data (incremental)
    Object.values(factorConfigs).forEach((config) => {
      const series = seriesRefsRef.current[config.name];
      if (!series) return;

      const points = mergedFactors[config.name] || [];
      if (points.length === 0) {
        // Only clear bar-based series on timeframe change, keep tick-based
        if (timeframeChanged && config.name !== 'trade_rate') {
          try {
            series.setData([]);
          } catch (e) {
            console.warn(`[FactorChart] Failed to clear series for ${config.name}:`, e);
          }
        }
        return;
      }

      try {
        // Use update for incremental updates to preserve chart position
        // Sort points by time to ensure correct order
        const sortedPoints = [...points].sort((a, b) => a.time - b.time);
        series.setData(sortedPoints);
      } catch (e) {
        console.error(`[FactorChart] Failed to update series for ${config.name}:`, e);
      }
    });
  }, [tickFactorState, barFactorState, timeframe]); // Include timeframe to detect changes

  // ── Sync series visibility with config ─────────────────────────────────
  useEffect(() => {
    Object.values(factorConfigs).forEach((config) => {
      const series = seriesRefsRef.current[config.name];
      if (series) {
        series.applyOptions({ visible: config.visible });
      }
    });
  }, [factorConfigs]);

  // ── Bootstrap factors on symbol/timeframe change ──────────────────────
  useEffect(() => {
    if (!symbol) return;

    const symbolUpper = symbol.toUpperCase();

    // Fetch historical factors for both tick-based and bar-based
    // Don't specify time range - backend returns all available data
    // This works for both live mode and replay mode with historical dates
    fetchFactors(moduleId, symbolUpper, undefined, undefined, undefined, 'tick'); // TradeRate
    fetchFactors(moduleId, symbolUpper, undefined, undefined, undefined, timeframe); // EMA

    // Subscribe to real-time updates via tickDataStore
    // Note: No need for cleanup unsubscribe - backend coordinator.cleanup() handles it
    // when the symbol is unsubscribed from ticks/quotes
    const tickStore = useTickDataStore.getState();
    tickStore.subscribeFactors(symbolUpper, 'tick'); // TradeRate and tick-based
    tickStore.subscribeFactors(symbolUpper, timeframe); // EMA and bar-based for this timeframe
  }, [symbol, timeframe, moduleId, fetchFactors]);

  // ── Toggle factor visibility ───────────────────────────────────────────
  const toggleFactorVisibility = useCallback((factorName: string) => {
    setFactorConfigs((prev) => {
      const config = prev[factorName];
      if (!config) return prev;

      const updated = {
        ...prev,
        [factorName]: { ...config, visible: !config.visible },
      };

      // Update series visibility
      const series = seriesRefsRef.current[factorName];
      if (series) {
        series.applyOptions({ visible: !config.visible });
      }

      return updated;
    });
  }, []);

  // ── Render ─────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col h-full bg-zinc-900 text-zinc-100">
      {/* Header - Two rows like ChartModule */}
      <div className="px-2 py-1.5 border-b border-zinc-800 flex flex-col gap-1 bg-zinc-900/50">
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

        {/* Row 2: Timeframe selector + Factor toggles */}
        <div className="flex items-center justify-between flex-wrap gap-2">
          {/* Timeframe Pills */}
          <div className="flex items-center gap-0.5 bg-zinc-950/50 rounded-md p-0.5">
            {TIMEFRAMES.map((tf) => (
              <button
                key={tf}
                onClick={() => handleTimeframeChange(tf)}
                className={`px-2 py-0.5 text-[10px] font-mono rounded transition-all ${
                  tf === timeframe
                    ? 'bg-zinc-700 text-zinc-100 shadow-sm'
                    : 'text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800/50'
                }`}
              >
                {tf}
              </button>
            ))}
          </div>

          {/* Factor Toggles */}
          <div className="flex items-center gap-1">
            {Object.values(factorConfigs).map((config) => (
              <button
                key={config.name}
                onClick={() => toggleFactorVisibility(config.name)}
                className={`flex items-center gap-1.5 px-2 py-0.5 text-[10px] rounded-md transition-all ${
                  config.visible
                    ? 'bg-zinc-800 text-zinc-200'
                    : 'bg-transparent text-zinc-600 hover:text-zinc-400'
                }`}
              >
                {/* Color Dot */}
                <span
                  className={`w-1.5 h-1.5 rounded-full transition-all ${
                    config.visible ? 'opacity-100' : 'opacity-30'
                  }`}
                  style={{ backgroundColor: config.color }}
                />
                <span className="font-medium">{config.displayName}</span>
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Chart Container - fills entire space like ChartModule */}
      <div className="flex-1 min-h-0 relative">
        <div
          className="absolute inset-0"
          ref={chartContainerRef}
          style={{
            transform: zoom !== 1 ? `scale(${1 / zoom})` : undefined,
            transformOrigin: 'top left',
            width: zoom !== 1 ? `${zoom * 100}%` : undefined,
            height: zoom !== 1 ? `${zoom * 100}%` : undefined,
          }}
        />

        {/* Loading/Error Overlay */}
        {(tickFactorState?.loading || barFactorState?.loading) && (
          <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/80 pointer-events-none">
            <div className="text-sm text-zinc-400">Loading factors...</div>
          </div>
        )}
        {(tickFactorState?.error || barFactorState?.error) && (
          <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/80 pointer-events-none">
            <div className="text-sm text-red-400">
              Error: {tickFactorState?.error || barFactorState?.error}
            </div>
          </div>
        )}
        {!symbol && (
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <span className="text-gray-600 text-sm">Add a symbol to see factors</span>
          </div>
        )}
      </div>
    </div>
  );
}
