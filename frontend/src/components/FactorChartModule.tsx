/**
 * FactorChartModule - Real-time Factor Visualization
 *
 * Displays computed factors (EMA, TradeRate) using TradingView Lightweight Charts.
 * Follows the ChartModule's symbol and timeframe (passive/sub-follower mode).
 *
 * Features:
 * - Syncs symbol from selectedSymbol prop (follows ChartModule)
 * - Syncs timeframe from settings (sub-follower of ChartModule)
 * - Dual Y-axis for different factor scales (EMA ~price, TradeRate ~rate)
 * - Toggle visibility for each factor
 * - Auto-subscribe/unsubscribe on symbol/timeframe change
 */

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { createChart, IChartApi, ISeriesApi, LineSeries, ColorType, CrosshairMode } from 'lightweight-charts';
import { Eye, EyeOff, TrendingUp, ChevronDown } from 'lucide-react';
import { useFactorDataStore, factorStoreKey } from '../stores/factorDataStore';
import { useTickDataStore } from '../stores/tickDataStore';
import type { ModuleProps, ChartTimeframe } from '../types';

// ============================================================================
// Constants
// ============================================================================

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
}: ModuleProps) {
  // ── Symbol & Timeframe (follows ChartModule) ──────────────────────────
  const [symbol, setSymbol] = useState(selectedSymbol || '');

  // Timeframe from settings (independent mode)
  const timeframe: ChartTimeframe = settings?.chart?.timeframe ?? '5m';

  // Timeframe selector dropdown state
  const [showTfMenu, setShowTfMenu] = useState(false);
  const TIMEFRAMES: ChartTimeframe[] = ['10s', '1m', '5m', '15m', '30m', '1h', '4h', '1D'];

  const handleTimeframeChange = (tf: ChartTimeframe) => {
    onSettingsChange?.({ chart: { timeframe: tf } });
    setShowTfMenu(false);
  };

  // Sync symbol from external sync group (ChartModule)
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    }
  }, [selectedSymbol]);

  // ── Factor Configuration ───────────────────────────────────────────────
  const [factorConfigs, setFactorConfigs] = useState<Record<string, FactorConfig>>({
    ema_20: {
      name: 'ema_20',
      displayName: 'EMA(20)',
      color: '#3b82f6', // blue
      priceScaleId: 'left',
      visible: true,
    },
    trade_rate: {
      name: 'trade_rate',
      displayName: 'TradeRate',
      color: '#f97316', // orange
      priceScaleId: 'right',
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

    return () => {
      ro.disconnect();
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
    // Subscribe to BOTH tick-based factors (TradeRate) and bar-based factors (EMA) for current timeframe
    const tickStore = useTickDataStore.getState();
    tickStore.subscribeFactors(symbolUpper, 'tick'); // TradeRate and tick-based
    tickStore.subscribeFactors(symbolUpper, timeframe); // EMA and bar-based for this timeframe

    return () => {
      tickStore.unsubscribeFactors(symbolUpper, 'tick');
      tickStore.unsubscribeFactors(symbolUpper, timeframe);
    };
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
    <div className="flex flex-col h-full bg-zinc-900 text-zinc-100 p-3">
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <TrendingUp className="w-5 h-5 text-blue-400" />
          <h3 className="text-sm font-semibold">
            Factor Chart {symbol && `- ${symbol}`}
          </h3>
        </div>

        {/* Timeframe selector dropdown for easy debugging */}
        <div className="relative">
          <button
            onClick={() => setShowTfMenu(!showTfMenu)}
            className="flex items-center gap-1 px-2 py-1 text-xs font-medium rounded bg-zinc-800 text-zinc-400 hover:bg-zinc-700 transition-colors"
          >
            {timeframe}
            <ChevronDown className="w-3 h-3" />
          </button>
          {showTfMenu && (
            <div className="absolute top-full left-0 mt-1 bg-zinc-800 border border-zinc-700 rounded shadow-xl z-50 min-w-[80px]">
              {TIMEFRAMES.map((tf) => (
                <button
                  key={tf}
                  onClick={() => handleTimeframeChange(tf)}
                  className={`w-full text-left px-2 py-1 text-xs hover:bg-zinc-700 ${
                    tf === timeframe ? 'bg-blue-600 text-white' : 'text-zinc-400'
                  }`}
                >
                  {tf}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Factor Toggles with timeframe labels */}
        <div className="flex gap-2">
          {Object.values(factorConfigs).map((config) => {
            // Show timeframe for each factor: EMA uses bar timeframe, TradeRate uses tick
            const factorTimeframe = config.name === 'trade_rate' ? 'tick' : timeframe;
            return (
              <button
                key={config.name}
                onClick={() => toggleFactorVisibility(config.name)}
                className="flex items-center gap-1.5 px-2 py-1 text-xs font-medium rounded transition-colors bg-zinc-800 hover:bg-zinc-700"
                style={{ color: config.visible ? config.color : '#71717a' }}
                title={`${config.displayName} (${factorTimeframe})`}
              >
                {config.visible ? (
                  <Eye className="w-3.5 h-3.5" />
                ) : (
                  <EyeOff className="w-3.5 h-3.5" />
                )}
                {config.displayName}
                <span className="text-[10px] opacity-60 ml-0.5">({factorTimeframe})</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* Chart Container */}
      <div className="flex-1 relative bg-zinc-950 rounded border border-zinc-800">
        <div ref={chartContainerRef} className="absolute inset-0" />

        {/* Loading/Error Overlay */}
        {(tickFactorState?.loading || barFactorState?.loading) && (
          <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/80">
            <div className="text-sm text-zinc-400">Loading factors...</div>
          </div>
        )}
        {(tickFactorState?.error || barFactorState?.error) && (
          <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/80">
            <div className="text-sm text-red-400">
              Error: {tickFactorState?.error || barFactorState?.error}
            </div>
          </div>
        )}
        {!symbol && (
          <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/80">
            <div className="text-sm text-zinc-500">
              Waiting for symbol from Chart module...
            </div>
          </div>
        )}
      </div>

      {/* Footer Info */}
      <div className="mt-3 text-xs text-zinc-500">
        {symbol && (
          <>
            <span className="font-medium text-zinc-400">{symbol}</span>
            <span className="ml-2 text-zinc-600">•</span>
            <span className="ml-2">{timeframe}</span>
            {/* Merge factors from both states for display */}
            {(() => {
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
              const factorCount = Object.keys(mergedFactors).length;
              const pointCount = Object.values(mergedFactors).reduce((sum, points) => sum + points.length, 0);
              return factorCount > 0 ? (
                <>
                  <span className="ml-2 text-zinc-600">•</span>
                  <span className="ml-2">
                    {factorCount} factors • {pointCount} points
                  </span>
                </>
              ) : null;
            })()}
          </>
        )}
      </div>
    </div>
  );
}
