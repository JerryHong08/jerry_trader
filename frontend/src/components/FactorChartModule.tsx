/**
 * FactorChartModule - Real-time Factor Visualization
 *
 * Displays computed factors (EMA, TradeRate) using TradingView Lightweight Charts.
 * Follows the ChartModule's symbol and timeframe (passive/follower mode).
 *
 * Features:
 * - Syncs symbol from selectedSymbol prop (follows ChartModule)
 * - Syncs timeframe from settings (follows ChartModule)
 * - Manual timeframe override available
 * - Dual Y-axis for different factor scales (EMA ~price, TradeRate ~rate)
 * - Toggle visibility for each factor
 * - Auto-subscribe/unsubscribe on symbol/timeframe change
 */

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { createChart, IChartApi, ISeriesApi, ColorType, CrosshairMode } from 'lightweight-charts';
import { Eye, EyeOff, TrendingUp } from 'lucide-react';
import { useFactorDataStore, factorStoreKey } from '../stores/factorDataStore';
import { useTickDataStore } from '../stores/tickDataStore';
import type { ModuleProps, ChartTimeframe } from '../types';

// ============================================================================
// Constants
// ============================================================================

const TIMEFRAMES: ChartTimeframe[] = [
  '10s', '1m', '5m', '15m', '30m', '1h', '4h', '1D', '1W', '1M',
];

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
  settings,
  onSettingsChange,
}: ModuleProps) {
  // ── Symbol & Timeframe (follows ChartModule) ──────────────────────────
  const [symbol, setSymbol] = useState(selectedSymbol || '');

  // Timeframe from settings (follows ChartModule), default '5m'
  const defaultTimeframe: ChartTimeframe = settings?.chart?.timeframe ?? '5m';
  const [timeframe, setTimeframe] = useState<ChartTimeframe>(defaultTimeframe);

  // Sync symbol from external sync group (ChartModule)
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    }
  }, [selectedSymbol]);

  // Sync timeframe from settings (ChartModule changes)
  useEffect(() => {
    if (settings?.chart?.timeframe && settings.chart.timeframe !== timeframe) {
      setTimeframe(settings.chart.timeframe);
    }
  }, [settings?.chart?.timeframe]);

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
  const factorState = symbol ? symbolFactors[factorStoreKey(moduleId, symbol)] : undefined;

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

  // ── Render factor series ───────────────────────────────────────────────
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !factorState || factorState.loading) return;

    // Remove all existing series
    Object.values(seriesRefsRef.current).forEach((series) => {
      try {
        chart.removeSeries(series);
      } catch (e) {
        console.warn('[FactorChart] Failed to remove series:', e);
      }
    });
    seriesRefsRef.current = {};

    // Create series for each factor
    Object.values(factorConfigs).forEach((config) => {
      const points = factorState.factors[config.name] || [];
      if (points.length === 0) return;

      try {
        const series = chart.addLineSeries({
          color: config.color,
          lineWidth: 2,
          priceScaleId: config.priceScaleId,
          visible: config.visible,
          title: config.displayName,
        });

        series.setData(points);
        seriesRefsRef.current[config.name] = series;
      } catch (e) {
        console.error(`[FactorChart] Failed to add series for ${config.name}:`, e);
      }
    });

    chart.timeScale().fitContent();
  }, [factorState, factorConfigs]);

  // ── Bootstrap factors on symbol/timeframe change ──────────────────────
  useEffect(() => {
    if (!symbol) return;

    const symbolUpper = symbol.toUpperCase();

    // Calculate time range based on timeframe
    const now = Date.now();
    let fromMs: number;
    switch (timeframe) {
      case '10s':
      case '1m':
      case '5m':
        fromMs = now - 3600000; // 1 hour
        break;
      case '15m':
      case '30m':
        fromMs = now - 14400000; // 4 hours
        break;
      case '1h':
      case '4h':
        fromMs = now - 86400000; // 1 day
        break;
      case '1D':
      case '1W':
      case '1M':
        fromMs = now - 604800000; // 1 week
        break;
      default:
        fromMs = now - 3600000;
    }

    // Fetch historical factors
    fetchFactors(moduleId, symbolUpper, fromMs, now);

    // Subscribe to real-time updates via tickDataStore
    const tickStore = useTickDataStore.getState();
    tickStore.subscribeFactors(symbolUpper);

    return () => {
      tickStore.unsubscribeFactors(symbolUpper);
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

  // ── Manual timeframe change ────────────────────────────────────────────
  const handleTimeframeChange = (tf: ChartTimeframe) => {
    setTimeframe(tf);
    // Optionally update settings to sync back to ChartModule
    // onSettingsChange?.({ ...settings, chart: { ...settings?.chart, timeframe: tf } });
  };

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

        {/* Timeframe Selector */}
        <div className="flex gap-1">
          {TIMEFRAMES.map((tf) => (
            <button
              key={tf}
              onClick={() => handleTimeframeChange(tf)}
              className={`px-2 py-1 text-xs font-medium rounded transition-colors ${
                timeframe === tf
                  ? 'bg-blue-600 text-white'
                  : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'
              }`}
            >
              {tf}
            </button>
          ))}
        </div>

        {/* Factor Toggles */}
        <div className="flex gap-2">
          {Object.values(factorConfigs).map((config) => (
            <button
              key={config.name}
              onClick={() => toggleFactorVisibility(config.name)}
              className="flex items-center gap-1.5 px-2 py-1 text-xs font-medium rounded transition-colors bg-zinc-800 hover:bg-zinc-700"
              style={{ color: config.visible ? config.color : '#71717a' }}
            >
              {config.visible ? (
                <Eye className="w-3.5 h-3.5" />
              ) : (
                <EyeOff className="w-3.5 h-3.5" />
              )}
              {config.displayName}
            </button>
          ))}
        </div>
      </div>

      {/* Chart Container */}
      <div className="flex-1 relative bg-zinc-950 rounded border border-zinc-800">
        <div ref={chartContainerRef} className="absolute inset-0" />

        {/* Loading/Error Overlay */}
        {factorState?.loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/80">
            <div className="text-sm text-zinc-400">Loading factors...</div>
          </div>
        )}
        {factorState?.error && (
          <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/80">
            <div className="text-sm text-red-400">Error: {factorState.error}</div>
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
            {factorState && !factorState.loading && (
              <>
                <span className="ml-2 text-zinc-600">•</span>
                <span className="ml-2">
                  {Object.keys(factorState.factors).length} factors •{' '}
                  {Object.values(factorState.factors).reduce((sum, points) => sum + points.length, 0)} points
                </span>
              </>
            )}
          </>
        )}
      </div>
    </div>
  );
}
