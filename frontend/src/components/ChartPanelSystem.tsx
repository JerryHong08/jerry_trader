/**
 * ChartPanelSystem - TradingView-style panel-based chart system.
 *
 * Architecture:
 * - Single module with multiple collapsible/closable panels
 * - All panels share same symbol/timeframe
 * - Price panel: OHLCV bars + optional overlay factors (EMA, VWAP)
 * - Factor panels: Standalone factor charts (TradeRate, RSI, etc.)
 *
 * Data Flow:
 * - Bars: chartDataStore (shared with legacy ChartModule)
 * - Factors: factorDataStore
 * - Real-time: tickDataStore trade/quote subscriptions
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { createChart, IChartApi, ISeriesApi, LineSeries, ColorType, CrosshairMode, CandlestickSeries, HistogramSeries } from 'lightweight-charts';
import { Wifi, WifiOff, RefreshCw, Plus, Loader2 } from 'lucide-react';
import type { Time } from 'lightweight-charts';
import type { ModuleProps, ChartTimeframe, ChartPanel } from '../types';
import { DEFAULT_PANELS } from '../types';
import { useTickDataStore, type Trade, type Quote } from '../stores/tickDataStore';
import { useChartDataStore, chartStoreKey } from '../stores/chartDataStore';
import { useFactorDataStore, factorStoreKey } from '../stores/factorDataStore';
import { ChartPanelWrapper } from './ChartPanelHeader';

// ── Constants ──────────────────────────────────────────────────────────────────

const TIMEFRAMES: ChartTimeframe[] = ['10s', '1m', '5m', '15m', '30m', '1h', '4h', '1D', '1W', '1M'];

// Timeframe to duration in seconds
const TIMEFRAME_DURATION: Record<ChartTimeframe, number> = {
  '10s': 10,
  '1m': 60,
  '5m': 300,
  '15m': 900,
  '30m': 1800,
  '1h': 3600,
  '4h': 14400,
  '1D': 86400,
  '1W': 604800,
  '1M': 2592000, // 30 days approximation
};

const BFF_URL =
  typeof import.meta !== 'undefined' && import.meta.env?.VITE_CHART_BFF_URL
    ? (import.meta.env.VITE_CHART_BFF_URL as string)
    : 'http://localhost:8000';

// ── Types ──────────────────────────────────────────────────────────────────────

interface FactorSpec {
  id: string;
  type: 'bar' | 'trade' | 'quote';
  display: {
    name: string;
    color: string;
    priceScale: 'left' | 'right';
    mode: 'overlay' | 'panel';
  };
  timeframes: string[];
}

// ── Component ──────────────────────────────────────────────────────────────────

export function ChartPanelSystem({
  moduleId,
  onRemove,
  selectedSymbol,
  onSymbolSelect,
  settings,
  onSettingsChange,
  zoom = 1,
}: ModuleProps) {
  // ── Symbol & Timeframe ──────────────────────────────────────────────────────
  const [symbol, setSymbol] = useState(selectedSymbol || '');
  const [input, setInput] = useState('');
  const timeframe: ChartTimeframe = settings?.chart?.timeframe ?? '5m';

  // ── Panel State ──────────────────────────────────────────────────────────────
  const [panels, setPanels] = useState<ChartPanel[]>(
    settings?.chart?.panels ?? DEFAULT_PANELS
  );
  const [availableFactors, setAvailableFactors] = useState<FactorSpec[]>([]);
  const [showAddPanel, setShowAddPanel] = useState(false);

  // ── Stores ───────────────────────────────────────────────────────────────────
  const symbols = useTickDataStore((s) => s.symbols);
  const connected = useTickDataStore((s) => s.connected);
  const addSymbols = useTickDataStore((s) => s.addSymbols);
  const removeSymbolFromStore = useTickDataStore((s) => s.removeSymbol);
  const reconnect = useTickDataStore((s) => s.reconnect);
  const symbolData = useTickDataStore((s) => s.symbolData);

  // Chart data store (OHLCV bars)
  const fetchBars = useChartDataStore((s) => s.fetchBars);
  const symbolBars = useChartDataStore((s) => s.symbolBars);
  const fetchTrigger = useChartDataStore((s) => s.fetchTriggers[symbol?.toUpperCase() ?? ''] ?? 0);
  const chartState = symbol ? symbolBars[chartStoreKey(moduleId, symbol)] : undefined;

  // Factor data store
  const fetchFactors = useFactorDataStore((s) => s.fetchFactors);
  const symbolFactors = useFactorDataStore((s) => s.symbolFactors);

  // ── Fetch available factors from API ─────────────────────────────────────────
  useEffect(() => {
    const fetchFactorSpecs = async () => {
      try {
        const response = await fetch(`${BFF_URL}/api/factors/specs`);
        if (!response.ok) throw new Error('Failed to fetch factor specs');
        const data = await response.json();
        setAvailableFactors(data.factors || []);
      } catch (err) {
        console.error('[ChartPanelSystem] Failed to load factor specs:', err);
        // Fallback
        setAvailableFactors([
          { id: 'ema_20', type: 'bar', display: { name: 'EMA(20)', color: '#3b82f6', priceScale: 'left', mode: 'overlay' }, timeframes: [] },
          { id: 'trade_rate', type: 'trade', display: { name: 'TradeRate', color: '#f97316', priceScale: 'right', mode: 'panel' }, timeframes: ['trade'] },
        ]);
      }
    };
    fetchFactorSpecs();
  }, []);

  // ── Sync symbol from external sync group ─────────────────────────────────────
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    }
  }, [selectedSymbol]);

  // ── React to global symbol list changes (subscribe/unsubscribe) ─────────────
  useEffect(() => {
    if (symbol && !symbols.includes(symbol)) {
      // Symbol was removed from global list → clear local state
      setSymbol('');
    } else if (!symbol && selectedSymbol && symbols.includes(selectedSymbol)) {
      // Symbol was re-added to global list → restore from sync group
      setSymbol(selectedSymbol);
    }
  }, [symbols, symbol, selectedSymbol]);

  // ── Data fetching on symbol/timeframe change ─────────────────────────────────
  useEffect(() => {
    if (!symbol) return;

    const symbolUpper = symbol.toUpperCase();

    // Fetch bars
    fetchBars(moduleId, symbolUpper, timeframe);

    // Fetch factors for overlays and panels
    fetchFactors(moduleId, symbolUpper, undefined, undefined, undefined, 'trade');
    fetchFactors(moduleId, symbolUpper, undefined, undefined, undefined, timeframe);

    // Subscribe to real-time factor updates
    const tickStore = useTickDataStore.getState();
    tickStore.subscribeFactors(symbolUpper, 'trade');
    tickStore.subscribeFactors(symbolUpper, timeframe);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol, timeframe, moduleId, fetchTrigger]);

  // ── Panel management ────────────────────────────────────────────────────────
  const togglePanelCollapse = useCallback((panelId: string) => {
    setPanels((prev) =>
      prev.map((p) =>
        p.id === panelId ? { ...p, collapsed: !p.collapsed } : p
      )
    );
  }, []);

  const closePanel = useCallback((panelId: string) => {
    setPanels((prev) => prev.map((p) =>
      p.id === panelId ? { ...p, visible: false } : p
    ));
  }, []);

  const addPanel = useCallback((factorId: string) => {
    const factor = availableFactors.find((f) => f.id === factorId);
    if (!factor) return;

    if (factor.display.mode === 'overlay') {
      // Add as overlay to price panel
      setPanels((prev) =>
        prev.map((p) =>
          p.id === 'price'
            ? { ...p, overlays: [...(p.overlays || []), factorId] }
            : p
        )
      );
    } else {
      // Add as new panel
      setPanels((prev) => [
        ...prev.filter((p) => p.id !== factorId),
        { id: factorId, type: 'panel', collapsed: false, visible: true },
      ]);
    }
    setShowAddPanel(false);
  }, [availableFactors]);

  const removeOverlay = useCallback((factorId: string) => {
    setPanels((prev) =>
      prev.map((p) =>
        p.id === 'price'
          ? { ...p, overlays: (p.overlays || []).filter((id) => id !== factorId) }
          : p
      )
    );
  }, []);

  // ── Symbol management ────────────────────────────────────────────────────────
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
    onSettingsChange?.({ chart: { timeframe: tf, panels } });
  };

  // ── Persist panel state ──────────────────────────────────────────────────────
  useEffect(() => {
    onSettingsChange?.({ chart: { timeframe, panels } });
  }, [panels]);

  // ── Quote data ───────────────────────────────────────────────────────────────
  const quote = symbol ? (symbolData[symbol]?.Q as Quote | undefined) : undefined;
  const latestTrade = symbol ? (symbolData[symbol]?.T as Trade | undefined) : null;

  // ── Render ───────────────────────────────────────────────────────────────────
  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header */}
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

        {/* Row 2: Timeframe selector + Add Panel + Price info */}
        <div className="flex items-center gap-2 flex-wrap">
          {/* Timeframe pills */}
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

          {/* Add Panel button */}
          <div className="relative">
            <button
              onClick={() => setShowAddPanel(!showAddPanel)}
              className="px-2 py-0.5 text-xs bg-zinc-800 hover:bg-zinc-700 text-gray-300 flex items-center gap-1"
            >
              <Plus className="w-3 h-3" />
              Add Panel
            </button>

            {showAddPanel && (
              <div className="absolute top-full left-0 mt-1 bg-zinc-800 border border-zinc-700 rounded shadow-lg z-50 min-w-[160px]">
                <div className="px-2 py-1 text-[10px] text-zinc-500 border-b border-zinc-700">
                  Overlays (on Price)
                </div>
                {availableFactors
                  .filter((f) => f.display.mode === 'overlay')
                  .map((factor) => {
                    const pricePanel = panels.find((p) => p.id === 'price');
                    const isActive = pricePanel?.overlays?.includes(factor.id);
                    return (
                      <button
                        key={factor.id}
                        onClick={() => isActive ? removeOverlay(factor.id) : addPanel(factor.id)}
                        className={`w-full px-2 py-1 text-xs text-left flex items-center gap-2 ${
                          isActive ? 'bg-zinc-700 text-white' : 'text-gray-300 hover:bg-zinc-700'
                        }`}
                      >
                        <span
                          className={`w-1.5 h-1.5 rounded-full`}
                          style={{ backgroundColor: factor.display.color }}
                        />
                        {factor.display.name}
                        {isActive && <span className="ml-auto text-green-400">✓</span>}
                      </button>
                    );
                  })}

                <div className="px-2 py-1 text-[10px] text-zinc-500 border-b border-zinc-700 border-t">
                  Panels
                </div>
                {availableFactors
                  .filter((f) => f.display.mode === 'panel')
                  .map((factor) => {
                    const isActive = panels.some((p) => p.id === factor.id && p.visible);
                    return (
                      <button
                        key={factor.id}
                        onClick={() => addPanel(factor.id)}
                        className={`w-full px-2 py-1 text-xs text-left flex items-center gap-2 ${
                          isActive ? 'bg-zinc-700 text-white' : 'text-gray-300 hover:bg-zinc-700'
                        }`}
                      >
                        <span
                          className={`w-1.5 h-1.5 rounded-full`}
                          style={{ backgroundColor: factor.display.color }}
                        />
                        {factor.display.name}
                        {isActive && <span className="ml-auto text-green-400">✓</span>}
                      </button>
                    );
                  })}
              </div>
            )}
          </div>

          <Loader2
            className={`w-3 h-3 text-blue-400 ${
              chartState?.loading ? 'animate-spin' : 'invisible'
            }`}
          />

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

      {/* Panels container */}
      <div className="flex-1 min-h-0 flex flex-col overflow-hidden">
        {panels.filter((p) => p.visible).map((panel) => (
          <ChartPanelWrapper
            key={panel.id}
            id={panel.id}
            title={panel.id === 'price' ? 'Price' : availableFactors.find((f) => f.id === panel.id)?.display.name || panel.id}
            collapsed={panel.collapsed}
            visible={panel.visible}
            canClose={panel.id !== 'price'} // Price panel cannot be closed
            onToggleCollapse={() => togglePanelCollapse(panel.id)}
            onClose={panel.id !== 'price' ? () => closePanel(panel.id) : undefined}
            className={panel.collapsed ? '' : 'flex-1 min-h-[100px]'}
          >
            {panel.id === 'price' ? (
              <PricePanel
                moduleId={moduleId}
                symbol={symbol}
                timeframe={timeframe}
                chartState={chartState}
                overlays={panel.overlays || []}
                availableFactors={availableFactors}
                zoom={zoom}
              />
            ) : (
              <FactorPanel
                moduleId={moduleId}
                symbol={symbol}
                factorId={panel.id}
                timeframe={timeframe}
                factorSpec={availableFactors.find((f) => f.id === panel.id)}
                zoom={zoom}
              />
            )}
          </ChartPanelWrapper>
        ))}
      </div>

      {/* Empty state */}
      {!symbol && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <span className="text-gray-600 text-sm">Add a symbol to see chart data</span>
        </div>
      )}
    </div>
  );
}

// ── PricePanel: OHLCV bars + overlay factors ────────────────────────────────────

interface PricePanelProps {
  moduleId: string;
  symbol: string;
  timeframe: ChartTimeframe;
  chartState: any;
  overlays: string[];
  availableFactors: FactorSpec[];
  zoom: number;
}

function PricePanel({
  moduleId,
  symbol,
  timeframe,
  chartState,
  overlays,
  availableFactors,
  zoom,
}: PricePanelProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const overlaySeriesRef = useRef<Record<string, ISeriesApi<'Line'>>>({});
  const overlayDataLengthRef = useRef<Record<string, number>>({});
  const currentBarRef = useRef<{ time: number; open: number; high: number; low: number; close: number; volume: number } | null>(null);
  const lastHistoricalBarTimeRef = useRef<number>(0);
  const barsLoadedRef = useRef<boolean>(false);
  const [chartReady, setChartReady] = useState(false);

  // Factor data for overlays
  const symbolFactors = useFactorDataStore((s) => s.symbolFactors);

  // Real-time trade data
  const symbolData = useTickDataStore((s) => s.symbolData);
  const latestTrade = symbol ? (symbolData[symbol]?.T as Trade | undefined) : null;
  const prevTradeRef = useRef<Trade | null>(null);

  // Initialize chart
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
        secondsVisible: timeframe === '10s',
      },
      rightPriceScale: { borderColor: '#3f3f46' },
      leftPriceScale: { borderColor: '#3f3f46', visible: false },
      crosshair: { mode: CrosshairMode.Normal },
    });

    chartRef.current = chart;

    // Candlestick series
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderUpColor: '#22c55e',
      borderDownColor: '#ef4444',
      wickUpColor: '#22c55e',
      wickDownColor: '#ef4444',
    });
    candleSeriesRef.current = candleSeries;

    // Volume series (pane below)
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: '', // Separate scale
    });
    chart.priceScale('').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    volumeSeriesRef.current = volumeSeries;

    // Mark chart as ready
    setChartReady(true);

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
      volumeSeriesRef.current = null;
      overlaySeriesRef.current = {};
      setChartReady(false);
      barsLoadedRef.current = false;
      currentBarRef.current = null;
    };
  }, []);

  // Reset bars loaded state when symbol/timeframe changes
  useEffect(() => {
    barsLoadedRef.current = false;
    currentBarRef.current = null;
    prevTradeRef.current = null;
    overlayDataLengthRef.current = {};
  }, [symbol, timeframe]);

  // Render bars
  useEffect(() => {
    if (!chartRef.current || !candleSeriesRef.current) return;
    if (!chartState?.bars?.length) return;

    const candleData = chartState.bars.map((bar: any) => ({
      time: bar.time as Time,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
    }));

    candleSeriesRef.current.setData(candleData);

    const volumeData = chartState.bars.map((bar: any) => ({
      time: bar.time as Time,
      value: bar.volume,
      color: bar.close >= bar.open ? '#22c55e40' : '#ef444440',
    }));

    volumeSeriesRef.current?.setData(volumeData);

    chartRef.current.timeScale().fitContent();

    // Initialize current bar from last historical bar for real-time updates
    const lastBar = chartState.bars[chartState.bars.length - 1];
    if (lastBar) {
      const lastBarTime = typeof lastBar.time === 'number' ? lastBar.time : parseInt(String(lastBar.time));
      lastHistoricalBarTimeRef.current = lastBarTime;
      // Initialize current bar from last historical bar
      currentBarRef.current = {
        time: lastBarTime,
        open: lastBar.open,
        high: lastBar.high,
        low: lastBar.low,
        close: lastBar.close,
        volume: lastBar.volume,
      };
    }

    // Mark bars as loaded and skip the initial trade that's already in the store
    barsLoadedRef.current = true;
    prevTradeRef.current = latestTrade;
  }, [chartState?.lastFetchTime]);

  // Real-time trade updates - build current bar from live trades
  useEffect(() => {
    // Skip until bars are loaded and chart is ready
    if (!barsLoadedRef.current || !candleSeriesRef.current) return;
    if (!latestTrade || latestTrade === prevTradeRef.current) return;

    prevTradeRef.current = latestTrade;
    const barDuration = TIMEFRAME_DURATION[timeframe];
    const tradeTime = Math.floor(latestTrade.timestamp / 1000); // ms to seconds
    const barStart = Math.floor(tradeTime / barDuration) * barDuration;

    // Skip if bar time is older than last historical bar
    if (barStart < lastHistoricalBarTimeRef.current) return;

    // Initialize or update current bar
    if (!currentBarRef.current || currentBarRef.current.time !== barStart) {
      // New bar - start from this trade
      currentBarRef.current = {
        time: barStart,
        open: latestTrade.price,
        high: latestTrade.price,
        low: latestTrade.price,
        close: latestTrade.price,
        volume: latestTrade.size,
      };
    } else {
      // Update existing bar
      currentBarRef.current = {
        ...currentBarRef.current,
        high: Math.max(currentBarRef.current.high, latestTrade.price),
        low: Math.min(currentBarRef.current.low, latestTrade.price),
        close: latestTrade.price,
        volume: currentBarRef.current.volume + latestTrade.size,
      };
    }

    // Update the candlestick series
    candleSeriesRef.current.update({
      time: currentBarRef.current.time as Time,
      open: currentBarRef.current.open,
      high: currentBarRef.current.high,
      low: currentBarRef.current.low,
      close: currentBarRef.current.close,
    });

    // Update volume
    volumeSeriesRef.current?.update({
      time: currentBarRef.current.time as Time,
      value: currentBarRef.current.volume,
      color: currentBarRef.current.close >= currentBarRef.current.open ? '#22c55e40' : '#ef444440',
    });
  }, [latestTrade, timeframe]);

  // Render overlays
  useEffect(() => {
    if (!chartRef.current || !symbol || !chartReady) return;

    // Get factor data for each overlay
    for (const factorId of overlays) {
      const factorSpec = availableFactors.find((f) => f.id === factorId);
      if (!factorSpec) continue;

      const factorKey = factorStoreKey(moduleId, symbol, timeframe);
      const factorState = symbolFactors[factorKey];
      const factorData = factorState?.factors?.[factorId];

      if (!factorData) continue;

      // Create overlay series if needed - overlays share price scale with candlesticks
      if (!overlaySeriesRef.current[factorId]) {
        const series = chartRef.current.addSeries(LineSeries, {
          color: factorSpec.display.color,
          lineWidth: 2,
          // Overlays share the right price scale with candlesticks (no separate scale)
        });
        overlaySeriesRef.current[factorId] = series;
        overlayDataLengthRef.current[factorId] = 0;
      }

      const currentLength = factorData.length;
      const previousLength = overlayDataLengthRef.current[factorId] || 0;

      // Initial load or reset - use setData
      if (previousLength === 0 || currentLength <= previousLength) {
        // Deduplicate by time (keep last value per timestamp)
        const seen = new Map<number, number>();
        for (const point of factorData) {
          const t = typeof point.time === 'number' ? point.time : parseInt(String(point.time));
          seen.set(t, point.value);
        }
        const lineData = Array.from(seen.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([time, value]) => ({ time: time as Time, value }));
        overlaySeriesRef.current[factorId].setData(lineData);
      } else {
        // Incremental update - ensure time is strictly increasing
        // Track the last time we actually added to the series
        let lastAddedTime = factorData[previousLength - 1]?.time;
        for (let i = previousLength; i < currentLength; i++) {
          const point = factorData[i];
          // Skip if time is not greater than the last added time
          if (lastAddedTime !== undefined && point.time <= lastAddedTime) {
            console.warn(`[PricePanel] Skipping duplicate/out-of-order factor point: ${factorId} time=${point.time}, lastAdded=${lastAddedTime}`);
            continue;
          }
          overlaySeriesRef.current[factorId].update({
            time: point.time as Time,
            value: point.value,
          });
          lastAddedTime = point.time;
        }
      }

      overlayDataLengthRef.current[factorId] = currentLength;
    }

    // Remove unused overlay series
    for (const factorId of Object.keys(overlaySeriesRef.current)) {
      if (!overlays.includes(factorId)) {
        chartRef.current.removeSeries(overlaySeriesRef.current[factorId]);
        delete overlaySeriesRef.current[factorId];
        delete overlayDataLengthRef.current[factorId];
      }
    }
  }, [overlays, symbolFactors, symbol, timeframe, moduleId, availableFactors, chartReady]);

  return (
    <div
      className="h-full"
      ref={chartContainerRef}
      style={{
        transform: zoom !== 1 ? `scale(${1 / zoom})` : undefined,
        transformOrigin: 'top left',
        width: zoom !== 1 ? `${zoom * 100}%` : undefined,
        height: zoom !== 1 ? `${zoom * 100}%` : undefined,
      }}
    />
  );
}

// ── FactorPanel: Standalone factor chart ────────────────────────────────────────

interface FactorPanelProps {
  moduleId: string;
  symbol: string;
  factorId: string;
  timeframe: ChartTimeframe;
  factorSpec?: FactorSpec;
  zoom: number;
}

function FactorPanel({
  moduleId,
  symbol,
  factorId,
  timeframe,
  factorSpec,
  zoom,
}: FactorPanelProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const hasFitContentRef = useRef<boolean>(false);
  const lastRenderedTimeRef = useRef<number>(0);  // Track last rendered timestamp
  const dataVersionRef = useRef<number>(0);  // Track data version for reset detection

  // Determine factor type for data fetching
  const factorType = factorSpec?.type || 'bar';
  const factorTimeframe = factorType === 'trade' ? 'trade' : timeframe;

  // Get factor data
  const fetchFactors = useFactorDataStore((s) => s.fetchFactors);
  const symbolFactors = useFactorDataStore((s) => s.symbolFactors);
  const factorKey = factorStoreKey(moduleId, symbol, factorTimeframe);
  const factorState = symbol ? symbolFactors[factorKey] : undefined;
  const factorData = factorState?.factors?.[factorId];

  // Fetch factor data on mount
  useEffect(() => {
    if (!symbol) return;
    fetchFactors(moduleId, symbol.toUpperCase(), undefined, undefined, undefined, factorTimeframe);
  }, [symbol, factorTimeframe, moduleId, fetchFactors]);

  // Initialize chart
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
      seriesRef.current = null;
      hasFitContentRef.current = false;
      lastRenderedTimeRef.current = 0;
      dataVersionRef.current = 0;
    };
  }, []);

  // Reset state when symbol/timeframe changes
  useEffect(() => {
    lastRenderedTimeRef.current = 0;
    dataVersionRef.current = 0;
    hasFitContentRef.current = false;
  }, [symbol, factorTimeframe]);

  // Render factor data
  useEffect(() => {
    if (!chartRef.current || !factorData) return;

    // Create series if needed
    if (!seriesRef.current && factorSpec) {
      const series = chartRef.current.addSeries(LineSeries, {
        color: factorSpec.display.color,
        lineWidth: 2,
        priceScaleId: factorSpec.display.priceScale === 'left' ? 'left' : 'right',
      });
      seriesRef.current = series;
    }

    if (seriesRef.current && factorData.length > 0) {
      // Track data version to detect reset (e.g., refetch)
      const currentVersion = factorState?.lastFetchTime || 0;
      const isReset = currentVersion !== dataVersionRef.current || lastRenderedTimeRef.current === 0;

      if (isReset) {
        // Initial load or reset - use setData
        // Deduplicate by time (keep last value per timestamp) to prevent
        // lightweight-charts assertion error on duplicate timestamps
        const seen = new Map<number, number>();
        for (const point of factorData) {
          const t = typeof point.time === 'number' ? point.time : parseInt(String(point.time));
          seen.set(t, point.value);
        }
        const lineData = Array.from(seen.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([time, value]) => ({ time: time as Time, value }));
        seriesRef.current.setData(lineData);

        // Update tracking refs
        const lastPoint = factorData[factorData.length - 1];
        const lastTime = typeof lastPoint.time === 'number' ? lastPoint.time : parseInt(String(lastPoint.time));
        lastRenderedTimeRef.current = lastTime;
        dataVersionRef.current = currentVersion;

        // Fit content on initial load
        if (!hasFitContentRef.current) {
          chartRef.current.timeScale().fitContent();
          hasFitContentRef.current = true;
        }
      } else {
        // Incremental update - only update points newer than last rendered
        for (const point of factorData) {
          const pointTime = typeof point.time === 'number' ? point.time : parseInt(String(point.time));
          if (pointTime > lastRenderedTimeRef.current) {
            seriesRef.current.update({
              time: pointTime as Time,
              value: point.value,
            });
            lastRenderedTimeRef.current = pointTime;
          }
        }
      }
    }
  }, [factorData, factorSpec, factorState?.lastFetchTime]);

  return (
    <div
      className="h-full"
      ref={chartContainerRef}
      style={{
        transform: zoom !== 1 ? `scale(${1 / zoom})` : undefined,
        transformOrigin: 'top left',
        width: zoom !== 1 ? `${zoom * 100}%` : undefined,
        height: zoom !== 1 ? `${zoom * 100}%` : undefined,
      }}
    />
  );
}

export default ChartPanelSystem;
