/**
 * BacktestChartModule - Multi-pane chart system for backtest visualization.
 *
 * Architecture (TradingView-style):
 * - Price panel: OHLCV bars + entry/exit markers (fills remaining space)
 * - Volume panel: Optional volume histogram
 * - Factor panels: Independent charts for each factor (fixed height)
 * - All panels share synchronized time axis
 * - Resizable panels by dragging grip handle
 *
 * Features:
 * - Signal detail panel with full factor breakdown
 * - Factor comparison mode (multiple factors in one panel)
 * - Time range selector
 * - Export chart data
 * - Batch factor loading (single API call)
 */

import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  ISeriesApi,
  createSeriesMarkers,
  CandlestickSeries,
  LineSeries,
  HistogramSeries,
  Time,
} from 'lightweight-charts';
import { useLightweightChart } from '../hooks/useLightweightChart';
import { Plus, Loader2, Download, Info, X } from 'lucide-react';
import type { ModuleProps } from '../types';
import { ChartPanelWrapper } from './ChartPanelHeader';

// ============================================================================
// Constants
// ============================================================================

const BACKTEST_API_URL = (() => {
  const defaultHost = typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  return (import.meta.env.VITE_BACKTEST_API_URL as string | undefined) || `http://${defaultHost}:5005/api/backtest`;
})();


const ENTRY_MARKER_COLOR = '#10b981';
const EXIT_PROFIT_COLOR = '#22c55e';
const EXIT_LOSS_COLOR = '#ef4444';

interface BacktestSignal {
  type: 'entry' | 'exit';
  time: number;
  price: number;
  factors?: Record<string, number>;
  return_pct?: number;
  event_name?: string;
  max_price?: number;
  min_price?: number;
  exit_reason?: string;
}

const MIN_PANEL_HEIGHT = 100;
const DEFAULT_FACTOR_PANEL_HEIGHT = 150;

// Factor display configs with category grouping
const FACTOR_CONFIGS: Record<string, { name: string; color: string; category: string }> = {
  trade_rate: { name: 'Trade Rate', color: '#f97316', category: 'tick' },
  relative_volume: { name: 'Rel Vol', color: '#f59e0b', category: 'bar' },
  quote_rate: { name: 'Quote Rate', color: '#22c55e', category: 'quote' },
  bid_ask_spread: { name: 'Spread (bps)', color: '#ec4899', category: 'quote' },
  order_imbalance: { name: 'Order Imb', color: '#6366f1', category: 'quote' },
  ema_20: { name: 'EMA 20', color: '#3b82f6', category: 'bar' },
  price_direction: { name: 'Price Dir', color: '#8b5cf6', category: 'tick' },
  gap_percent: { name: 'Gap %', color: '#ef4444', category: 'meta' },
  entry_gap_pct: { name: 'Entry Gap %', color: '#14b8a6', category: 'meta' },
  signal_density_3m: { name: 'Signal Density 3m', color: '#a78bfa', category: 'derived' },
  signal_density_5m: { name: 'Signal Density 5m', color: '#c084fc', category: 'derived' },
};

// ============================================================================
// Types
// ============================================================================

interface BacktestData {
  meta: {
    ticker: string;
    date?: string;
    event?: string;
    total_signals: number;
    bar_count: number;
    factor_names?: string[];
  };
  bars: Array<{ time: number; open: number; high: number; low: number; close: number; volume?: number }>;
  signals: Array<{
    time: number;
    type: 'entry' | 'exit';
    price: number;
    return_pct?: number;
    factors?: Record<string, number>;
    event_name?: string;
    max_price?: number;
    min_price?: number;
    exit_reason?: string;
  }>;
  factors?: Array<{ time: number; [key: string]: number }>;
}

interface FactorTimeline {
  factor: string;
  timeline: Array<{ time: number; value: number }>;
  points: number;
}

interface FactorPanelConfig {
  id: string;
  height: number;
  collapsed: boolean;
  factors: string[]; // Support multiple factors for comparison mode
  comparisonMode: boolean;
}

interface PanelHeightState {
  price: number | null; // null means flex-1 (auto)
  volume: number;
  factors: Record<string, number>;
}

// ============================================================================
// Main Component
// ============================================================================

export function BacktestChartModule({
  moduleId,
  onRemove,
  settings,
  onSettingsChange,
}: ModuleProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  // State
  const [data, setData] = useState<BacktestData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [experimentId, setExperimentId] = useState<string>('');
  const [ticker, setTicker] = useState<string>('');
  const [factorPanels, setFactorPanels] = useState<FactorPanelConfig[]>([]);
  const [factorTimelines, setFactorTimelines] = useState<Record<string, FactorTimeline>>({});
  const [showAddPanel, setShowAddPanel] = useState(false);

  // Panel heights - state managed for consistency
  const [panelHeights, setPanelHeights] = useState<PanelHeightState>({
    price: null, // flex-1 by default
    factors: {},
  });

  // Selected signal for detail panel
  const [selectedSignal, setSelectedSignal] = useState<BacktestSignal | null>(null);

  // Signal detail panel state
  const [showSignalDetail, setShowSignalDetail] = useState(false);

  // Time range state
  const [timeRange, setTimeRange] = useState<{ from: number; to: number } | null>(null);

  // Drag resize state
  const draggingRef = useRef<{
    panelId: string | 'price' | 'volume';
    startY: number;
    startHeight: number;
    panelHeights: PanelHeightState;
  } | null>(null);

  // Listen for ticker selection
  useEffect(() => {
    const handleTickerSelect = (event: CustomEvent<{ experiment_id: string; ticker: string }>) => {
      const { experiment_id, ticker } = event.detail;
      setExperimentId(experiment_id);
      setTicker(ticker);
      loadChartDataAsync(experiment_id, ticker);
    };

    window.addEventListener('backtest:ticker-selected', handleTickerSelect as EventListener);
    return () => window.removeEventListener('backtest:ticker-selected', handleTickerSelect as EventListener);
  }, []);

  // Load chart data
  const loadChartDataAsync = async (expId: string, tickerSymbol: string) => {
    if (!expId || !tickerSymbol) {
      setError('Enter experiment ID and ticker');
      return;
    }

    setLoading(true);
    setError(null);
    setFactorTimelines({});
    setFactorPanels([]);
    setSelectedSignal(null);
    setShowSignalDetail(false);

    try {
      const response = await fetch(`${BACKTEST_API_URL}/chart/${expId}/${tickerSymbol.toUpperCase()}`);
      if (response.ok) {
        const json = await response.json();
        setData(json);

        // Set initial time range based on data
        if (json.bars && json.bars.length > 0) {
          const firstBar = json.bars[0].time;
          const lastBar = json.bars[json.bars.length - 1].time;
          setTimeRange({ from: firstBar, to: lastBar });
        }
      } else if (response.status === 404) {
        setError(`No signals for ${tickerSymbol} in experiment ${expId}`);
      } else {
        setError(`API error: ${response.status}`);
      }
    } catch (e) {
      setError(`Error loading data: ${e}`);
    } finally {
      setLoading(false);
    }
  };

  // Batch load all factors - single API call
  const batchLoadFactorTimelines = useCallback(async (factorIds: string[]) => {
    if (!experimentId || !ticker || factorIds.length === 0) return;

    // Filter to factors not yet loaded
    const toLoad = factorIds.filter(id => !factorTimelines[id]);
    if (toLoad.length === 0) return;

    try {
      // Single batch API call
      const response = await fetch(
        `${BACKTEST_API_URL}/factors-timeline-batch/${experimentId}/${ticker}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            factors: toLoad,
            window_minutes: 60,
            resolution: '1m',
          }),
        }
      );

      if (response.ok) {
        const json = await response.json();
        const newTimelines: Record<string, FactorTimeline> = {};

        for (const factorId of toLoad) {
          if (json.timelines && json.timelines[factorId]) {
            const timelineData = json.timelines[factorId];
            const validTimeline = (timelineData.timeline || []).filter(
              (point: { time: number; value: number }) =>
                point.time != null && point.value != null &&
                Number.isFinite(point.time) && Number.isFinite(point.value)
            );
            newTimelines[factorId] = {
              factor: factorId,
              timeline: validTimeline,
              points: validTimeline.length,
            };
          }
        }

        setFactorTimelines(prev => ({ ...prev, ...newTimelines }));
      }
    } catch (e) {
      // Fallback: load individually if batch fails
      console.warn('Batch load failed, falling back to individual:', e);
      for (const factorId of toLoad) {
        fetchFactorTimeline(factorId);
      }
    }
  }, [experimentId, ticker, factorTimelines]);

  // Individual factor timeline fetch (fallback)
  const fetchFactorTimeline = async (factorId: string) => {
    if (!experimentId || !ticker) return;

    try {
      const response = await fetch(
        `${BACKTEST_API_URL}/factors-timeline/${experimentId}/${ticker}?factor=${factorId}&window_minutes=60&resolution=1m`
      );
      if (response.ok) {
        const json = await response.json();
        const validTimeline = (json.timeline || []).filter((point: { time: number; value: number }) =>
          point.time != null && point.value != null && Number.isFinite(point.time) && Number.isFinite(point.value)
        );
        setFactorTimelines(prev => ({
          ...prev,
          [factorId]: {
            factor: factorId,
            timeline: validTimeline,
            points: validTimeline.length,
          }
        }));
      }
    } catch (e) {
      console.warn(`Failed to fetch factor ${factorId}:`, e);
    }
  };

  // Add factor panel
  const addFactorPanel = useCallback((factorId: string) => {
    const existing = factorPanels.find(p => p.factors.includes(factorId));
    if (existing) {
      setFactorPanels(prev => prev.map(p =>
        p.id === existing.id ? { ...p, collapsed: false } : p
      ));
    } else {
      const newPanelId = `factor-${factorId}-${Date.now()}`;
      setFactorPanels(prev => [...prev, {
        id: newPanelId,
        height: DEFAULT_FACTOR_PANEL_HEIGHT,
        collapsed: false,
        factors: [factorId],
        comparisonMode: false,
      }]);
      setPanelHeights(prev => ({
        ...prev,
        factors: { ...prev.factors, [newPanelId]: DEFAULT_FACTOR_PANEL_HEIGHT },
      }));
    }
    setShowAddPanel(false);
  }, [factorPanels]);

  // Add factor to existing panel (comparison mode)
  const addFactorToPanel = useCallback((panelId: string, factorId: string) => {
    setFactorPanels(prev => prev.map(p =>
      p.id === panelId
        ? { ...p, factors: [...p.factors.filter(f => f !== factorId), factorId], comparisonMode: true }
        : p
    ));
  }, []);

  // Remove factor panel
  const removeFactorPanel = useCallback((panelId: string) => {
    setFactorPanels(prev => prev.filter(p => p.id !== panelId));
    setPanelHeights(prev => {
      const newFactors = { ...prev.factors };
      delete newFactors[panelId];
      return { ...prev, factors: newFactors };
    });
  }, []);

  // Toggle collapse
  const toggleCollapse = useCallback((panelId: string) => {
    setFactorPanels(prev => prev.map(p =>
      p.id === panelId ? { ...p, collapsed: !p.collapsed } : p
    ));
  }, []);

  // Drag resize handler - state managed
  const handleDragStart = useCallback((panelId: string | 'price', e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();

    // Get current height from state or DOM
    let startHeight: number;
    if (panelId === 'price') {
      const priceEl = containerRef.current?.querySelector('.price-panel');
      startHeight = priceEl?.clientHeight || 400;
    } else {
      startHeight = panelHeights.factors[panelId] || DEFAULT_FACTOR_PANEL_HEIGHT;
    }

    draggingRef.current = {
      panelId,
      startY: e.clientY,
      startHeight,
      panelHeights: { ...panelHeights },
    };
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';

    const handleMouseMove = (e: MouseEvent) => {
      if (!draggingRef.current) return;
      const deltaY = e.clientY - draggingRef.current.startY;
      const newHeight = Math.max(MIN_PANEL_HEIGHT, draggingRef.current.startHeight + deltaY);

      if (draggingRef.current.panelId === 'price') {
        // Price panel: set explicit height (overrides flex-1)
        setPanelHeights(prev => ({ ...prev, price: newHeight }));
      } else {
        setPanelHeights(prev => ({
          ...prev,
          factors: { ...prev.factors, [draggingRef.current!.panelId]: newHeight },
        }));
      }
    };

    const handleMouseUp = () => {
      draggingRef.current = null;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
  }, [panelHeights]);

  // Export chart data
  const handleExport = useCallback(() => {
    if (!data) return;

    const exportData = {
      meta: data.meta,
      bars: data.bars,
      signals: data.signals,
      factor_timelines: factorTimelines,
      exported_at: new Date().toISOString(),
    };

    const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `backtest_${experimentId}_${ticker}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
  }, [data, experimentId, ticker, factorTimelines]);

  // Load timelines for visible panels - batch mode
  useEffect(() => {
    const allFactors = factorPanels
      .filter(p => !p.collapsed)
      .flatMap(p => p.factors);

    if (allFactors.length > 0) {
      batchLoadFactorTimelines(allFactors);
    }
  }, [factorPanels, batchLoadFactorTimelines]);

  // Summary stats
  const summary = useMemo(() => {
    if (!data) return null;
    const entries = data.signals.filter(s => s.type === 'entry');
    const exits = data.signals.filter(s => s.type === 'exit');
    const totalReturn = exits.reduce((sum, s) => sum + (s.return_pct || 0), 0);
    const avgReturn = exits.length > 0 ? totalReturn / exits.length : 0;
    const winCount = exits.filter(s => s.return_pct && s.return_pct > 0).length;
    const winRate = exits.length > 0 ? winCount / exits.length : 0;
    return { totalSignals: entries.length, avgReturn, winRate };
  }, [data]);

  const availableFactors = useMemo(() => data?.meta.factor_names || [], [data]);

  // Factor categories for grouped display
  const factorCategories = useMemo(() => {
    const categories: Record<string, string[]> = {};
    for (const factorId of availableFactors) {
      const config = FACTOR_CONFIGS[factorId];
      const category = config?.category || 'other';
      if (!categories[category]) categories[category] = [];
      categories[category].push(factorId);
    }
    return categories;
  }, [availableFactors]);

  return (
    <div ref={containerRef} className="h-full flex flex-col bg-zinc-900 overflow-hidden">
      {/* Main Header */}
      <div className="h-7 px-2 py-0.5 border-b border-zinc-800 flex items-center justify-between gap-2 bg-zinc-900 flex-shrink-0">
        <div className="flex items-center gap-2">
          <input
            type="text"
            placeholder="Exp ID"
            value={experimentId}
            onChange={(e) => setExperimentId(e.target.value)}
            className="w-20 bg-zinc-800 border border-zinc-700 px-1.5 py-0.5 text-xs focus:outline-none focus:border-zinc-500"
          />
          <input
            type="text"
            placeholder="Ticker"
            value={ticker}
            onChange={(e) => setTicker(e.target.value.toUpperCase())}
            className="w-14 bg-zinc-800 border border-zinc-700 px-1.5 py-0.5 text-xs focus:outline-none focus:border-zinc-500"
          />
          <button
            onClick={() => loadChartDataAsync(experimentId, ticker)}
            disabled={loading || !experimentId || !ticker}
            className="px-1.5 py-0.5 bg-emerald-600 hover:bg-emerald-500 text-white text-xs rounded disabled:opacity-50"
          >
            Load
          </button>
          <Loader2 className={`w-3 h-3 text-blue-400 ${loading ? 'animate-spin' : 'hidden'}`} />
          {data && (
            <span className="text-xs text-zinc-400">{data.meta.ticker} {data.meta.date}</span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {summary && (
            <div className="flex items-center gap-1.5 text-xs">
              <span className="text-zinc-500">Sig:{summary.totalSignals}</span>
              <span className={summary.avgReturn >= 0 ? 'text-green-400' : 'text-red-400'}>
                {summary.avgReturn.toFixed(1)}%
              </span>
            </div>
          )}

          {/* Export button */}
          {data && (
            <button
              onClick={handleExport}
              className="p-1 hover:bg-zinc-700 rounded"
              title="Export Chart Data"
            >
              <Download className="w-3 h-3 text-zinc-400" />
            </button>
          )}

          {/* Add Factor button */}
          {availableFactors.length > 0 && (
            <div className="relative">
              <button
                onClick={() => setShowAddPanel(!showAddPanel)}
                className="px-1.5 py-0.5 text-xs bg-zinc-800 hover:bg-zinc-700 text-zinc-300 flex items-center gap-1"
              >
                <Plus className="w-3 h-3" />
                Add
              </button>
              {showAddPanel && (
                <div className="absolute top-full right-0 mt-1 bg-zinc-800 border border-zinc-700 rounded shadow-lg z-50 max-h-[200px] overflow-y-auto w-48">
                  {/* Factor categories */}
                  {Object.entries(factorCategories).map(([category, factors]) => (
                    <div key={category} className="border-b border-zinc-700 last:border-b-0">
                      <div className="px-2 py-1 text-xs text-zinc-500 uppercase">{category}</div>
                      {factors.map(factorId => {
                        const config = FACTOR_CONFIGS[factorId] || { name: factorId, color: '#3b82f6' };
                        const isActive = factorPanels.some(p => p.factors.includes(factorId));
                        return (
                          <button
                            key={factorId}
                            onClick={() => addFactorPanel(factorId)}
                            className={`w-full px-2 py-1 text-xs text-left flex items-center gap-2 whitespace-nowrap ${
                              isActive ? 'bg-zinc-700 text-white' : 'text-zinc-300 hover:bg-zinc-700'
                            }`}
                          >
                            <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: config.color }} />
                            {config.name}
                          </button>
                        );
                      })}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Panels Container */}
      <div className="flex-1 min-h-0 flex flex-col overflow-hidden">
        {/* Price Panel */}
        <div
          className="price-panel min-h-[100px] flex flex-col"
          style={panelHeights.price ? { height: panelHeights.price, flex: 'none' } : { flex: 1 }}
        >
          <ChartPanelWrapper
            id="price"
            title={data?.meta.ticker || 'Price'}
            collapsed={false}
            visible={true}
            canClose={false}
            onToggleCollapse={() => {}}
            className="flex-1 min-h-0"
            headerRight={summary && (
              <div className="flex items-center gap-2 text-xs">
                <span className="text-zinc-500">Win: {(summary.winRate * 100).toFixed(0)}%</span>
                {selectedSignal && (
                  <button
                    onClick={() => setShowSignalDetail(true)}
                    className="p-0.5 hover:bg-zinc-700 rounded"
                    title="View Signal Details"
                  >
                    <Info className="w-3 h-3 text-zinc-400" />
                  </button>
                )}
              </div>
            )}
          >
            <PricePanel
              data={data}
              onSignalHover={setSelectedSignal}
              onSignalClick={(sig) => {
                setSelectedSignal(sig);
                setShowSignalDetail(true);
              }}
              timeRange={timeRange}
              onTimeRangeChange={setTimeRange}
            />
          </ChartPanelWrapper>
          {/* Grip handle - visible divider */}
          <div
            className="grip-handle h-4 bg-zinc-700 cursor-row-resize flex items-center justify-center transition-colors flex-shrink-0"
            onMouseDown={(e) => handleDragStart('price', e)}
          >
            <div className="flex items-center gap-1">
              <div className="grip-dot w-1.5 h-1.5 rounded-full" style={{ backgroundColor: '#71717a' }} />
              <div className="grip-dot w-1.5 h-1.5 rounded-full" style={{ backgroundColor: '#71717a' }} />
              <div className="grip-dot w-1.5 h-1.5 rounded-full" style={{ backgroundColor: '#71717a' }} />
            </div>
          </div>
        </div>

        {/* Factor Panels */}
        {factorPanels.map((panel) => {
          const height = panelHeights.factors[panel.id] || panel.height;
          const timelines = panel.factors.map(f => factorTimelines[f]).filter(Boolean);
          const lastValues = panel.factors.map(f => {
            const tl = factorTimelines[f];
            return tl?.timeline[tl.timeline.length - 1]?.value;
          });

          return (
            <div
              key={panel.id}
              className="flex flex-col flex-shrink-0"
              style={{ height: panel.collapsed ? 'auto' : height + 32 }}
            >
              <ChartPanelWrapper
                id={panel.id}
                title={panel.factors.map(f => FACTOR_CONFIGS[f]?.name || f).join(' + ')}
                color={panel.factors.length === 1 ? FACTOR_CONFIGS[panel.factors[0]]?.color : undefined}
                collapsed={panel.collapsed}
                visible={true}
                canClose={true}
                onToggleCollapse={() => toggleCollapse(panel.id)}
                onClose={() => removeFactorPanel(panel.id)}
                className={panel.collapsed ? '' : 'flex-1 min-h-0'}
                headerRight={
                  <div className="flex items-center gap-1">
                    {lastValues.filter(v => v !== undefined).map((v, i) => (
                      <span
                        key={i}
                        className="text-2xs font-mono px-1 rounded"
                        style={{
                          color: FACTOR_CONFIGS[panel.factors[i]]?.color || '#3b82f6',
                          backgroundColor: (FACTOR_CONFIGS[panel.factors[i]]?.color || '#3b82f6') + '20'
                        }}
                      >
                        {v.toFixed(2)}
                      </span>
                    ))}
                    {!panel.collapsed && panel.factors.length === 1 && availableFactors.length > 1 && (
                      <button
                        onClick={() => setShowAddPanel(true)}
                        className="p-0.5 hover:bg-zinc-700 rounded"
                        title="Add another factor for comparison"
                      >
                        <Plus className="w-3 h-3 text-zinc-400" />
                      </button>
                    )}
                  </div>
                }
              >
                <FactorPanelContent
                  panelId={panel.id}
                  factors={panel.factors}
                  timelines={timelines}
                  comparisonMode={panel.comparisonMode}
                />
              </ChartPanelWrapper>
              {/* Grip handle */}
              {!panel.collapsed && (
                <div
                  className="grip-handle h-4 bg-zinc-700 cursor-row-resize flex items-center justify-center transition-colors flex-shrink-0"
                  onMouseDown={(e) => handleDragStart(panel.id, e)}
                >
                  <div className="flex items-center gap-0.5">
                    <div className="grip-dot w-1.5 h-1.5 rounded-full" style={{ backgroundColor: '#71717a' }} />
                    <div className="grip-dot w-1.5 h-1.5 rounded-full" style={{ backgroundColor: '#71717a' }} />
                    <div className="grip-dot w-1.5 h-1.5 rounded-full" style={{ backgroundColor: '#71717a' }} />
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Signal Detail Panel */}
      {showSignalDetail && selectedSignal && (
        <SignalDetailPanel
          signal={selectedSignal}
          onClose={() => setShowSignalDetail(false)}
        />
      )}

      {/* Error state */}
      {error && !data && (
        <div className="absolute inset-0 flex items-center justify-center bg-zinc-900/80 z-30">
          <div className="text-zinc-400 text-sm text-center">
            {error}
            <div className="mt-1 text-xs">Select from Results Browser</div>
          </div>
        </div>
      )}
    </div>
  );
}

// ============================================================================
// PricePanel - Fixed time scale sync
// ============================================================================

interface PricePanelProps {
  data: BacktestData | null;
  onSignalHover: (sig: BacktestSignal | null) => void;
  onSignalClick: (sig: BacktestSignal) => void;
  timeRange: { from: number; to: number } | null;
  onTimeRangeChange: (range: { from: number; to: number } | null) => void;
}

function PricePanel({ data, onSignalHover, onSignalClick, timeRange, onTimeRangeChange }: PricePanelProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const { chartRef, chartReady } = useLightweightChart({ container: containerRef });
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);

  // Create series when chart is ready
  useEffect(() => {
    if (!chartRef.current) return;

    const chart = chartRef.current;

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
    });
    candleSeriesRef.current = candleSeries;

    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: '#6366f1',
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    volumeSeriesRef.current = volumeSeries;

    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
      borderVisible: false,
    });
  }, [chartReady]);

  // Subscribe to visible logical range for time range selector
  useEffect(() => {
    if (!chartRef.current) return;

    const handler = (logicalRange: { from: number; to: number } | null) => {
      if (logicalRange) {
        onTimeRangeChange({ from: logicalRange.from, to: logicalRange.to });
      }
    };
    chartRef.current.timeScale().subscribeVisibleLogicalRangeChange(handler);

    return () => {
      chartRef.current?.timeScale().unsubscribeVisibleLogicalRangeChange(handler);
    };
  }, [chartReady]);

  useEffect(() => {
    if (!chartRef.current || !candleSeriesRef.current || !data) return;

    candleSeriesRef.current.setData(
      data.bars.map(bar => ({
        time: bar.time as Time,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      }))
    );

    // Set volume data (shares same time scale as candles)
    if (volumeSeriesRef.current) {
      const volumeData = data.bars
        .filter(bar => bar.volume != null)
        .map(bar => ({
          time: bar.time as Time,
          value: bar.volume!,
          color: bar.close >= bar.open ? '#26a69a80' : '#ef535080',
        }));
      volumeSeriesRef.current.setData(volumeData);
    }

    const markers = data.signals.map(sig => ({
      time: sig.time as Time,
      position: sig.type === 'entry' ? 'belowBar' : 'aboveBar',
      color: sig.type === 'entry'
        ? ENTRY_MARKER_COLOR
        : (sig.return_pct && sig.return_pct > 0 ? EXIT_PROFIT_COLOR : EXIT_LOSS_COLOR),
      shape: sig.type === 'entry' ? 'arrowUp' : 'arrowDown',
      text: sig.type === 'entry' ? 'Buy' : `Sell ${sig.return_pct?.toFixed(1) ?? ''}%`,
    }));

    createSeriesMarkers(candleSeriesRef.current, markers);

    chartRef.current.timeScale().fitContent();

    // Crosshair move handler - improved signal matching
    chartRef.current.subscribeCrosshairMove((param) => {
      if (!param.time || !data) {
        onSignalHover(null);
        return;
      }
      const hoverTime = param.time as number;

      // Find closest signal within 60 seconds
      const signal = data.signals.reduce((closest, sig) => {
        const diff = Math.abs(sig.time - hoverTime);
        if (diff < 60 && (!closest || diff < Math.abs(closest.time - hoverTime))) {
          return sig;
        }
        return closest;
      }, null as BacktestData['signals'][0] | null);

      if (signal) {
        onSignalHover({
          type: signal.type,
          time: signal.time,
          price: signal.price,
          factors: signal.factors,
          return_pct: signal.return_pct,
          event_name: signal.event_name,
          max_price: signal.max_price,
          min_price: signal.min_price,
          exit_reason: signal.exit_reason,
        });
      } else {
        onSignalHover(null);
      }
    });
  }, [data, chartReady, onSignalHover]);

  // Handle click on markers
  useEffect(() => {
    if (!chartRef.current || !data) return;

    // Subscribe to click events
    chartRef.current.subscribeClick((param) => {
      if (!param.time || !data) return;

      const clickTime = param.time as number;
      const signal = data.signals.find(sig => Math.abs(sig.time - clickTime) < 60);

      if (signal) {
        onSignalClick({
          type: signal.type,
          time: signal.time,
          price: signal.price,
          factors: signal.factors,
          return_pct: signal.return_pct,
          event_name: signal.event_name,
          max_price: signal.max_price,
          min_price: signal.min_price,
          exit_reason: signal.exit_reason,
        });
      }
    });
  }, [data, onSignalClick]);

  return <div ref={containerRef} className="h-full w-full" />;
}

// ============================================================================
// FactorPanelContent - Multi-factor support
// ============================================================================

interface FactorPanelContentProps {
  panelId: string;
  factors: string[];
  timelines: FactorTimeline[];
  comparisonMode: boolean;
}

function FactorPanelContent({ panelId, factors, timelines, comparisonMode }: FactorPanelContentProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const { chartRef, chartReady } = useLightweightChart({ container: containerRef });
  const seriesRefs = useRef<Map<string, ISeriesApi<'Line'>>>(new Map());

  // Create / remove series when factors change
  useEffect(() => {
    if (!chartRef.current) return;

    const chart = chartRef.current;
    const currentFactors = new Set(factors);

    for (const [factorId, series] of seriesRefs.current.entries()) {
      if (!currentFactors.has(factorId)) {
        chart.removeSeries(series);
        seriesRefs.current.delete(factorId);
      }
    }

    for (const factorId of factors) {
      if (!seriesRefs.current.has(factorId)) {
        const config = FACTOR_CONFIGS[factorId] || { name: factorId, color: '#3b82f6' };
        const series = chart.addSeries(LineSeries, {
          color: config.color,
          lineWidth: 2,
          crosshairMarkerVisible: true,
        });
        seriesRefs.current.set(factorId, series);
      }
    }
  }, [chartReady, factors]);

  useEffect(() => {
    if (!chartRef.current || !chartReady || timelines.length === 0) return;

    // Set data for each factor series
    for (const timeline of timelines) {
      const series = seriesRefs.current.get(timeline.factor);
      if (!series) continue;

      const uniqueData = new Map<number, number>();
      for (const point of timeline.timeline) {
        if (Number.isFinite(point.time) && Number.isFinite(point.value)) {
          uniqueData.set(point.time, point.value);
        }
      }

      const lineData = Array.from(uniqueData.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([time, value]) => ({ time: time as Time, value }));

      if (lineData.length > 0) {
        series.setData(lineData);
      }
    }
  }, [timelines, chartReady, factors]);

  return <div ref={containerRef} className="h-full w-full" />;
}

// ============================================================================
// SignalDetailPanel - Full factor breakdown
// ============================================================================

interface SignalDetailPanelProps {
  signal: {
    type: 'entry' | 'exit';
    time: number;
    price: number;
    factors?: Record<string, number>;
    return_pct?: number;
    event_name?: string;
    max_price?: number;
    min_price?: number;
    exit_reason?: string;
  };
  onClose: () => void;
}

function SignalDetailPanel({ signal, onClose }: SignalDetailPanelProps) {
  const timeStr = new Date(signal.time * 1000).toLocaleTimeString();
  const dateStr = new Date(signal.time * 1000).toLocaleDateString();

  // Group factors by category
  const groupedFactors = useMemo(() => {
    if (!signal.factors) return {};

    const groups: Record<string, Array<{ name: string; value: number; rawName: string }>> = {};

    for (const [key, value] of Object.entries(signal.factors)) {
      if (typeof value !== 'number') continue;
      const config = FACTOR_CONFIGS[key];
      const category = config?.category || 'other';
      const displayName = config?.name || key;

      if (!groups[category]) groups[category] = [];
      groups[category].push({ name: displayName, value, rawName: key });
    }

    return groups;
  }, [signal.factors]);

  return (
    <div className="absolute bottom-0 left-0 right-0 bg-zinc-800 border-t border-zinc-700 z-40 max-h-[50%] overflow-auto">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-zinc-700 bg-zinc-900">
        <div className="flex items-center gap-3">
          <span className={`text-sm font-medium ${signal.type === 'entry' ? 'text-emerald-400' : 'text-red-400'}`}>
            {signal.type === 'entry' ? '⬆ Entry' : '⬇ Exit'}
          </span>
          <span className="text-xs text-zinc-400">{dateStr} {timeStr}</span>
          {signal.event_name && (
            <span className="text-xs bg-zinc-700 px-1.5 rounded text-zinc-300">{signal.event_name}</span>
          )}
        </div>
        <button onClick={onClose} className="p-1 hover:bg-zinc-700 rounded">
          <X className="w-4 h-4 text-zinc-400" />
        </button>
      </div>

      {/* Content */}
      <div className="p-3">
        {/* Price info */}
        <div className="grid grid-cols-2 gap-4 mb-3">
          <div>
            <div className="text-xs text-zinc-500">Price</div>
            <div className="text-sm font-mono text-zinc-200">${signal.price.toFixed(4)}</div>
          </div>
          {signal.type === 'exit' && signal.return_pct !== undefined && (
            <>
              <div>
                <div className="text-xs text-zinc-500">Return</div>
                <div className={`text-sm font-mono ${signal.return_pct >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {signal.return_pct.toFixed(2)}%
                </div>
              </div>
              {signal.max_price && (
                <div>
                  <div className="text-xs text-zinc-500">Max Price</div>
                  <div className="text-sm font-mono text-zinc-200">${signal.max_price.toFixed(4)}</div>
                </div>
              )}
              {signal.min_price && (
                <div>
                  <div className="text-xs text-zinc-500">Min Price</div>
                  <div className="text-sm font-mono text-zinc-200">${signal.min_price.toFixed(4)}</div>
                </div>
              )}
              {signal.exit_reason && (
                <div>
                  <div className="text-xs text-zinc-500">Exit Reason</div>
                  <div className="text-sm text-zinc-200">{signal.exit_reason}</div>
                </div>
              )}
            </>
          )}
        </div>

        {/* Factors breakdown */}
        {signal.factors && Object.keys(groupedFactors).length > 0 && (
          <div>
            <div className="text-xs text-zinc-500 mb-2">Factor Breakdown</div>
            <div className="space-y-2">
              {Object.entries(groupedFactors).map(([category, items]) => (
                <div key={category} className="bg-zinc-900 rounded p-2">
                  <div className="text-xs text-zinc-400 uppercase mb-1">{category}</div>
                  <div className="grid grid-cols-2 gap-x-4 gap-y-1">
                    {items.map(({ name, value, rawName }) => (
                      <div key={rawName} className="flex items-center justify-between text-xs">
                        <span className="text-zinc-300">{name}</span>
                        <span
                          className="font-mono"
                          style={{ color: FACTOR_CONFIGS[rawName]?.color || '#3b82f6' }}
                        >
                          {value.toFixed(2)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default BacktestChartModule;
