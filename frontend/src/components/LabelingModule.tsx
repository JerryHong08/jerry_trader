import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  type IChartApi,
  type ISeriesApi,
  type SeriesMarker,
  type Time,
  LineSeries,
  CandlestickSeries,
  HistogramSeries,
  createSeriesMarkers,
} from 'lightweight-charts';
import {
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  ChevronRight as ChevronRightIcon,
  Loader2,
  Maximize2,
  Plus,
  Search,
  Settings,
  X,
} from 'lucide-react';
import type { ModuleProps } from '../types';
import { useLightweightChart } from '../hooks/useLightweightChart';

const BACKTEST_API_URL = '/api/backtest';

const LABEL_COLORS: Record<string, string> = {
  '1': '#10b981',
  '0': '#ef4444',
  '-1': '#f59e0b',
  '': '#52525b',
};

const IGNITION_LABELS: Record<string, string> = {
  vol_exp: 'VolExp',
  range_breakout: 'RangeBrk',
  trade_rate_surge: 'RateSurge',
  sustained_activity: 'SustainAct',
  spread_collapse: 'Spread↓',
};

const IGNITION_COLORS: Record<string, string> = {
  vol_exp: '#f97316',
  range_breakout: '#a855f7',
  trade_rate_surge: '#06b6d4',
  sustained_activity: '#84cc16',
  spread_collapse: '#f59e0b',
};

// ─── Types ───

type LabelValue = '' | '1' | '0' | '-1';

interface CsvRow {
  date: string;
  ticker: string;
  label: string;
  label_notes: string;
  /** All raw values keyed by header name (strings). Numeric coercion happens on access. */
  _raw: Record<string, string>;
}

function csvNum(row: CsvRow, col: string): number {
  const v = row._raw[col];
  if (v === undefined || v === '') return NaN;
  return parseFloat(v);
}

function csvStr(row: CsvRow, col: string): string {
  return (row._raw[col] || '').trim();
}

// ─── Dynamic column discovery ───

type ColumnType = 'identity' | 'label' | 'numeric' | 'text';

interface ColumnMeta {
  header: string;
  type: ColumnType;
  shortLabel: string;
  isTimestamp?: boolean;
}

interface ColumnStats {
  min: number;
  max: number;
  mean: number;
  median: number;
  std: number;
  count: number;
}

const IDENTITY_COLS = new Set(['date', 'ticker']);
const LABEL_COLS = new Set(['label', 'label_notes']);

function formatMs(ms: number): string {
  const d = new Date(ms);
  const hh = d.getUTCHours().toString().padStart(2, '0');
  const mm = d.getUTCMinutes().toString().padStart(2, '0');
  const ss = d.getUTCSeconds().toString().padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

function discoverColumns(headers: string[], sampleRows: CsvRow[]): ColumnMeta[] {
  return headers.map((h) => {
    if (IDENTITY_COLS.has(h)) return { header: h, type: 'identity' as const, shortLabel: h === 'date' ? 'Date' : 'Ticker' };
    if (LABEL_COLS.has(h)) return { header: h, type: 'label' as const, shortLabel: h === 'label' ? 'Lbl' : 'Notes' };

    const sample = sampleRows.slice(0, 30);
    let allNumeric = true;
    let hasAny = false;
    for (const r of sample) {
      const v = r._raw[h];
      if (v === '' || v === undefined) continue;
      hasAny = true;
      if (isNaN(parseFloat(v))) { allNumeric = false; break; }
    }
    const type = (allNumeric && hasAny) ? 'numeric' : 'text';
    const parts = h.split('_');
    const short = parts.length > 1 ? parts.slice(-2).join('_') : h;
    const isTimestamp = h.endsWith('_ms') && type === 'numeric';
    return { header: h, type, shortLabel: short, isTimestamp };
  });
}

function computeColumnStats(rows: CsvRow[], col: string): ColumnStats | null {
  const vals: number[] = [];
  for (const r of rows) {
    const v = csvNum(r, col);
    if (!isNaN(v)) vals.push(v);
  }
  if (vals.length < 2) return null;
  vals.sort((a, b) => a - b);
  const n = vals.length;
  const sum = vals.reduce((a, b) => a + b, 0);
  const mean = sum / n;
  const median = n % 2 === 0 ? (vals[n / 2 - 1] + vals[n / 2]) / 2 : vals[Math.floor(n / 2)];
  const variance = vals.reduce((acc, v) => acc + (v - mean) ** 2, 0) / n;
  return { min: vals[0], max: vals[n - 1], mean, median, std: Math.sqrt(variance), count: n };
}

function numericColor(value: number, stats: ColumnStats): string {
  if (stats.max === stats.min) return '#a1a1aa';
  const t = (value - stats.min) / (stats.max - stats.min);
  // Blend red (#ef4444) at t=0 through neutral (#a1a1aa) at t=0.5 to green (#22c55e) at t=1
  const r = Math.round(239 + (34 - 239) * t);
  const g = Math.round(68 + (197 - 68) * t);
  const b = Math.round(68 + (94 - 68) * t);
  return `rgb(${r},${g},${b})`;
}

interface LabelState {
  label: LabelValue;
  notes: string;
  breakLabel: string;
}

interface FactorSpec {
  id: string;
  name: string;
  color: string;
  priceScale: 'left' | 'right';
  mode: 'overlay' | 'panel';
  source: 'bar' | 'tick';
}

interface PanelState {
  id: string;
  type: 'price' | 'panel';
  factorId?: string;
  collapsed: boolean;
}

interface FactorPoint {
  time: number;
  value: number;
}

interface OHLCVBar {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface ChartData {
  ticker: string;
  date: string;
  candidate: {
    entry_ms: number;
    entry_time: string;
    entry_price: number;
    gain_at_entry: number;
    prev_close: number;
    volume_at_entry: number;
    relative_volume: number;
    max_gain: number;
  };
  trades: Array<{ time: number; price: number; size: number }>;
  quotes: Array<{ time: number; bid: number; ask: number; bidSize: number; askSize: number }>;
  session_end_ms: number;
  ignition: Record<string, { ms: number; time: number; price: number } | null>;
  breakout_peaks: Record<string, { ms: number; time: number; price: number; pct: number } | null>;
  peak_metrics: {
    mfe_pct: number;
    mae_pct: number;
    final_pnl_pct: number;
    exit_reason: string;
    duration_min: number;
  } | null;
  trade_count: number;
  quote_count: number;
  tick_factors: Record<string, FactorPoint[]>;
  bars: OHLCVBar[];
  bar_factors: Record<string, FactorPoint[]>;
  timeframe: string;
}

interface CsvFileInfo {
  name: string;
  path: string;
  size: number;
  valid: boolean;
}

// ─── CSV Helpers ───

/** Deduplicate by time: keep last entry per timestamp (Lightweight Charts requires strict asc). */
function dedupByTime<T extends { time: number }>(data: T[]): T[] {
  const seen = new Map<number, T>();
  for (const item of data) {
    seen.set(item.time, item);
  }
  return Array.from(seen.values()).sort((a, b) => a.time - b.time);
}

function parseCsv(text: string): CsvRow[] {
  const lines = text.trim().split('\n');
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map((h) => h.trim());
  const rows: CsvRow[] = [];
  for (let i = 1; i < lines.length; i++) {
    // Split on commas, but only up to headers.length
    const parts = lines[i].split(',');
    const raw: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      raw[headers[j]] = (parts[j] || '').trim();
    }
    // Carry forward any trailing content as label_notes (commas inside notes)
    if (parts.length > headers.length) {
      raw['label_notes'] = parts.slice(headers.length - 1).join(',').trim();
    }
    const date = raw['date'] || '';
    const ticker = raw['ticker'] || '';
    const label = raw['label'] || '';
    if (!date || !ticker) continue;
    rows.push({
      date,
      ticker,
      label,
      label_notes: raw['label_notes'] || '',
      _raw: raw,
    });
  }
  return rows;
}

function rowKey(r: CsvRow): string {
  return `${r.date}:${r.ticker}`;
}

// ─── PricePanel ──────────────────────────────────────────────────────────────────

interface PricePanelProps {
  chartData: ChartData;
  chartApiRef: React.MutableRefObject<IChartApi | null>;
  priceMode: 'pct' | 'price';
  zoom: number;
  overlays: string[];
  availableFactors: FactorSpec[];
  /** Ref to all FactorPanel chart instances for direct imperative sync (no React state lag) */
  factorChartsRef: React.MutableRefObject<Map<string, IChartApi>>;
  /** Stored so new factor panels can immediately sync on registration */
  lastLogicalRangeRef: React.MutableRefObject<{ from: number; to: number } | null>;
}

function PricePanel({
  chartData,
  chartApiRef,
  priceMode,
  zoom,
  overlays,
  availableFactors,
  factorChartsRef,
  lastLogicalRangeRef,
}: PricePanelProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const { chartRef, chartReady } = useLightweightChart({
    container: containerRef,
    options: { timeScale: { secondsVisible: true } },
  });
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const entryLineRef = useRef<ISeriesApi<'Line'> | null>(null);
  const markersPluginRef = useRef<ReturnType<typeof createSeriesMarkers> | null>(null);
  const overlaySeriesRef = useRef<Map<string, ISeriesApi<'Line'>>>(new Map());
  const entryPrice = chartData.candidate.entry_price;
  // Guard against zero entry price which would produce Infinity in pct mode
  const safeEntryPrice = entryPrice && entryPrice > 0 ? entryPrice : 1;
  // Track previous state so we only fitContent on ticker change
  const prevBarsRef = useRef<OHLCVBar[] | null>(null);
  const prevPriceModeRef = useRef(priceMode);

  // Expose chart API for reset-zoom
  useEffect(() => {
    if (chartReady) chartApiRef.current = chartRef.current;
    return () => { chartApiRef.current = null; };
  }, [chartReady, chartRef, chartApiRef]);

  // Transform bar data based on priceMode
  const transformBar = useCallback(
    (bar: OHLCVBar) => {
      if (priceMode === 'pct') {
        const pct = (v: number) => ((v - safeEntryPrice) / safeEntryPrice) * 100;
        return {
          time: bar.time as Time,
          open: pct(bar.open),
          high: pct(bar.high),
          low: pct(bar.low),
          close: pct(bar.close),
        };
      }
      return {
        time: bar.time as Time,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
      };
    },
    [priceMode, safeEntryPrice]
  );

  const priceFormat = useMemo(
    () =>
      priceMode === 'pct'
        ? { type: 'custom' as const, formatter: (v: number) => v.toFixed(1) + '%', minMove: 0.01 }
        : { type: 'price' as const, precision: 2, minMove: 0.01 },
    [priceMode]
  );

  // Create series on chart ready
  useEffect(() => {
    if (!chartRef.current || !chartReady) return;
    const chart = chartRef.current;

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderUpColor: '#22c55e',
      borderDownColor: '#ef4444',
      wickUpColor: '#22c55e',
      wickDownColor: '#ef4444',
      priceFormat,
    });
    candleSeriesRef.current = candleSeries;

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    });
    chart.priceScale('').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    volumeSeriesRef.current = volumeSeries;

    const entrySeries = chart.addSeries(LineSeries, {
      color: '#10b981',
      lineWidth: 1,
      lineStyle: 2,
      lastValueVisible: false,
      priceFormat,
    });
    entryLineRef.current = entrySeries;

    // Timeline sync: since bars and factors share the same time grid after
    // server-side resampling, their logical indices are identical. Push the
    // price chart's logical range directly to factor charts — no time
    // conversion, no clamping issues.
    const syncHandler = (logicalRange: { from: number; to: number } | null) => {
      if (!logicalRange) return;
      lastLogicalRangeRef.current = logicalRange;
      factorChartsRef.current.forEach((factorChart) => {
        try {
          factorChart.timeScale().setVisibleLogicalRange(logicalRange);
        } catch { /* factor chart may not be ready yet */ }
      });
    };
    chart.timeScale().subscribeVisibleLogicalRangeChange(syncHandler);

    return () => {
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(syncHandler);
      markersPluginRef.current?.detach();
      markersPluginRef.current = null;
    };
  }, [chartReady]);

  // Update priceFormat on mode change
  useEffect(() => {
    if (!candleSeriesRef.current) return;
    candleSeriesRef.current.applyOptions({ priceFormat });
  }, [priceFormat]);

  // Render bars + entry line + markers
  useEffect(() => {
    if (!chartReady || !chartRef.current || !chartData.bars.length) return;
    const chart = chartRef.current;

    // Candles — deduplicate by time + filter out non-finite OHLC values
    const fin = (v: number) => Number.isFinite(v);
    const candleData = dedupByTime(
      chartData.bars.map(transformBar).filter(
        (b) => fin(b.open) && fin(b.high) && fin(b.low) && fin(b.close)
      )
    );
    if (candleData.length > 0 && candleSeriesRef.current) {
      candleSeriesRef.current.setData(candleData);
    }

    // Volume — also dedup by time
    const volumeData = dedupByTime(
      chartData.bars.map((bar) => ({
        time: bar.time as Time,
        value: fin(bar.volume) ? bar.volume : 0,
        color: bar.close >= bar.open ? '#22c55e40' : '#ef444440',
      }))
    );
    volumeSeriesRef.current?.setData(volumeData);

    // Entry line — single point if only one candle, otherwise span min→max
    if (fin(entryPrice) && candleData.length > 0) {
      const entryValue = priceMode === 'pct' ? 0 : entryPrice;
      const minTime = candleData[0].time;
      const maxTime = candleData[candleData.length - 1].time;
      const lineData = minTime === maxTime
        ? [{ time: minTime as Time, value: entryValue }]
        : [
            { time: minTime as Time, value: entryValue },
            { time: maxTime as Time, value: entryValue },
          ];
      entryLineRef.current?.setData(lineData);
    }

    // Ignition markers
    const markers: SeriesMarker<Time>[] = [];
    if (chartData.candidate.entry_ms) {
      markers.push({
        time: Math.floor(chartData.candidate.entry_ms / 1000) as Time,
        position: 'aboveBar',
        color: '#10b981',
        shape: 'arrowUp',
        text: 'E',
        size: 2,
      });
    }
    for (const [method, ig] of Object.entries(chartData.ignition)) {
      if (ig?.time) {
        markers.push({
          time: ig.time as Time,
          position: 'belowBar',
          color: IGNITION_COLORS[method] || '#f59e0b',
          shape: 'circle',
          text: IGNITION_LABELS[method] || method,
          size: 2,
        });
      }
    }
    // Breakout peak markers
    const BREAKOUT_PEAK_DEFS: Record<string, { label: string; color: string }> = {
      peak_1min: { label: '1m', color: '#3b82f6' },
      peak_3min: { label: '3m', color: '#06b6d4' },
      peak_5min: { label: '5m', color: '#ec4899' },
    };
    for (const [key, bp] of Object.entries(chartData.breakout_peaks || {})) {
      if (bp?.time) {
        const def = BREAKOUT_PEAK_DEFS[key] || { label: key, color: '#a1a1aa' };
        markers.push({
          time: bp.time as Time,
          position: 'aboveBar',
          color: def.color,
          shape: 'square',
          text: def.label,
          size: 2,
        });
      }
    }
    markers.sort((a, b) => (a.time as number) - (b.time as number));
    if (candleSeriesRef.current) {
      try {
        if (markersPluginRef.current) {
          markersPluginRef.current.setMarkers(markers);
        } else {
          markersPluginRef.current = createSeriesMarkers(candleSeriesRef.current, markers);
        }
      } catch (err) {
        // If plugin became stale (e.g. series recreated), detach and recreate
        console.warn('[ignition] marker update failed, recreating plugin:', err);
        try { markersPluginRef.current?.detach(); } catch { /* ignore */ }
        markersPluginRef.current = createSeriesMarkers(candleSeriesRef.current, markers);
      }
    }

    if (prevBarsRef.current !== chartData.bars) {
      prevBarsRef.current = chartData.bars;
      chart.timeScale().fitContent();
    }
  }, [chartReady, chartData.bars, priceMode, entryPrice, transformBar, chartData.ignition, chartData.candidate]);

  // On price-mode toggle, re-enable autoScale on the price scale so the
  // Y-axis re-fits to the new candle values. User zooming puts the price
  // scale into manual mode; this resets it without touching the X-axis.
  useEffect(() => {
    if (!chartRef.current || !chartReady) return;
    if (prevPriceModeRef.current === priceMode) return;
    prevPriceModeRef.current = priceMode;

    chartRef.current.priceScale('right').applyOptions({ autoScale: true });
  }, [priceMode, chartReady]);

  // Render overlay lines (EMA, VWAP) on the price chart
  useEffect(() => {
    if (!chartReady || !chartRef.current) return;
    const chart = chartRef.current;

    // Remove stale overlays
    overlaySeriesRef.current.forEach((series, id) => {
      if (!overlays.includes(id)) {
        chart.removeSeries(series);
        overlaySeriesRef.current.delete(id);
      }
    });

    for (const factorId of overlays) {
      const spec = availableFactors.find((f) => f.id === factorId);
      if (!spec) continue;

      const data = chartData.bar_factors[factorId];
      if (!data || data.length === 0) continue;

      if (!overlaySeriesRef.current.has(factorId)) {
        const series = chart.addSeries(LineSeries, {
          color: spec.color,
          lineWidth: 1,
          lastValueVisible: false,
          priceFormat,
        });
        overlaySeriesRef.current.set(factorId, series);
      }

      const series = overlaySeriesRef.current.get(factorId)!;
      const transformed = dedupByTime(data).map((d) => ({
        time: d.time as Time,
        value: spec.id === 'vwap_deviation' ? d.value : transformOverlayValue(d.value, spec, priceMode, entryPrice),
      }));
      series.setData(transformed);
    }
  }, [chartReady, overlays, chartData.bar_factors, priceMode, entryPrice, priceFormat, availableFactors]);

  return (
    <div
      ref={containerRef}
      className="h-full w-full"
      style={
        zoom !== 1
          ? { transform: `scale(${1 / zoom})`, transformOrigin: 'top left', width: `${zoom * 100}%`, height: `${zoom * 100}%` }
          : undefined
      }
    />
  );
}

/** Transform overlay values for % mode: price-based overlays (EMA, VWAP) become % from entry. */
function transformOverlayValue(value: number, spec: FactorSpec, priceMode: 'pct' | 'price', entryPrice: number): number {
  if (priceMode === 'pct' && spec.source === 'bar' && ['ema_20', 'vwap'].includes(spec.id)) {
    return ((value - entryPrice) / entryPrice) * 100;
  }
  return value;
}

// ─── FactorPanel ────────────────────────────────────────────────────────────────

interface FactorPanelProps {
  factorId: string;
  factorSpec: FactorSpec;
  chartData: ChartData;
  zoom: number;
  /** Callback to register this factor's chart for direct time-range sync from the price panel */
  onRegisterChart: (id: string, chart: IChartApi | null) => void;
}

function FactorPanel({ factorId, factorSpec, chartData, zoom, onRegisterChart }: FactorPanelProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const { chartRef, chartReady } = useLightweightChart({ container: containerRef });
  const seriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const syncRafRef = useRef<number>(0);

  // Get factor data from the right source
  const factorData = factorSpec.source === 'tick'
    ? chartData.tick_factors[factorId]
    : chartData.bar_factors[factorId];

  // Create series + set data, then sync via onRegisterChart after a rAF.
  // The rAF lets lightweight-charts' internal auto-fit (triggered by setData)
  // complete before we call setVisibleLogicalRange, otherwise the chart
  // remains locked to the right edge.
  useEffect(() => {
    if (!chartRef.current || !chartReady || !factorSpec) return;

    if (!seriesRef.current) {
      seriesRef.current = chartRef.current.addSeries(LineSeries, {
        color: factorSpec.color,
        lineWidth: 1,
        lastValueVisible: false,
      });
    }

    if (factorData && factorData.length > 0) {
      const points = dedupByTime(factorData).map((d) => ({
        time: d.time as Time,
        value: d.value,
      }));
      seriesRef.current.setData(points);
    } else {
      seriesRef.current.setData([]);
    }

    if (syncRafRef.current) cancelAnimationFrame(syncRafRef.current);
    syncRafRef.current = requestAnimationFrame(() => {
      syncRafRef.current = 0;
      if (chartRef.current) {
        onRegisterChart(factorId, chartRef.current);
      }
    });
  }, [chartReady, factorSpec, factorData, factorId, onRegisterChart]);

  // Unregister on unmount
  useEffect(() => {
    return () => {
      if (syncRafRef.current) cancelAnimationFrame(syncRafRef.current);
      onRegisterChart(factorId, null);
    };
  }, [factorId, onRegisterChart]);

  return (
    <div
      ref={containerRef}
      className="h-full w-full"
      style={
        zoom !== 1
          ? { transform: `scale(${1 / zoom})`, transformOrigin: 'top left', width: `${zoom * 100}%`, height: `${zoom * 100}%` }
          : undefined
      }
    />
  );
}

// ─── Panel Header ────────────────────────────────────────────────────────────────

function PanelHeader({
  title,
  color,
  collapsed,
  canClose,
  onToggleCollapse,
  onClose,
  rightContent,
}: {
  title: string;
  color?: string;
  collapsed: boolean;
  canClose?: boolean;
  onToggleCollapse: () => void;
  onClose?: () => void;
  rightContent?: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between px-2 py-0.5 bg-zinc-800 border-b border-zinc-700 select-none flex-shrink-0">
      <div className="flex items-center gap-1">
        {color && <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />}
        <button onClick={onToggleCollapse} className="p-0.5 hover:bg-zinc-700 rounded" title={collapsed ? 'Expand' : 'Collapse'}>
          {collapsed ? <ChevronRightIcon className="w-3 h-3 text-zinc-400" /> : <ChevronDown className="w-3 h-3 text-zinc-400" />}
        </button>
        <span className="text-2xs font-medium text-zinc-300">{title}</span>
      </div>
      <div className="flex items-center gap-1">
        {rightContent}
        {canClose && onClose && (
          <button onClick={onClose} className="p-0.5 hover:bg-zinc-700 rounded" title="Close panel">
            <X className="w-3 h-3 text-zinc-500 hover:text-red-400" />
          </button>
        )}
      </div>
    </div>
  );
}

// ─── Main Component ──────────────────────────────────────────────────────────────

export default function LabelingModule({ moduleId, onRemove, zoom = 1, settings, onSettingsChange }: ModuleProps) {
  const [rows, setRows] = useState<CsvRow[]>([]);
  const [labels, setLabels] = useState<Record<string, LabelState>>({});
  const [currentIndex, setCurrentIndex] = useState(0);
  const [chartData, setChartData] = useState<ChartData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [dateFilter, setDateFilter] = useState('');
  const [labelFilter, setLabelFilter] = useState('all');
  const [sortColumn, setSortColumn] = useState('date');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [priceMode, setPriceMode] = useState<'pct' | 'price'>('pct');
  const [csvFiles, setCsvFiles] = useState<CsvFileInfo[]>([]);
  const [csvFileName, setCsvFileName] = useState('');
  const [autoLoadAttempted, setAutoLoadAttempted] = useState(false);
  const [igParams, setIgParams] = useState({
    spread_bps: 100,
    vol_exp_ratio: 3.0,
    surge_sigma: 3.0,
    sustained_min_trades: 3,
  });
  const chartApiRef = useRef<IChartApi | null>(null);

  // Frontend cache of full chart data responses — revisiting a ticker is instant
  const chartDataCacheRef = useRef<Map<string, ChartData>>(new Map());
  // Track which (date:ticker) keys have been submitted for backend pre-fetch
  const prefetchedRef = useRef<Set<string>>(new Set());

  // Registry of all FactorPanel chart instances for direct imperative time-range sync
  const factorChartsRef = useRef<Map<string, IChartApi>>(new Map());

  // Factor specs from backend
  const [availableFactors, setAvailableFactors] = useState<FactorSpec[]>([]);

  // Panel state — restored from layout config settings
  const [panels, setPanels] = useState<PanelState[]>(() => {
    const saved = settings?.labeling?.panels;
    if (saved && saved.length > 0) {
      return saved
        .filter((p) => p.visible !== false)
        .map((p) => ({ id: p.id, type: p.type, factorId: p.id !== 'price' ? p.id : undefined, collapsed: p.collapsed }));
    }
    return [{ id: 'price', type: 'price', collapsed: false }];
  });
  const [showAddPanel, setShowAddPanel] = useState(false);
  const [showIgParams, setShowIgParams] = useState(false);
  const [igDraft, setIgDraft] = useState(igParams);
  // Keep draft in sync when params are reset externally
  useEffect(() => { setIgDraft(igParams); }, [igParams]);

  // Active overlays on the price panel — restored from layout config
  const [activeOverlays, setActiveOverlays] = useState<string[]>(() => {
    const pricePanel = settings?.labeling?.panels?.find((p) => p.id === 'price');
    return pricePanel?.overlays ?? ['ema_20', 'vwap'];
  });

  // ── Dynamic sidebar state ──
  const [columnMeta, setColumnMeta] = useState<ColumnMeta[]>([]);
  const [colVis, setColVis] = useState<Record<string, boolean>>({});
  const [showColumnMenu, setShowColumnMenu] = useState(false);
  const columnMenuRef = useRef<HTMLDivElement>(null);
  const [statsTooltip, setStatsTooltip] = useState<{ col: string; x: number; y: number } | null>(null);

  // Compute column stats for all numeric feature columns
  const columnStats = useMemo(() => {
    const stats: Record<string, ColumnStats> = {};
    for (const col of columnMeta) {
      if (col.type === 'numeric') {
        const s = computeColumnStats(rows, col.header);
        if (s) stats[col.header] = s;
      }
    }
    return stats;
  }, [rows, columnMeta]);

  // Visible non-pinned columns for sidebar rendering
  const visibleFeatureCols = useMemo(
    () => columnMeta.filter((c) => c.type !== 'identity' && c.type !== 'label' && colVis[c.header] !== false),
    [columnMeta, colVis],
  );

  // Keep onSettingsChange in a ref so it doesn't trigger useEffect re-runs
  const onSettingsChangeRef = useRef(onSettingsChange);
  onSettingsChangeRef.current = onSettingsChange;

  // Auto-persist whenever panels or overlays change
  useEffect(() => {
    const configPanels = panels.map((p) => ({
      id: p.id,
      type: p.type,
      collapsed: p.collapsed,
      visible: true,
      ...(p.type === 'price' ? { overlays: activeOverlays } : {}),
    }));
    onSettingsChangeRef.current?.({ labeling: { panels: configPanels } });
  }, [panels, activeOverlays]);

  // Last synced logical range — applied immediately when a new factor chart registers
  const lastLogicalRangeRef = useRef<{ from: number; to: number } | null>(null);

  // Stable callback for FactorPanel to register/unregister its chart.
  // Applies the last known logical range immediately so newly-added panels
  // sync without waiting for the next user scroll/zoom.
  const registerFactorChart = useCallback((id: string, chart: IChartApi | null) => {
    if (chart) {
      factorChartsRef.current.set(id, chart);
      const lr = lastLogicalRangeRef.current;
      if (lr) {
        try {
          chart.timeScale().setVisibleLogicalRange(lr);
        } catch { /* chart not ready yet */ }
      }
    } else {
      factorChartsRef.current.delete(id);
    }
  }, []);

  // ── Load factor specs ──
  useEffect(() => {
    fetch(`${BACKTEST_API_URL}/label/factors`)
      .then((r) => r.json())
      .then((data: { factors: FactorSpec[] }) => setAvailableFactors(data.factors))
      .catch(() => {
        // Fallback defaults
        setAvailableFactors([
          { id: 'ema_20', name: 'EMA(20)', color: '#3b82f6', priceScale: 'right', mode: 'overlay', source: 'bar' },
          { id: 'vwap', name: 'VWAP', color: '#f59e0b', priceScale: 'right', mode: 'overlay', source: 'bar' },
          { id: 'vwap_deviation', name: 'VWAP Dev%', color: '#ec4899', priceScale: 'right', mode: 'panel', source: 'bar' },
          { id: 'relative_volume', name: 'RelVol', color: '#f97316', priceScale: 'right', mode: 'panel', source: 'bar' },
          { id: 'trade_rate', name: 'TradeRate', color: '#f97316', priceScale: 'right', mode: 'panel', source: 'tick' },
          { id: 'large_trade_ratio', name: 'LargeTrade', color: '#a855f7', priceScale: 'right', mode: 'panel', source: 'tick' },
          { id: 'aggressor_ratio', name: 'Aggressor', color: '#06b6d4', priceScale: 'right', mode: 'panel', source: 'tick' },
        ]);
      });
  }, []);

  // ── Derived state ──
  const availableDates = useMemo(() => {
    const dates = new Set(rows.map((r) => r.date));
    return Array.from(dates).sort();
  }, [rows]);

  const labelStats = useMemo(() => {
    let total = rows.length;
    let labeled = 0;
    let trueMomo = 0;
    let falseMomo = 0;
    let skipped = 0;
    for (const r of rows) {
      const key = rowKey(r);
      const lbl = labels[key]?.label || r.label || '';
      if (lbl !== '') {
        labeled++;
        if (lbl === '1') trueMomo++;
        else if (lbl === '0') falseMomo++;
        else if (lbl === '-1') skipped++;
      }
    }
    return { total, labeled, trueMomo, falseMomo, skipped };
  }, [rows, labels]);

  const filteredRows = useMemo(() => {
    let result = rows.filter((r) => {
      const key = rowKey(r);
      const lbl = labels[key]?.label ?? r.label ?? '';
      if (dateFilter && r.date !== dateFilter) return false;
      if (labelFilter === 'unlabeled' && lbl !== '') return false;
      if (labelFilter === 'true' && lbl !== '1') return false;
      if (labelFilter === 'false' && lbl !== '0') return false;
      if (labelFilter === 'skip' && lbl !== '-1') return false;
      if (searchQuery) {
        const q = searchQuery.toLowerCase();
        if (!r.ticker.toLowerCase().includes(q)) return false;
      }
      return true;
    });
    // Sort by any discovered column
    const col = columnMeta.find((c) => c.header === sortColumn);
    if (col) {
      result = [...result].sort((a, b) => {
        let va: string | number, vb: string | number;
        if (col.header === 'ticker') {
          va = a.ticker; vb = b.ticker;
        } else if (col.header === 'date') {
          va = a.date; vb = b.date;
        } else if (col.type === 'numeric') {
          va = csvNum(a, col.header); vb = csvNum(b, col.header);
          if (isNaN(va) && isNaN(vb)) return 0;
          if (isNaN(va)) return 1;
          if (isNaN(vb)) return -1;
        } else {
          va = csvStr(a, col.header); vb = csvStr(b, col.header);
        }
        const cmp = va < vb ? -1 : va > vb ? 1 : 0;
        return sortDir === 'asc' ? cmp : -cmp;
      });
    }
    return result;
  }, [rows, labels, dateFilter, labelFilter, searchQuery, sortColumn, sortDir, columnMeta]);

  const currentRow = filteredRows[currentIndex] || null;

  // ── Auto-load CSV files from backend on mount ──
  useEffect(() => {
    if (autoLoadAttempted) return;
    fetch(`${BACKTEST_API_URL}/label/files`)
      .then((r) => r.json())
      .then((data: { files: CsvFileInfo[]; data_dir: string }) => {
        setCsvFiles(data.files.filter((f) => f.valid));
        const firstValid = data.files.find((f) => f.valid);
        if (firstValid) loadCsvFile(firstValid.name);
      })
      .catch(() => {})
      .finally(() => setAutoLoadAttempted(true));
  }, [autoLoadAttempted]);

  const loadCsvFile = useCallback((filename: string) => {
    setCsvFileName(filename);
    // Clear caches when loading a new file
    chartDataCacheRef.current.clear();
    prefetchedRef.current.clear();
    fetch(`${BACKTEST_API_URL}/label/file?filename=${encodeURIComponent(filename)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.json();
      })
      .then((data: { filename: string; content: string }) => {
        const parsed = parseCsv(data.content);
        setRows(parsed);
        const restored: Record<string, LabelState> = {};
        for (const r of parsed) {
          const lbl = r.label;
          const blk = (r._raw['break_label'] || '').trim();
          if (lbl && ['1', '0', '-1'].includes(lbl)) {
            restored[rowKey(r)] = { label: lbl as LabelValue, notes: r.label_notes, breakLabel: blk };
          } else if (blk) {
            restored[rowKey(r)] = { label: '' as LabelValue, notes: '', breakLabel: blk };
          }
        }
        setLabels((prev) => ({ ...restored, ...prev }));
        setCurrentIndex(0);
        setError(null);

        // Discover columns from CSV content
        const headerLine = data.content.split('\n')[0];
        const headers = headerLine.split(',').map((h: string) => h.trim());
        const meta = discoverColumns(headers, parsed);
        setColumnMeta(meta);

        // Reconcile visibility with saved settings
        const savedVis = settings?.labeling?.columnVis || {};
        const merged: Record<string, boolean> = {};
        for (const col of meta) {
          if (col.type === 'identity' || col.type === 'label') {
            merged[col.header] = true;
          } else {
            merged[col.header] = savedVis[col.header] !== undefined ? savedVis[col.header] : true;
          }
        }
        setColVis(merged);
      })
      .catch((e) => setError(`Failed to load ${filename}: ${e.message}`));
  }, []);

  // ── Column visibility management ──
  const toggleColumn = useCallback((header: string) => {
    setColVis((prev) => {
      const next = { ...prev, [header]: !prev[header] };
      const persisted: Record<string, boolean> = {};
      for (const col of columnMeta) {
        if (col.type !== 'identity' && col.type !== 'label') {
          persisted[col.header] = next[col.header];
        }
      }
      onSettingsChangeRef.current?.({ labeling: { ...settings?.labeling, columnVis: persisted } });
      return next;
    });
  }, [columnMeta, settings?.labeling]);

  // ── Labeling (auto-saves to backend) ──
  const saveLabelToBackend = useCallback(
    (key: string, label: LabelValue, notes: string, breakLabel?: string) => {
      if (!csvFileName) return;
      fetch(`${BACKTEST_API_URL}/label/save`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename: csvFileName, key, label, notes, break_label: breakLabel || '' }),
      }).catch(() => {});
    },
    [csvFileName]
  );

  const applyLabel = useCallback(
    (value: LabelValue) => {
      if (!currentRow) return;
      const key = rowKey(currentRow);
      const notes = labels[key]?.notes || '';
      const breakLabel = labels[key]?.breakLabel || '';
      setLabels((prev) => ({ ...prev, [key]: { label: value, notes: prev[key]?.notes || '', breakLabel } }));
      saveLabelToBackend(key, value, notes, breakLabel);
      // When unlabeled filter is active, the current ticker will disappear from
      // filteredRows on re-render — don't explicitly jump, or it'll double-jump.
      // A useEffect will clamp currentIndex if it goes out of bounds.
      if (labelFilter === 'unlabeled') return;
      if (value !== '') {
        setCurrentIndex((i) => Math.min(filteredRows.length - 1, i + 1));
      }
    },
    [currentRow, filteredRows.length, labels, labelFilter, saveLabelToBackend]
  );

  // Clamp currentIndex when filteredRows shrinks (e.g., after labeling under unlabeled filter)
  const prevFilteredLenRef = useRef(filteredRows.length);
  useEffect(() => {
    if (filteredRows.length === 0) return;
    if (filteredRows.length < prevFilteredLenRef.current) {
      setCurrentIndex((i) => Math.min(i, filteredRows.length - 1));
    }
    prevFilteredLenRef.current = filteredRows.length;
  }, [filteredRows.length]);

  const setNotes = useCallback(
    (notes: string) => {
      if (!currentRow) return;
      const key = rowKey(currentRow);
      const label = labels[key]?.label ?? (currentRow.label as LabelValue);
      const breakLabel = labels[key]?.breakLabel ?? '';
      setLabels((prev) => ({ ...prev, [key]: { label, notes, breakLabel } }));
      saveLabelToBackend(key, label, notes, breakLabel);
    },
    [currentRow, labels, saveLabelToBackend]
  );

  // ── Chart fetch (frontend cache + backend pre-fetch) ──

  // Fire-and-forget: warm backend cache for the next BATCH_SIZE tickers so
  // subsequent single-ticker requests skip ClickHouse entirely.
  const prefetchNearby = useCallback(
    (row: CsvRow) => {
      const idx = rows.findIndex((r) => rowKey(r) === rowKey(row));
      if (idx === -1) return;

      const BATCH_SIZE = 10;
      const end = Math.min(rows.length, idx + BATCH_SIZE);

      const toPrefetch: string[] = [];
      for (let i = idx + 1; i < end; i++) {
        const r = rows[i];
        if (!r) continue;
        const key = rowKey(r);
        if (chartDataCacheRef.current.has(key)) continue;
        if (prefetchedRef.current.has(key)) continue;
        toPrefetch.push(r.ticker);
        prefetchedRef.current.add(key);
      }

      if (toPrefetch.length === 0) return;

      fetch(`${BACKTEST_API_URL}/label/prefetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ date: row.date, tickers: toPrefetch }),
      }).catch(() => {});
    },
    [rows]
  );

  // Track previous igParams to bypass cache on param change
  const prevIgParamsRef = useRef(igParams);

  useEffect(() => {
    if (!currentRow) { setChartData(null); return; }
    const key = rowKey(currentRow);

    const paramsChanged = (
      prevIgParamsRef.current.spread_bps !== igParams.spread_bps ||
      prevIgParamsRef.current.vol_exp_ratio !== igParams.vol_exp_ratio ||
      prevIgParamsRef.current.surge_sigma !== igParams.surge_sigma ||
      prevIgParamsRef.current.sustained_min_trades !== igParams.sustained_min_trades
    );
    prevIgParamsRef.current = igParams;

    // Frontend cache hit — instant navigation to previously visited tickers
    // Bypass cache when ignition params changed (need fresh data from backend)
    if (!paramsChanged) {
      const cached = chartDataCacheRef.current.get(key);
      if (cached) {
        setChartData(cached);
        setError(null);
        prefetchNearby(currentRow);
        return;
      }
    }

    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`${BACKTEST_API_URL}/label/ticker/${currentRow.ticker}?date=${currentRow.date}&spread_bps=${igParams.spread_bps}&vol_exp_ratio=${igParams.vol_exp_ratio}&surge_sigma=${igParams.surge_sigma}&sustained_min_trades=${igParams.sustained_min_trades}`)
      .then((resp) => {
        if (!resp.ok) throw new Error(`${resp.status}`);
        return resp.json();
      })
      .then((data: ChartData) => {
        if (!cancelled) {
          chartDataCacheRef.current.set(key, data);
          setChartData(data);
          prefetchNearby(currentRow);
        }
      })
      .catch((e: Error) => { if (!cancelled) { setError(e.message); setChartData(null); } })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [currentRow, prefetchNearby, igParams]);

  // Toggle break_label — independent of MOMO label
  const toggleBreakLabel = useCallback(() => {
    if (!currentRow) return;
    const key = rowKey(currentRow);
    const label = labels[key]?.label ?? (currentRow.label as LabelValue) ?? '';
    const notes = labels[key]?.notes ?? '';
    const current = labels[key]?.breakLabel ?? '';
    const next = current === '1' ? '' : '1';
    setLabels((prev) => ({ ...prev, [key]: { label, notes, breakLabel: next } }));
    saveLabelToBackend(key, label, notes, next);
  }, [currentRow, labels, saveLabelToBackend]);

  // ── Keyboard shortcuts ──
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      switch (e.key) {
        case 'ArrowLeft': e.preventDefault(); setCurrentIndex((i) => Math.max(0, i - 1)); break;
        case 'ArrowRight': e.preventDefault(); setCurrentIndex((i) => Math.min(filteredRows.length - 1, i + 1)); break;
        case '1': applyLabel('1'); break;
        case '2': applyLabel('0'); break;
        case '3': applyLabel('-1'); break;
        case '0': applyLabel(''); break;
        case 'b': toggleBreakLabel(); break;
        case 'r': chartApiRef.current?.timeScale().fitContent(); break;
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [filteredRows.length, applyLabel, toggleBreakLabel]);

  // Close column menu on click outside
  useEffect(() => {
    if (!showColumnMenu) return;
    const handler = (e: MouseEvent) => {
      if (columnMenuRef.current && !columnMenuRef.current.contains(e.target as Node)) {
        setShowColumnMenu(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showColumnMenu]);

  // ── Panel management ──
  const togglePanelCollapse = useCallback((panelId: string) => {
    setPanels((prev) => prev.map((p) => p.id === panelId ? { ...p, collapsed: !p.collapsed } : p));
  }, []);

  const closePanel = useCallback((panelId: string) => {
    setPanels((prev) => prev.filter((p) => p.id !== panelId));
  }, []);

  const isPanelActive = useCallback((factorId: string) =>
    panels.some((p) => p.id === factorId),
  [panels]);

  const toggleOverlay = useCallback((factorId: string) => {
    setActiveOverlays((prev) =>
      prev.includes(factorId) ? prev.filter((id) => id !== factorId) : [...prev, factorId]
    );
  }, []);

  const addFactorPanel = useCallback((factor: FactorSpec) => {
    if (factor.mode === 'overlay') {
      toggleOverlay(factor.id);
    } else {
      setPanels((prev) => {
        if (prev.some((p) => p.id === factor.id)) {
          return prev.map((p) => p.id === factor.id ? { ...p, collapsed: false } : p);
        }
        return [...prev, { id: factor.id, type: 'panel', factorId: factor.id, collapsed: false }];
      });
    }
    setShowAddPanel(false);
  }, [toggleOverlay]);

  // ── Current label ──
  const currentLabelValue = currentRow
    ? labels[rowKey(currentRow)]?.label ?? (currentRow.label as LabelValue) ?? ''
    : '';
  const currentBreakLabel = currentRow
    ? labels[rowKey(currentRow)]?.breakLabel ?? ''
    : '';
  const currentNotes = currentRow
    ? labels[rowKey(currentRow)]?.notes ?? currentRow.label_notes ?? ''
    : '';

  // Filter available factors for the Add Panel dropdown
  const overlayFactors = availableFactors.filter((f) => f.mode === 'overlay');
  const addablePanelFactors = availableFactors.filter((f) => f.mode === 'panel' && !isPanelActive(f.id));

  // Active panel factor specs
  const activePanelFactors = useMemo(
    () => panels.filter((p) => p.type === 'panel').map((p) => ({
      panel: p,
      spec: availableFactors.find((f) => f.id === p.factorId),
    })).filter((x) => x.spec),
    [panels, availableFactors]
  );

  const pricePanelCollapsed = panels.find((p) => p.id === 'price')?.collapsed ?? false;

  // ── Render ──
  return (
    <div className="h-full flex flex-col bg-zinc-950 text-zinc-200 select-none">
      {/* Header */}
      <div className="h-9 px-3 border-b border-zinc-800 flex items-center gap-2 flex-shrink-0">
        {csvFiles.length > 0 ? (
          <select
            value={csvFileName}
            onChange={(e) => loadCsvFile(e.target.value)}
            className="text-xs bg-zinc-800 border border-zinc-700 rounded px-2 py-1 text-zinc-300 max-w-[200px]"
          >
            <option value="">Select CSV...</option>
            {csvFiles.map((f) => (
              <option key={f.name} value={f.name}>{f.name}</option>
            ))}
          </select>
        ) : (
          <span className="text-xs text-zinc-500">
            {autoLoadAttempted ? 'No CSV files found' : 'Scanning...'}
          </span>
        )}
        <span className="text-xs text-zinc-500 truncate">{csvFileName || 'No file'}</span>
        <span className="ml-auto text-xs text-zinc-500">
          {labelStats.labeled}/{labelStats.total} labeled
          {labelStats.trueMomo > 0 && ` | ${labelStats.trueMomo}T`}
          {labelStats.falseMomo > 0 && ` ${labelStats.falseMomo}F`}
          {labelStats.skipped > 0 && ` ${labelStats.skipped}S`}
        </span>
        <button onClick={onRemove} className="text-xs px-2 py-1 text-zinc-500 hover:text-zinc-300" title="Remove module">×</button>
      </div>

      {/* Body */}
      <div className="flex-1 flex min-h-0">
        {/* Left Sidebar */}
        <div className="w-[420px] border-r border-zinc-800 flex flex-col flex-shrink-0">
          <div className="p-2 space-y-1.5 border-b border-zinc-800">
            <select
              value={dateFilter}
              onChange={(e) => { setDateFilter(e.target.value); setCurrentIndex(0); }}
              className="w-full text-xs bg-zinc-800 border border-zinc-700 rounded px-2 py-1 text-zinc-300"
            >
              <option value="">All dates</option>
              {availableDates.map((d) => <option key={d} value={d}>{d}</option>)}
            </select>
            <div className="flex gap-0.5">
              {(['all', 'unlabeled', 'true', 'false', 'skip'] as const).map((f) => (
                <button
                  key={f}
                  onClick={() => { setLabelFilter(f); setCurrentIndex(0); }}
                  className={`flex-1 text-[10px] px-1 py-0.5 rounded ${
                    labelFilter === f ? 'bg-zinc-600 text-white' : 'bg-zinc-800 text-zinc-500 hover:bg-zinc-700'
                  }`}
                >
                  {f === 'all' ? 'All' : f === 'unlabeled' ? 'Unlabeled' : f === 'true' ? 'True' : f === 'false' ? 'False' : 'Skip'}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-1 relative">
              <div className="flex items-center gap-1 bg-zinc-800 border border-zinc-700 rounded px-2 py-1 text-xs flex-1">
                <Search className="w-3 h-3 text-zinc-500" />
                <input
                  type="text"
                  placeholder="Search ticker..."
                  value={searchQuery}
                  onChange={(e) => { setSearchQuery(e.target.value); setCurrentIndex(0); }}
                  className="bg-transparent border-none outline-none flex-1 text-zinc-300 placeholder:text-zinc-600"
                />
              </div>
              <button
                onClick={() => setShowColumnMenu(!showColumnMenu)}
                className={`p-1 rounded hover:bg-zinc-700 ${showColumnMenu ? 'bg-zinc-700' : ''}`}
                title="Manage columns"
              >
                <Settings className="w-3 h-3 text-zinc-500" />
              </button>
              {/* Column visibility popover */}
              {showColumnMenu && (
                <div ref={columnMenuRef} className="absolute top-full right-0 mt-1 bg-zinc-800 border border-zinc-700 rounded shadow-lg z-50 max-h-[400px] overflow-y-auto min-w-[200px]">
                  <div className="p-2 border-b border-zinc-700 flex gap-2">
                    <button onClick={() => {
                      const next: Record<string, boolean> = {};
                      for (const c of columnMeta) next[c.header] = true;
                      setColVis(next);
                    }} className="text-[10px] text-blue-400 hover:text-blue-300">Show All</button>
                    <button onClick={() => {
                      const next: Record<string, boolean> = {};
                      for (const c of columnMeta) next[c.header] = (c.type === 'identity' || c.type === 'label');
                      setColVis(next);
                      const persisted: Record<string, boolean> = {};
                      for (const c of columnMeta) {
                        if (c.type !== 'identity' && c.type !== 'label') persisted[c.header] = false;
                      }
                      onSettingsChangeRef.current?.({ labeling: { ...settings?.labeling, columnVis: persisted } });
                    }} className="text-[10px] text-zinc-400 hover:text-zinc-300">Hide All</button>
                  </div>
                  <div className="px-2 py-1 text-[10px] text-zinc-600 uppercase">Fixed</div>
                  {columnMeta.filter(c => c.type === 'identity' || c.type === 'label').map(c => (
                    <label key={c.header} className="flex items-center gap-1 px-2 py-0.5 text-xs text-zinc-500">
                      <input type="checkbox" checked disabled className="opacity-50 w-3 h-3" />
                      {c.shortLabel}
                    </label>
                  ))}
                  <div className="px-2 py-1 text-[10px] text-zinc-600 uppercase border-t border-zinc-700">Features</div>
                  {columnMeta.filter(c => c.type !== 'identity' && c.type !== 'label' && c.type !== 'text').map(c => (
                    <label key={c.header} className="flex items-center gap-1 px-2 py-0.5 text-xs text-zinc-300 hover:bg-zinc-700 cursor-pointer">
                      <input type="checkbox" checked={colVis[c.header] !== false}
                             onChange={() => toggleColumn(c.header)}
                             className="w-3 h-3" />
                      <span className="truncate text-[11px]" title={c.header}>{c.header}</span>
                      {c.type === 'numeric' && <span className="text-zinc-600 text-[10px] ml-auto">#</span>}
                    </label>
                  ))}
                  {columnMeta.filter(c => c.type === 'text').map(c => (
                    <label key={c.header} className="flex items-center gap-1 px-2 py-0.5 text-xs text-zinc-400 hover:bg-zinc-700 cursor-pointer">
                      <input type="checkbox" checked={colVis[c.header] !== false}
                             onChange={() => toggleColumn(c.header)}
                             className="w-3 h-3" />
                      <span className="truncate text-[11px]" title={c.header}>{c.header}</span>
                      <span className="text-zinc-600 text-[10px] ml-auto">T</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Column headers + ticker list — native table with sticky pinned columns */}
          <div className="flex-1 overflow-auto">
            <table className="w-max min-w-full text-xs">
              <thead className="sticky top-0 z-20 text-[10px] text-zinc-500 bg-zinc-900">
                <tr>
                  {/* Pinned column headers */}
                  <th className="sticky top-0 left-0 z-30 bg-zinc-900 w-[20px]" />
                  <th className="sticky top-0 z-30 bg-zinc-900 text-left font-medium" style={{ left: 20, minWidth: 52 }}>
                    {(() => { const active = sortColumn === 'ticker'; const arrow = active ? (sortDir === 'asc' ? '▲' : '▼') : ''; return (
                      <button onClick={() => { if (sortColumn === 'ticker') { setSortDir((d) => (d === 'asc' ? 'desc' : 'asc')); } else { setSortColumn('ticker'); setSortDir('desc'); setCurrentIndex(0); } }} className="hover:text-zinc-300">Ticker{arrow}</button>
                    ); })()}
                  </th>
                  <th className="sticky top-0 z-30 bg-zinc-900 w-[16px]" style={{ left: 72 }} />
                  <th className="sticky top-0 z-30 bg-zinc-900 w-[44px] text-right" style={{ left: 88 }}>
                    {(() => { const active = sortColumn === 'date'; const arrow = active ? (sortDir === 'asc' ? '▲' : '▼') : ''; return (
                      <button onClick={() => { if (sortColumn === 'date') { setSortDir((d) => (d === 'asc' ? 'desc' : 'asc')); } else { setSortColumn('date'); setSortDir('desc'); setCurrentIndex(0); } }} className="hover:text-zinc-300">D{arrow}</button>
                    ); })()}
                  </th>
                  <th className="sticky top-0 z-30 bg-zinc-900 w-[24px] text-center" style={{ left: 132 }}>
                    {(() => { const active = sortColumn === 'label'; const arrow = active ? (sortDir === 'asc' ? '▲' : '▼') : ''; return (
                      <button onClick={() => { if (sortColumn === 'label') { setSortDir((d) => (d === 'asc' ? 'desc' : 'asc')); } else { setSortColumn('label'); setSortDir('desc'); setCurrentIndex(0); } }} className="hover:text-zinc-300">L{arrow}</button>
                    ); })()}
                  </th>
                  {/* Feature column headers */}
                  {visibleFeatureCols.map((col) => {
                    const active = sortColumn === col.header;
                    const arrow = active ? (sortDir === 'asc' ? '▲' : '▼') : '';
                    const thClass = col.isTimestamp ? 'text-right font-normal px-0.5 bg-zinc-900' : 'w-14 text-right font-normal px-0.5 bg-zinc-900';
                    return (
                      <th key={col.header} className={thClass}>
                        <button
                          onClick={() => { if (sortColumn === col.header) { setSortDir((d) => (d === 'asc' ? 'desc' : 'asc')); } else { setSortColumn(col.header); setSortDir('desc'); setCurrentIndex(0); } }}
                          className="tabular-nums hover:text-zinc-300"
                          onMouseEnter={(e) => { if (!col.isTimestamp) { const rect = e.currentTarget.getBoundingClientRect(); setStatsTooltip({ col: col.header, x: rect.left, y: rect.bottom }); } }}
                          onMouseLeave={() => setStatsTooltip(null)}
                        >
                          {col.shortLabel}{arrow}
                        </button>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {filteredRows.map((row, i) => {
                  const key = rowKey(row);
                  const lbl = labels[key]?.label ?? row.label ?? '';
                  const isCurrent = i === currentIndex;
                  const hasNotes = !!(labels[key]?.notes || row.label_notes);
                  const hasBreak = !!(labels[key]?.breakLabel) || (row._raw['break_label'] === '1');
                  const stickyBg = isCurrent ? 'rgb(30 58 138 / 0.5)' : '#09090b';
                  const labelCode = lbl === '1' ? 'T' : lbl === '0' ? 'F' : lbl === '-1' ? 'S' : '-';
                  return (
                    <tr
                      key={key}
                      onClick={() => setCurrentIndex(i)}
                      className={`cursor-pointer border-b border-zinc-900 ${isCurrent ? 'bg-blue-900/40' : 'hover:bg-zinc-800/50'}`}
                    >
                      <td className="sticky left-0 z-10 px-0.5" style={{ backgroundColor: stickyBg }}>
                        <span className="w-2 h-2 rounded-full inline-block align-middle" style={{ backgroundColor: LABEL_COLORS[lbl] || LABEL_COLORS[''] }} />
                      </td>
                      <td className="sticky z-10 font-medium truncate max-w-[60px]" style={{ left: 20, backgroundColor: stickyBg }}>
                        {row.ticker}
                      </td>
                      <td className="sticky z-10" style={{ left: 72, backgroundColor: stickyBg }}>
                        <span className="flex gap-0.5">
                          {hasNotes && <span className="text-amber-400 text-[8px] font-bold" title={labels[key]?.notes || row.label_notes}>N</span>}
                          {hasBreak && <span className="text-cyan-400 text-[8px] font-bold" title="Breakout">B</span>}
                        </span>
                      </td>
                      <td className="sticky z-10 w-[44px] text-right tabular-nums text-zinc-600" style={{ left: 88, backgroundColor: stickyBg }}>
                        {row.date.slice(5)}
                      </td>
                      <td className="sticky z-10 w-[24px] text-center tabular-nums text-zinc-400" style={{ left: 132, backgroundColor: stickyBg }}>
                        {labelCode}
                      </td>
                      {visibleFeatureCols.map((col) => {
                        const val = csvNum(row, col.header);
                        const stats = columnStats[col.header];
                        const color = (!col.isTimestamp && stats && !isNaN(val)) ? numericColor(val, stats) : undefined;
                        const display = col.isTimestamp && !isNaN(val)
                          ? formatMs(val)
                          : !isNaN(val) ? val.toFixed(val < 1 ? 3 : val < 10 ? 2 : 1) : '-';
                        const title = col.isTimestamp && !isNaN(val)
                          ? new Date(val).toLocaleString()
                          : `${col.header}: ${!isNaN(val) ? val.toFixed(2) : 'N/A'}`;
                        const tdClass = col.isTimestamp ? 'text-right tabular-nums px-0.5 text-zinc-400' : 'w-14 text-right tabular-nums px-0.5';
                        return (
                          <td key={col.header} className={tdClass} style={color ? { color } : undefined} title={title}>
                            {display}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {filteredRows.length === 0 && rows.length > 0 && (
              <div className="text-xs text-zinc-600 p-4 text-center">No tickers match filters</div>
            )}
            {rows.length === 0 && (
              <div className="text-xs text-zinc-600 p-4 text-center">
                {autoLoadAttempted ? 'No CSV loaded' : 'Scanning for CSV files...'}
              </div>
            )}
          </div>
        </div>

        {/* Main Area */}
        <div className="flex-1 flex flex-col min-w-0">
          <div className="flex-1 min-h-0 flex flex-col overflow-hidden">
            {/* Price panel */}
            <div className={`flex-1 flex flex-col border-b border-zinc-800 ${!pricePanelCollapsed ? 'min-h-[200px]' : ''}`}>
              <PanelHeader
                title="Price"
                collapsed={pricePanelCollapsed}
                canClose={false}
                onToggleCollapse={() => togglePanelCollapse('price')}
                rightContent={
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => setPriceMode((m) => (m === 'pct' ? 'price' : 'pct'))}
                      className={`text-[10px] px-1.5 py-0.5 rounded border transition-colors ${
                        priceMode === 'pct' ? 'bg-blue-900/40 border-blue-700 text-blue-300' : 'bg-zinc-800 border-zinc-700 text-zinc-400'
                      }`}
                      title="Toggle price / % from entry"
                    >
                      {priceMode === 'pct' ? '%' : '$'}
                    </button>
                    <button
                      onClick={() => chartApiRef.current?.timeScale().fitContent()}
                      className="p-0.5 hover:bg-zinc-700 rounded border border-zinc-700 text-zinc-400"
                      title="Reset zoom [R]"
                    >
                      <Maximize2 className="w-3 h-3" />
                    </button>
                  </div>
                }
              />
              {!(pricePanelCollapsed) && (
                <div className="flex-1 min-h-0 relative overflow-hidden">
                  {loading && (
                    <div className="absolute inset-0 flex items-center justify-center bg-zinc-950/60 z-10">
                      <Loader2 className="w-8 h-8 text-zinc-400 animate-spin" />
                    </div>
                  )}
                  {error && !chartData && (
                    <div className="absolute inset-0 flex items-center justify-center text-red-400 text-sm">API Error: {error}</div>
                  )}
                  {!currentRow && rows.length === 0 && (
                    <div className="absolute inset-0 flex items-center justify-center text-zinc-600 text-sm">No CSV file loaded</div>
                  )}
                  {!currentRow && rows.length > 0 && (
                    <div className="absolute inset-0 flex items-center justify-center text-zinc-600 text-sm">Select a ticker from the list</div>
                  )}
                  {chartData && (
                    <PricePanel
                      chartData={chartData}
                      chartApiRef={chartApiRef}
                      priceMode={priceMode}
                      zoom={zoom}
                      overlays={activeOverlays}
                      availableFactors={availableFactors}
                      factorChartsRef={factorChartsRef}
                      lastLogicalRangeRef={lastLogicalRangeRef}
                    />
                  )}
                </div>
              )}
            </div>

            {/* Factor panels */}
            {activePanelFactors.map(({ panel, spec }) => (
              <div key={panel.id} className={`flex-shrink-0 flex flex-col border-b border-zinc-800 ${!panel.collapsed ? 'h-[120px]' : ''}`}>
                <PanelHeader
                  title={spec!.name}
                  color={spec!.color}
                  collapsed={panel.collapsed}
                  canClose
                  onToggleCollapse={() => togglePanelCollapse(panel.id)}
                  onClose={() => closePanel(panel.id)}
                  rightContent={
                    <span className="text-3xs font-mono px-1 rounded" style={{ color: spec!.color, backgroundColor: spec!.color + '18' }}>
                      {spec!.source === 'tick' ? 'tick' : chartData?.timeframe || 'bar'}
                    </span>
                  }
                />
                {!panel.collapsed && chartData && (
                  <div className="flex-1 min-h-0 overflow-hidden">
                    <FactorPanel
                      factorId={spec!.id}
                      factorSpec={spec!}
                      chartData={chartData}
                      zoom={zoom}
                      onRegisterChart={registerFactorChart}
                    />
                  </div>
                )}
              </div>
            ))}

            {/* Add Panel button */}
            <div className="relative flex-shrink-0 border-b border-zinc-800">
              <button
                onClick={() => setShowAddPanel(!showAddPanel)}
                className="w-full px-2 py-1 text-xs text-zinc-500 hover:text-zinc-300 hover:bg-zinc-900 flex items-center gap-1"
              >
                <Plus className="w-3 h-3" />
                Add Panel
              </button>
              {showAddPanel && (overlayFactors.length > 0 || addablePanelFactors.length > 0) && (
                <div className="absolute bottom-full left-0 mb-1 bg-zinc-800 border border-zinc-700 rounded shadow-lg z-50 min-w-[180px]">
                  {overlayFactors.length > 0 && (
                    <>
                      <div className="px-2 py-1 text-[10px] text-zinc-500 border-b border-zinc-700">Overlays (on Price)</div>
                      {overlayFactors.map((factor) => {
                        const isActive = activeOverlays.includes(factor.id);
                        return (
                          <button
                            key={factor.id}
                            onClick={() => toggleOverlay(factor.id)}
                            className={`w-full px-2 py-1 text-xs text-left flex items-center gap-2 ${
                              isActive ? 'bg-zinc-700 text-white' : 'text-zinc-300 hover:bg-zinc-700'
                            }`}
                          >
                            <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: factor.color }} />
                            {factor.name}
                            {isActive && <span className="ml-auto text-green-400 text-[10px]">ON</span>}
                          </button>
                        );
                      })}
                    </>
                  )}
                  {addablePanelFactors.length > 0 && (
                    <>
                      <div className="px-2 py-1 text-[10px] text-zinc-500 border-b border-zinc-700 border-t">Panels</div>
                      {addablePanelFactors.map((factor) => (
                        <button
                          key={factor.id}
                          onClick={() => addFactorPanel(factor)}
                          className="w-full px-2 py-1 text-xs text-left flex items-center gap-2 text-zinc-300 hover:bg-zinc-700"
                        >
                          <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: factor.color }} />
                          {factor.name}
                          <span className="ml-auto text-zinc-600 text-[10px]">{factor.source}</span>
                        </button>
                      ))}
                    </>
                  )}
                </div>
              )}
            </div>
          </div>

          {/* Info panel */}
          {chartData && (
            <div className="h-28 border-t border-zinc-800 px-3 py-1.5 flex gap-4 text-[11px] overflow-x-auto flex-shrink-0">
              <div className="flex flex-col gap-0.5 min-w-[120px]">
                <div className="text-zinc-500 text-[10px] uppercase tracking-wider">Entry</div>
                <div><span className="text-zinc-400">Time:</span> {chartData.candidate.entry_time} ET</div>
                <div className="flex items-center gap-1">
                  <span className="text-zinc-400">Price:</span>
                  <span className="inline-block w-3 h-0 border-t border-dashed border-green-500" />
                  <span>${chartData.candidate.entry_price.toFixed(2)}</span>
                </div>
                <div>
                  <span className="text-zinc-400">Gain:</span>{' '}
                  <span className={chartData.candidate.gain_at_entry > 0 ? 'text-green-400' : 'text-red-400'}>
                    {chartData.candidate.gain_at_entry > 0 ? '+' : ''}{chartData.candidate.gain_at_entry.toFixed(1)}%
                  </span>
                </div>
              </div>

              <div className="flex flex-col gap-0.5 min-w-[100px]">
                <div className="text-zinc-500 text-[10px] uppercase tracking-wider">Peak</div>
                {chartData.peak_metrics ? (
                  <>
                    <div>
                      <span className="text-zinc-400">MFE:</span>{' '}
                      <span className={chartData.peak_metrics.mfe_pct > 0 ? 'text-green-400' : 'text-red-400'}>
                        {chartData.peak_metrics.mfe_pct > 0 ? '+' : ''}{chartData.peak_metrics.mfe_pct.toFixed(1)}%
                      </span>
                    </div>
                    <div><span className="text-zinc-400">MAE:</span> <span className="text-red-400">{chartData.peak_metrics.mae_pct.toFixed(1)}%</span></div>
                    <div>
                      <span className="text-zinc-400">PnL:</span>{' '}
                      <span className={chartData.peak_metrics.final_pnl_pct > 0 ? 'text-green-400' : 'text-red-400'}>
                        {chartData.peak_metrics.final_pnl_pct > 0 ? '+' : ''}{chartData.peak_metrics.final_pnl_pct.toFixed(1)}%
                      </span>
                    </div>
                  </>
                ) : (
                  <div className="text-zinc-600">-</div>
                )}
              </div>

              <div className="flex flex-col gap-0.5 min-w-[120px]">
                <div className="text-zinc-500 text-[10px] uppercase tracking-wider">Breakout Peaks</div>
                {chartData.breakout_peaks ? (
                  <>
                    {(['peak_1min', 'peak_3min', 'peak_5min'] as const).map((key) => {
                      const bp = (chartData.breakout_peaks as Record<string, { pct: number } | null>)[key];
                      return (
                        <div key={key} className={bp ? 'text-zinc-300' : 'text-zinc-600'}>
                          <span className="inline-block w-1.5 h-1.5 rounded-sm mr-0.5"
                            style={{ backgroundColor: bp ? ({ peak_1min: '#3b82f6', peak_3min: '#06b6d4', peak_5min: '#ec4899' } as Record<string, string>)[key] : '#3f3f46' }} />
                          {key === 'peak_1min' ? '1m' : key === 'peak_3min' ? '3m' : '5m'}: {bp ? `${bp.pct > 0 ? '+' : ''}${bp.pct.toFixed(1)}%` : '-'}
                        </div>
                      );
                    })}
                  </>
                ) : <div className="text-zinc-600">-</div>}
              </div>

              <div className="flex flex-col gap-0.5 min-w-[180px]">
                <div className="text-zinc-500 text-[10px] uppercase tracking-wider">Ignition</div>
                <div className="flex flex-wrap gap-x-3 gap-y-0.5">
                  {Object.entries(chartData.ignition).map(([method, ig]) => (
                    <span key={method} className={ig ? 'text-zinc-300' : 'text-zinc-600'}>
                      <span className="inline-block w-2 h-2 rounded-full mr-0.5" style={{ backgroundColor: ig ? IGNITION_COLORS[method] : '#3f3f46' }} />
                      {IGNITION_LABELS[method]}
                      {ig ? ` ${formatMs(ig.time * 1000)}` : ''}
                    </span>
                  ))}
                </div>
              </div>

              {/* Ignition params toggle */}
              <button
                onClick={() => setShowIgParams(!showIgParams)}
                className="flex items-center gap-1 text-zinc-500 text-[10px] uppercase tracking-wider hover:text-zinc-300 transition-colors"
              >
                <ChevronDown className={`w-3 h-3 transition-transform ${showIgParams ? '' : '-rotate-90'}`} />
                Params
              </button>
              {showIgParams && (
                <div className="flex flex-wrap items-end gap-x-4 gap-y-1.5 bg-zinc-800/50 rounded px-2 py-1.5">
                  <label className="flex items-center gap-1 text-[10px] text-zinc-400">
                    <span className="w-16 shrink-0">Spread BPS</span>
                    <input type="number" min={20} max={300} step={10} value={igDraft.spread_bps}
                      onChange={(e) => setIgDraft(p => ({ ...p, spread_bps: Number(e.target.value) }))}
                      className="w-14 h-5 bg-zinc-700 border border-zinc-600 rounded px-1 text-zinc-200 text-[10px]" />
                  </label>
                  <label className="flex items-center gap-1 text-[10px] text-zinc-400">
                    <span className="w-16 shrink-0">Vol Ratio</span>
                    <input type="number" min={1.5} max={5} step={0.1} value={igDraft.vol_exp_ratio}
                      onChange={(e) => setIgDraft(p => ({ ...p, vol_exp_ratio: Number(e.target.value) }))}
                      className="w-14 h-5 bg-zinc-700 border border-zinc-600 rounded px-1 text-zinc-200 text-[10px]" />
                  </label>
                  <label className="flex items-center gap-1 text-[10px] text-zinc-400">
                    <span className="w-16 shrink-0">Surge σ</span>
                    <input type="number" min={1.5} max={5} step={0.1} value={igDraft.surge_sigma}
                      onChange={(e) => setIgDraft(p => ({ ...p, surge_sigma: Number(e.target.value) }))}
                      className="w-14 h-5 bg-zinc-700 border border-zinc-600 rounded px-1 text-zinc-200 text-[10px]" />
                  </label>
                  <label className="flex items-center gap-1 text-[10px] text-zinc-400">
                    <span className="w-16 shrink-0">Min Trades</span>
                    <input type="number" min={2} max={10} step={1} value={igDraft.sustained_min_trades}
                      onChange={(e) => setIgDraft(p => ({ ...p, sustained_min_trades: Number(e.target.value) }))}
                      className="w-14 h-5 bg-zinc-700 border border-zinc-600 rounded px-1 text-zinc-200 text-[10px]" />
                  </label>
                  <button
                    onClick={() => setIgParams({ ...igDraft })}
                    className="text-[10px] px-2 py-0.5 bg-amber-600 text-white rounded hover:bg-amber-500"
                  >Apply</button>
                  <button
                    onClick={() => { const d = { spread_bps: 100, vol_exp_ratio: 3.0, surge_sigma: 3.0, sustained_min_trades: 3 }; setIgDraft(d); setIgParams(d); }}
                    className="text-[10px] text-zinc-500 hover:text-zinc-300 underline"
                  >Reset</button>
                </div>
              )}

              <div className="flex flex-col gap-0.5 min-w-[100px]">
                <div className="text-zinc-500 text-[10px] uppercase tracking-wider">Data</div>
                <div className="text-zinc-400">{chartData.trade_count} trades</div>
                <div className="text-zinc-400">{chartData.bars.length} bars ({chartData.timeframe})</div>
              </div>
            </div>
          )}

          {/* Labeling Bar */}
          {currentRow && (
            <div className="h-14 border-t border-zinc-800 px-3 py-1.5 flex items-center gap-3 flex-shrink-0">
              <button
                onClick={() => applyLabel('1')}
                className={`text-xs px-3 py-1.5 rounded border font-medium transition-colors ${
                  currentLabelValue === '1' ? 'bg-green-800 border-green-600 text-green-200' : 'bg-zinc-800 border-zinc-700 hover:bg-green-900 text-zinc-400'
                }`}
              >
                True MOMO <span className="text-zinc-600 ml-1">[1]</span>
              </button>
              <button
                onClick={() => applyLabel('0')}
                className={`text-xs px-3 py-1.5 rounded border font-medium transition-colors ${
                  currentLabelValue === '0' ? 'bg-red-900 border-red-600 text-red-200' : 'bg-zinc-800 border-zinc-700 hover:bg-red-950 text-zinc-400'
                }`}
              >
                False MOMO <span className="text-zinc-600 ml-1">[2]</span>
              </button>
              <button
                onClick={() => applyLabel('-1')}
                className={`text-xs px-3 py-1.5 rounded border font-medium transition-colors ${
                  currentLabelValue === '-1' ? 'bg-amber-900 border-amber-600 text-amber-200' : 'bg-zinc-800 border-zinc-700 hover:bg-amber-950 text-zinc-400'
                }`}
              >
                Skip <span className="text-zinc-600 ml-1">[3]</span>
              </button>
              <button
                onClick={toggleBreakLabel}
                className={`text-xs px-3 py-1.5 rounded border font-medium transition-colors ${
                  currentBreakLabel === '1' ? 'bg-cyan-900 border-cyan-600 text-cyan-200' : 'bg-zinc-800 border-zinc-700 hover:bg-cyan-950 text-zinc-400'
                }`}
                title="Tag as breakout candidate"
              >
                Break <span className="text-zinc-600 ml-1">[b]</span>
              </button>
              <input
                type="text"
                placeholder="Notes..."
                value={currentNotes}
                onChange={(e) => setNotes(e.target.value)}
                className="flex-1 bg-zinc-800 border border-zinc-700 rounded px-2 py-1.5 text-xs text-zinc-300 placeholder:text-zinc-600 outline-none focus:border-zinc-500"
              />
              <div className="flex items-center gap-1 flex-shrink-0">
                <button onClick={() => setCurrentIndex((i) => Math.max(0, i - 1))} disabled={currentIndex === 0} className="p-1 hover:bg-zinc-800 rounded disabled:opacity-30">
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <span className="text-xs text-zinc-500 tabular-nums w-20 text-center">
                  {currentIndex + 1} / {filteredRows.length}
                </span>
                <button onClick={() => setCurrentIndex((i) => Math.min(filteredRows.length - 1, i + 1))} disabled={currentIndex >= filteredRows.length - 1} className="p-1 hover:bg-zinc-800 rounded disabled:opacity-30">
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Stats tooltip — fixed positioning, renders outside content flow */}
      {statsTooltip && columnStats[statsTooltip.col] && (() => {
        const s = columnStats[statsTooltip.col]!;
        return (
          <div
            className="fixed z-50 bg-zinc-800 border border-zinc-600 rounded px-2 py-1 text-[10px] shadow-lg pointer-events-none"
            style={{ left: statsTooltip.x, top: statsTooltip.y + 4 }}
          >
            <div className="text-zinc-400 mb-0.5 font-medium">{statsTooltip.col}</div>
            <div className="text-zinc-300">Min: <span className="text-red-400">{s.min.toFixed(2)}</span></div>
            <div className="text-zinc-300">Med: <span className="text-zinc-200">{s.median.toFixed(2)}</span></div>
            <div className="text-zinc-300">Max: <span className="text-green-400">{s.max.toFixed(2)}</span></div>
            <div className="text-zinc-300">Mean: <span className="text-zinc-200">{s.mean.toFixed(2)}</span></div>
            <div className="text-zinc-300">Std: <span className="text-zinc-200">{s.std.toFixed(2)}</span></div>
            <div className="text-zinc-500">n={s.count}</div>
          </div>
        );
      })()}
    </div>
  );
}
