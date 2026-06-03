import { useEffect, useRef, useState, useMemo } from 'react';
import {
  createChart, LineSeries, type IChartApi, type ISeriesApi, type LineData, type Time,
} from 'lightweight-charts';
import { RefreshCw } from 'lucide-react';
import { useOverviewChartData } from '../../hooks/useWebSocket';
import { formatTimestamp, parseTimestamp } from '../../hooks/useBackendTimestamps';
import { useMarketDataStore } from '../../stores/marketDataStore';
import { CHART_THEME } from '../../components/common/chartTheme';

// ---- Color palette -------------------------------------------------------------

const LINE_COLORS = [
  '#22c55e', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6',
  '#06b6d4', '#ec4899', '#84cc16', '#f97316', '#14b8a6',
  '#6366f1', '#e11d48', '#0ea5e9', '#a855f7', '#10b981',
  '#f43f5e', '#64748b', '#d4d4d8', '#a3e635', '#fdba74',
];

// ---- Props --------------------------------------------------------------------

interface MobileChartPageProps {
  symbol: string | null;
  onSelectSymbol?: (symbol: string) => void;
}

// ---- Component ----------------------------------------------------------------

export function MobileChartPage({ symbol: _symbol, onSelectSymbol }: MobileChartPageProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesMapRef = useRef<Map<string, ISeriesApi<'Line'>>>(new Map());
  const [chartReady, setChartReady] = useState(false);

  const { seriesData, rankData, timestamp, refresh, isConnected } = useOverviewChartData(20);
  const visibleTickers = useMarketDataStore((s) => s.visibleTickers);
  const timestampET = isConnected && timestamp ? formatTimestamp(parseTimestamp(timestamp)!) : null;

  // Only show tickers that are visible AND have series data — drop stale data when disconnected
  const tickers = useMemo(() => {
    if (!isConnected) return [];
    const visible = new Set(visibleTickers);
    return rankData
      .filter((r) => visible.has(r.symbol) && seriesData[r.symbol]?.data.length > 0)
      .slice(0, 20);
  }, [isConnected, rankData, seriesData, visibleTickers]);

  // ---- Chart lifecycle ---------------------------------------------------------

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const chart = createChart(el, {
      width: el.clientWidth,
      height: el.clientHeight,
      // Deep-merge CHART_THEME + overrides (same pattern as useLightweightChart hook)
      ...CHART_THEME,
      timeScale: {
        ...CHART_THEME.timeScale,
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        ...CHART_THEME.rightPriceScale,
        borderVisible: false,
      },
      layout: {
        ...CHART_THEME.layout,
      },
      grid: {
        ...CHART_THEME.grid,
        vertLines: { color: 'rgba(255,255,255,0.04)' },
        horzLines: { color: 'rgba(255,255,255,0.04)' },
      },
      crosshair: {
        ...CHART_THEME.crosshair,
        vertLine: { color: 'rgba(255,255,255,0.15)', style: 1, visible: true, labelVisible: false },
        horzLine: { color: 'rgba(255,255,255,0.15)', style: 1, visible: true, labelVisible: false },
      },
      handleScroll: { vertTouchDrag: false },
    });

    chartRef.current = chart;
    setChartReady(true);

    const ro = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      if (width > 0 && height > 0) {
        chart.applyOptions({ width, height });
      }
    });
    ro.observe(el);

    return () => {
      ro.disconnect();
      setChartReady(false);
      seriesMapRef.current.clear();
      chart.remove();
      chartRef.current = null;
    };
  }, []);

  // ---- Series sync — gated on chartReady ---------------------------------------

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !chartReady) return;

    const existing = seriesMapRef.current;
    const wanted = new Set(tickers.map((t) => t.symbol));

    // Add / update series
    tickers.forEach((item, idx) => {
      const sd = seriesData[item.symbol];
      if (!sd?.data?.length) return;

      const color = LINE_COLORS[idx % LINE_COLORS.length];
      const lineData: LineData[] = sd.data.map((d) => ({
        time: d.time as Time,
        value: d.value,
      }));

      let series = existing.get(item.symbol);
      if (series) {
        series.setData(lineData);
        series.applyOptions({ color, lineWidth: 1.5 });
      } else {
        series = chart.addSeries(LineSeries, {
          color,
          lineWidth: 1.5,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
        series.setData(lineData);
        existing.set(item.symbol, series);
      }
    });

    // Remove stale series
    existing.forEach((series, sym) => {
      if (!wanted.has(sym)) {
        chart.removeSeries(series);
        existing.delete(sym);
      }
    });

    chart.timeScale().fitContent();
  }, [chartReady, tickers, seriesData]);

  // ---- Render ------------------------------------------------------------------

  return (
    <div className="h-full flex flex-col">
      {/* Thin controls */}
      <div className="flex-shrink-0 flex items-center justify-between px-2 py-1 bg-zinc-900">
        <div className="flex items-center gap-2">
          <span className="text-2xs text-zinc-500">{tickers.length} tickers</span>
          {timestampET && (
            <span className="text-2xs text-zinc-600">Updated: {timestampET}</span>
          )}
        </div>
        <button onClick={refresh} className="p-1 hover:bg-zinc-800 rounded text-zinc-400" title="Refresh">
          <RefreshCw className="w-3 h-3" />
        </button>
      </div>

      {/* Chart area */}
      <div className="flex-1 min-h-0 relative">
        <div ref={containerRef} className="absolute inset-0" />
        {tickers.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center text-zinc-500 text-sm bg-black z-10">
            No visible tickers — enable some in Market tab
          </div>
        )}
      </div>

      {/* Ticker legend */}
      {tickers.length > 0 && (
        <div className="flex-shrink-0 border-t border-zinc-800 overflow-x-auto overscroll-contain">
          <div className="flex gap-0.5 px-2 py-2">
            {tickers.map((item, idx) => (
              <button
                key={item.symbol}
                onClick={() => onSelectSymbol?.(item.symbol)}
                className="flex-shrink-0 flex items-center gap-1.5 px-2.5 py-1.5 rounded bg-zinc-900 hover:bg-zinc-800 transition-colors text-xs min-h-[36px]"
              >
                <span
                  className="w-2 h-2 rounded-full flex-shrink-0"
                  style={{ backgroundColor: LINE_COLORS[idx % LINE_COLORS.length] }}
                />
                <span className="font-semibold">{item.symbol}</span>
                <span className="tabular-nums text-zinc-400">
                  ${item.price.toFixed(2)}
                </span>
                <span
                  className={`tabular-nums ${
                    (item.changePercent ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'
                  }`}
                >
                  {(item.changePercent ?? 0) >= 0 ? '+' : ''}
                  {item.changePercent.toFixed(2)}%
                </span>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
