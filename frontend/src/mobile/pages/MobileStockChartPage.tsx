import { useState, useEffect, useRef, useMemo } from 'react';
import {
  CandlestickSeries, HistogramSeries, type IChartApi, type ISeriesApi,
  type Time,
} from 'lightweight-charts';
import { ChevronRight, Loader2 } from 'lucide-react';
import type { ChartTimeframe } from '../../types';
import { TIMEFRAME_DURATION } from '../../constants/timeframes';
import { useChartDataStore, chartStoreKey, type OHLCVBar } from '../../stores/chartDataStore';
import { useTickDataStore, type Trade } from '../../stores/tickDataStore';
import { useLightweightChart } from '../../hooks/useLightweightChart';
import { SymbolSearch } from '../../components/common/SymbolSearch';
import { useMarketDataStore } from '../../stores/marketDataStore';
import { IS_DEMO, getMockCandlestickData } from '../../data/mockData';
import { formatNumber } from '../../utils/format';

// ---- Constants ---------------------------------------------------------------

const MODULE_ID = 'mobile-chart';
const TIMEFRAMES: ChartTimeframe[] = ['1m', '5m', '15m', '30m', '1h', '4h', '1D'];

// ---- Props -------------------------------------------------------------------

interface MobileStockChartPageProps {
  symbol: string | null;
  onSelectSymbol?: (symbol: string) => void;
  onNavigateToDetail?: () => void;
}

// ---- Component ---------------------------------------------------------------

export function MobileStockChartPage({
  symbol,
  onSelectSymbol,
  onNavigateToDetail,
}: MobileStockChartPageProps) {
  // -- Symbol & timeframe --
  const [currentSymbol, setCurrentSymbol] = useState(symbol || '');
  const [timeframe, setTimeframe] = useState<ChartTimeframe>('5m');

  // Sync symbol from parent
  useEffect(() => {
    if (symbol && symbol !== currentSymbol) setCurrentSymbol(symbol);
  }, [symbol]);

  // -- Store selectors --
  const fetchBars = useChartDataStore((s) => s.fetchBars);
  const symbolBars = useChartDataStore((s) => s.symbolBars);
  const addSymbols = useTickDataStore((s) => s.addSymbols);
  const connected = useTickDataStore((s) => s.connected);

  const chartKey = currentSymbol ? chartStoreKey(MODULE_ID, currentSymbol) : '';
  const chartState = chartKey ? symbolBars[chartKey] : undefined;

  const latestTrade = useTickDataStore((s) =>
    currentSymbol ? (s.symbolData[currentSymbol]?.T as Trade | undefined) : null
  );

  // Available symbols for search
  const symbolsKey = useMarketDataStore((s) =>
    Array.from(s.entities.keys()).sort().join(',')
  );
  const availableSymbols = useMemo(() => (symbolsKey ? symbolsKey.split(',') : []), [symbolsKey]);

  // -- Demo data --
  const [demoBars, setDemoBars] = useState<OHLCVBar[]>([]);

  useEffect(() => {
    if (!IS_DEMO || !currentSymbol) return;
    setDemoBars(getMockCandlestickData(currentSymbol));
  }, [currentSymbol]);

  // -- Live data fetching --
  useEffect(() => {
    if (IS_DEMO || !currentSymbol) return;
    fetchBars(MODULE_ID, currentSymbol, timeframe);
  }, [currentSymbol, timeframe, fetchBars]);

  // Subscribe to tick data for real-time updates
  useEffect(() => {
    if (IS_DEMO || !currentSymbol) return;
    const store = useTickDataStore.getState();
    if (!store.symbols.includes(currentSymbol)) {
      addSymbols([currentSymbol], ['T']);
    }
  }, [currentSymbol, addSymbols]);

  // -- Chart --
  const containerRef = useRef<HTMLDivElement>(null);
  const { chartRef, chartReady } = useLightweightChart({
    container: containerRef,
    options: {
      timeScale: { timeVisible: true, secondsVisible: false },
      rightPriceScale: { borderVisible: false },
      handleScroll: { vertTouchDrag: false },
    },
  });

  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);

  // Create series
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !chartReady) return;

    const cs = chart.addSeries(CandlestickSeries, {
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderUpColor: '#22c55e',
      borderDownColor: '#ef4444',
      wickUpColor: '#22c55e',
      wickDownColor: '#ef4444',
    });
    candleSeriesRef.current = cs;

    const vs = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    });
    chart.priceScale('').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    volumeSeriesRef.current = vs;
  }, [chartReady]);

  // -- Render bars --
  const bars = IS_DEMO ? demoBars : (chartState?.bars ?? []);

  useEffect(() => {
    const cs = candleSeriesRef.current;
    const vs = volumeSeriesRef.current;
    if (!cs || !vs || bars.length === 0) return;

    cs.setData(bars.map((b) => ({
      time: Number(b.time) as Time,
      open: b.open, high: b.high, low: b.low, close: b.close,
    })));

    vs.setData(bars.map((b) => ({
      time: Number(b.time) as Time,
      value: b.volume,
      color: b.close >= b.open ? '#22c55e40' : '#ef444440',
    })));

    chartRef.current?.timeScale().fitContent();
  }, [bars]);

  // -- Real-time trade updates (live mode only) --
  const prevTradeRef = useRef<Trade | null>(null);
  const currentBarRef = useRef<OHLCVBar | null>(null);
  const barDuration = TIMEFRAME_DURATION[timeframe];

  useEffect(() => {
    if (IS_DEMO || !latestTrade || !candleSeriesRef.current || bars.length === 0) return;
    if (latestTrade === prevTradeRef.current) return;

    prevTradeRef.current = latestTrade;
    const tradeSec = Math.floor(latestTrade.timestamp / 1000);
    const barTime = Math.floor(tradeSec / barDuration) * barDuration;

    const cb = currentBarRef.current;
    if (!cb || cb.time !== barTime) {
      // New bar
      currentBarRef.current = {
        time: barTime,
        open: latestTrade.price,
        high: latestTrade.price,
        low: latestTrade.price,
        close: latestTrade.price,
        volume: latestTrade.size,
      };
    } else {
      currentBarRef.current = {
        ...cb,
        high: Math.max(cb.high, latestTrade.price),
        low: Math.min(cb.low, latestTrade.price),
        close: latestTrade.price,
        volume: cb.volume + latestTrade.size,
      };
    }

    const bar = currentBarRef.current;
    try {
      candleSeriesRef.current.update({
        time: bar.time as Time,
        open: bar.open, high: bar.high, low: bar.low, close: bar.close,
      });
      volumeSeriesRef.current?.update({
        time: bar.time as Time,
        value: bar.volume,
        color: bar.close >= bar.open ? '#22c55e40' : '#ef444440',
      });
    } catch { /* ignore */ }
  }, [latestTrade, barDuration, bars.length]);

  // Reset on symbol/tf change
  useEffect(() => {
    currentBarRef.current = null;
    prevTradeRef.current = null;
  }, [currentSymbol, timeframe]);

  // -- Price info --
  const lastBar = bars[bars.length - 1];
  const priceInfo = latestTrade
    ? { price: latestTrade.price }
    : lastBar
      ? { price: lastBar.close }
      : null;

  const changeInfo = lastBar && bars.length > 1
    ? { change: lastBar.close - bars[0].open, pct: ((lastBar.close - bars[0].open) / bars[0].open) * 100 }
    : null;

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Controls */}
      <div className="flex-shrink-0 px-2 py-1.5 space-y-1.5 border-b border-zinc-800">
        {/* Symbol search + price */}
        <div className="flex items-center gap-2">
          <div className="flex-1 min-w-0" style={{ maxWidth: 180 }}>
            <SymbolSearch
              value={currentSymbol}
              onChange={(s) => {
                setCurrentSymbol(s);
                onSelectSymbol?.(s);
              }}
              availableSymbols={availableSymbols}
              placeholder="Symbol..."
              useConfirmButton
            />
          </div>
          {chartState?.loading && (
            <Loader2 className="w-4 h-4 animate-spin text-blue-400 flex-shrink-0" />
          )}
          {priceInfo && (
            <div className="flex items-center gap-2 text-xs ml-auto flex-shrink-0">
              <span className="text-white font-mono tabular-nums">
                ${formatNumber(priceInfo.price)}
              </span>
              {changeInfo && (
                <span className={`font-mono tabular-nums ${changeInfo.change >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {changeInfo.change >= 0 ? '+' : ''}{formatNumber(changeInfo.change)}
                  {' '}({changeInfo.change >= 0 ? '+' : ''}{formatNumber(changeInfo.pct)}%)
                </span>
              )}
            </div>
          )}
        </div>

        {/* Timeframe pills */}
        <div className="flex items-center gap-0.5 overflow-x-auto">
          {TIMEFRAMES.map((tf) => (
            <button
              key={tf}
              onClick={() => setTimeframe(tf)}
              className={`flex-shrink-0 px-2 py-0.5 text-2xs font-mono rounded transition-all ${
                tf === timeframe
                  ? 'bg-zinc-700 text-zinc-100'
                  : 'text-zinc-500 hover:text-zinc-300'
              }`}
            >
              {tf}
            </button>
          ))}
        </div>
      </div>

      {/* Chart */}
      <div className="flex-1 min-h-0 relative">
        <div ref={containerRef} className="absolute inset-0" />
        {!currentSymbol && (
          <div className="absolute inset-0 flex items-center justify-center text-zinc-500 text-sm bg-black/50 z-10">
            Enter a symbol to view chart
          </div>
        )}
        {currentSymbol && bars.length === 0 && !chartState?.loading && (
          <div className="absolute inset-0 flex items-center justify-center text-zinc-500 text-sm bg-black/50 z-10">
            {IS_DEMO
              ? 'No mock data for this symbol'
              : connected
                ? 'No chart data — try a different timeframe'
                : 'Disconnected — check connection'}
          </div>
        )}
      </div>

      {/* Footer — cross-tab nav */}
      {currentSymbol && onNavigateToDetail && (
        <div className="flex-shrink-0 border-t border-zinc-800 px-2 py-1.5">
          <button
            onClick={onNavigateToDetail}
            className="flex items-center justify-between w-full px-3 py-2 bg-zinc-800 hover:bg-zinc-700 rounded text-xs text-zinc-300 transition-colors"
          >
            <span>View {currentSymbol} Detail</span>
            <ChevronRight className="w-4 h-4 text-zinc-500" />
          </button>
        </div>
      )}
    </div>
  );
}
