import React, { useState, useEffect } from 'react';
import { ComposedChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { Clock } from 'lucide-react';
import type { ModuleProps, ChartTimeframe } from '../types';
import { SymbolSearch } from './common/SymbolSearch';

// Timeframe configurations
const TIMEFRAME_CONFIG: Record<ChartTimeframe, { label: string; bars: number; dateFormat: (date: Date) => string }> = {
  '1m': { label: '1m', bars: 60, dateFormat: (d) => `${d.getHours()}:${d.getMinutes().toString().padStart(2, '0')}` },
  '5m': { label: '5m', bars: 78, dateFormat: (d) => `${d.getHours()}:${d.getMinutes().toString().padStart(2, '0')}` },
  '15m': { label: '15m', bars: 26, dateFormat: (d) => `${d.getHours()}:${d.getMinutes().toString().padStart(2, '0')}` },
  '30m': { label: '30m', bars: 48, dateFormat: (d) => `${d.getHours()}:${d.getMinutes().toString().padStart(2, '0')}` },
  '1h': { label: '1h', bars: 24, dateFormat: (d) => `${d.getHours()}:00` },
  '4h': { label: '4h', bars: 42, dateFormat: (d) => `${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:00` },
  '1D': { label: '1D', bars: 60, dateFormat: (d) => `${d.getMonth() + 1}/${d.getDate()}` },
  '1W': { label: '1W', bars: 52, dateFormat: (d) => `${d.getMonth() + 1}/${d.getDate()}` },
  '1M': { label: '1M', bars: 24, dateFormat: (d) => `${d.getMonth() + 1}/${d.getFullYear().toString().slice(2)}` },
};

const TIMEFRAME_ORDER: ChartTimeframe[] = ['1m', '5m', '15m', '30m', '1h', '4h', '1D', '1W', '1M'];

// Generate mock candlestick data for different symbols and timeframes
const generateCandlestickData = (symbol: string, timeframe: ChartTimeframe): any[] => {
  const config = TIMEFRAME_CONFIG[timeframe];
  const data: any[] = [];
  const basePrice = Math.random() * 200 + 50;
  let currentPrice = basePrice;
  const now = Date.now();

  // Calculate time step based on timeframe
  const timeSteps: Record<ChartTimeframe, number> = {
    '1m': 60 * 1000,
    '5m': 5 * 60 * 1000,
    '15m': 15 * 60 * 1000,
    '30m': 30 * 60 * 1000,
    '1h': 60 * 60 * 1000,
    '4h': 4 * 60 * 60 * 1000,
    '1D': 24 * 60 * 60 * 1000,
    '1W': 7 * 24 * 60 * 60 * 1000,
    '1M': 30 * 24 * 60 * 60 * 1000,
  };

  const timeStep = timeSteps[timeframe];

  for (let i = config.bars; i >= 0; i--) {
    const date = new Date(now - i * timeStep);
    const volatility = 0.02;
    const change = (Math.random() - 0.5) * currentPrice * volatility;

    const open = currentPrice;
    const close = currentPrice + change;
    const high = Math.max(open, close) * (1 + Math.random() * 0.01);
    const low = Math.min(open, close) * (1 - Math.random() * 0.01);

    data.push({
      date: config.dateFormat(date),
      timestamp: date.getTime(),
      open,
      high,
      low,
      close,
      isGreen: close > open,
    });

    currentPrice = close;
  }

  return data;
};

const AVAILABLE_SYMBOLS = [
  'AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'AMD',
  'NFLX', 'COIN', 'PLTR', 'RIVN', 'LCID', 'SOFI', 'BABA', 'NIO'
];

// Custom Candlestick Shape
const CandlestickShape = (props: any) => {
  const { x, y, width, height, payload } = props;

  if (!payload || width < 1) return null;

  const isGreen = payload.isGreen;
  const color = isGreen ? '#10b981' : '#ef4444';

  // Calculate scale from the parent chart
  const chartHeight = props.height || 300;
  const yMin = props.yMin || 0;
  const yMax = props.yMax || 100;

  const scale = (value: number) => {
    const ratio = (value - yMin) / (yMax - yMin);
    return chartHeight - (ratio * chartHeight);
  };

  // Positions
  const wickX = x + width / 2;
  const bodyWidth = Math.max(width * 0.7, 2);
  const bodyX = x + (width - bodyWidth) / 2;

  const highY = scale(payload.high);
  const lowY = scale(payload.low);
  const openY = scale(payload.open);
  const closeY = scale(payload.close);

  const bodyTop = Math.min(openY, closeY);
  const bodyHeight = Math.abs(openY - closeY) || 1;

  return (
    <g>
      {/* Wick */}
      <line
        x1={wickX}
        y1={highY}
        x2={wickX}
        y2={lowY}
        stroke={color}
        strokeWidth={1}
      />
      {/* Body */}
      <rect
        x={bodyX}
        y={bodyTop}
        width={bodyWidth}
        height={bodyHeight}
        fill={color}
      />
    </g>
  );
};

export function ChartModule({ onRemove, selectedSymbol, onSymbolSelect, settings, onSettingsChange }: ModuleProps) {
  const [symbol, setSymbol] = useState(selectedSymbol || 'AAPL');
  const [chartData, setChartData] = useState<any[]>([]);
  const [timeframe, setTimeframe] = useState<ChartTimeframe>(settings?.chart?.timeframe || '1D');

  // Update symbol when selectedSymbol changes from sync
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    }
  }, [selectedSymbol]);

  // Update timeframe from settings
  useEffect(() => {
    if (settings?.chart?.timeframe && settings.chart.timeframe !== timeframe) {
      setTimeframe(settings.chart.timeframe);
    }
  }, [settings?.chart?.timeframe]);

  // Update chart data when symbol or timeframe changes
  useEffect(() => {
    const data = generateCandlestickData(symbol, timeframe);
    setChartData(data);
  }, [symbol, timeframe]);

  const handleSymbolChange = (newSymbol: string) => {
    setSymbol(newSymbol);
    onSymbolSelect?.(newSymbol);
  };

  const handleTimeframeChange = (newTimeframe: ChartTimeframe) => {
    setTimeframe(newTimeframe);
    onSettingsChange?.({ chart: { timeframe: newTimeframe } });
  };

  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-zinc-800 border border-zinc-700 p-2 text-xs">
          <div className="mb-1">{data.date}</div>
          <div>O: ${data.open.toFixed(2)}</div>
          <div>H: ${data.high.toFixed(2)}</div>
          <div>L: ${data.low.toFixed(2)}</div>
          <div>C: ${data.close.toFixed(2)}</div>
        </div>
      );
    }
    return null;
  };

  // Calculate Y-axis domain
  const yMin = Math.min(...chartData.map(d => d.low)) * 0.99;
  const yMax = Math.max(...chartData.map(d => d.high)) * 1.01;

  const currentPrice = chartData.length > 0 ? chartData[chartData.length - 1].close : 0;
  const firstPrice = chartData.length > 0 ? chartData[0].open : 0;
  const priceChange = currentPrice - firstPrice;
  const priceChangePercent = firstPrice > 0 ? (priceChange / firstPrice) * 100 : 0;
  const isPositive = priceChange >= 0;

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header with Symbol Info and Controls */}
      <div className="p-3 border-b border-zinc-800">
        <div className="flex items-start gap-3">
          {/* Symbol Info and Timeframe - Left side */}
          <div className="flex-1 min-w-0">
            {/* Symbol Label and Price Info */}
            {chartData.length > 0 && (
              <div className="flex items-center gap-3 mb-2">
                <div className="text-sm text-gray-400">Symbol: <span className="text-white">{symbol}</span></div>
                <div className="flex items-baseline gap-2">
                  <span className="text-lg">${currentPrice.toFixed(2)}</span>
                  <span className={`text-sm ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                    {isPositive ? '+' : ''}{priceChange.toFixed(2)} ({isPositive ? '+' : ''}{priceChangePercent.toFixed(2)}%)
                  </span>
                </div>
              </div>
            )}

            {/* Timeframe Selector */}
            <div className="flex items-center gap-1 flex-wrap">
              {TIMEFRAME_ORDER.map((tf) => (
                <button
                  key={tf}
                  onClick={() => handleTimeframeChange(tf)}
                  className={`px-2 py-1 text-xs transition-colors ${
                    timeframe === tf
                      ? 'bg-white text-black'
                      : 'bg-zinc-800 hover:bg-zinc-700'
                  }`}
                >
                  {tf}
                </button>
              ))}
            </div>
          </div>

          {/* Search Bar - Right side with responsive width */}
          <div className="flex-shrink-0" style={{ width: '180px', minWidth: '140px', maxWidth: '280px' }}>
            <SymbolSearch
              value={symbol}
              onChange={handleSymbolChange}
              availableSymbols={AVAILABLE_SYMBOLS}
              placeholder="Symbol..."
              useConfirmButton={true}
            />
          </div>
        </div>
      </div>

      {/* Chart */}
      <div className="flex-1 p-2" style={{ minHeight: 0, height: '100%' }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
            <XAxis
              dataKey="date"
              stroke="#71717a"
              tick={{ fill: '#a1a1aa', fontSize: 11 }}
              interval="preserveStartEnd"
            />
            <YAxis
              domain={[yMin, yMax]}
              stroke="#71717a"
              tick={{ fill: '#a1a1aa', fontSize: 11 }}
              tickFormatter={(value) => `$${value.toFixed(0)}`}
            />
            <Tooltip content={<CustomTooltip />} />
            <Bar
              dataKey="high"
              shape={(props) => <CandlestickShape {...props} yMin={yMin} yMax={yMax} />}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
