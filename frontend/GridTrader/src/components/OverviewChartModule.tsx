import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from 'recharts';
import { Filter, Focus, TrendingUp } from 'lucide-react';
import type { ModuleProps, RankItem, TickerState } from '../types';
import { useBackendTimestamp } from '../hooks/useBackendTimestamps';

// State history data structure for each ticker
interface StateHistoryPoint {
  timestamp: number;
  state: TickerState;
}

interface TickerDataWithHistory extends RankItem {
  stateHistory: StateHistoryPoint[];
}

const STATE_COLORS: Record<TickerState, string> = {
  'confirming': '#eab308',
  'on-watch': '#3b82f6',
  'dead': '#6b7280',
  'fresh-new': '#a855f7',
  'running-up': '#10b981',
};

// Generate mock data with state history
const generateMockRankData = (): TickerDataWithHistory[] => {
  const symbols = ['AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'AMD', 'NFLX', 'COIN', 'PLTR', 'RIVN', 'LCID', 'SOFI', 'BABA', 'NIO', 'SHOP', 'SQ', 'PYPL', 'ROKU'];
  const states: TickerState[] = ['confirming', 'on-watch', 'dead', 'fresh-new', 'running-up'];
  const now = Date.now();

  return symbols.map((symbol) => {
    const currentState = states[Math.floor(Math.random() * states.length)];

    // Generate state history for pre-market (3-5 state changes during pre-market hours)
    const numStateChanges = Math.floor(Math.random() * 3) + 3;
    const stateHistory: StateHistoryPoint[] = [];

    // Pre-market duration in minutes: 4:00 AM to 9:30 AM = 330 minutes
    const premarketMinutes = 330;

    for (let i = numStateChanges; i >= 0; i--) {
      const minutesAgo = (premarketMinutes / numStateChanges) * i;
      const timestamp = now - minutesAgo * 60 * 1000;
      const state = i === 0 ? currentState : states[Math.floor(Math.random() * states.length)];
      stateHistory.push({ timestamp, state });
    }

    // Sort by timestamp descending (newest first)
    stateHistory.sort((a, b) => b.timestamp - a.timestamp);

    return {
      symbol,
      price: Math.random() * 500 + 50,
      change: Math.random() * 20 - 5,
      changePercent: Math.random() * 15,
      volume: Math.random() * 100000000,
      marketCap: Math.random() * 1000000000000,
      state: currentState,
      float: Math.random() * 500000000,
      relativeVolumeDaily: Math.random() * 5 + 0.5,
      relativeVolume5min: Math.random() * 10 + 0.2,
      stateHistory,
    };
  }).sort((a, b) => b.changePercent - a.changePercent).slice(0, 20);
};

// Generate mock price history with state changes for all symbols
// Returns structured data with segments pre-calculated
// Now focused on pre-market hours: 4:00 AM - 9:30 AM
const generateOverviewData = (rankData: TickerDataWithHistory[]) => {
  const data: any[] = [];
  const now = Date.now();

  // Pre-market hours: 4:00 AM to 9:30 AM = 330 minutes
  // Generate data points every 5 minutes = 67 data points
  const intervalMinutes = 5;
  const totalMinutes = 330; // 5.5 hours
  const numPoints = Math.floor(totalMinutes / intervalMinutes) + 1;

  // Create time series data points
  for (let i = 0; i < numPoints; i++) {
    const minutesFromStart = i * intervalMinutes;
    const timestamp = now - (totalMinutes - minutesFromStart) * 60 * 1000;

    // Calculate display time (4:00 AM + minutes)
    const totalMinutesFromMidnight = 4 * 60 + minutesFromStart; // 4:00 AM = 240 minutes from midnight
    const hours = Math.floor(totalMinutesFromMidnight / 60);
    const minutes = totalMinutesFromMidnight % 60;

    // Format time as "4:00", "5:30", etc.
    const timeLabel = `${hours}:${minutes.toString().padStart(2, '0')}`;

    const dataPoint: any = {
      date: timeLabel,
      timestamp,
    };

    // Generate price data for each symbol
    rankData.forEach((item) => {
      const volatility = 0.03;
      const trend = minutesFromStart / totalMinutes;
      const noise = (Math.random() - 0.5) * volatility * 100;
      const value = (trend * item.changePercent) + noise;

      // Find the state at this timestamp
      let currentState = item.state;
      for (const sh of item.stateHistory) {
        if (sh.timestamp <= timestamp) {
          currentState = sh.state;
          break;
        }
      }

      dataPoint[`${item.symbol}_value`] = value;
      dataPoint[`${item.symbol}_state`] = currentState;
    });

    data.push(dataPoint);
  }

  // Now create segment data keys for each ticker
  const segmentInfo: Record<string, Array<{key: string, color: string, startIdx: number, endIdx: number}>> = {};

  rankData.forEach((item) => {
    const symbol = item.symbol;
    const segments: Array<{key: string, color: string, startIdx: number, endIdx: number}> = [];
    let currentState: TickerState | null = null;
    let segmentStart = 0;
    let segmentIndex = 0;

    data.forEach((point, idx) => {
      const pointState = point[`${symbol}_state`] as TickerState;

      if (pointState !== currentState) {
        if (currentState !== null) {
          // Close previous segment
          const segmentKey = `${symbol}_seg${segmentIndex}`;
          segments.push({
            key: segmentKey,
            color: STATE_COLORS[currentState],
            startIdx: segmentStart,
            endIdx: idx,
          });
          segmentIndex++;
        }
        // Start new segment
        segmentStart = idx;
        currentState = pointState;
      }
    });

    // Close final segment
    if (currentState !== null) {
      const segmentKey = `${symbol}_seg${segmentIndex}`;
      segments.push({
        key: segmentKey,
        color: STATE_COLORS[currentState],
        startIdx: segmentStart,
        endIdx: data.length - 1,
      });
    }

    segmentInfo[symbol] = segments;
  });

  // Add segment values to data points
  Object.entries(segmentInfo).forEach(([symbol, segments]) => {
    segments.forEach((segment) => {
      data.forEach((point, idx) => {
        if (idx >= segment.startIdx && idx <= segment.endIdx) {
          point[segment.key] = point[`${symbol}_value`];
        } else {
          point[segment.key] = null;
        }
      });
    });
  });

  return { data, segmentInfo };
};

export function OverviewChartModule({
  onRemove,
  syncGroup,
  onSyncGroupChange,
  selectedSymbol,
  onSymbolSelect,
  settings,
  onSettingsChange
}: ModuleProps) {
  const [rankData, setRankData] = useState<TickerDataWithHistory[]>([]);
  const [chartData, setChartData] = useState<any[]>([]);
  const [segmentInfo, setSegmentInfo] = useState<Record<string, Array<{key: string, color: string, startIdx: number, endIdx: number}>>>({});
  const defaultStates: TickerState[] = settings?.overviewChart?.selectedStates || ['running-up', 'fresh-new', 'on-watch', 'confirming'];
  const [selectedStates, setSelectedStates] = useState<Set<TickerState>>(new Set(defaultStates));
  const [showFilter, setShowFilter] = useState(false);
  const [focusMode, setFocusMode] = useState(settings?.overviewChart?.focusMode || false);
  const [timeRange, setTimeRange] = useState<string>(settings?.overviewChart?.timeRange || 'all');
  const [showTimeRangeMenu, setShowTimeRangeMenu] = useState(false);

  const allStates: TickerState[] = ['confirming', 'on-watch', 'dead', 'fresh-new', 'running-up'];

  const timeRangeOptions = [
    { value: 'all', label: 'All', minutes: null },
    { value: '2h', label: 'Last 2 Hours', minutes: 120 },
    { value: '1h', label: 'Last Hour', minutes: 60 },
    { value: '30m', label: 'Last 30 Minutes', minutes: 30 },
    { value: '5m', label: 'Last 5 Minutes', minutes: 5 },
    { value: '1m', label: 'Last 1 Minute', minutes: 1 },
  ];

  // Backend timestamp for market data domain (same as Rank List)
  const backendTimestamp = useBackendTimestamp('market-data');

  // Initialize data
  useEffect(() => {
    const mockData = generateMockRankData();
    const { data, segmentInfo: segments } = generateOverviewData(mockData);
    setRankData(mockData);
    setChartData(data);
    setSegmentInfo(segments);
  }, []);

  // Update settings from props
  useEffect(() => {
    if (settings?.overviewChart?.selectedStates) {
      setSelectedStates(new Set(settings.overviewChart.selectedStates));
    }
    if (settings?.overviewChart?.focusMode !== undefined) {
      setFocusMode(settings.overviewChart.focusMode);
    }
    if (settings?.overviewChart?.timeRange !== undefined) {
      setTimeRange(settings.overviewChart.timeRange);
    }
  }, [settings?.overviewChart?.selectedStates, settings?.overviewChart?.focusMode, settings?.overviewChart?.timeRange]);

  const toggleState = (state: TickerState) => {
    const newStates = new Set(selectedStates);
    if (newStates.has(state)) {
      newStates.delete(state);
    } else {
      newStates.add(state);
    }
    setSelectedStates(newStates);
    onSettingsChange?.({
      overviewChart: {
        selectedStates: Array.from(newStates),
        focusMode,
        timeRange,
      }
    });
  };

  const toggleFocusMode = () => {
    const newFocusMode = !focusMode;
    setFocusMode(newFocusMode);
    onSettingsChange?.({
      overviewChart: {
        selectedStates: Array.from(selectedStates),
        focusMode: newFocusMode,
        timeRange,
      }
    });
  };

  const getStateColor = (state: TickerState): string => {
    switch (state) {
      case 'confirming': return 'bg-yellow-600';
      case 'on-watch': return 'bg-blue-600';
      case 'dead': return 'bg-gray-600';
      case 'fresh-new': return 'bg-purple-600';
      case 'running-up': return 'bg-green-600';
      default: return 'bg-gray-600';
    }
  };

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      // Group by symbol (remove segment suffix)
      const symbolData: Record<string, {value: number, state: TickerState, color: string}> = {};

      payload.forEach((entry: any) => {
        if (entry.value !== null && entry.dataKey.includes('_seg')) {
          const symbol = entry.dataKey.split('_seg')[0];
          if (!symbolData[symbol]) {
            const state = entry.payload[`${symbol}_state`];
            symbolData[symbol] = {
              value: entry.value,
              state,
              color: entry.stroke,
            };
          }
        }
      });

      return (
        <div className="bg-zinc-800 border border-zinc-700 p-2 text-xs max-h-64 overflow-auto">
          <div className="mb-1">{label}</div>
          {Object.entries(symbolData).map(([symbol, data]) => (
            <div key={symbol} className="flex items-center gap-2">
              <div
                className="w-2 h-2 rounded-full"
                style={{ backgroundColor: data.color }}
              />
              <span style={{ color: data.color }}>
                {symbol}: {data.value.toFixed(2)}%
              </span>
              <span className={`px-1 py-0.5 text-[10px] ${getStateColor(data.state)} rounded`}>
                {data.state}
              </span>
            </div>
          ))}
        </div>
      );
    }
    return null;
  };

  // Custom legend to show only one entry per ticker
  const CustomLegend = ({ payload }: any) => {
    // Group by symbol
    const symbolMap: Record<string, {color: string}> = {};
    payload.forEach((entry: any) => {
      if (entry.dataKey.includes('_seg')) {
        const symbol = entry.dataKey.split('_seg')[0];
        if (!symbolMap[symbol]) {
          symbolMap[symbol] = { color: entry.color };
        }
      }
    });

    return (
      <div className="flex flex-wrap gap-3 justify-center mt-2">
        {Object.entries(symbolMap).map(([symbol, data]) => (
          <div key={symbol} className="flex items-center gap-1 text-xs">
            <div
              className="w-3 h-0.5"
              style={{ backgroundColor: data.color }}
            />
            <span>{symbol}</span>
          </div>
        ))}
      </div>
    );
  };

  // Determine which tickers to show
  let displayedRankData: TickerDataWithHistory[];

  if (focusMode && syncGroup && selectedSymbol) {
    // Focus mode: show only selected symbol if its current state matches the filter
    displayedRankData = rankData.filter(
      item => item.symbol === selectedSymbol && selectedStates.has(item.state)
    );
  } else {
    // Normal mode: filter by selected states
    displayedRankData = rankData.filter(item => selectedStates.has(item.state));
  }

  // Apply time range filter to chart data
  let filteredChartData = chartData;
  const selectedTimeRangeOption = timeRangeOptions.find(opt => opt.value === timeRange);

  if (selectedTimeRangeOption && selectedTimeRangeOption.minutes !== null) {
    // Filter to show only the last N minutes
    const cutoffTime = Date.now() - selectedTimeRangeOption.minutes * 60 * 1000;
    filteredChartData = chartData.filter(point => point.timestamp >= cutoffTime);
  }

  const handleTimeRangeChange = (value: string) => {
    setTimeRange(value);
    setShowTimeRangeMenu(false);
    onSettingsChange?.({
      overviewChart: {
        selectedStates: Array.from(selectedStates),
        focusMode,
        timeRange: value,
      }
    });
  };

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header */}
      <div className="p-3 border-b border-zinc-800">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span className="text-sm">
              {focusMode && syncGroup && selectedSymbol
                ? `Focused: ${selectedSymbol}`
                : 'Top 20 Movers Overview'}
            </span>
            <span className="text-xs text-gray-500">
              ({displayedRankData.length} tickers)
            </span>
            <span className="text-xs text-gray-500">
              Updated: {backendTimestamp}
            </span>
          </div>

          <div className="flex items-center gap-2">
            {/* Time Range Button */}
            <div className="relative">
              <button
                onClick={() => setShowTimeRangeMenu(!showTimeRangeMenu)}
                className="flex items-center gap-2 px-3 py-1 bg-zinc-800 hover:bg-zinc-700 transition-colors text-sm"
              >
                {timeRangeOptions.find(opt => opt.value === timeRange)?.label || 'All'}
              </button>

              {showTimeRangeMenu && (
                <div className="absolute right-0 top-full mt-1 bg-zinc-800 border border-zinc-700 shadow-xl z-50 min-w-[160px]">
                  <div className="p-2">
                    {timeRangeOptions.map((option) => (
                      <button
                        key={option.value}
                        onClick={() => handleTimeRangeChange(option.value)}
                        className={`w-full text-left px-2 py-1.5 hover:bg-zinc-700 text-sm ${
                          timeRange === option.value ? 'bg-zinc-700 text-white' : 'text-gray-300'
                        }`}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>

            {/* Filter Button */}
            <div className="relative">
              <button
                onClick={() => setShowFilter(!showFilter)}
                className="flex items-center gap-2 px-3 py-1 bg-zinc-800 hover:bg-zinc-700 transition-colors text-sm"
              >
                <Filter className="w-4 h-4" />
                Filter
              </button>

              {showFilter && (
                <div className="absolute right-0 top-full mt-1 bg-zinc-800 border border-zinc-700 shadow-xl z-50 min-w-[160px]">
                  <div className="p-2">
                    {allStates.map((state) => (
                      <label
                        key={state}
                        className="flex items-center gap-2 px-2 py-1.5 hover:bg-zinc-700 cursor-pointer text-sm"
                      >
                        <input
                          type="checkbox"
                          checked={selectedStates.has(state)}
                          onChange={() => toggleState(state)}
                          className="w-4 h-4"
                        />
                        <span className={`px-2 py-0.5 text-xs ${getStateColor(state)} rounded`}>
                          {state}
                        </span>
                      </label>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Chart */}
      <div className="flex-1 p-2 overflow-hidden" style={{ minHeight: 0 }}>
        {displayedRankData.length === 0 ? (
          <div className="h-full flex items-center justify-center text-gray-500">
            <div className="text-center">
              <TrendingUp className="w-12 h-12 mx-auto mb-2 opacity-50" />
              <p>No tickers to display</p>
              <p className="text-xs mt-1">
                {focusMode ? 'Select a ticker from a synced module' : 'Adjust your filters'}
              </p>
            </div>
          </div>
        ) : (
          <div className="w-full h-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={filteredChartData} margin={{ top: 10, right: 20, left: 10, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
                <XAxis
                  dataKey="date"
                  stroke="#71717a"
                  tick={{ fill: '#a1a1aa', fontSize: 11 }}
                  interval="preserveStartEnd"
                />
                <YAxis
                  stroke="#71717a"
                  tick={{ fill: '#a1a1aa', fontSize: 11 }}
                  tickFormatter={(value) => `${value.toFixed(0)}%`}
                />
                <Tooltip content={<CustomTooltip />} />
                <Legend content={<CustomLegend />} />

                {/* Render line segments for each displayed ticker */}
                {displayedRankData.map((item) => {
                  const segments = segmentInfo[item.symbol] || [];
                  return segments.map((segment, idx) => (
                    <Line
                      key={segment.key}
                      type="monotone"
                      dataKey={segment.key}
                      stroke={segment.color}
                      strokeWidth={2}
                      dot={false}
                      isAnimationActive={false}
                      connectNulls={false}
                      legendType="none"
                    />
                  ));
                })}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </div>
  );
}
