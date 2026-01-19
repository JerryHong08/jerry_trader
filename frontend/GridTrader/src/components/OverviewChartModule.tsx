import React, { useState, useEffect, useRef, useMemo, useCallback, memo } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend, Brush, ReferenceLine } from 'recharts';
import { Filter, Focus, TrendingUp, Wifi, WifiOff, ZoomIn, ZoomOut, Maximize2 } from 'lucide-react';
import type { ModuleProps, RankItem, TickerState } from '../types';
import { useBackendTimestamp, timestampStore, parseTimestamp } from '../hooks/useBackendTimestamps';
import { useOverviewChartData, useWebSocketConnection, useChartSubscriptions, type ChartSegment, type TickerDataWithHistory } from '../hooks/useWebSocket';

// State history data structure for each ticker (also exported from useWebSocket)
interface StateHistoryPoint {
  timestamp: number;
  state: TickerState;
}

interface LocalTickerDataWithHistory extends RankItem {
  stateHistory: StateHistoryPoint[];
}

const STATE_COLORS: Record<TickerState, string> = {
  'Best': '#10b981',      // Green
  'Good': '#34d399',      // Emerald
  'OnWatch': '#3b82f6',   // Blue
  'NotGood': '#eab308',   // Yellow
  'Bad': '#6b7280',       // Gray
};

// Generate mock data with state history
const generateMockRankData = (): LocalTickerDataWithHistory[] => {
  const symbols = ['AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'AMD', 'NFLX', 'COIN', 'PLTR', 'RIVN', 'LCID', 'SOFI', 'BABA', 'NIO', 'SHOP', 'SQ', 'PYPL', 'ROKU'];
  const states: TickerState[] = ['Best', 'Good', 'OnWatch', 'NotGood', 'Bad'];
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
const generateOverviewData = (rankData: LocalTickerDataWithHistory[]) => {
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
  const [rankData, setRankData] = useState<(LocalTickerDataWithHistory | TickerDataWithHistory)[]>([]);
  const [chartData, setChartData] = useState<any[]>([]);
  const [segmentInfo, setSegmentInfo] = useState<Record<string, Array<{key: string, color: string, startIdx: number, endIdx: number}>>>({});
  const [useMockData, setUseMockData] = useState(false); // Toggle for mock/live data
  // Default to ALL states selected so chart shows all tickers
  const allStatesDefault: TickerState[] = ['Best', 'Good', 'OnWatch', 'NotGood', 'Bad'];
  const defaultStates: TickerState[] = settings?.overviewChart?.selectedStates || allStatesDefault;
  const [selectedStates, setSelectedStates] = useState<Set<TickerState>>(new Set(defaultStates));
  const [showFilter, setShowFilter] = useState(false);
  const [focusMode, setFocusMode] = useState(settings?.overviewChart?.focusMode || false);
  const [timeRange, setTimeRange] = useState<string>(settings?.overviewChart?.timeRange || 'all');
  const [showTimeRangeMenu, setShowTimeRangeMenu] = useState(false);

  // Brush/zoom state for interactive chart
  const [brushStartIndex, setBrushStartIndex] = useState<number | undefined>(undefined);
  const [brushEndIndex, setBrushEndIndex] = useState<number | undefined>(undefined);
  const [hoveredTicker, setHoveredTicker] = useState<string | null>(null);
  const hoverTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const allStates: TickerState[] = ['Best', 'Good', 'OnWatch', 'NotGood', 'Bad'];

  const timeRangeOptions = [
    { value: 'all', label: 'All', minutes: null },
    { value: 'free', label: 'Free (drag)', minutes: null },  // Free mode for brush
    { value: '2h', label: 'Last 2 Hours', minutes: 120 },
    { value: '1h', label: 'Last Hour', minutes: 60 },
    { value: '30m', label: 'Last 30 Minutes', minutes: 30 },
    { value: '5m', label: 'Last 5 Minutes', minutes: 5 },
    { value: '1m', label: 'Last 1 Minute', minutes: 1 },
  ];

  // Backend timestamp for market data domain (same as Rank List)
  const backendTimestamp = useBackendTimestamp('market-data');

  // WebSocket connection for live data
  const connectionStatus = useWebSocketConnection();
  const {
    chartData: liveChartData,
    segmentInfo: liveSegmentInfo,
    rankData: liveRankData,
    timestamp: liveTimestamp,
    isConnected,
    refresh
  } = useOverviewChartData();

  // Chart subscriptions from RankList
  const { subscribedTickers } = useChartSubscriptions();

  // Use live data when connected and not using mock mode
  useEffect(() => {
    if (useMockData) {
      // Mock mode is enabled - always use mock data
      const mockData = generateMockRankData();
      const { data, segmentInfo: segments } = generateOverviewData(mockData);
      setRankData(mockData);
      setChartData(data);
      setSegmentInfo(segments);
    } else if (isConnected) {
      // Live mode - use live data from backend
      if (liveChartData.length > 0) {
        // Have chart data from backend
        setChartData(liveChartData);
        setSegmentInfo(liveSegmentInfo);
        setRankData(liveRankData);
        // Update timestamp store with live timestamp
        const parsedTime = parseTimestamp(liveTimestamp);
        if (parsedTime) {
          timestampStore.updateTimestamp('market-data', parsedTime);
        }
      } else if (liveRankData.length > 0) {
        // Connected but no chart data yet - generate chart from rankData
        const convertedRankData = liveRankData.map(item => ({
          ...item,
          stateHistory: [{ timestamp: Date.now(), state: item.state }]
        } as LocalTickerDataWithHistory));
        const { data, segmentInfo: segments } = generateOverviewData(convertedRankData);
        setRankData(convertedRankData);
        setChartData(data);
        setSegmentInfo(segments);
        // Update timestamp
        const parsedTime = parseTimestamp(liveTimestamp);
        if (parsedTime) {
          timestampStore.updateTimestamp('market-data', parsedTime);
        }
      }
    }
  }, [liveChartData, liveSegmentInfo, liveRankData, liveTimestamp, isConnected, useMockData]);

  // Initialize with mock data on first render
  useEffect(() => {
    // Only run once on mount - show mock data initially
    const mockData = generateMockRankData();
    const { data, segmentInfo: segments } = generateOverviewData(mockData);
    setRankData(mockData);
    setChartData(data);
    setSegmentInfo(segments);
  }, []); // Empty deps = run once on mount

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

  const toggleState = useCallback((state: TickerState) => {
    setSelectedStates(prev => {
      const newStates = new Set(prev);
      if (newStates.has(state)) {
        newStates.delete(state);
      } else {
        newStates.add(state);
      }
      onSettingsChange?.({
        overviewChart: {
          selectedStates: Array.from(newStates),
          focusMode,
          timeRange,
        }
      });
      return newStates;
    });
  }, [focusMode, timeRange, onSettingsChange]);

  const toggleFocusMode = useCallback(() => {
    setFocusMode(prev => {
      const newFocusMode = !prev;
      onSettingsChange?.({
        overviewChart: {
          selectedStates: Array.from(selectedStates),
          focusMode: newFocusMode,
          timeRange,
        }
      });
      return newFocusMode;
    });
  }, [selectedStates, timeRange, onSettingsChange]);

  const getStateColor = useCallback((state: TickerState): string => {
    switch (state) {
      case 'Best': return 'bg-green-600';
      case 'Good': return 'bg-emerald-500';
      case 'OnWatch': return 'bg-blue-600';
      case 'NotGood': return 'bg-yellow-600';
      case 'Bad': return 'bg-gray-600';
      default: return 'bg-gray-600';
    }
  }, []);

  // Memoized CustomTooltip - only shows the hovered ticker
  const CustomTooltip = useMemo(() => {
    return memo(({ active, payload, label }: any) => {
      if (!active || !payload || payload.length === 0) return null;

      // If no ticker is hovered, find the one with max value at this point
      let targetSymbol = hoveredTicker;

      if (!targetSymbol) {
        // Find the ticker with the maximum absolute value at this point
        let maxValue = -Infinity;
        payload.forEach((entry: any) => {
          if (entry.value !== null && entry.dataKey.includes('_seg')) {
            const absVal = Math.abs(entry.value);
            if (absVal > maxValue) {
              maxValue = absVal;
              targetSymbol = entry.dataKey.split('_seg')[0];
            }
          }
        });
      }

      if (!targetSymbol) return null;

      // Find data for the target ticker
      let tickerData: { value: number; state: TickerState; color: string; stateReason?: string } | null = null;

      for (const entry of payload) {
        if (entry.value !== null && entry.dataKey.includes('_seg')) {
          const symbol = entry.dataKey.split('_seg')[0];
          if (symbol === targetSymbol) {
            const state = entry.payload[`${symbol}_state`];
            const stateReason = entry.payload[`${symbol}_stateReason`];
            tickerData = {
              value: entry.value,
              state,
              color: entry.stroke,
              stateReason,
            };
            break;
          }
        }
      }

      if (!tickerData) return null;

      const stateColorClass = (() => {
        switch (tickerData.state) {
          case 'Best': return 'bg-green-600';
          case 'Good': return 'bg-emerald-500';
          case 'OnWatch': return 'bg-blue-600';
          case 'NotGood': return 'bg-yellow-600';
          case 'Bad': return 'bg-gray-600';
          default: return 'bg-gray-600';
        }
      })();

      return (
        <div className="bg-zinc-800 border border-zinc-700 p-2 text-xs shadow-lg">
          <div className="mb-1 text-gray-400">{label}</div>
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <div
                className="w-2 h-2 rounded-full"
                style={{ backgroundColor: tickerData.color }}
              />
              <span className="font-medium" style={{ color: tickerData.color }}>
                {targetSymbol}
              </span>
              <span style={{ color: tickerData.color }}>
                {tickerData.value >= 0 ? '+' : ''}{tickerData.value.toFixed(2)}%
              </span>
            </div>
            <div className="flex items-center gap-2 ml-4">
              <span className={`px-1 py-0.5 text-[10px] ${stateColorClass} rounded`}>
                {tickerData.state}
              </span>
              {tickerData.stateReason && (
                <span className="text-gray-400 text-[10px]">
                  {tickerData.stateReason}
                </span>
              )}
            </div>
          </div>
        </div>
      );
    });
  }, [hoveredTicker]);

  // Memoized CustomLegend
  const CustomLegend = useMemo(() => {
    return memo(({ payload }: any) => {
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
    });
  }, []);

  // Memoize displayed rank data to avoid recalculation on every render
  const displayedRankData = useMemo(() => {
    let result: TickerDataWithHistory[];

    if (focusMode && syncGroup && selectedSymbol) {
      // Focus mode: show only selected symbol if its current state matches the filter
      result = rankData.filter(
        item => item.symbol === selectedSymbol && selectedStates.has(item.state)
      );
    } else {
      // Normal mode: filter by selected states AND subscription status
      result = rankData.filter(item => {
        const matchesState = selectedStates.has(item.state);
        // If we have subscriptions, filter by them; otherwise show all
        const matchesSubscription = subscribedTickers.size === 0 || subscribedTickers.has(item.symbol);
        return matchesState && matchesSubscription;
      });
    }
    return result;
  }, [rankData, focusMode, syncGroup, selectedSymbol, selectedStates, subscribedTickers]);

  // Memoize filtered chart data
  const filteredChartData = useMemo(() => {
    const selectedTimeRangeOption = timeRangeOptions.find(opt => opt.value === timeRange);

    // In 'free' mode, use brush indices; otherwise use time-based filtering
    if (timeRange === 'free' && brushStartIndex !== undefined && brushEndIndex !== undefined) {
      // Free mode with brush - data is already filtered by Brush component
      return chartData;
    } else if (selectedTimeRangeOption && selectedTimeRangeOption.minutes !== null && chartData.length > 0) {
      // Use the latest timestamp from chart data (not machine time) for replay mode compatibility
      const latestTimestamp = Math.max(...chartData.map(p => p.timestamp || 0));
      const cutoffTime = latestTimestamp - selectedTimeRangeOption.minutes * 60 * 1000;
      return chartData.filter(point => point.timestamp >= cutoffTime);
    }
    return chartData;
  }, [chartData, timeRange, brushStartIndex, brushEndIndex]);

  // Handle brush change for free mode - memoized
  const handleBrushChange = useCallback((brushData: { startIndex?: number; endIndex?: number }) => {
    if (brushData.startIndex !== undefined && brushData.endIndex !== undefined) {
      setBrushStartIndex(brushData.startIndex);
      setBrushEndIndex(brushData.endIndex);
    }
  }, []);

  // Reset zoom to show all data - memoized
  const resetZoom = useCallback(() => {
    setBrushStartIndex(undefined);
    setBrushEndIndex(undefined);
    setTimeRange('all');
  }, []);

  const handleTimeRangeChange = useCallback((value: string) => {
    setTimeRange(value);
    setShowTimeRangeMenu(false);
    // Reset brush when changing time range
    if (value !== 'free') {
      setBrushStartIndex(undefined);
      setBrushEndIndex(undefined);
    }
    onSettingsChange?.({
      overviewChart: {
        selectedStates: Array.from(selectedStates),
        focusMode,
        timeRange: value,
      }
    });
  }, [selectedStates, focusMode, onSettingsChange]);

  // Throttled hover handlers to reduce state updates
  const handleLineMouseEnter = useCallback((symbol: string) => {
    if (hoverTimeoutRef.current) {
      clearTimeout(hoverTimeoutRef.current);
    }
    setHoveredTicker(symbol);
  }, []);

  const handleLineMouseLeave = useCallback(() => {
    if (hoverTimeoutRef.current) {
      clearTimeout(hoverTimeoutRef.current);
    }
    // Debounce the leave to avoid flickering between lines
    hoverTimeoutRef.current = setTimeout(() => {
      setHoveredTicker(null);
    }, 50);
  }, []);

  const handleChartMouseLeave = useCallback(() => {
    if (hoverTimeoutRef.current) {
      clearTimeout(hoverTimeoutRef.current);
    }
    setHoveredTicker(null);
  }, []);

  const getConnectionStatusColor = useCallback(() => {
    switch (connectionStatus) {
      case 'connected': return 'text-green-500';
      case 'connecting': return 'text-yellow-500';
      case 'error': return 'text-red-500';
      default: return 'text-gray-500';
    }
  }, [connectionStatus]);

  // Memoize the lines to render
  const chartLines = useMemo(() => {
    return displayedRankData.flatMap((item) => {
      const segments = segmentInfo[item.symbol] || [];
      return segments.map((segment) => ({
        key: segment.key,
        symbol: item.symbol,
        color: segment.color,
      }));
    });
  }, [displayedRankData, segmentInfo]);

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header */}
      <div className="p-3 border-b border-zinc-800">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            {/* Connection status indicator */}
            <div className={`flex items-center gap-1 ${getConnectionStatusColor()}`} title={`Status: ${connectionStatus}`}>
              {isConnected ? <Wifi className="w-3 h-3" /> : <WifiOff className="w-3 h-3" />}
            </div>
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
            {/* Mock/Live toggle */}
            <button
              onClick={() => setUseMockData(!useMockData)}
              className={`text-xs px-2 py-0.5 rounded ${useMockData ? 'bg-yellow-600' : 'bg-green-700'}`}
              title={useMockData ? 'Using mock data' : 'Using live data'}
            >
              {useMockData ? 'Mock' : 'Live'}
            </button>
          </div>

          <div className="flex items-center gap-2">
            {/* Reset zoom button (only for free mode with active zoom) */}
            {timeRange === 'free' && (brushStartIndex !== undefined || brushEndIndex !== undefined) && (
              <button
                onClick={resetZoom}
                className="p-1 hover:bg-zinc-800 transition-colors rounded text-xs text-gray-400 flex items-center gap-1"
                title="Reset zoom"
              >
                <Maximize2 className="w-3 h-3" />
                Reset
              </button>
            )}
            {/* Refresh button (only for live mode) */}
            {!useMockData && (
              <button
                onClick={refresh}
                className="p-1 hover:bg-zinc-800 transition-colors rounded text-xs text-gray-400"
                title="Refresh data"
              >
                ↻
              </button>
            )}
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
              <LineChart
                data={filteredChartData}
                margin={{ top: 10, right: 20, left: 10, bottom: timeRange === 'free' ? 40 : 10 }}
                onMouseLeave={handleChartMouseLeave}
              >
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

                {/* Render line segments using memoized line data */}
                {chartLines.map((line) => (
                  <Line
                    key={line.key}
                    type="monotone"
                    dataKey={line.key}
                    stroke={line.color}
                    strokeWidth={hoveredTicker === line.symbol ? 3 : 2}
                    strokeOpacity={hoveredTicker && hoveredTicker !== line.symbol ? 0.3 : 1}
                    dot={false}
                    isAnimationActive={false}
                    connectNulls={false}
                    legendType="none"
                    onMouseEnter={() => handleLineMouseEnter(line.symbol)}
                    onMouseLeave={handleLineMouseLeave}
                    style={{ cursor: 'pointer' }}
                  />
                ))}

                {/* Brush for free zoom mode */}
                {timeRange === 'free' && (
                  <Brush
                    dataKey="date"
                    height={30}
                    stroke="#3b82f6"
                    fill="#1f2937"
                    onChange={handleBrushChange}
                    startIndex={brushStartIndex}
                    endIndex={brushEndIndex}
                  />
                )}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </div>
  );
}
