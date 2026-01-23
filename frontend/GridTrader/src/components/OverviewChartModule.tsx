/**
 * OverviewChartModule - High-performance Overview Chart Module
 *
 * Uses TradingView's Lightweight Charts for optimal rendering performance.
 *
 * Features:
 * 1. Render one line per ticker
 * 2. Hover & Tooltip
 * 3. Focus Mode & Segmentation (when <= 5 tickers)
 * 4. Time Range & Zoom
 */

import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createChart, IChartApi, ISeriesApi, LineSeries, LineData, Time, ColorType, CrosshairMode, LineStyle, PriceScaleMode } from 'lightweight-charts';
import { Filter, Focus, TrendingUp, Wifi, WifiOff, ZoomIn, ZoomOut, Maximize2, RotateCcw } from 'lucide-react';
import type { ModuleProps, RankItem, TickerState } from '../types';
import { useBackendTimestamp, timestampStore, parseTimestamp } from '../hooks/useBackendTimestamps';
import { useOverviewChartData, useWebSocketConnection, useTickerVisibility, setTopN as sendSetTopN, type TickerDataWithHistory } from '../hooks/useWebSocket';
import { GridItem } from './GridItem';

// ============================================================================
// Types & Constants
// ============================================================================

interface StateHistoryPoint {
  timestamp: number;
  state: TickerState;
}

interface LocalTickerDataWithHistory extends RankItem {
  stateHistory: StateHistoryPoint[];
}

// Line colors for each ticker (rotating palette)
const LINE_COLORS = [
  '#2962FF', // Blue
  '#FF6D00', // Orange
  '#00C853', // Green
  '#D500F9', // Purple
  '#FF1744', // Red
  '#00B8D4', // Cyan
  '#FFEA00', // Yellow
  '#F50057', // Pink
  '#00E676', // Light Green
  '#651FFF', // Deep Purple
];

// State colors for segmentation mode
const STATE_COLORS: Record<TickerState, string> = {
  'Best': '#10b981',      // Green
  'Good': '#34d399',      // Emerald
  'OnWatch': '#3b82f6',   // Blue
  'NotGood': '#eab308',   // Yellow
  'Bad': '#6b7280',       // Gray
};

// Helper function to format time in US/New_York timezone
const formatTimeNY = (timestamp: number): string => {
  const date = new Date(timestamp * 1000); // Lightweight Charts uses seconds
  return date.toLocaleTimeString('en-US', {
    timeZone: 'America/New_York',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
};

const formatDateTimeNY = (timestamp: number): string => {
  const date = new Date(timestamp * 1000); // Lightweight Charts uses seconds
  return date.toLocaleString('en-US', {
    timeZone: 'America/New_York',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
};

// Chart theme matching dark mode
const CHART_THEME = {
  layout: {
    background: { type: ColorType.Solid, color: '#18181b' }, // zinc-900
    textColor: '#a1a1aa', // zinc-400
  },
  grid: {
    vertLines: { color: '#27272a' }, // zinc-800
    horzLines: { color: '#27272a' }, // zinc-800
  },
  crosshair: {
    mode: CrosshairMode.Magnet,
    vertLine: {
      color: '#52525b', // zinc-600
      width: 1 as const,
      style: 2, // Dashed
      labelBackgroundColor: '#3f3f46', // zinc-700
    },
    horzLine: {
      color: '#52525b',
      width: 1 as const,
      style: 2,
      labelBackgroundColor: '#3f3f46',
    },
  },
  localization: {
    timeFormatter: (time: number) => {
      // This controls the crosshair label on the time scale
      const date = new Date(time * 1000);
      return date.toLocaleTimeString('en-US', {
        timeZone: 'America/New_York',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      });
    },
  },
  timeScale: {
    borderColor: '#27272a',
    timeVisible: true,
    secondsVisible: false,
    tickMarkFormatter: (time: number) => formatTimeNY(time),
    rightOffset: 60, // Add space so lines end in middle of chart
  },
  rightPriceScale: {
    borderColor: '#27272a',
  },
};

// Note: Backend sends data in Lightweight Charts ready format.
// This eliminates frontend conversion overhead for better performance.

// ============================================================================
// Custom Tooltip Component
// ============================================================================

interface TooltipData {
  symbol: string;
  value: number;
  state: TickerState;
  time: string;
  color: string;
}

interface TooltipProps {
  data: TooltipData | null;
  position: { x: number; y: number };
  visible: boolean;
}

const ChartTooltip: React.FC<TooltipProps> = ({ data, position, visible }) => {
  if (!visible || !data) return null;

  return (
    <div
      className="absolute z-50 bg-zinc-800 border border-zinc-700 rounded-md p-2 text-xs shadow-lg pointer-events-none"
      style={{
        left: position.x + 12,
        top: position.y - 40,
        transform: 'translateX(0)',
      }}
    >
      <div className="flex items-center gap-2 mb-1">
        <div
          className="w-2 h-2 rounded-full"
          style={{ backgroundColor: data.color }}
        />
        <span className="font-semibold text-white">{data.symbol}</span>
      </div>
      <div className="text-gray-400">{data.time}</div>
      <div className="flex items-center gap-2 mt-1">
        <span className={data.value >= 0 ? 'text-green-400' : 'text-red-400'}>
          {data.value >= 0 ? '+' : ''}{data.value.toFixed(2)}%
        </span>
        <span
          className="px-1.5 py-0.5 rounded text-[10px] font-medium"
          style={{ backgroundColor: STATE_COLORS[data.state] + '40', color: STATE_COLORS[data.state] }}
        >
          {data.state}
        </span>
      </div>
    </div>
  );
};

// ============================================================================
// Legend Component
// ============================================================================

interface LegendProps {
  items: { symbol: string; color: string; visible: boolean }[];
  onToggle: (symbol: string) => void;
  hoveredSymbol: string | null;
  onHover: (symbol: string | null) => void;
}

const ChartLegend: React.FC<LegendProps> = ({ items, onToggle, hoveredSymbol, onHover }) => {
  return (
    <div className="flex flex-wrap gap-2 px-2 py-1 bg-zinc-900/50 border-t border-zinc-800">
      {items.map((item) => (
        <button
          key={item.symbol}
          className={`flex items-center gap-1 px-1.5 py-0.5 rounded text-xs transition-all ${
            item.visible
              ? hoveredSymbol === item.symbol
                ? 'bg-zinc-700'
                : 'hover:bg-zinc-800'
              : 'opacity-40'
          }`}
          onClick={() => onToggle(item.symbol)}
          onMouseEnter={() => onHover(item.symbol)}
          onMouseLeave={() => onHover(null)}
        >
          <div
            className="w-3 h-0.5"
            style={{ backgroundColor: item.visible ? item.color : '#52525b' }}
          />
          <span className={item.visible ? 'text-white' : 'text-zinc-500'}>{item.symbol}</span>
        </button>
      ))}
    </div>
  );
};

// ============================================================================
// Main Component
// ============================================================================

// Type for segmented series - each ticker can have multiple series for different states
type SegmentedSeries = {
  mainSeries: ISeriesApi<'Line'>;
  segmentSeries: Map<string, ISeriesApi<'Line'>>; // key: `${symbol}_${stateIndex}`
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
  // Refs
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesMapRef = useRef<Map<string, SegmentedSeries>>(new Map());
  const resizeObserverRef = useRef<ResizeObserver | null>(null);
  const hasInitialFitRef = useRef(false); // Track if we've done the initial fitContent
  const lastDataTimestampRef = useRef<Map<string, number>>(new Map()); // Track last data point time per symbol
  const previousSeriesDataRef = useRef<Record<string, { data: { time: number; value: number }[]; states: { time: number; state: TickerState }[] }>>({}); // Track previous series data for incremental updates
  const seriesInitializedRef = useRef<Set<string>>(new Set()); // Track which series have been initialized with setData
  const timeRangeAppliedRef = useRef<string | null>(null); // Track which time range has been applied to avoid re-applying on data updates

  // State
  const [rankData, setRankData] = useState<LocalTickerDataWithHistory[]>([]);
  const [chartSeriesData, setChartSeriesData] = useState<Map<string, { data: LineData<Time>[]; states: { time: Time; state: TickerState }[] }>>(new Map());
  const [chartReady, setChartReady] = useState(false);
  const [containerReady, setContainerReady] = useState(false);

  // UI State
  const [tooltip, setTooltip] = useState<{ data: TooltipData | null; position: { x: number; y: number }; visible: boolean }>({
    data: null,
    position: { x: 0, y: 0 },
    visible: false,
  });
  // Note: visibleSeries removed - now using visibleTickers from useTickerVisibility (pure UI state)
  const [hoveredSymbol, setHoveredSymbol] = useState<string | null>(null);
  const [showFilter, setShowFilter] = useState(false);
  const [showTimeRangeMenu, setShowTimeRangeMenu] = useState(false);

  // Settings
  const allStatesDefault: TickerState[] = ['Best', 'Good', 'OnWatch', 'NotGood', 'Bad'];
  const defaultStates: TickerState[] = settings?.overviewChart?.selectedStates || allStatesDefault;
  const [selectedStates, setSelectedStates] = useState<Set<TickerState>>(new Set(defaultStates));
  const [focusMode, setFocusMode] = useState(settings?.overviewChart?.focusMode || false);
  const [timeRange, setTimeRange] = useState<string>(settings?.overviewChart?.timeRange || 'all');
  const [useLogScale, setUseLogScale] = useState(true); // Logarithmic scale by default
  const [isFollowingLatest, setIsFollowingLatest] = useState(true); // Track if chart should follow latest data
  const isUserInteractingRef = useRef(false); // Track if user is currently interacting
  const [topN, setTopN] = useState<number>(settings?.overviewChart?.topN || 20); // Number of top tickers to display
  const [showTopNMenu, setShowTopNMenu] = useState(false);

  const topNOptions = [10, 15, 20, 30, 50];

  const timeRangeOptions = [
    { value: 'all', label: 'All', minutes: null },
    { value: '2h', label: '2H', minutes: 120 },
    { value: '1h', label: '1H', minutes: 60 },
    { value: '30m', label: '30M', minutes: 30 },
    { value: '5m', label: '5M', minutes: 5 },
  ];

  // Backend connection
  const connectionStatus = useWebSocketConnection();
  const {
    seriesData: liveSeriesData,
    rankData: liveRankData,
    timestamp: liveTimestamp,
    isConnected,
    refresh
  } = useOverviewChartData(topN);
  const { visibleTickers, toggleVisibility, isVisible, showAll } = useTickerVisibility();

  const backendTimestamp = useBackendTimestamp('market-data');

  // ============================================================================
  // Chart Initialization
  // ============================================================================

  // Use callback ref to detect when container is mounted
  const chartContainerCallback = useCallback((node: HTMLDivElement | null) => {
    if (node) {
      chartContainerRef.current = node;
      setContainerReady(true);
    }
  }, []);

  useEffect(() => {
    if (!containerReady || !chartContainerRef.current) {
      return;
    }

    // Don't recreate if already exists
    if (chartRef.current) {
      return;
    }

    // Create chart
    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      layout: CHART_THEME.layout,
      grid: CHART_THEME.grid,
      crosshair: CHART_THEME.crosshair,
      timeScale: CHART_THEME.timeScale,
      rightPriceScale: {
        ...CHART_THEME.rightPriceScale,
        mode: PriceScaleMode.Logarithmic, // Start with log scale
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: true,
      },
      handleScale: {
        axisPressedMouseMove: true,
        mouseWheel: true,
        pinch: true,
      },
    });

    chartRef.current = chart;

    // Handle crosshair move for tooltip
    chart.subscribeCrosshairMove((param) => {
      if (!param.time || !param.point) {
        setTooltip(prev => ({ ...prev, visible: false }));
        return;
      }

      // Find the series being hovered
      let foundData: TooltipData | null = null;

      seriesMapRef.current.forEach((segSeries, symbol) => {
        // Check main series
        let data = param.seriesData.get(segSeries.mainSeries);

        // If not found in main, check segment series
        if (!data || !('value' in data)) {
          segSeries.segmentSeries.forEach((segmentSeries) => {
            const segData = param.seriesData.get(segmentSeries);
            if (segData && 'value' in segData) {
              data = segData;
            }
          });
        }

        if (data && 'value' in data) {
          const seriesData = chartSeriesData.get(symbol);
          const stateAtTime = seriesData?.states.find(s => s.time === param.time);
          const colorIndex = Array.from(seriesMapRef.current.keys()).indexOf(symbol);

          if (hoveredSymbol === null || hoveredSymbol === symbol) {
            foundData = {
              symbol,
              value: data.value as number,
              state: stateAtTime?.state || 'OnWatch',
              time: formatTime(param.time as number),
              color: LINE_COLORS[colorIndex % LINE_COLORS.length],
            };
          }
        }
      });

      if (foundData) {
        setTooltip({
          data: foundData,
          position: { x: param.point.x, y: param.point.y },
          visible: true,
        });
      }
    });

    // Setup resize observer
    resizeObserverRef.current = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (entry.target === chartContainerRef.current && chartRef.current) {
          chartRef.current.applyOptions({
            width: entry.contentRect.width,
            height: entry.contentRect.height,
          });
        }
      }
    });
    resizeObserverRef.current.observe(chartContainerRef.current);

    // Listen for user scroll/drag interactions on the time scale
    // When user manually scrolls, disable "follow latest" mode
    chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
      if (isUserInteractingRef.current) {
        setIsFollowingLatest(false);
      }
    });

    // Track mouse interactions to detect user-initiated changes
    const container = chartContainerRef.current;
    const handleMouseDown = () => { isUserInteractingRef.current = true; };
    const handleMouseUp = () => { setTimeout(() => { isUserInteractingRef.current = false; }, 100); };
    const handleWheel = () => {
      isUserInteractingRef.current = true;
      setIsFollowingLatest(false);
      setTimeout(() => { isUserInteractingRef.current = false; }, 100);
    };

    container.addEventListener('mousedown', handleMouseDown);
    container.addEventListener('mouseup', handleMouseUp);
    container.addEventListener('mouseleave', handleMouseUp);
    container.addEventListener('wheel', handleWheel);

    // Mark chart as ready
    setChartReady(true);

    return () => {
      resizeObserverRef.current?.disconnect();
      container.removeEventListener('mousedown', handleMouseDown);
      container.removeEventListener('mouseup', handleMouseUp);
      container.removeEventListener('mouseleave', handleMouseUp);
      container.removeEventListener('wheel', handleWheel);
      chart.remove();
      chartRef.current = null;
      seriesMapRef.current.clear();
      setChartReady(false);
    };
  }, [containerReady]);

  // ============================================================================
  // Data Loading
  // ============================================================================

  // Handle live data when available
  useEffect(() => {
    if (isConnected && liveRankData.length > 0) {
      // Convert live rank data to our format
      const convertedData: LocalTickerDataWithHistory[] = liveRankData.map(item => ({
        ...item,
        stateHistory: [{ timestamp: Date.now(), state: item.state }],
      }));

      // Check if we have series data from backend
      const hasSeriesData = liveSeriesData && Object.keys(liveSeriesData).length > 0;

      if (hasSeriesData) {
        // Use backend format data directly - validate and convert
        const symbols = Object.keys(liveSeriesData);

        // Always build validated data for chartSeriesData
        const newSeriesData = new Map<string, { data: LineData<Time>[]; states: { time: Time; state: TickerState }[] }>();
        let hasNewData = false;

        symbols.forEach(symbol => {
          const backendData = liveSeriesData[symbol];
          if (backendData) {
            // Validate and convert time values to numbers
            const validData = backendData.data
              .filter(d => typeof d.time === 'number' && typeof d.value === 'number')
              .map(d => ({ time: d.time as Time, value: d.value }));
            const validStates = backendData.states
              .filter(s => typeof s.time === 'number')
              .map(s => ({ time: s.time as Time, state: s.state }));

            // Check if this is new data (not just a re-render)
            const lastTimestamp = lastDataTimestampRef.current.get(symbol) || 0;
            const latestTime = validData.length > 0 ? (validData[validData.length - 1].time as number) : 0;

            if (latestTime > lastTimestamp) {
              hasNewData = true;
              lastDataTimestampRef.current.set(symbol, latestTime);
            }

            newSeriesData.set(symbol, {
              data: validData,
              states: validStates,
            });
          }
        });

        // Only update state if we have new data to avoid unnecessary re-renders
        if (hasNewData || chartSeriesData.size === 0) {
          setRankData(convertedData);
          setChartSeriesData(newSeriesData);
          previousSeriesDataRef.current = liveSeriesData;
          // Clear initialization so chart effect will call setData with fresh validated data
          seriesInitializedRef.current.clear();
        }
      } else {
        // Fallback: do nothing.
        setRankData(convertedData);
      }

      // Note: No longer setting visibleSeries here - using visibleTickers from Zustand store instead
      // The visibleTickers are pure UI state managed by useTickerVisibility hook

      if (liveTimestamp) {
        const parsedTime = parseTimestamp(liveTimestamp);
        if (parsedTime) {
          timestampStore.updateTimestamp('market-data', parsedTime);
        }
      }
    }
    // Keep existing data if no data source available
  }, [isConnected, liveRankData, liveSeriesData, liveTimestamp]);

  // ============================================================================
  // Series Management
  // ============================================================================

  // Get filtered rank data based on selected states and subscriptions
  const displayedRankData = useMemo(() => {
    if (focusMode && syncGroup && selectedSymbol) {
      const result = rankData.filter(
        item => item.symbol === selectedSymbol && selectedStates.has(item.state)
      );
      return result;
    }

    const result = rankData.filter(item => {
      const matchesState = selectedStates.has(item.state);
      // Filter by visibility - use visibleTickers from Zustand store (pure UI state)
      const isTickerVisible = visibleTickers.has(item.symbol);
      return matchesState && isTickerVisible;
    });
    return result;
  }, [rankData, focusMode, syncGroup, selectedSymbol, selectedStates, visibleTickers]);

  // Check if we should enable segmentation (<=5 tickers)
  const enableSegmentation = useMemo(() => {
    return displayedRankData.length <= 5;
  }, [displayedRankData.length]);

  // Update chart series when data changes
  useEffect(() => {
    if (!chartReady || !chartRef.current) {
      return;
    }
    if (displayedRankData.length === 0) {
      return;
    }
    if (chartSeriesData.size === 0) {
      return;
    }

    const chart = chartRef.current;
    const currentSymbols = new Set(displayedRankData.map(d => d.symbol));

    // Remove series that are no longer displayed
    seriesMapRef.current.forEach((segSeries, symbol) => {
      if (!currentSymbols.has(symbol)) {
        // Remove main series
        try {
          chart.removeSeries(segSeries.mainSeries);
        } catch (e) {
          // Series might already be removed
        }
        // Remove all segment series
        segSeries.segmentSeries.forEach(s => {
          try {
            chart.removeSeries(s);
          } catch (e) {
            // Series might already be removed
          }
        });
        seriesMapRef.current.delete(symbol);
        seriesInitializedRef.current.delete(symbol); // Clear initialization tracking
        lastDataTimestampRef.current.delete(symbol); // Clear timestamp tracking
      }
    });

    // Add or update series
    displayedRankData.forEach((item, index) => {
      const seriesData = chartSeriesData.get(item.symbol);
      if (!seriesData || seriesData.data.length === 0) {
        return;
      }

      let segSeries = seriesMapRef.current.get(item.symbol);
      const baseColor = LINE_COLORS[index % LINE_COLORS.length];

      if (!segSeries) {
        // Create new main series
        const mainSeries = chart.addSeries(LineSeries, {
          color: baseColor,
          lineWidth: 2,
          title: item.symbol,
          priceLineVisible: false,
          lastValueVisible: true,
          crosshairMarkerVisible: true,
          crosshairMarkerRadius: 4,
        });
        segSeries = {
          mainSeries,
          segmentSeries: new Map(),
        };
        seriesMapRef.current.set(item.symbol, segSeries);
      }

      // Handle segmentation mode - use state colors for line segments
      if (enableSegmentation && seriesData.states.length > 0) {
        // Hide main series and show segmented series
        segSeries.mainSeries.applyOptions({ visible: false });

        // Group data points by state segments
        const segments: { state: TickerState; data: LineData<Time>[] }[] = [];
        let currentSegment: { state: TickerState; data: LineData<Time>[] } | null = null;

        seriesData.data.forEach((point, idx) => {
          const stateAtPoint = seriesData.states[idx]?.state || 'OnWatch';

          if (!currentSegment || currentSegment.state !== stateAtPoint) {
            // Start new segment - add overlap point for continuity
            if (currentSegment && currentSegment.data.length > 0) {
              // Add last point of previous segment to new segment for continuity
              currentSegment = { state: stateAtPoint, data: [currentSegment.data[currentSegment.data.length - 1]] };
            } else {
              currentSegment = { state: stateAtPoint, data: [] };
            }
            segments.push(currentSegment);
          }
          currentSegment.data.push(point);
        });

        // Create/update segment series
        const neededSegmentKeys = new Set<string>();
        segments.forEach((segment, segIdx) => {
          const segKey = `${item.symbol}_seg${segIdx}`;
          neededSegmentKeys.add(segKey);

          let segmentSeries = segSeries!.segmentSeries.get(segKey);
          if (!segmentSeries) {
            segmentSeries = chart.addSeries(LineSeries, {
              color: STATE_COLORS[segment.state],
              lineWidth: 2,
              priceLineVisible: false,
              lastValueVisible: false,
              crosshairMarkerVisible: false,
            });
            segSeries!.segmentSeries.set(segKey, segmentSeries);
          }

          segmentSeries.applyOptions({
            color: STATE_COLORS[segment.state],
            visible: true,
          });
          // Validate segment data before setData
          const validSegmentData = segment.data.filter(
            (d): d is LineData<Time> => typeof d.time === 'number' && typeof d.value === 'number'
          );
          if (validSegmentData.length > 0) {
            segmentSeries.setData(validSegmentData);
          }
        });

        // Remove unused segment series
        segSeries.segmentSeries.forEach((s, key) => {
          if (!neededSegmentKeys.has(key)) {
            chart.removeSeries(s);
            segSeries!.segmentSeries.delete(key);
          }
        });
      } else {
        // Non-segmentation mode - use main series only
        // Only call setData if this series hasn't been initialized yet
        if (!seriesInitializedRef.current.has(item.symbol)) {
          segSeries.mainSeries.applyOptions({ visible: true, color: baseColor });
          // Validate data before setData - ensure all times are numbers
          const validData = seriesData.data.filter(
            (d): d is LineData<Time> => typeof d.time === 'number' && typeof d.value === 'number'
          );
          if (validData.length > 0) {
            segSeries.mainSeries.setData(validData);
            seriesInitializedRef.current.add(item.symbol);
          }
        } else {
          // Series already initialized - just update options if needed
          segSeries.mainSeries.applyOptions({ visible: true, color: baseColor });
        }

        // Hide/remove segment series
        segSeries.segmentSeries.forEach(s => {
          s.applyOptions({ visible: false });
        });
      }

      // Highlight on hover
      const isHovered = hoveredSymbol === item.symbol;
      const lineWidth = isHovered ? 3 : 2;
      segSeries.mainSeries.applyOptions({ lineWidth });
      segSeries.segmentSeries.forEach(s => s.applyOptions({ lineWidth }));
    });

    // Only fit content on initial data load, not on subsequent updates
    if (!hasInitialFitRef.current && chartSeriesData.size > 0) {
      chart.timeScale().fitContent();
      hasInitialFitRef.current = true;
    }
  }, [chartReady, displayedRankData, chartSeriesData, enableSegmentation, hoveredSymbol]);

  // ============================================================================
  // Time Range Control
  // ============================================================================

  useEffect(() => {
    if (!chartRef.current || !chartSeriesData.size) return;

    // Only apply time range constraints when following latest data
    // When user has manually scrolled, don't reset the view
    if (!isFollowingLatest) {
      return;
    }

    // Skip if we've already applied this time range (avoid glitch on data updates)
    if (timeRangeAppliedRef.current === timeRange) {
      return;
    }

    const chart = chartRef.current;
    const selectedOption = timeRangeOptions.find(opt => opt.value === timeRange);

    if (selectedOption && selectedOption.minutes !== null) {
      // Get the latest time from data
      const allTimes: number[] = [];
      chartSeriesData.forEach((data) => {
        data.data.forEach(d => allTimes.push(d.time as number));
      });

      if (allTimes.length > 0) {
        const maxTime = Math.max(...allTimes);
        const rangeSeconds = selectedOption.minutes * 60;
        const minTime = maxTime - rangeSeconds;

        // First, set the visible range to show the data
        chart.timeScale().setVisibleRange({
          from: minTime as Time,
          to: maxTime as Time,
        });

        // Then use scrollToPosition to add right offset
        const offsetBars = selectedOption.minutes;

        setTimeout(() => {
          if (chartRef.current) {
            chartRef.current.timeScale().scrollToPosition(offsetBars, false);
          }
        }, 0);

        // Mark this time range as applied
        timeRangeAppliedRef.current = timeRange;
      }
    } else {
      chart.timeScale().fitContent();
      timeRangeAppliedRef.current = timeRange;
    }
  }, [timeRange, chartSeriesData, isFollowingLatest]);

  // ============================================================================
  // Event Handlers
  // ============================================================================

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
      hasInitialFitRef.current = false; // Reset to allow new fit on focus mode change
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

  const handleTimeRangeChange = useCallback((value: string) => {
    setTimeRange(value);
    setShowTimeRangeMenu(false);
    setIsFollowingLatest(true); // Re-enable following when user explicitly selects a time range
    onSettingsChange?.({
      overviewChart: {
        selectedStates: Array.from(selectedStates),
        focusMode,
        timeRange: value,
      }
    });
  }, [selectedStates, focusMode, onSettingsChange]);

  // Note: toggleSeriesVisibility removed - now using toggleVisibility from useTickerVisibility
  // This syncs legend toggles with RankList eye icons and backend

  const resetZoom = useCallback(() => {
    if (!chartRef.current || !chartSeriesData.size) return;

    const chart = chartRef.current;
    const selectedOption = timeRangeOptions.find(opt => opt.value === timeRange);

    // Re-enable following latest data and clear the applied ref to force re-apply
    setIsFollowingLatest(true);
    timeRangeAppliedRef.current = null; // Clear to force re-apply

    if (selectedOption && selectedOption.minutes !== null) {
      // Apply current time range
      const allTimes: number[] = [];
      chartSeriesData.forEach((data) => {
        data.data.forEach(d => allTimes.push(d.time as number));
      });

      if (allTimes.length > 0) {
        const maxTime = Math.max(...allTimes);
        const rangeSeconds = selectedOption.minutes * 60;
        const minTime = maxTime - rangeSeconds;

        chart.timeScale().setVisibleRange({
          from: minTime as Time,
          to: maxTime as Time,
        });

        // Add right offset using scrollToPosition
        const offsetBars = selectedOption.minutes;
        setTimeout(() => {
          if (chartRef.current) {
            chartRef.current.timeScale().scrollToPosition(offsetBars, false);
          }
        }, 0);
      }
    } else {
      // 'All' mode - just fit content
      chart.timeScale().fitContent();
    }
  }, [timeRange, chartSeriesData]);

  const getConnectionStatusColor = useCallback(() => {
    switch (connectionStatus) {
      case 'connected': return 'text-green-500';
      case 'connecting': return 'text-yellow-500';
      case 'error': return 'text-red-500';
      default: return 'text-gray-500';
    }
  }, [connectionStatus]);

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

  // Legend items - use visibleTickers for visibility state (pure UI state)
  const legendItems = useMemo(() => {
    return rankData.map((item, index) => ({
      symbol: item.symbol,
      color: LINE_COLORS[index % LINE_COLORS.length],
      visible: visibleTickers.has(item.symbol),
    }));
  }, [rankData, visibleTickers]);

  // ============================================================================
  // Render
  // ============================================================================

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header */}
      <div className="p-3 border-b border-zinc-800">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className={`flex items-center gap-1 ${getConnectionStatusColor()}`} title={`Status: ${connectionStatus}`}>
              {connectionStatus === 'connected' ? <Wifi size={14} /> : <WifiOff size={14} />}
            </div>
            <span className="text-sm">
              <TrendingUp size={14} className="inline mr-1" />
              Overview
            </span>
            <span className="text-xs text-gray-500">
              ({displayedRankData.length} tickers)
            </span>
            {enableSegmentation && (
              <span className="text-xs text-blue-400 bg-blue-400/20 px-1.5 py-0.5 rounded">
                Segmentation
              </span>
            )}
            <span className="text-xs text-gray-500">
              Updated: {backendTimestamp}
            </span>
          </div>

          <div className="flex items-center gap-2">
            {/* Logarithmic Scale Toggle */}
            <button
              onClick={() => {
                const newLogScale = !useLogScale;
                setUseLogScale(newLogScale);
                if (chartRef.current) {
                  chartRef.current.applyOptions({
                    rightPriceScale: {
                      mode: newLogScale ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
                    },
                  });
                }
              }}
              className={`px-2 py-1 text-xs rounded ${useLogScale ? 'bg-purple-600' : 'bg-zinc-700'}`}
              title={useLogScale ? 'Logarithmic Scale (click for Linear)' : 'Linear Scale (click for Logarithmic)'}
            >
              {useLogScale ? 'Log' : 'Lin'}
            </button>

            {/* Time Range Selector */}
            <div className="relative">
              <button
                onClick={() => setShowTimeRangeMenu(!showTimeRangeMenu)}
                className="flex items-center gap-1 px-2 py-1 text-xs bg-zinc-700 hover:bg-zinc-600 rounded"
              >
                {timeRangeOptions.find(opt => opt.value === timeRange)?.label || 'All'}
              </button>
              {showTimeRangeMenu && (
                <div className="absolute right-0 top-full mt-1 bg-zinc-800 border border-zinc-700 rounded-md shadow-lg z-50">
                  {timeRangeOptions.map((option) => (
                    <button
                      key={option.value}
                      onClick={() => handleTimeRangeChange(option.value)}
                      className={`block w-full px-3 py-1.5 text-xs text-left hover:bg-zinc-700 ${
                        timeRange === option.value ? 'bg-zinc-700' : ''
                      }`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Top N Selector */}
            <div className="relative">
              <button
                onClick={() => setShowTopNMenu(!showTopNMenu)}
                className="flex items-center gap-1 px-2 py-1 text-xs bg-zinc-700 hover:bg-zinc-600 rounded"
                title="Number of top tickers to display"
              >
                Top {topN}
              </button>
              {showTopNMenu && (
                <div className="absolute right-0 top-full mt-1 bg-zinc-800 border border-zinc-700 rounded-md shadow-lg z-50">
                  {topNOptions.map((n) => (
                    <button
                      key={n}
                      onClick={() => {
                        setTopN(n);
                        setShowTopNMenu(false);
                        // Send to backend to persist the setting
                        sendSetTopN(n);
                      }}
                      className={`block w-full px-3 py-1.5 text-xs text-left hover:bg-zinc-700 ${
                        topN === n ? 'bg-zinc-700' : ''
                      }`}
                    >
                      Top {n}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Reset Zoom / Back to Latest */}
            <button
              onClick={resetZoom}
              className={`p-1.5 rounded flex items-center gap-1 ${
                !isFollowingLatest
                  ? 'bg-yellow-600 text-white animate-pulse'
                  : 'text-gray-400 hover:text-white hover:bg-zinc-700'
              }`}
              title={isFollowingLatest ? 'Reset Zoom' : 'Back to Latest (currently in free scroll mode)'}
            >
              <RotateCcw size={14} />
              {!isFollowingLatest && <span className="text-xs">Live</span>}
            </button>

            {/* Focus Mode Toggle */}
            <button
              onClick={toggleFocusMode}
              className={`p-1.5 rounded ${focusMode ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white hover:bg-zinc-700'}`}
              title={focusMode ? 'Focus Mode ON' : 'Focus Mode OFF'}
            >
              <Focus size={14} />
            </button>

            {/* State Filter */}
            <div className="relative">
              <button
                onClick={() => setShowFilter(!showFilter)}
                className="p-1.5 text-gray-400 hover:text-white hover:bg-zinc-700 rounded"
                title="Filter States"
              >
                <Filter size={14} />
              </button>
              {showFilter && (
                <div className="absolute right-0 top-full mt-1 bg-zinc-800 border border-zinc-700 rounded-md p-2 shadow-lg z-50">
                  <div className="text-xs text-gray-400 mb-2">Filter by State</div>
                  {['Best', 'Good', 'OnWatch', 'NotGood', 'Bad'].map((state) => (
                    <label
                      key={state}
                      className="flex items-center gap-2 py-1 cursor-pointer hover:bg-zinc-700 px-1 rounded"
                    >
                      <input
                        type="checkbox"
                        checked={selectedStates.has(state as TickerState)}
                        onChange={() => toggleState(state as TickerState)}
                        className="rounded"
                      />
                      <span className={`w-2 h-2 rounded-full ${getStateColor(state as TickerState)}`} />
                      <span className="text-xs">{state}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>

            {/* Refresh Button */}
            <button
              onClick={() => {
                setIsFollowingLatest(true); // Re-enable following so time range is applied
                timeRangeAppliedRef.current = null; // Clear to force re-apply time range
                refresh();
              }}
              className="px-2 py-1 text-xs bg-zinc-700 hover:bg-zinc-600 rounded"
            >
              Refresh
            </button>
          </div>
        </div>
      </div>

      {/* Chart Container */}
      <div className="flex-1 relative overflow-hidden" style={{ minHeight: 0 }}>
        {/* Always render chart container, but show empty state overlay when no data */}
        <div
          ref={chartContainerCallback}
          className="w-full h-full"
        />
        {displayedRankData.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center text-gray-500 bg-zinc-900/80">
            <div className="text-center">
              <TrendingUp size={48} className="mx-auto mb-2 opacity-50" />
              <p>No tickers to display</p>
              <p className="text-xs mt-1">Adjust filters or subscribe to tickers</p>
            </div>
          </div>
        )}
        <ChartTooltip
          data={tooltip.data}
          position={tooltip.position}
          visible={tooltip.visible}
        />
      </div>

      {/* Legend */}
      {displayedRankData.length > 0 && (
        <ChartLegend
          items={legendItems.filter(item => displayedRankData.some(d => d.symbol === item.symbol))}
          onToggle={toggleVisibility}
          hoveredSymbol={hoveredSymbol}
          onHover={setHoveredSymbol}
        />
      )}
    </div>
  );
}

// ============================================================================
// Helper Functions
// ============================================================================

function formatTime(timestamp: number): string {
  const date = new Date(timestamp * 1000);
  return date.toLocaleTimeString('en-US', {
    timeZone: 'America/New_York',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

export default OverviewChartModule;
