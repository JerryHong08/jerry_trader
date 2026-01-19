import React, { useState, useEffect, useRef, useMemo, useCallback, memo } from 'react';
import { TrendingUp, TrendingDown, ArrowUpDown, ArrowUp, ArrowDown, Settings, X, Newspaper, Wifi, WifiOff, Eye, EyeOff } from 'lucide-react';
import type { ModuleProps, RankItem, TickerState, RankListSortColumn, RankListSortDirection } from '../types';
import { useBackendTimestamp, timestampStore, parseTimestamp } from '../hooks/useBackendTimestamps';
import { useRankListData, useWebSocketConnection, useChartSubscriptions } from '../hooks/useWebSocket';

// Default column configuration
const DEFAULT_COLUMNS: RankListSortColumn[] = [
  'symbol',
  'state',
  'news',
  'price',
  'change',
  'changePercent',
  'volume',
  'float',
  'relativeVolumeDaily',
  'relativeVolume5min',
  'marketCap',
];

const DEFAULT_COLUMN_WIDTHS: Record<string, number> = {
  '#': 50,
  'symbol': 100,
  'state': 120,
  'news': 80,
  'price': 100,
  'change': 90,
  'changePercent': 100,
  'volume': 90,
  'float': 90,
  'relativeVolumeDaily': 110,
  'relativeVolume5min': 110,
  'marketCap': 110,
};

const COLUMN_LABELS: Record<string, string> = {
  'symbol': 'Symbol',
  'state': 'State',
  'news': 'News',
  'price': 'Price',
  'change': 'Change',
  'changePercent': 'Change %',
  'volume': 'Volume',
  'float': 'Float',
  'relativeVolumeDaily': 'Rel Vol (D)',
  'relativeVolume5min': 'Rel Vol (5m)',
  'marketCap': 'Market Cap',
};

// Mock data for top gainers
const generateMockData = (): RankItem[] => {
  const symbols = ['AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'AMD', 'NFLX', 'COIN', 'PLTR', 'RIVN', 'LCID', 'SOFI', 'BABA', 'NIO', 'SHOP', 'SQ', 'PYPL', 'ROKU'];
  const states: TickerState[] = ['Best', 'Good', 'OnWatch', 'NotGood', 'Bad'];

  return symbols.map((symbol, index) => {
    // Randomly assign news timing: 30% fresh (< 1hr), 30% old (1-24hrs), 40% no news
    const rand = Math.random();
    let latestNewsTime: number | undefined;
    const now = Date.now();

    if (rand < 0.3) {
      // Fresh news (within last hour)
      latestNewsTime = now - Math.random() * 60 * 60 * 1000;
    } else if (rand < 0.6) {
      // Old news (1-24 hours ago)
      latestNewsTime = now - (1 + Math.random() * 23) * 60 * 60 * 1000;
    }
    // else no news (latestNewsTime remains undefined)

    return {
      symbol,
      price: Math.random() * 500 + 50,
      change: Math.random() * 20 - 5,
      changePercent: Math.random() * 15,
      volume: Math.random() * 100000000,
      marketCap: Math.random() * 1000000000000,
      state: states[Math.floor(Math.random() * states.length)],
      float: Math.random() * 500000000,
      relativeVolumeDaily: Math.random() * 5 + 0.5,
      relativeVolume5min: Math.random() * 10 + 0.2,
      latestNewsTime,
    };
  });
};

const getStateColor = (state: TickerState): string => {
  switch (state) {
    case 'Best': return 'bg-green-600';
    case 'Good': return 'bg-emerald-500';
    case 'OnWatch': return 'bg-blue-600';
    case 'NotGood': return 'bg-yellow-600';
    case 'Bad': return 'bg-gray-600';
    default: return 'bg-gray-600';
  }
};

const getNewsAge = (timestamp?: number): 'fresh' | 'old' | 'none' => {
  if (!timestamp) return 'none';
  const ageHours = (Date.now() - timestamp) / (1000 * 60 * 60);
  if (ageHours < 1) return 'fresh';
  if (ageHours < 24) return 'old';
  return 'none';
};

export function RankList({ onRemove, selectedSymbol, onSymbolSelect, settings, onSettingsChange }: ModuleProps) {
  const [data, setData] = useState<RankItem[]>([]);
  const [useMockData, setUseMockData] = useState(false); // Toggle for mock/live data
  const [sortColumn, setSortColumn] = useState<RankListSortColumn>(settings?.rankList?.sortColumn || 'changePercent');
  const [sortDirection, setSortDirection] = useState<RankListSortDirection>(settings?.rankList?.sortDirection || 'desc');
  const [visibleColumns, setVisibleColumns] = useState<RankListSortColumn[]>(
    settings?.rankList?.visibleColumns || DEFAULT_COLUMNS
  );
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>(
    settings?.rankList?.columnWidths || DEFAULT_COLUMN_WIDTHS
  );
  const [columnOrder, setColumnOrder] = useState<RankListSortColumn[]>(
    settings?.rankList?.columnOrder || DEFAULT_COLUMNS
  );
  const [showColumnSettings, setShowColumnSettings] = useState(false);

  // Chart subscription management
  const { subscribedTickers, toggleSubscription, isSubscribed, subscribeAll } = useChartSubscriptions();

  // Auto-subscribe top N tickers when data loads (if no subscriptions yet)
  useEffect(() => {
    if (data.length > 0 && subscribedTickers.size === 0) {
      // Subscribe top 20 by default
      const top20 = data.slice(0, 20).map(item => item.symbol);
      subscribeAll(top20);
    }
  }, [data, subscribedTickers.size, subscribeAll]);
  const [resizingColumn, setResizingColumn] = useState<string | null>(null);
  const [draggedColumn, setDraggedColumn] = useState<RankListSortColumn | null>(null);
  const [dragOverColumn, setDragOverColumn] = useState<RankListSortColumn | null>(null);

  // Backend timestamp for market data domain
  const backendTimestamp = useBackendTimestamp('market-data');

  // WebSocket connection for live data
  const connectionStatus = useWebSocketConnection();
  const { data: liveData, timestamp: liveTimestamp, isConnected, refresh } = useRankListData();

  // Use refs for resize state to avoid re-renders during drag
  const resizeStateRef = useRef<{
    column: string | null;
    startX: number;
    startWidth: number;
    currentWidth: number;
  }>({
    column: null,
    startX: 0,
    startWidth: 0,
    currentWidth: 0,
  });

  // Use live data when connected and not using mock mode
  useEffect(() => {
    if (useMockData) {
      // Mock mode is enabled - always use mock data
      setData(generateMockData());
      const interval = setInterval(() => {
        setData(generateMockData());
      }, 5000);
      return () => clearInterval(interval);
    } else if (isConnected && liveData.length > 0) {
      // Live mode with connected backend and data available
      setData(liveData);
      // Update timestamp store with live timestamp
      const parsedTime = parseTimestamp(liveTimestamp);
      if (parsedTime) {
        timestampStore.updateTimestamp('market-data', parsedTime);
      }
    }
    // If not using mock and either not connected or no data, keep existing state
    // This prevents auto-switching to mock data when connection is temporarily lost
  }, [liveData, liveTimestamp, isConnected, useMockData]);

  const formatNumber = useCallback((num: number, decimals = 2) => {
    return num.toFixed(decimals).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }, []);

  const formatVolume = useCallback((vol: number) => {
    if (vol >= 1e9) return `${(vol / 1e9).toFixed(2)}B`;
    if (vol >= 1e6) return `${(vol / 1e6).toFixed(2)}M`;
    if (vol >= 1e3) return `${(vol / 1e3).toFixed(2)}K`;
    return vol.toString();
  }, []);

  const handleRowClick = useCallback((symbol: string) => {
    onSymbolSelect?.(symbol);
  }, [onSymbolSelect]);

  const handleSort = useCallback((column: RankListSortColumn) => {
    let newDirection: RankListSortDirection = 'desc';

    if (column === sortColumn) {
      newDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
      newDirection = column === 'symbol' || column === 'state' ? 'asc' : 'desc';
    }

    const newSettings = {
      rankList: {
        sortColumn: column,
        sortDirection: newDirection,
        visibleColumns,
        columnWidths,
        columnOrder,
      },
    };

    setSortColumn(column);
    setSortDirection(newDirection);
    onSettingsChange?.(newSettings);
  }, [sortColumn, sortDirection, visibleColumns, columnWidths, columnOrder, onSettingsChange]);

  const getSortIcon = useCallback((column: RankListSortColumn) => {
    if (sortColumn !== column) {
      return <ArrowUpDown className="w-3 h-3 opacity-30" />;
    }
    return sortDirection === 'asc'
      ? <ArrowUp className="w-3 h-3" />
      : <ArrowDown className="w-3 h-3" />;
  }, [sortColumn, sortDirection]);

  const toggleColumnVisibility = useCallback((column: RankListSortColumn) => {
    const newVisibleColumns = visibleColumns.includes(column)
      ? visibleColumns.filter(c => c !== column)
      : [...visibleColumns, column];

    setVisibleColumns(newVisibleColumns);

    onSettingsChange?.({
      rankList: {
        sortColumn,
        sortDirection,
        visibleColumns: newVisibleColumns,
        columnWidths,
        columnOrder,
      },
    });
  }, [sortColumn, sortDirection, visibleColumns, columnWidths, columnOrder, onSettingsChange]);

  // Column resizing - using refs to avoid re-renders
  const handleResizeStart = (e: React.MouseEvent, column: string) => {
    e.preventDefault();
    e.stopPropagation();

    const startWidth = columnWidths[column] || DEFAULT_COLUMN_WIDTHS[column];
    resizeStateRef.current = {
      column,
      startX: e.clientX,
      startWidth,
      currentWidth: startWidth,
    };

    setResizingColumn(column);
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  };

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (resizeStateRef.current.column) {
        e.preventDefault();
        const diff = e.clientX - resizeStateRef.current.startX;
        const newWidth = Math.max(50, resizeStateRef.current.startWidth + diff);
        resizeStateRef.current.currentWidth = newWidth;

        // Directly manipulate DOM for smooth resize without re-renders
        const headerCells = document.querySelectorAll(`th[data-column="${resizeStateRef.current.column}"]`);
        headerCells.forEach((cell) => {
          (cell as HTMLElement).style.width = `${newWidth}px`;
        });
      }
    };

    const handleMouseUp = () => {
      if (resizeStateRef.current.column) {
        const column = resizeStateRef.current.column;
        const newWidth = resizeStateRef.current.currentWidth;

        // Update state only once at the end
        const newColumnWidths = { ...columnWidths, [column]: newWidth };
        setColumnWidths(newColumnWidths);

        onSettingsChange?.({
          rankList: {
            sortColumn,
            sortDirection,
            visibleColumns,
            columnWidths: newColumnWidths,
            columnOrder,
          },
        });

        // Reset
        resizeStateRef.current = {
          column: null,
          startX: 0,
          startWidth: 0,
          currentWidth: 0,
        };
        setResizingColumn(null);
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
      }
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [sortColumn, sortDirection, visibleColumns, columnWidths, columnOrder, onSettingsChange]);

  // Column drag and drop
  const handleDragStart = (e: React.DragEvent, column: RankListSortColumn) => {
    if (resizingColumn) {
      e.preventDefault();
      return;
    }
    setDraggedColumn(column);
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDragOver = (e: React.DragEvent, column: RankListSortColumn) => {
    e.preventDefault();
    setDragOverColumn(column);
  };

  const handleDrop = (e: React.DragEvent, targetColumn: RankListSortColumn) => {
    e.preventDefault();
    if (draggedColumn && draggedColumn !== targetColumn) {
      const newOrder = [...columnOrder];
      const draggedIdx = newOrder.indexOf(draggedColumn);
      const targetIdx = newOrder.indexOf(targetColumn);

      newOrder.splice(draggedIdx, 1);
      newOrder.splice(targetIdx, 0, draggedColumn);

      setColumnOrder(newOrder);

      onSettingsChange?.({
        rankList: {
          sortColumn,
          sortDirection,
          visibleColumns,
          columnWidths,
          columnOrder: newOrder,
        },
      });
    }
    setDraggedColumn(null);
    setDragOverColumn(null);
  };

  // Memoize sorted data to avoid re-sorting on every render
  const sortedData = useMemo(() => {
    return [...data].sort((a, b) => {
      let aVal: any = a[sortColumn as keyof RankItem];
      let bVal: any = b[sortColumn as keyof RankItem];

      // Handle news sorting
      if (sortColumn === 'news') {
        aVal = a.latestNewsTime || 0;
        bVal = b.latestNewsTime || 0;
      }

      // Handle string comparison
      if (sortColumn === 'symbol' || sortColumn === 'state') {
        aVal = String(aVal).toLowerCase();
        bVal = String(bVal).toLowerCase();
      }

      if (sortDirection === 'asc') {
        return aVal > bVal ? 1 : -1;
      } else {
        return aVal < bVal ? 1 : -1;
      }
    });
  }, [data, sortColumn, sortDirection]);

  // Memoize ordered visible columns
  const orderedVisibleColumns = useMemo(() => {
    return columnOrder.filter(col => visibleColumns.includes(col));
  }, [columnOrder, visibleColumns]);

  const renderCell = (column: RankListSortColumn, item: RankItem, index: number) => {
    switch (column) {
      case 'symbol':
        return (
          <div className="flex items-center gap-1">
            {item.changePercent > 0 ? (
              <TrendingUp className="w-3 h-3 text-green-500" />
            ) : (
              <TrendingDown className="w-3 h-3 text-red-500" />
            )}
            {item.symbol}
          </div>
        );

      case 'state':
        return (
          <span className={`px-2 py-0.5 text-xs ${getStateColor(item.state)} rounded`}>
            {item.state}
          </span>
        );

      case 'news':
        const newsAge = getNewsAge(item.latestNewsTime);
        if (newsAge === 'none') return null;
        return (
          <div className="flex items-center justify-center">
            {newsAge === 'fresh' ? (
              <div className="bg-red-600 text-white px-2 py-0.5 text-xs rounded flex items-center gap-1">
                <Newspaper className="w-3 h-3" />
                NEW
              </div>
            ) : (
              <Newspaper className="w-4 h-4 text-gray-400" />
            )}
          </div>
        );

      case 'price':
        return <span>${formatNumber(item.price)}</span>;

      case 'change':
        return (
          <span className={item.change >= 0 ? 'text-green-500' : 'text-red-500'}>
            {item.change >= 0 ? '+' : ''}{formatNumber(item.change)}
          </span>
        );

      case 'changePercent':
        return (
          <span className={item.changePercent >= 0 ? 'text-green-500' : 'text-red-500'}>
            {item.changePercent >= 0 ? '+' : ''}{formatNumber(item.changePercent)}%
          </span>
        );

      case 'volume':
        return <span className="text-gray-400">{formatVolume(item.volume)}</span>;

      case 'float':
        return <span className="text-gray-400">{formatVolume(item.float)}</span>;

      case 'relativeVolumeDaily':
        return <span className="text-gray-400">{item.relativeVolumeDaily.toFixed(2)}x</span>;

      case 'relativeVolume5min':
        return <span className="text-gray-400">{item.relativeVolume5min.toFixed(2)}x</span>;

      case 'marketCap':
        return <span className="text-gray-400">${formatVolume(item.marketCap)}</span>;

      default:
        return null;
    }
  };

  const getColumnAlignment = (column: RankListSortColumn): string => {
    if (column === 'symbol' || column === 'state' || column === 'news') {
      return 'text-left';
    }
    return 'text-right';
  };

  const getConnectionStatusColor = () => {
    switch (connectionStatus) {
      case 'connected': return 'text-green-500';
      case 'connecting': return 'text-yellow-500';
      case 'error': return 'text-red-500';
      default: return 'text-gray-500';
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Column Settings Button */}
      <div className="flex items-center justify-between p-2 border-b border-zinc-800 bg-zinc-900">
        <div className="flex items-center gap-3">
          {/* Connection status indicator */}
          <div className={`flex items-center gap-1 ${getConnectionStatusColor()}`} title={`Status: ${connectionStatus}`}>
            {isConnected ? <Wifi className="w-3 h-3" /> : <WifiOff className="w-3 h-3" />}
          </div>
          <div className="text-sm text-gray-400">
            {data.length} symbols
          </div>
          <div className="text-xs text-gray-500">
            Updated: {backendTimestamp}
          </div>
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
          {!useMockData && (
            <button
              onClick={refresh}
              className="p-1 hover:bg-zinc-800 transition-colors rounded text-xs text-gray-400"
              title="Refresh data"
            >
              ↻
            </button>
          )}
          <button
            onClick={() => setShowColumnSettings(!showColumnSettings)}
            className="p-1 hover:bg-zinc-800 transition-colors rounded"
            title="Column Settings"
          >
            <Settings className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Column Settings Panel */}
      {showColumnSettings && (
        <div className="border-b border-zinc-800 bg-zinc-900 p-3">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm">Visible Columns</span>
            <button
              onClick={() => setShowColumnSettings(false)}
              className="p-1 hover:bg-zinc-800 transition-colors rounded"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
          <div className="grid grid-cols-3 gap-2">
            {DEFAULT_COLUMNS.map(column => (
              <label key={column} className="flex items-center gap-2 text-xs cursor-pointer hover:bg-zinc-800 p-1 rounded">
                <input
                  type="checkbox"
                  checked={visibleColumns.includes(column)}
                  onChange={() => toggleColumnVisibility(column)}
                  className="cursor-pointer"
                />
                <span>{COLUMN_LABELS[column]}</span>
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-zinc-800 border-b border-zinc-700">
            <tr>
              {/* Chart subscription toggle column */}
              <th
                className="text-center p-2 text-gray-400 relative"
                style={{ width: 40 }}
                title="Toggle chart display"
              >
                <Eye className="w-3 h-3 mx-auto" />
              </th>
              <th
                className="text-left p-2 text-gray-400 relative"
                style={{ width: columnWidths['#'] || DEFAULT_COLUMN_WIDTHS['#'] }}
              >
                #
              </th>
              {orderedVisibleColumns.map(column => (
                <th
                  key={column}
                  data-column={column}
                  draggable={!resizingColumn}
                  onDragStart={(e) => handleDragStart(e, column)}
                  onDragOver={(e) => handleDragOver(e, column)}
                  onDrop={(e) => handleDrop(e, column)}
                  className={`p-2 text-gray-400 hover:text-white transition-colors select-none relative ${getColumnAlignment(column)} ${
                    dragOverColumn === column && draggedColumn !== column ? 'bg-blue-600/20' : ''
                  }`}
                  style={{ width: columnWidths[column] || DEFAULT_COLUMN_WIDTHS[column] }}
                >
                  <div
                    className="flex items-center gap-1 justify-between cursor-pointer"
                    onClick={() => handleSort(column)}
                  >
                    <span className="flex items-center gap-1">
                      {COLUMN_LABELS[column]}
                      {getSortIcon(column)}
                    </span>
                  </div>
                  {/* Resize Handle */}
                  <div
                    className="absolute right-0 top-0 bottom-0 w-2 cursor-col-resize hover:bg-blue-500 transition-colors z-10"
                    onMouseDown={(e) => handleResizeStart(e, column)}
                    onClick={(e) => e.stopPropagation()}
                  />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedData.map((item, index) => (
              <tr
                key={item.symbol}
                onClick={() => handleRowClick(item.symbol)}
                className={`border-b border-zinc-800 hover:bg-zinc-800/50 transition-colors cursor-pointer ${
                  selectedSymbol === item.symbol ? 'bg-zinc-700/50' : ''
                }`}
              >
                {/* Chart subscription toggle */}
                <td className="p-2 text-center">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleSubscription(item.symbol);
                    }}
                    className={`p-1 rounded transition-colors ${
                      isSubscribed(item.symbol)
                        ? 'text-green-500 hover:text-green-400'
                        : 'text-gray-600 hover:text-gray-400'
                    }`}
                    title={isSubscribed(item.symbol) ? 'Hide from chart' : 'Show in chart'}
                  >
                    {isSubscribed(item.symbol) ? (
                      <Eye className="w-4 h-4" />
                    ) : (
                      <EyeOff className="w-4 h-4" />
                    )}
                  </button>
                </td>
                <td className="p-2 text-gray-500">{index + 1}</td>
                {orderedVisibleColumns.map(column => (
                  <td key={column} className={`p-2 ${getColumnAlignment(column)}`}>
                    {renderCell(column, item, index)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
