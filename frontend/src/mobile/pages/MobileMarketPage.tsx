import { useState, useMemo, useCallback } from 'react';
import {
  TrendingUp, TrendingDown, ArrowUpDown, ArrowUp, ArrowDown,
  Settings, X, Newspaper, Eye, EyeOff,
  Loader2, RefreshCw,
} from 'lucide-react';
import { useRankListData, useTickerVisibility } from '../../hooks/useWebSocket';
import { formatTimestamp, parseTimestamp } from '../../hooks/useBackendTimestamps';
import { formatNumber, formatVolume } from '../../utils/format';
import type { RankItem, RankListSortColumn, RankListSortDirection } from '../../types';

// ---- Column config ------------------------------------------------------------

const ALL_COLUMNS: RankListSortColumn[] = [
  'symbol', 'state', 'news', 'price', 'changePercent',
  'volume', 'float', 'relativeVolumeDaily', 'relativeVolume5min',
  'marketCap', 'vwap',
];

const COLUMN_LABELS: Record<string, string> = {
  symbol: 'Symbol', state: 'State', news: 'News', price: 'Price',
  changePercent: 'Chg%', volume: 'Vol', float: 'Float',
  relativeVolumeDaily: 'RelVol(D)', relativeVolume5min: 'RelVol(5m)',
  marketCap: 'MktCap', vwap: 'VWAP',
};

const COLUMN_MIN_W: Record<string, number> = {
  symbol: 80, state: 72, news: 52, price: 72, changePercent: 68,
  volume: 60, float: 60, relativeVolumeDaily: 72, relativeVolume5min: 72,
  marketCap: 72, vwap: 64,
};

const STATE_COLORS: Record<string, string> = {
  Best: 'bg-green-600', Good: 'bg-emerald-500', OnWatch: 'bg-blue-600',
  NotGood: 'bg-yellow-600', Bad: 'bg-zinc-600',
};

// ---- Cell renderer ------------------------------------------------------------

function renderCell(column: RankListSortColumn, item: RankItem) {
  switch (column) {
    case 'symbol':
      return (
        <div className="flex items-center gap-1">
          {(item.changePercent ?? 0) > 0
            ? <TrendingUp className="w-3 h-3 text-green-500 flex-shrink-0" />
            : <TrendingDown className="w-3 h-3 text-red-500 flex-shrink-0" />
          }
          <span className="font-semibold">{item.symbol}</span>
        </div>
      );
    case 'state':
      return (
        <span className={`px-1.5 py-0.5 text-3xs rounded whitespace-nowrap ${STATE_COLORS[item.state] || 'bg-zinc-600'}`}>
          {item.state}
        </span>
      );
    case 'news':
      if (item.hasNews === undefined)
        return <Loader2 className="w-3 h-3 animate-spin text-zinc-500" />;
      if (!item.hasNews)
        return <Newspaper className="w-3.5 h-3.5 text-zinc-600" />;
      return (
        <span className="bg-red-600 text-white px-1.5 py-0.5 text-3xs rounded flex items-center gap-0.5 whitespace-nowrap">
          <Newspaper className="w-3 h-3" />NEWS
        </span>
      );
    case 'price':
      return <span className="tabular-nums">${formatNumber(item.price)}</span>;
    case 'changePercent':
      return (
        <span className={`tabular-nums ${(item.changePercent ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
          {(item.changePercent ?? 0) >= 0 ? '+' : ''}{formatNumber(item.changePercent)}%
        </span>
      );
    case 'volume':
      return <span className="text-zinc-400 tabular-nums">{formatVolume(item.volume)}</span>;
    case 'float':
      if (item.float === undefined) return <Loader2 className="w-3 h-3 animate-spin text-zinc-500" />;
      if (item.float === null || item.float === 0) return <span className="text-zinc-600">N/A</span>;
      return <span className="text-zinc-400 tabular-nums">{formatVolume(item.float)}</span>;
    case 'relativeVolumeDaily':
      return <span className="text-zinc-400 tabular-nums">{item.relativeVolumeDaily?.toFixed(2) ?? '-'}x</span>;
    case 'relativeVolume5min':
      return <span className="text-zinc-400 tabular-nums">{item.relativeVolume5min?.toFixed(2) ?? '-'}x</span>;
    case 'marketCap':
      if (item.marketCap === undefined) return <Loader2 className="w-3 h-3 animate-spin text-zinc-500" />;
      if (item.marketCap === null || item.marketCap === 0) return <span className="text-zinc-600">N/A</span>;
      return <span className="text-zinc-400 tabular-nums">${formatVolume(item.marketCap)}</span>;
    case 'vwap':
      return <span className="tabular-nums">${formatNumber(item.vwap)}</span>;
    default:
      return null;
  }
}

// ---- Props --------------------------------------------------------------------

interface MobileMarketPageProps {
  onSelectSymbol: (symbol: string) => void;
}

// ---- Component ----------------------------------------------------------------

export function MobileMarketPage({ onSelectSymbol }: MobileMarketPageProps) {
  const { data: rawData, timestamp, refresh, isConnected } = useRankListData();
  // Drop stale data when disconnected
  const data = isConnected ? rawData : [];
  const timestampET = isConnected && timestamp ? formatTimestamp(parseTimestamp(timestamp)!) : null;
  const { toggleVisibility, isVisible } = useTickerVisibility();

  const [sortColumn, setSortColumn] = useState<RankListSortColumn>('changePercent');
  const [sortDirection, setSortDirection] = useState<RankListSortDirection>('desc');
  const [visibleColumns, setVisibleColumns] = useState<RankListSortColumn[]>(ALL_COLUMNS);
  const [showColSettings, setShowColSettings] = useState(false);

  const handleSort = useCallback((col: RankListSortColumn) => {
    if (col === sortColumn) {
      setSortDirection((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortColumn(col);
      setSortDirection(col === 'symbol' || col === 'state' ? 'asc' : 'desc');
    }
  }, [sortColumn]);

  const sortedData = useMemo(() => {
    return [...data].sort((a, b) => {
      let aVal: unknown = a[sortColumn as keyof RankItem];
      let bVal: unknown = b[sortColumn as keyof RankItem];
      if (sortColumn === 'news') {
        aVal = a.hasNews === true ? 1 : a.hasNews === false ? 0 : -1;
        bVal = b.hasNews === true ? 1 : b.hasNews === false ? 0 : -1;
      }
      if (sortColumn === 'symbol' || sortColumn === 'state') {
        aVal = String(aVal).toLowerCase();
        bVal = String(bVal).toLowerCase();
      }
      if (sortDirection === 'asc') return (aVal as number) > (bVal as number) ? 1 : -1;
      return (aVal as number) < (bVal as number) ? 1 : -1;
    });
  }, [data, sortColumn, sortDirection]);

  const sortIcon = (col: RankListSortColumn) => {
    if (sortColumn !== col) return <ArrowUpDown className="w-3 h-3 opacity-30" />;
    return sortDirection === 'asc' ? <ArrowUp className="w-3 h-3" /> : <ArrowDown className="w-3 h-3" />;
  };

  const toggleCol = (col: RankListSortColumn) => {
    setVisibleColumns((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]
    );
  };

  return (
    <div className="h-full flex flex-col">
      {/* Thin controls */}
      <div className="flex-shrink-0 flex items-center justify-between px-2 py-1 bg-zinc-900">
        <div className="flex items-center gap-2">
          <span className="text-2xs text-zinc-500">{data.length} symbols</span>
          {timestampET && (
            <span className="text-2xs text-zinc-600">Updated: {timestampET}</span>
          )}
        </div>
        <div className="flex items-center gap-0.5">
          <button onClick={refresh} className="p-1 hover:bg-zinc-800 rounded text-zinc-400" title="Refresh">
            <RefreshCw className="w-3 h-3" />
          </button>
          <button
            onClick={() => setShowColSettings(!showColSettings)}
            className={`p-1 rounded ${showColSettings ? 'bg-zinc-700 text-white' : 'text-zinc-400 hover:bg-zinc-800'}`}
            title="Columns"
          >
            <Settings className="w-3 h-3" />
          </button>
        </div>
      </div>

      {/* Column visibility panel */}
      {showColSettings && (
        <div className="flex-shrink-0 px-3 py-2 border-b border-zinc-800 bg-zinc-900">
          <div className="flex items-center justify-between mb-1.5">
            <span className="text-xs text-zinc-400">Visible Columns</span>
            <button onClick={() => setShowColSettings(false)} className="text-zinc-500 hover:text-white">
              <X className="w-3.5 h-3.5" />
            </button>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {ALL_COLUMNS.map((col) => (
              <button
                key={col}
                onClick={() => toggleCol(col)}
                className={`text-3xs px-2 py-1 rounded transition-colors ${
                  visibleColumns.includes(col) ? 'bg-white text-black' : 'bg-zinc-800 text-zinc-500'
                }`}
              >
                {COLUMN_LABELS[col]}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Table */}
      <div className="flex-1 min-h-0 overflow-auto overscroll-contain">
        {sortedData.length === 0 ? (
          <div className="text-center text-zinc-500 py-12 text-sm">
            {data.length === 0 ? 'Waiting for data...' : 'No results'}
          </div>
        ) : (
          <table className="w-max min-w-full text-xs">
            <thead className="sticky top-0 z-10 bg-zinc-800">
              <tr>
                <th className="sticky left-0 z-20 bg-zinc-800 px-1 py-1.5 text-center" style={{ width: 28 }}>
                  <Eye className="w-3 h-3 text-zinc-500 mx-auto" />
                </th>
                <th className="sticky z-20 bg-zinc-800 px-1 py-1.5 text-zinc-500 text-left" style={{ left: 28, width: 28 }}>#</th>
                {visibleColumns.map((col) => (
                  <th
                    key={col}
                    onClick={() => handleSort(col)}
                    className="px-2 py-1.5 text-zinc-400 font-normal cursor-pointer hover:text-white select-none"
                    style={{ minWidth: COLUMN_MIN_W[col] || 64 }}
                  >
                    <div className="flex items-center gap-1">
                      <span>{COLUMN_LABELS[col]}</span>
                      {sortIcon(col)}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedData.map((item, idx) => (
                <tr
                  key={item.symbol}
                  onClick={() => onSelectSymbol(item.symbol)}
                  className="border-b border-zinc-800/50 hover:bg-zinc-800/50 active:bg-zinc-700/50 cursor-pointer"
                >
                  <td className="sticky left-0 z-10 bg-zinc-900 px-1 py-2 text-center" style={{ width: 28 }}>
                    <button
                      onClick={(e) => { e.stopPropagation(); toggleVisibility(item.symbol); }}
                      className={`p-0.5 rounded ${isVisible(item.symbol) ? 'text-emerald-400' : 'text-zinc-600'}`}
                    >
                      {isVisible(item.symbol) ? <Eye className="w-3.5 h-3.5" /> : <EyeOff className="w-3.5 h-3.5" />}
                    </button>
                  </td>
                  <td className="sticky z-10 bg-zinc-900 px-1 py-2 text-zinc-500 text-3xs" style={{ left: 28, width: 28 }}>
                    {idx + 1}
                  </td>
                  {visibleColumns.map((col) => (
                    <td key={col} className="px-2 py-2 whitespace-nowrap" style={{ minWidth: COLUMN_MIN_W[col] || 64 }}>
                      {renderCell(col, item)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
