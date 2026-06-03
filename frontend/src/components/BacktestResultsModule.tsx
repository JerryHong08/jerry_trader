/**
 * BacktestResultsModule - Multi-layer results browser with pagination
 *
 * Features:
 * - Level 1: Experiments table (experiment_id, date, status, signals, avg_return, win_rate)
 * - Level 2: Drill-down to tickers table per experiment
 * - Auto-refresh when experiment completes
 * - Click ticker to load chart in BacktestChartModule
 * - Virtual scrolling for large datasets
 */

import React, { useState, useEffect, useMemo, useRef } from 'react';
import { RefreshCw, ChevronRight, ChevronDown, TrendingUp, TrendingDown, Clock, Signal } from 'lucide-react';
import type { ModuleProps } from '../types';

// ============================================================================
// Types
// ============================================================================

interface ExperimentRow {
  experiment_id: string;
  date: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  total_signals: number;
  avg_return: number;
  win_rate: number;
  created_at: string | null;
}

interface TickerRow {
  ticker: string;
  signal_count: number;
  avg_return: number;
  win_rate: number;
}

// ============================================================================
// Constants
// ============================================================================

const BACKTEST_SELECT_EVENT = 'backtest:result-selected';
const BACKTEST_API_URL = (() => {
  const defaultHost = typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  return (import.meta.env.VITE_BACKTEST_API_URL as string | undefined) || `http://${defaultHost}:5005/api/backtest`;
})();

const PAGE_SIZE = 50; // Experiments per page
const TICKER_PAGE_SIZE = 30; // Tickers per expanded view

// ============================================================================
// Component
// ============================================================================

export function BacktestResultsModule({
  moduleId,
  onRemove,
  syncGroup,
  onSyncGroupChange,
  settings,
  onSettingsChange,
}: ModuleProps) {
  const [experiments, setExperiments] = useState<ExperimentRow[]>([]);
  const [expandedExperiments, setExpandedExperiments] = useState<Set<string>>(new Set());
  const [tickersMap, setTickersMap] = useState<Map<string, TickerRow[]>>(new Map());
  const [loading, setLoading] = useState(false);
  const [selectedExperimentId, setSelectedExperimentId] = useState<string | null>(null);
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null);

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);

  // Virtual scroll ref
  const scrollContainerRef = useRef<HTMLDivElement>(null);

  // Load experiments on mount
  useEffect(() => {
    loadExperiments();
  }, [currentPage]);

  // Auto-refresh when experiment completes
  useEffect(() => {
    const handleExperimentComplete = () => {
      loadExperiments();
    };

    window.addEventListener('backtest:experiment-complete', handleExperimentComplete as EventListener);
    return () => {
      window.removeEventListener('backtest:experiment-complete', handleExperimentComplete as EventListener);
    };
  }, []);

  const loadExperiments = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${BACKTEST_API_URL}/experiments`);
      if (response.ok) {
        const data = await response.json();
        const allExperiments: ExperimentRow[] = (data.experiments || []).map((exp: Record<string, unknown>) => ({
          experiment_id: exp.experiment_id,
          date: exp.date,
          status: exp.status,
          total_signals: exp.total_signals || 0,
          avg_return: 0,
          win_rate: 0,
          created_at: exp.created_at,
        }));

        setTotalCount(allExperiments.length);
        setExperiments(allExperiments);

        // Load diagnostics for completed experiments (paginated)
        const completedExps = allExperiments
          .filter(e => e.status === 'completed')
          .slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

        for (const exp of completedExps) {
          loadExperimentDiagnostics(exp.experiment_id);
        }
      }
    } catch (e) {
      console.error('Failed to load experiments:', e);
    } finally {
      setLoading(false);
    }
  };

  const loadExperimentDiagnostics = async (experimentId: string) => {
    try {
      const response = await fetch(`${BACKTEST_API_URL}/diagnostics/${experimentId}`);
      if (response.ok) {
        const data = await response.json();
        setExperiments(prev => prev.map(exp =>
          exp.experiment_id === experimentId
            ? { ...exp, avg_return: data.avg_return || 0, win_rate: data.win_rate || 0 }
            : exp
        ));
      }
    } catch (e) {
      console.error('Failed to load diagnostics:', e);
    }
  };

  const loadExperimentTickers = async (experimentId: string) => {
    try {
      const response = await fetch(`${BACKTEST_API_URL}/experiments/${experimentId}/tickers`);
      if (response.ok) {
        const data = await response.json();
        setTickersMap(prev => new Map(prev).set(experimentId, data.tickers || []));
      }
    } catch (e) {
      console.error('Failed to load tickers:', e);
    }
  };

  const handleExperimentExpand = async (experimentId: string) => {
    const newExpanded = new Set(expandedExperiments);
    if (newExpanded.has(experimentId)) {
      newExpanded.delete(experimentId);
    } else {
      newExpanded.add(experimentId);
      if (!tickersMap.has(experimentId)) {
        await loadExperimentTickers(experimentId);
      }
    }
    setExpandedExperiments(newExpanded);
  };

  const handleTickerSelect = (experimentId: string, ticker: string) => {
    setSelectedExperimentId(experimentId);
    setSelectedTicker(ticker);

    // Dispatch event for BacktestChartModule
    window.dispatchEvent(new CustomEvent(BACKTEST_SELECT_EVENT, {
      detail: { experiment_id: experimentId, date: '', status: 'completed' }
    }));

    // Also dispatch ticker event for chart to load directly
    window.dispatchEvent(new CustomEvent('backtest:ticker-selected', {
      detail: { experiment_id: experimentId, ticker: ticker }
    }));
  };

  // Paginated experiments
  const paginatedExperiments = useMemo(() => {
    return experiments.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);
  }, [experiments, currentPage]);

  const totalPages = Math.ceil(totalCount / PAGE_SIZE);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'text-emerald-400';
      case 'running': return 'text-yellow-400';
      case 'failed': return 'text-red-400';
      default: return 'text-zinc-400';
    }
  };

  const getReturnColor = (returnPct: number) => {
    return returnPct >= 0 ? 'text-emerald-400' : 'text-red-400';
  };

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header */}
      <div className="p-3 border-b border-zinc-800 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Signal className="w-4 h-4 text-zinc-400" />
          <span className="text-sm font-medium">Results Browser</span>
          <span className="text-xs text-zinc-500">({totalCount} experiments)</span>
        </div>
        <button
          onClick={loadExperiments}
          disabled={loading}
          className="p-1 hover:bg-zinc-700 rounded disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 text-zinc-400 ${loading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {/* Table Header */}
      <div className="px-3 py-2 border-b border-zinc-800 bg-zinc-800 text-xs text-zinc-400 grid grid-cols-[1fr_80px_70px_60px_60px_60px] gap-2">
        <div>Experiment</div>
        <div>Date</div>
        <div>Status</div>
        <div>Signals</div>
        <div>Return</div>
        <div>Win%</div>
      </div>

      {/* Experiments List with virtual scroll */}
      <div ref={scrollContainerRef} className="flex-1 overflow-auto">
        {experiments.length === 0 && !loading && (
          <div className="p-4 text-xs text-zinc-500 text-center">
            No experiments yet. Run a backtest to generate signals.
          </div>
        )}

        {paginatedExperiments.map(exp => (
          <div key={exp.experiment_id} className="border-b border-zinc-800">
            {/* Experiment Row */}
            <div
              className={`px-3 py-2 grid grid-cols-[1fr_80px_70px_60px_60px_60px] gap-2 text-xs cursor-pointer hover:bg-zinc-800 ${
                selectedExperimentId === exp.experiment_id ? 'bg-zinc-800' : ''
              }`}
              onClick={() => handleExperimentExpand(exp.experiment_id)}
            >
              <div className="flex items-center gap-1">
                {expandedExperiments.has(exp.experiment_id) ? (
                  <ChevronDown className="w-3 h-3 text-zinc-500" />
                ) : (
                  <ChevronRight className="w-3 h-3 text-zinc-500" />
                )}
                <span className="text-zinc-300 truncate">{exp.experiment_id}</span>
              </div>
              <div className="text-zinc-400">{exp.date}</div>
              <div className={getStatusColor(exp.status)}>{exp.status}</div>
              <div className="text-zinc-400">{exp.total_signals}</div>
              <div className={getReturnColor(exp.avg_return)}>
                {exp.avg_return !== 0 ? `${exp.avg_return.toFixed(1)}%` : '-'}
              </div>
              <div className="text-zinc-400">
                {exp.win_rate !== 0 ? `${(exp.win_rate * 100).toFixed(0)}%` : '-'}
              </div>
            </div>

            {/* Expanded Tickers */}
            {expandedExperiments.has(exp.experiment_id) && (
              <div className="bg-zinc-950 pl-6 pr-3 py-1">
                {tickersMap.get(exp.experiment_id)?.length === 0 && (
                  <div className="text-xs text-zinc-500 py-2">No tickers with signals</div>
                )}
                {tickersMap.get(exp.experiment_id)?.slice(0, TICKER_PAGE_SIZE).map(tickerRow => (
                  <div
                    key={tickerRow.ticker}
                    className={`py-1 px-2 text-xs grid grid-cols-[60px_60px_60px_60px] gap-2 cursor-pointer hover:bg-zinc-800 rounded ${
                      selectedTicker === tickerRow.ticker && selectedExperimentId === exp.experiment_id
                        ? 'bg-zinc-800 border border-zinc-600'
                        : ''
                    }`}
                    onClick={() => handleTickerSelect(exp.experiment_id, tickerRow.ticker)}
                  >
                    <div className="text-zinc-300 font-medium">{tickerRow.ticker}</div>
                    <div className="text-zinc-400">{tickerRow.signal_count}</div>
                    <div className={getReturnColor(tickerRow.avg_return)}>
                      {tickerRow.avg_return.toFixed(1)}%
                    </div>
                    <div className="text-zinc-400">
                      {(tickerRow.win_rate * 100).toFixed(0)}%
                    </div>
                  </div>
                ))}
                {tickersMap.get(exp.experiment_id)?.length > TICKER_PAGE_SIZE && (
                  <div className="text-xs text-zinc-500 py-1 text-center">
                    +{tickersMap.get(exp.experiment_id)!.length - TICKER_PAGE_SIZE} more tickers
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="p-2 border-t border-zinc-800 flex items-center justify-center gap-2">
          <button
            onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
            disabled={currentPage === 1}
            className="px-2 py-1 text-xs bg-zinc-800 hover:bg-zinc-700 rounded disabled:opacity-50"
          >
            Prev
          </button>
          <span className="text-xs text-zinc-400">
            Page {currentPage} of {totalPages}
          </span>
          <button
            onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
            disabled={currentPage === totalPages}
            className="px-2 py-1 text-xs bg-zinc-800 hover:bg-zinc-700 rounded disabled:opacity-50"
          >
            Next
          </button>
        </div>
      )}

      {/* Selected Info */}
      {selectedExperimentId && selectedTicker && (
        <div className="p-2 border-t border-zinc-800 text-xs text-zinc-400 flex items-center justify-between">
          <span>
            Selected: <span className="text-zinc-300">{selectedTicker}</span> in <span className="text-zinc-300">{selectedExperimentId}</span>
          </span>
          <span className="text-emerald-400">
            View in BacktestChartModule
          </span>
        </div>
      )}

      {/* Footer */}
      <div className="p-2 border-t border-zinc-800 text-xs text-zinc-500 text-center">
        Click experiment to expand · Click ticker to view chart
      </div>
    </div>
  );
}
