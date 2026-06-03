/**
 * BacktestConfigModule - Unified configuration + progress panel
 *
 * Features:
 * - Date range selection (start_date, end_date)
 * - Event selection dropdown (from events.yaml)
 * - Parameter configuration (hold_duration, exit_threshold)
 * - Run Backtest button
 * - WebSocket progress (real-time status, progress bar, logs)
 * - Result list display
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Play, Calendar, ChevronDown, FileJson, RefreshCw, Wifi, WifiOff, Loader2, CheckCircle2, XCircle, Terminal, Activity, Download } from 'lucide-react';
import type { ModuleProps } from '../types';

// ============================================================================
// Types
// ============================================================================

interface BacktestConfig {
  startDate: string;
  endDate: string;
  event: string;
  holdDuration: number;
  exitThreshold: number;
}

interface ExportedResult {
  filename: string;
  experiment_id: string;
  date: string;
  status: string;
  total_signals: number;
  avg_return?: number;
  win_rate?: number;
}

type ConnectionStatus = 'connecting' | 'connected' | 'disconnected';
type ExperimentStatus = 'pending' | 'running' | 'completed' | 'failed';

interface LogEntry {
  id: number;
  type: 'progress' | 'signal' | 'error' | 'complete' | 'connection';
  message: string;
  timestamp: Date;
}

interface WebSocketMessage {
  type: 'progress' | 'signal' | 'error' | 'complete' | 'connection';
  date?: string;
  step?: string;
  percent?: number;
  ticker?: string;
  entry_time?: number;
  entry_price?: number;
  message?: string;
  experiment_id?: string;
  total_signals?: number;
}

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_EVENT_OPTIONS = [
  { value: 'gap_up_watch', label: 'Gap Up Watch' },
  { value: 'momentum_entry', label: 'Momentum Entry' },
];

const BACKTEST_SELECT_EVENT = 'backtest:result-selected';

const getBacktestApiUrl = (): string => {
  const defaultHost = typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  return (import.meta.env.VITE_BACKTEST_API_URL as string | undefined) || `http://${defaultHost}:5005/api/backtest`;
};
const BACKTEST_API_URL = getBacktestApiUrl();

const getWsUrl = (): string => {
  const defaultHost = typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  return (import.meta.env.VITE_BACKTEST_WS_URL as string | undefined) || `ws://${defaultHost}:5005/ws/backtest`;
};
const WS_URL = getWsUrl();
const MAX_RECONNECT_ATTEMPTS = 3;
const RECONNECT_DELAY_MS = 2000;

// ============================================================================
// Component
// ============================================================================

export function BacktestConfigModule({
  moduleId,
  onRemove,
  syncGroup,
  onSyncGroupChange,
  settings,
  onSettingsChange,
}: ModuleProps) {
  // Config state
  const [config, setConfig] = useState<BacktestConfig>({
    startDate: '2026-03-13',
    endDate: '2026-03-13',
    event: 'gap_up_watch',
    holdDuration: 10,
    exitThreshold: 5,
  });

  const [results, setResults] = useState<ExportedResult[]>([]);
  const [eventOptions, setEventOptions] = useState(DEFAULT_EVENT_OPTIONS);
  const [loading, setLoading] = useState(false);
  const [selectedResult, setSelectedResult] = useState<string | null>(null);

  // WebSocket/Progress state
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('disconnected');
  const [experimentStatus, setExperimentStatus] = useState<ExperimentStatus>('pending');
  const [currentStep, setCurrentStep] = useState<string>('');
  const [progressPercent, setProgressPercent] = useState<number>(0);
  const [totalSignals, setTotalSignals] = useState<number>(0);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [reconnectAttempts, setReconnectAttempts] = useState<number>(0);
  const [currentExperimentId, setCurrentExperimentId] = useState<string | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const logIdRef = useRef<number>(0);
  const logContainerRef = useRef<HTMLDivElement>(null);
  const reconnectAttemptsRef = useRef<number>(0);

  // Add log entry
  const addLog = useCallback((type: LogEntry['type'], message: string) => {
    const id = ++logIdRef.current;
    setLogs(prev => [...prev.slice(-50), { id, type, message, timestamp: new Date() }]);
  }, []);

  // Scroll to bottom of logs
  useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [logs]);

  // WebSocket message handler
  const handleMessage = useCallback((data: WebSocketMessage) => {
    switch (data.type) {
      case 'connection':
        addLog('connection', data.message || 'Connected');
        setConnectionStatus('connected');
        setReconnectAttempts(0);
        break;

      case 'progress':
        setCurrentStep(data.step || '');
        setProgressPercent(data.percent || 0);
        setExperimentStatus('running');
        addLog('progress', `[${data.step}] ${data.percent}%`);
        break;

      case 'signal':
        setTotalSignals(prev => prev + 1);
        addLog('signal', `Signal: ${data.ticker} @ ${data.entry_price}`);
        break;

      case 'error':
        setExperimentStatus('failed');
        addLog('error', data.message || 'Error');
        break;

      case 'complete':
        setExperimentStatus('completed');
        setProgressPercent(100);
        setTotalSignals(data.total_signals || 0);
        addLog('complete', `Completed: ${data.total_signals} signals`);
        window.dispatchEvent(new CustomEvent('backtest:experiment-complete', {
          detail: { experiment_id: data.experiment_id, total_signals: data.total_signals }
        }));
        // Auto-refresh results
        loadAvailableResults();
        break;
    }
  }, [addLog]);

  // Connect WebSocket
  const connectWebSocket = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close();
    }

    setConnectionStatus('connecting');
    addLog('connection', 'Connecting...');

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnectionStatus('connected');
      reconnectAttemptsRef.current = 0;
      setReconnectAttempts(0);
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as WebSocketMessage;
        handleMessage(data);
      } catch (e) {
        addLog('error', `Invalid message: ${event.data}`);
      }
    };

    ws.onerror = () => {
      addLog('error', 'WebSocket error');
    };

    ws.onclose = () => {
      setConnectionStatus('disconnected');
      addLog('connection', 'Disconnected');

      const attempts = reconnectAttemptsRef.current;
      if (attempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttemptsRef.current = attempts + 1;
        setReconnectAttempts(attempts + 1);
        setTimeout(connectWebSocket, RECONNECT_DELAY_MS);
      }
    };
  }, [handleMessage, addLog]);

  // Connect on mount
  useEffect(() => {
    connectWebSocket();
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, []);

  // Load events and results on mount
  useEffect(() => {
    loadEvents();
    loadAvailableResults();
  }, []);

  const loadEvents = async () => {
    try {
      const response = await fetch(`${BACKTEST_API_URL}/events`);
      if (response.ok) {
        const data = await response.json();
        const events = data.events || [];
        const options = events.map((e: string) => ({
          value: e,
          label: e.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
        }));
        if (options.length > 0) {
          setEventOptions(options);
          if (!events.includes(config.event)) {
            setConfig(prev => ({ ...prev, event: events[0] }));
          }
        }
      }
    } catch (e) {
      console.error('Failed to fetch events:', e);
    }
  };

  const loadAvailableResults = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${BACKTEST_API_URL}/experiments`);
      if (response.ok) {
        const data = await response.json();
        const experiments = data.experiments || [];

        const parsedResults: ExportedResult[] = experiments.map((exp: Record<string, unknown>) => ({
          filename: exp.experiment_id,
          experiment_id: exp.experiment_id,
          date: exp.date || '',
          status: exp.status,
          total_signals: exp.total_signals || 0,
          avg_return: exp.avg_return,
          win_rate: exp.win_rate,
        }));

        setResults(parsedResults);
      }
    } catch (e) {
      console.error('Error loading results:', e);
    } finally {
      setLoading(false);
    }
  };

  // Unified refresh - loads both events and results
  const handleRefresh = async () => {
    setLoading(true);
    await Promise.all([loadEvents(), loadAvailableResults()]);
    setLoading(false);
  };

  const handleResultSelect = (result: ExportedResult) => {
    setSelectedResult(result.filename);
    window.dispatchEvent(new CustomEvent(BACKTEST_SELECT_EVENT, {
      detail: { experiment_id: result.experiment_id, date: result.date, status: result.status }
    }));
  };

  const handleExport = async (experimentId: string, format: 'json' | 'csv') => {
    try {
      const response = await fetch(`${BACKTEST_API_URL}/export/${experimentId}?format=${format}`);
      if (!response.ok) {
        throw new Error(`Export failed: ${response.status}`);
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${experimentId}.${format}`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);
    } catch (e) {
      console.error('Export error:', e);
    }
  };

  const handleConfigChange = (key: keyof BacktestConfig, value: string | number) => {
    setConfig(prev => ({ ...prev, [key]: value }));
  };

  const handleRunBacktest = async () => {
    setLoading(true);
    try {
      const response = await fetch(`${BACKTEST_API_URL}/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          date: config.startDate,
          events: [config.event],
          tickers: null,
          hold_duration_minutes: config.holdDuration,
        }),
      });

      if (!response.ok) {
        throw new Error(`API error: ${response.status}`);
      }

      const data = await response.json();
      const experimentId = data.experiment_id;

      setCurrentExperimentId(experimentId);
      setExperimentStatus('pending');
      setProgressPercent(0);
      setTotalSignals(0);
      setCurrentStep('');
      addLog('connection', `Experiment started: ${experimentId}`);

    } catch (e) {
      addLog('error', `Failed to start: ${e}`);
    } finally {
      setLoading(false);
    }
  };

  // Status icons
  const ConnectionIcon = () => {
    switch (connectionStatus) {
      case 'connecting': return <Loader2 className="w-3 h-3 text-yellow-400 animate-spin" />;
      case 'connected': return <Wifi className="w-3 h-3 text-emerald-400" />;
      case 'disconnected': return <WifiOff className="w-3 h-3 text-red-400" />;
    }
  };

  const ExperimentIcon = () => {
    switch (experimentStatus) {
      case 'pending': return <Activity className="w-3 h-3 text-zinc-400" />;
      case 'running': return <Loader2 className="w-3 h-3 text-yellow-400 animate-spin" />;
      case 'completed': return <CheckCircle2 className="w-3 h-3 text-emerald-400" />;
      case 'failed': return <XCircle className="w-3 h-3 text-red-400" />;
    }
  };

  const getLogColor = (type: LogEntry['type']): string => {
    switch (type) {
      case 'progress': return 'text-zinc-400';
      case 'signal': return 'text-emerald-400';
      case 'error': return 'text-red-400';
      case 'complete': return 'text-emerald-500';
      case 'connection': return 'text-blue-400';
      default: return 'text-zinc-400';
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'text-emerald-400';
      case 'running': return 'text-yellow-400';
      case 'failed': return 'text-red-400';
      default: return 'text-zinc-400';
    }
  };

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header */}
      <div className="p-2 border-b border-zinc-800 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium">Backtest</span>
          {currentExperimentId && (
            <span className="text-xs text-zinc-500 truncate max-w-[100px]">{currentExperimentId}</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <ConnectionIcon />
          <ExperimentIcon />
          <button
            onClick={handleRefresh}
            disabled={loading}
            className="p-1 hover:bg-zinc-700 rounded disabled:opacity-50"
          >
            <RefreshCw className={`w-3 h-3 text-zinc-400 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Config Section */}
      <div className="p-3 space-y-3 border-b border-zinc-800">
        {/* Date Range */}
        <div className="space-y-1">
          <label className="text-xs text-zinc-400 flex items-center gap-1">
            <Calendar className="w-3 h-3" />
            Date Range
          </label>
          <div className="flex gap-2">
            <input
              type="date"
              value={config.startDate}
              onChange={(e) => handleConfigChange('startDate', e.target.value)}
              className="flex-1 bg-zinc-800 border border-zinc-700 px-2 py-1 text-xs focus:outline-none focus:border-zinc-500"
            />
            <span className="text-zinc-500 self-center text-xs">to</span>
            <input
              type="date"
              value={config.endDate}
              onChange={(e) => handleConfigChange('endDate', e.target.value)}
              className="flex-1 bg-zinc-800 border border-zinc-700 px-2 py-1 text-xs focus:outline-none focus:border-zinc-500"
            />
          </div>
        </div>

        {/* Event Selection */}
        <div className="space-y-1">
          <label className="text-xs text-zinc-400">Event</label>
          <div className="relative">
            <select
              value={config.event}
              onChange={(e) => handleConfigChange('event', e.target.value)}
              className="w-full appearance-none bg-zinc-800 border border-zinc-700 px-2 py-1 pr-6 text-xs focus:outline-none focus:border-zinc-500 cursor-pointer"
            >
              {eventOptions.map(opt => (
                <option key={opt.value} value={opt.value}>{opt.label}</option>
              ))}
            </select>
            <ChevronDown className="w-3 h-3 text-zinc-400 absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none" />
          </div>
        </div>

        {/* Parameters + Run Button */}
        <div className="flex gap-2 items-end">
          <div className="flex-1 space-y-1">
            <span className="text-xs text-zinc-500">Hold (min)</span>
            <input
              type="number"
              value={config.holdDuration}
              onChange={(e) => handleConfigChange('holdDuration', parseInt(e.target.value) || 10)}
              className="w-full bg-zinc-800 border border-zinc-700 px-2 py-1 text-xs focus:outline-none focus:border-zinc-500"
              min={1}
              max={60}
            />
          </div>
          <button
            onClick={handleRunBacktest}
            disabled={loading || experimentStatus === 'running'}
            className="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white py-1.5 rounded text-xs flex items-center justify-center gap-1 disabled:opacity-50"
          >
            <Play className="w-3 h-3" />
            {experimentStatus === 'running' ? 'Running...' : 'Run'}
          </button>
        </div>
      </div>

      {/* Progress Section */}
      <div className="p-2 border-b border-zinc-800">
        <div className="flex items-center justify-between text-xs text-zinc-400 mb-1">
          <span>{currentStep || 'Idle'}</span>
          <span>{progressPercent}%</span>
        </div>
        <div className="h-1.5 bg-zinc-800 rounded-full overflow-hidden">
          <div
            className="h-full bg-emerald-500 transition-all duration-300"
            style={{ width: `${progressPercent}%` }}
          />
        </div>
        <div className="flex items-center justify-between text-xs mt-1">
          <span className="text-zinc-400">
            Status: <span className={getStatusColor(experimentStatus)}>{experimentStatus}</span>
          </span>
          <span className="text-zinc-400">
            Signals: <span className="text-emerald-400">{totalSignals}</span>
          </span>
        </div>
      </div>

      {/* Log Output */}
      <div className="h-24 overflow-hidden border-b border-zinc-800">
        <div className="p-1 border-b border-zinc-800 flex items-center gap-1">
          <Terminal className="w-3 h-3 text-zinc-400" />
          <span className="text-xs text-zinc-400">Log</span>
        </div>
        <div
          ref={logContainerRef}
          className="h-full overflow-auto p-1 font-mono text-xs bg-zinc-950"
        >
          {logs.length === 0 && (
            <div className="text-zinc-500">Waiting...</div>
          )}
          {logs.slice(-20).map(log => (
            <div key={log.id} className={`${getLogColor(log.type)} mb-0.5`}>
              <span className="text-zinc-500">[{log.timestamp.toLocaleTimeString()}]</span>
              <span className="ml-1">{log.message}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Results List */}
      <div className="flex-1 min-h-0 flex flex-col">
        <div className="p-2 border-b border-zinc-800 flex items-center justify-between">
          <label className="text-xs text-zinc-400">Results ({results.length})</label>
          <button
            onClick={handleRefresh}
            disabled={loading}
            className="p-1 hover:bg-zinc-700 rounded disabled:opacity-50"
          >
            <RefreshCw className={`w-3 h-3 text-zinc-400 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto overflow-x-hidden scrollbar-thin scrollbar-track-zinc-900 scrollbar-thumb-zinc-600">
          {results.length === 0 && !loading && (
            <div className="text-xs text-zinc-500 py-4 text-center">
              No results. Run a backtest.
            </div>
          )}
          {loading && results.length === 0 && (
            <div className="text-xs text-zinc-500 py-4 text-center">
              Loading...
            </div>
          )}
          <div className="p-1 space-y-1">
            {results.map(result => (
              <div key={result.experiment_id} className="flex gap-1">
                <button
                  onClick={() => handleResultSelect(result)}
                  className={`flex-1 text-left p-2 rounded text-xs transition-colors ${
                    selectedResult === result.filename
                      ? 'bg-zinc-700 border border-emerald-600'
                      : 'bg-zinc-800 hover:bg-zinc-700 border border-transparent'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-zinc-200 font-medium truncate">{result.experiment_id}</span>
                    <span className={`px-1.5 py-0.5 rounded text-xs ${
                      result.status === 'completed' ? 'bg-emerald-900/50 text-emerald-400' :
                      result.status === 'running' ? 'bg-yellow-900/50 text-yellow-400' :
                      result.status === 'failed' ? 'bg-red-900/50 text-red-400' :
                      'bg-zinc-700 text-zinc-400'
                    }`}>
                      {result.status}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 mt-1 text-zinc-500">
                    <span>{result.date || '—'}</span>
                    <span>·</span>
                    <span>{result.total_signals} sig</span>
                    {result.avg_return !== undefined && (
                      <>
                        <span>·</span>
                        <span className={result.avg_return >= 0 ? 'text-emerald-400' : 'text-red-400'}>
                          {result.avg_return.toFixed(1)}%
                        </span>
                      </>
                    )}
                    {result.win_rate !== undefined && (
                      <>
                        <span>·</span>
                        <span className="text-zinc-400">
                          {(result.win_rate * 100).toFixed(0)}% win
                        </span>
                      </>
                    )}
                  </div>
                </button>
                {result.status === 'completed' && (
                  <div className="flex gap-0.5">
                    <button
                      onClick={() => handleExport(result.experiment_id, 'json')}
                      className="p-1.5 bg-zinc-800 hover:bg-zinc-700 rounded text-xs"
                      title="Export JSON"
                    >
                      <Download className="w-3 h-3 text-blue-400" />
                    </button>
                    <button
                      onClick={() => handleExport(result.experiment_id, 'csv')}
                      className="p-1.5 bg-zinc-800 hover:bg-zinc-700 rounded text-xs"
                      title="Export CSV"
                    >
                      <FileJson className="w-3 h-3 text-emerald-400" />
                    </button>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="p-1 border-t border-zinc-800 text-xs text-zinc-500 text-center">
        WS: {connectionStatus} | Select experiment → enter ticker in Chart
      </div>
    </div>
  );
}
