import { useState, useEffect, useCallback } from 'react';
import type { ModuleProps, ModuleType } from '../types';

// ── API ──────────────────────────────────────────────────────────────────

const API_URL = (() => {
  const host = typeof window !== 'undefined' ? window.location.hostname : 'localhost';
  return (import.meta as any).env?.VITE_ANALYSIS_API_URL || `http://${host}:5006/api/analysis`;
})();

interface SliceData {
  label: string;
  count: number;
  metrics: Record<string, number>;
}

interface OutcomeData {
  symbol: string;
  score: string;
  is_catalyst: boolean;
  title: string;
  trigger_time: string;
  trigger_price: number | null;
  max_return: number | null;
  return: number | null;
}

interface TraceData {
  date: string;
  dedup: boolean;
  horizon: string;
  total_outcomes: number;
  summary: {
    total_classified: number;
    catalysts: number;
    non_catalysts: number;
    with_return_data: number;
    false_negatives_gt_10pct: number;
  };
  slices: SliceData[];
  top_outcomes: OutcomeData[];
  false_negatives: OutcomeData[];
}

// ── Formatting ───────────────────────────────────────────────────────────

function pct(v: number | null | undefined): string {
  if (v == null) return 'N/A';
  return (v * 100).toFixed(1) + '%';
}

const scoreColors: Record<string, string> = {
  '8-10': '#3fb950',
  '6-7': '#d29922',
  '4-5': '#e67e22',
  '1-3': '#8b949e',
};

// ── Component ────────────────────────────────────────────────────────────

export default function AnalysisModule({ moduleId }: ModuleProps) {
  const [date, setDate] = useState(() => {
    const d = new Date();
    return d.toISOString().slice(0, 10).replace(/-/g, '');
  });
  const [dedup, setDedup] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<TraceData | null>(null);
  const [view, setView] = useState<'slices' | 'top' | 'fn'>('slices');

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(
        `${API_URL}/classifier-trace?date=${date}&dedup=${dedup}&horizon=15m`
      );
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `HTTP ${res.status}`);
      }
      const json: TraceData = await res.json();
      setData(json);
    } catch (e: any) {
      setError(e.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, [date, dedup]);

  useEffect(() => {
    fetchData();
  }, []); // Only on mount; user clicks "Refresh" to re-fetch

  // ── Render ──────────────────────────────────────────────────────────

  return (
    <div className="h-full flex flex-col bg-[#0d1117] text-[#c9d1d9] text-sm overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-3 px-3 py-2 border-b border-[#21262d] shrink-0">
        <span className="font-semibold text-[#f0f6fc] text-xs uppercase tracking-wider">
          Classifier Trace
        </span>
        <input
          type="text"
          value={date}
          onChange={(e) => setDate(e.target.value)}
          placeholder="YYYYMMDD"
          className="bg-[#161b22] border border-[#30363d] rounded px-2 py-0.5 text-xs w-20 text-center"
        />
        <label className="flex items-center gap-1 text-xs text-[#8b949e] cursor-pointer select-none">
          <input
            type="checkbox"
            checked={dedup}
            onChange={(e) => setDedup(e.target.checked)}
            className="accent-[#3fb950]"
          />
          dedup
        </label>
        <button
          onClick={fetchData}
          disabled={loading}
          className="ml-auto bg-[#238636] hover:bg-[#2ea043] text-white text-xs px-3 py-1 rounded disabled:opacity-50"
        >
          {loading ? 'Loading...' : 'Refresh'}
        </button>
      </div>

      {/* Error */}
      {error && (
        <div className="px-3 py-2 text-xs text-[#f85149] bg-[#1f0d0d] border-b border-[#f85149]/30">
          {error}
        </div>
      )}

      {/* Content */}
      {!data && !loading && !error && (
        <div className="flex-1 flex items-center justify-center text-[#8b949e] text-xs">
          Click Refresh to load data
        </div>
      )}

      {data && (
        <>
          {/* Summary bar */}
          <div className="grid grid-cols-5 gap-2 px-3 py-2 border-b border-[#21262d] shrink-0 text-center text-xs">
            <div>
              <div className="font-bold text-sm">{data.summary.total_classified}</div>
              <div className="text-[#8b949e]">Classified</div>
            </div>
            <div>
              <div className="font-bold text-sm text-[#3fb950]">{data.summary.catalysts}</div>
              <div className="text-[#8b949e]">Catalysts</div>
            </div>
            <div>
              <div className="font-bold text-sm text-[#8b949e]">{data.summary.non_catalysts}</div>
              <div className="text-[#8b949e]">Non-Cat</div>
            </div>
            <div>
              <div className="font-bold text-sm">{data.summary.with_return_data}</div>
              <div className="text-[#8b949e]">w/Returns</div>
            </div>
            <div>
              <div className="font-bold text-sm text-[#f85149]">{data.summary.false_negatives_gt_10pct}</div>
              <div className="text-[#8b949e]">FN &gt;+10%</div>
            </div>
          </div>

          {/* View tabs */}
          <div className="flex gap-0 px-3 py-1 border-b border-[#21262d] shrink-0">
            {(['slices', 'top', 'fn'] as const).map((v) => (
              <button
                key={v}
                onClick={() => setView(v)}
                className={`px-3 py-1 text-xs rounded-t transition-colors ${
                  view === v
                    ? 'bg-[#161b22] text-[#f0f6fc] border border-[#30363d] border-b-[#161b22] -mb-px'
                    : 'text-[#8b949e] hover:text-[#c9d1d9]'
                }`}
              >
                {v === 'slices' ? 'Slices' : v === 'top' ? 'Top 10' : 'False Neg'}
              </button>
            ))}
          </div>

          {/* Slice view */}
          {view === 'slices' && (
            <div className="flex-1 overflow-auto px-3 py-2">
              <table className="w-full text-xs border-collapse">
                <thead>
                  <tr className="text-[#8b949e] border-b border-[#21262d] text-left">
                    <th className="py-1 pr-3">Score</th>
                    <th className="py-1 pr-3 text-right">n</th>
                    <th className="py-1 pr-3 text-right">Avg Max</th>
                    <th className="py-1 pr-3 text-right">&gt; +5%</th>
                    <th className="py-1 pr-3 text-right">&gt; +10%</th>
                    <th className="py-1 pr-3 text-right">&gt; +20%</th>
                  </tr>
                </thead>
                <tbody>
                  {data.slices.map((sl) => {
                    const n = sl.count;
                    return (
                      <tr key={sl.label} className="border-b border-[#21262d]/50 hover:bg-[#161b22]/50">
                        <td className="py-1 pr-3 font-bold" style={{ color: scoreColors[sl.label] || '#8b949e' }}>
                          {sl.label}
                        </td>
                        <td className="py-1 pr-3 text-right">{n}</td>
                        <td className="py-1 pr-3 text-right">{pct(sl.metrics['avg_max_return'])}</td>
                        <td className="py-1 pr-3 text-right">
                          <b>{Math.round((sl.metrics['hit_rate_gt_5pct'] || 0) * n)}</b>{' '}
                          <span className="text-[#8b949e]">({pct(sl.metrics['hit_rate_gt_5pct'])})</span>
                        </td>
                        <td className="py-1 pr-3 text-right">
                          <b>{Math.round((sl.metrics['hit_rate_gt_10pct'] || 0) * n)}</b>{' '}
                          <span className="text-[#8b949e]">({pct(sl.metrics['hit_rate_gt_10pct'])})</span>
                        </td>
                        <td className="py-1 pr-3 text-right">
                          <b>{Math.round((sl.metrics['hit_rate_gt_20pct'] || 0) * n)}</b>{' '}
                          <span className="text-[#8b949e]">({pct(sl.metrics['hit_rate_gt_20pct'])})</span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {/* Top 10 view */}
          {view === 'top' && (
            <div className="flex-1 overflow-auto px-3 py-2">
              <table className="w-full text-xs border-collapse">
                <thead>
                  <tr className="text-[#8b949e] border-b border-[#21262d] text-left">
                    <th className="py-1 pr-2"></th>
                    <th className="py-1 pr-3">Sym</th>
                    <th className="py-1 pr-3">Score</th>
                    <th className="py-1 pr-3 text-right">Max 15m</th>
                    <th className="py-1">Title</th>
                  </tr>
                </thead>
                <tbody>
                  {data.top_outcomes.map((o) => (
                    <tr key={o.symbol + o.trigger_time} className="border-b border-[#21262d]/50 hover:bg-[#161b22]/50">
                      <td className="py-1 pr-2">{o.is_catalyst ? '✅' : '❌'}</td>
                      <td className="py-1 pr-3 font-bold">{o.symbol}</td>
                      <td className="py-1 pr-3">{o.score}</td>
                      <td className="py-1 pr-3 text-right text-[#3fb950] font-bold">{pct(o.max_return)}</td>
                      <td className="py-1 text-[#8b949e] max-w-[240px] truncate" title={o.title}>
                        {o.title.slice(0, 55)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* False negatives view */}
          {view === 'fn' && (
            <div className="flex-1 overflow-auto px-3 py-2">
              <table className="w-full text-xs border-collapse">
                <thead>
                  <tr className="text-[#8b949e] border-b border-[#21262d] text-left">
                    <th className="py-1 pr-3">Sym</th>
                    <th className="py-1 pr-3">Score</th>
                    <th className="py-1 pr-3 text-right">Max 15m</th>
                    <th className="py-1">Title</th>
                  </tr>
                </thead>
                <tbody>
                  {data.false_negatives.map((o) => (
                    <tr key={o.symbol + o.max_return} className="border-b border-[#21262d]/50 hover:bg-[#161b22]/50">
                      <td className="py-1 pr-3 font-bold">{o.symbol}</td>
                      <td className="py-1 pr-3">{o.score}</td>
                      <td className="py-1 pr-3 text-right text-[#f85149] font-bold">{pct(o.max_return)}</td>
                      <td className="py-1 text-[#8b949e] max-w-[240px] truncate" title={o.title}>
                        {o.title.slice(0, 50)}
                      </td>
                    </tr>
                  ))}
                  {data.false_negatives.length === 0 && (
                    <tr><td colSpan={4} className="py-4 text-center text-[#8b949e]">None</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
