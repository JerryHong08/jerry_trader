import { useState, useEffect, useMemo } from 'react';
import {
  Newspaper, Filter, ExternalLink, Search, X, RefreshCw, ChevronDown, ChevronRight,
} from 'lucide-react';
import type { NewsProcessorResult } from '../../types';
import { subscribeNewsProcessorResults, reconnectAgentWebSocket } from '../../hooks/useWebSocket';

// ---- Props -------------------------------------------------------------------

interface MobileNewsPageProps {
  onSelectSymbol?: (symbol: string) => void;
}

// ---- Component ---------------------------------------------------------------

export function MobileNewsPage({ onSelectSymbol }: MobileNewsPageProps) {
  const [results, setResults] = useState<NewsProcessorResult[]>([]);
  const [showFilters, setShowFilters] = useState(false);
  const [model, setModel] = useState('all');
  const [symbolFilter, setSymbolFilter] = useState('');
  const [catalyst, setCatalyst] = useState<'all' | 'true' | 'false'>('true');
  const [searchText, setSearchText] = useState('');
  const [availableModels, setAvailableModels] = useState<string[]>([]);

  // Subscribe to news processor results
  useEffect(() => {
    const unsub = subscribeNewsProcessorResults((data) => {
      const result: NewsProcessorResult = {
        id: data.timestamp || Date.now().toString(),
        model: data.model,
        symbol: data.symbol,
        is_catalyst: data.is_catalyst,
        classification: data.classification,
        score: data.score,
        title: data.title,
        published_time: data.published_time,
        current_time: data.current_time,
        explanation: data.explanation,
        url: data.url,
        content_preview: data.content_preview,
        sources: data.sources,
        source_from: data.source_from,
        timestamp: data.timestamp,
      };
      setResults((prev) => [result, ...prev].slice(0, 200));
      setAvailableModels((prev) => {
        const models = new Set([...prev, result.model]);
        return Array.from(models);
      });
    });
    return unsub;
  }, []);

  // Filter
  const filtered = useMemo(() => {
    return results.filter((r) => {
      if (model !== 'all' && r.model !== model) return false;
      if (symbolFilter && !r.symbol.toLowerCase().includes(symbolFilter.toLowerCase())) return false;
      if (catalyst !== 'all') {
        const want = catalyst === 'true';
        if (r.is_catalyst !== want) return false;
      }
      if (searchText) {
        const lower = searchText.toLowerCase();
        const explText = typeof r.explanation === 'object'
          ? JSON.stringify(r.explanation).toLowerCase()
          : String(r.explanation || '').toLowerCase();
        if (
          !r.title.toLowerCase().includes(lower) &&
          !r.symbol.toLowerCase().includes(lower) &&
          !explText.includes(lower) &&
          !r.content_preview.toLowerCase().includes(lower)
        ) return false;
      }
      return true;
    });
  }, [results, model, symbolFilter, catalyst, searchText]);

  const stats = useMemo(() => ({
    total: filtered.length,
    catalysts: filtered.filter((r) => r.is_catalyst).length,
  }), [filtered]);

  const formatTime = (ts: string) => {
    try {
      const d = new Date(ts);
      if (isNaN(d.getTime())) return ts;
      return d.toLocaleString();
    } catch { return ts; }
  };

  const getExplanation = (exp: unknown): string => {
    if (typeof exp === 'string') return exp;
    if ((exp as Record<string, unknown>)?.raw) return (exp as Record<string, unknown>).raw as string;
    return JSON.stringify(exp);
  };

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header */}
      <div className="flex-shrink-0 border-b border-zinc-800 px-3 py-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Newspaper className="w-4 h-4 text-blue-400" />
            <span className="text-sm font-medium">News Room</span>
            <span className="text-2xs text-zinc-500">
              ({stats.total} · {stats.catalysts} catalysts)
            </span>
          </div>
          <div className="flex items-center gap-1">
            <button
              onClick={reconnectAgentWebSocket}
              className="p-1 text-zinc-500 hover:text-white"
              title="Reconnect"
            >
              <RefreshCw className="w-3.5 h-3.5" />
            </button>
            <button
              onClick={() => setShowFilters(!showFilters)}
              className={`flex items-center gap-1 px-2 py-1 text-2xs rounded ${showFilters ? 'bg-zinc-700 text-white' : 'bg-zinc-800 text-zinc-400'}`}
            >
              <Filter className="w-3 h-3" />
              Filters
            </button>
          </div>
        </div>
      </div>

      {/* Collapsible filters */}
      {showFilters && (
        <div className="flex-shrink-0 border-b border-zinc-800 px-3 py-2 space-y-2 bg-zinc-900/50">
          <div className="flex gap-2">
            <select
              value={model}
              onChange={(e) => setModel(e.target.value)}
              className="flex-1 px-2 py-1 text-xs bg-zinc-800 border border-zinc-700 rounded text-zinc-300"
            >
              <option value="all">All Models</option>
              {availableModels.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
            <select
              value={catalyst}
              onChange={(e) => setCatalyst(e.target.value as 'all' | 'true' | 'false')}
              className="px-2 py-1 text-xs bg-zinc-800 border border-zinc-700 rounded text-zinc-300"
            >
              <option value="all">All</option>
              <option value="true">Catalyst</option>
              <option value="false">Non-catalyst</option>
            </select>
          </div>
          <div className="flex gap-2">
            <input
              type="text"
              value={symbolFilter}
              onChange={(e) => setSymbolFilter(e.target.value)}
              placeholder="Symbol..."
              className="flex-1 px-2 py-1 text-xs bg-zinc-800 border border-zinc-700 rounded text-zinc-300 placeholder:text-zinc-600"
            />
            <input
              type="text"
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              placeholder="Search..."
              className="flex-1 px-2 py-1 text-xs bg-zinc-800 border border-zinc-700 rounded text-zinc-300 placeholder:text-zinc-600"
            />
          </div>
        </div>
      )}

      {/* News list */}
      <div className="flex-1 overflow-auto">
        {filtered.length === 0 ? (
          <div className="text-center text-zinc-500 text-sm py-12">
            {results.length === 0 ? 'Waiting for news...' : 'No matching results'}
          </div>
        ) : (
          <div className="divide-y divide-zinc-800/50">
            {filtered.map((item) => (
              <div key={item.id} className="px-3 py-2.5 hover:bg-zinc-800/30 transition-colors">
                {/* Title row */}
                <div className="flex items-start gap-1.5 mb-1">
                  <button
                    onClick={() => onSelectSymbol?.(item.symbol)}
                    className="text-3xs font-bold bg-blue-600 text-white px-1.5 py-0.5 rounded flex-shrink-0"
                  >
                    {item.symbol}
                  </button>
                  <span className="text-xs flex-1">{item.title}</span>
                </div>

                {/* Meta row */}
                <div className="flex items-center gap-2 text-3xs text-zinc-500 mb-1.5">
                  <span>{item.model}</span>
                  <span>{item.score}/10</span>
                  {item.is_catalyst && (
                    <span className="text-emerald-400 font-semibold">✓ catalyst</span>
                  )}
                  <span>{formatTime(item.published_time)}</span>
                </div>

                {/* Explanation preview */}
                <p className="text-2xs text-zinc-400 line-clamp-2 mb-1.5">
                  {getExplanation(item.explanation)}
                </p>

                {/* Link */}
                {item.url && (
                  <a
                    href={item.url} target="_blank" rel="noopener noreferrer"
                    className="text-3xs text-blue-500 flex items-center gap-0.5"
                  >
                    <ExternalLink className="w-3 h-3" /> Read
                  </a>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
