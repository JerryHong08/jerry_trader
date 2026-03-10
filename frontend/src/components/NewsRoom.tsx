import React, { useState, useEffect, useMemo } from 'react';
import { Newspaper, Filter, ExternalLink, TrendingUp, TrendingDown, Search, X } from 'lucide-react';
import type { ModuleProps, NewsProcessorResult } from '../types';
import { subscribeNewsProcessorResults } from '../hooks/useWebSocket';

interface NewsRoomFilters {
  model: string;
  symbol: string;
  is_catalyst: 'all' | 'true' | 'false';
  searchText: string;
}

export default function NewsRoom({ moduleId }: ModuleProps) {
  const [results, setResults] = useState<NewsProcessorResult[]>([]);
  const [filters, setFilters] = useState<NewsRoomFilters>({
    model: 'all',
    symbol: '',
    is_catalyst: 'all',
    searchText: '',
  });
  const [showFilters, setShowFilters] = useState(true);
  const [availableModels, setAvailableModels] = useState<string[]>([]);

  // Subscribe to news processor results via websocket
  useEffect(() => {
    const unsubscribe = subscribeNewsProcessorResults((data) => {
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

      setResults((prev) => [result, ...prev].slice(0, 500)); // Keep latest 500

      // Update available models
      setAvailableModels((prev) => {
        const models = new Set([...prev, result.model]);
        return Array.from(models);
      });
    });

    return unsubscribe;
  }, []);

  // Filter results
  const filteredResults = useMemo(() => {
    return results.filter((result) => {
      // Model filter
      if (filters.model !== 'all' && result.model !== filters.model) {
        return false;
      }

      // Symbol filter
      if (filters.symbol && !result.symbol.toLowerCase().includes(filters.symbol.toLowerCase())) {
        return false;
      }

      // Catalyst filter
      if (filters.is_catalyst !== 'all') {
        const filterValue = filters.is_catalyst === 'true';
        if (result.is_catalyst !== filterValue) {
          return false;
        }
      }

      // Search text filter (search in title, symbol, explanation)
      if (filters.searchText) {
        const searchLower = filters.searchText.toLowerCase();
        const explanationText = typeof result.explanation === 'object'
          ? JSON.stringify(result.explanation).toLowerCase()
          : String(result.explanation || '').toLowerCase();

        const matches =
          result.title.toLowerCase().includes(searchLower) ||
          result.symbol.toLowerCase().includes(searchLower) ||
          explanationText.includes(searchLower) ||
          result.content_preview.toLowerCase().includes(searchLower);

        if (!matches) {
          return false;
        }
      }

      return true;
    });
  }, [results, filters]);

  // Stats
  const stats = useMemo(() => {
    return {
      total: filteredResults.length,
      catalysts: filteredResults.filter((r) => r.is_catalyst).length,
      nonCatalysts: filteredResults.filter((r) => !r.is_catalyst).length,
    };
  }, [filteredResults]);

  const formatTime = (timeStr: string) => {
    try {
      const date = new Date(timeStr);
      if (isNaN(date.getTime())) return timeStr;
      return date.toLocaleString();
    } catch {
      return timeStr;
    }
  };

  const getExplanationText = (explanation: any) => {
    if (typeof explanation === 'string') {
      return explanation;
    }
    if (explanation?.raw) {
      return explanation.raw;
    }
    return JSON.stringify(explanation, null, 2);
  };

  return (
    <div className="h-full flex flex-col bg-zinc-900 text-white">
      {/* Header */}
      <div className="flex-none border-b border-zinc-800 p-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Newspaper className="w-5 h-5 text-blue-400" />
            <h2 className="text-lg font-semibold">News Room</h2>
            <span className="text-sm text-gray-400">
              ({stats.total} results, {stats.catalysts} catalysts)
            </span>
          </div>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className="flex items-center gap-1 px-2.5 py-1.5 text-xs bg-zinc-800 hover:bg-zinc-700 rounded transition-colors text-gray-400 hover:text-white"
          >
            <Filter className="w-3.5 h-3.5" />
            {showFilters ? 'Hide' : 'Filters'}
          </button>
        </div>
      </div>

      {/* Filters */}
      {showFilters && (
        <div className="flex-none border-b border-zinc-800 p-2.5 bg-zinc-900/50">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-2">
            {/* Model filter */}
            <div>
              <label className="block text-xs text-gray-500 mb-1">Model</label>
              <select
                value={filters.model}
                onChange={(e) => setFilters({ ...filters, model: e.target.value })}
                className="w-full px-3 py-1.5 text-xs bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 text-gray-300"
              >
                <option value="all">All Models</option>
                {availableModels.map((model) => (
                  <option key={model} value={model}>{model}</option>
                ))}
              </select>
            </div>

            {/* Symbol filter */}
            <div>
              <label className="block text-xs text-gray-500 mb-1">Symbol</label>
              <div className="relative">
                <input
                  type="text"
                  value={filters.symbol}
                  onChange={(e) => setFilters({ ...filters, symbol: e.target.value })}
                  placeholder="Filter..."
                  className="w-full px-3 py-1.5 text-xs bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 pr-8 text-gray-300 placeholder:text-gray-600"
                />
                {filters.symbol && (
                  <button
                    onClick={() => setFilters({ ...filters, symbol: '' })}
                    className="absolute right-1 top-1/2 -translate-y-1/2 p-1 hover:bg-zinc-700 rounded"
                  >
                    <X className="w-3 h-3" />
                  </button>
                )}
              </div>
            </div>

            {/* Catalyst filter */}
            <div>
              <label className="block text-xs text-gray-500 mb-1">Catalyst</label>
              <select
                value={filters.is_catalyst}
                onChange={(e) => setFilters({ ...filters, is_catalyst: e.target.value as any })}
                className="w-full px-3 py-1.5 text-xs bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 text-gray-300"
              >
                <option value="all">All</option>
                <option value="true">✅ Catalysts Only</option>
                <option value="false">❌ Non-Catalysts Only</option>
              </select>
            </div>

            {/* Search text */}
            <div>
              <label className="block text-xs text-gray-500 mb-1">Search</label>
              <div className="relative">
                <Search className="w-3.5 h-3.5 absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-600" />
                <input
                  type="text"
                  value={filters.searchText}
                  onChange={(e) => setFilters({ ...filters, searchText: e.target.value })}
                  placeholder="Search..."
                  className="w-full pl-8 pr-8 py-1.5 text-xs bg-zinc-800 border border-zinc-700 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 text-gray-300 placeholder:text-gray-600"
                />
                {filters.searchText && (
                  <button
                    onClick={() => setFilters({ ...filters, searchText: '' })}
                    className="absolute right-1 top-1/2 -translate-y-1/2 p-1 hover:bg-zinc-700 rounded"
                  >
                    <X className="w-3 h-3" />
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Results grid */}
      <div className="flex-1 overflow-y-auto p-3">
        {filteredResults.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-gray-500">
            <Newspaper className="w-12 h-12 mb-2 opacity-50" />
            <p>No news results yet</p>
            <p className="text-sm">Results will appear here in real-time as news is processed</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-3">
            {filteredResults.map((result) => (
              <div
                key={result.id}
                className={`p-3 rounded border transition-all ${
                  result.is_catalyst
                    ? 'bg-green-900/20 border-green-500/40 hover:bg-green-900/30'
                    : 'bg-zinc-800/50 border-zinc-700 hover:bg-zinc-800/70'
                }`}
              >
                {/* Header with symbol and catalyst indicator */}
                <div className="flex items-start justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <span className="text-lg font-bold text-blue-400">{result.symbol}</span>
                    {result.is_catalyst ? (
                      <TrendingUp className="w-5 h-5 text-green-400" />
                    ) : (
                      <TrendingDown className="w-5 h-5 text-gray-500" />
                    )}
                    <span className={`text-sm font-semibold ${result.is_catalyst ? 'text-green-400' : 'text-gray-400'}`}>
                      {result.score}
                    </span>
                  </div>
                  <span className="text-xs text-gray-500">{result.model}</span>
                </div>

                {/* Title */}
                <h3 className="text-sm font-semibold mb-2 line-clamp-2">{result.title}</h3>

                {/* Explanation */}
                <div className="mb-2">
                  <p className="text-xs text-gray-400 line-clamp-3">
                    {getExplanationText(result.explanation)}
                  </p>
                </div>

                {/* Content preview */}
                {result.content_preview && (
                  <p className="text-xs text-gray-500 mb-2 line-clamp-2">
                    {result.content_preview}
                  </p>
                )}

                {/* Footer with time and link */}
                <div className="flex items-center justify-between text-xs text-gray-500 pt-2 border-t border-zinc-700">
                  <span title={`Published: ${formatTime(result.published_time)}`}>
                    {formatTime(result.published_time).split(',')[0]}
                  </span>
                  {result.url && (
                    <a
                      href={result.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-1 text-blue-400 hover:text-blue-300 transition-colors"
                    >
                      <span>Read</span>
                      <ExternalLink className="w-3 h-3" />
                    </a>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
