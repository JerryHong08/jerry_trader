import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Building2, Globe, TrendingUp, DollarSign, Users, Calendar, BarChart3, Newspaper, RefreshCw, ExternalLink, Sparkles, AlertCircle, Loader2 } from 'lucide-react';
import type { ModuleProps, NewsArticle, StockDetailView } from '../types';
import { SymbolSearch } from './common/SymbolSearch';
import { useBackendTimestamp } from '../hooks/useBackendTimestamps';
import { useMarketDataStore } from '../stores/marketDataStore';
import { getCachedProfile, getCachedNews, setCachedProfile, setCachedNews, setDataStatus, subscribeNewsUpdates } from '../hooks/useWebSocket';
import { IS_DEMO, MOCK_PROFILES, getMockNews } from '../data/mockData';

// Get patchStaticData action for updating RankList when profile is fetched
const patchStaticData = (symbol: string, data: { float?: number; marketCap?: number; hasNews?: boolean }) => {
  useMarketDataStore.getState().patchStaticData(symbol, data);
};

// Configuration - use Vite env variable or default
const BFF_HTTP_URL =
  (typeof import.meta !== 'undefined' && import.meta.env?.VITE_BFF_URL)
    ? (import.meta.env.VITE_BFF_URL as string)
    : 'http://localhost:5001';
// Fallback for empty string from .env.ghpages
const BFF_URL = BFF_HTTP_URL || 'http://localhost:5001';

interface StockFundamentals {
  symbol: string;
  companyName: string;
  country: string;
  sector: string;
  industry: string;
  float: number | string;
  marketCap: number | string;
  range: string;
  averageVolume: number | string;
  ipoDate: string;
  fullTimeEmployees: number | string;
  ceo: string;
  website: string;
  description: string;
  exchange: string;
  address: string;
  city: string;
  state: string;
  image: string;
  lastUpdated: string;
}

interface BackendNewsArticle {
  symbol: string;
  title: string;
  url: string;
  sources: string;
  published_time: string;
  text?: string;
}

// Fetch stock profile from backend (or return mock in demo mode)
const fetchStockProfile = async (symbol: string): Promise<StockFundamentals | null> => {
  if (IS_DEMO) {
    return (MOCK_PROFILES[symbol] as StockFundamentals | undefined) ?? null;
  }
  try {
    const response = await fetch(`${BFF_URL}/api/stock/${symbol}/profile`);
    if (!response.ok) {
      console.warn(`Failed to fetch profile for ${symbol}: ${response.status}`);
      return null;
    }
    const responseData = await response.json();

    // Backend returns { ticker, profile: {...}, timestamp } or { error: ... }
    if (responseData.error) {
      console.warn(`Profile error for ${symbol}: ${responseData.error}`);
      return null;
    }

    return responseData.profile as StockFundamentals;
  } catch (error) {
    console.error(`Error fetching profile for ${symbol}:`, error);
    return null;
  }
};

// Fetch stock news from backend (or return mock in demo mode)
const fetchStockNews = async (
  symbol: string,
  limit: number = 10,
  refresh: boolean = false
): Promise<{ articles: NewsArticle[]; queued: boolean }> => {
  if (IS_DEMO) {
    return { articles: getMockNews(symbol).slice(0, limit), queued: false };
  }
  try {
    const url = `${BFF_URL}/api/stock/${symbol}/news?limit=${limit}${refresh ? '&refresh=true' : ''}`;
    const response = await fetch(url);
    if (!response.ok) {
      console.warn(`Failed to fetch news for ${symbol}: ${response.status}`);
      return { articles: [], queued: false };
    }
    const responseData = await response.json();

    // Backend returns { ticker, news: [...], count, queued, timestamp }
    const articles: BackendNewsArticle[] = responseData.news || [];

    // Transform backend format to frontend NewsArticle format
    const mapped = articles.map((article, index) => ({
      id: `${symbol}-news-${index}`,
      title: article.title,
      source: article.sources,
      publishedAt: article.published_time,
      url: article.url,
      summary: article.text || '',
      isNew: false, // Could be determined by comparing with last fetch timestamp
    }));

    return { articles: mapped, queued: Boolean(responseData.queued) };
  } catch (error) {
    console.error(`Error fetching news for ${symbol}:`, error);
    return { articles: [], queued: false };
  }
};

const formatNumber = (num: number, decimals = 2): string => {
  return num.toFixed(decimals).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
};

const formatLargeNumber = (num: number): string => {
  if (num >= 1e12) return `$${(num / 1e12).toFixed(2)}T`;
  if (num >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
  if (num >= 1e6) return `$${(num / 1e6).toFixed(2)}M`;
  if (num >= 1e3) return `$${(num / 1e3).toFixed(2)}K`;
  return `$${num.toFixed(2)}`;
};

const formatVolume = (vol: number): string => {
  if (vol >= 1e9) return `${(vol / 1e9).toFixed(2)}B`;
  if (vol >= 1e6) return `${(vol / 1e6).toFixed(2)}M`;
  if (vol >= 1e3) return `${(vol / 1e3).toFixed(2)}K`;
  return vol.toString();
};

const formatTimestampET = (isoDate: string): string => {
  if (!isoDate) return '-';
  const date = new Date(isoDate);
  // Check for invalid date
  if (isNaN(date.getTime())) return isoDate || '-';
  // Format to America/New_York timezone
  const formatted = date.toLocaleString('en-US', {
    timeZone: 'America/New_York',
    month: '2-digit',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  return `${formatted} ET`;
};

const sortNewsByPublishedAt = (articles: NewsArticle[]): NewsArticle[] => {
  return [...articles].sort((a, b) => {
    const aTime = Date.parse(a.publishedAt || '') || 0;
    const bTime = Date.parse(b.publishedAt || '') || 0;
    return bTime - aTime;
  });
};

const AVAILABLE_SYMBOLS = [
  'AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'AMD',
  'NFLX', 'COIN', 'PLTR', 'RIVN', 'LCID', 'SOFI', 'BABA', 'NIO'
];

export function StockDetail({ onRemove, selectedSymbol, settings, onSettingsChange }: ModuleProps) {
  // Get first ticker from entities map as fallback default (stable selector - avoids infinite loop)
  const firstTicker = useMarketDataStore((state) => {
    // Get first entity by iterating the map - this is stable because we only extract a string
    const entries = Array.from(state.entities.entries());
    if (entries.length === 0) return '';
    // Sort by rank to get the top gainer
    entries.sort((a, b) => (a[1].rank ?? 999) - (b[1].rank ?? 999));
    return entries[0]?.[0] ?? '';
  });

  const [symbol, setSymbol] = useState(selectedSymbol || '');
  const [fundamentals, setFundamentals] = useState<StockFundamentals | null>(null);
  const [news, setNews] = useState<NewsArticle[]>([]);
  const [newsCount, setNewsCount] = useState('10');
  // Per-ticker loading state maps
  const [loadingNewsFor, setLoadingNewsFor] = useState<string | null>(null);
  const [loadingProfileFor, setLoadingProfileFor] = useState<string | null>(null);
  const [profileError, setProfileError] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<StockDetailView>(settings?.stockDetail?.view || 'fundamentals');

  // Cache refs to avoid re-fetching data for the same ticker
  const profileCache = useRef<Map<string, StockFundamentals>>(new Map());
  const newsCache = useRef<Map<string, NewsArticle[]>>(new Map());

  // Check if current ticker is loading
  const isLoadingNews = loadingNewsFor === symbol;
  const isLoadingProfile = loadingProfileFor === symbol;

  // Backend timestamp for stock detail domain
  const backendTimestamp = useBackendTimestamp('stock-detail');

  // Update symbol when selectedSymbol changes from sync, or use first ticker if symbol is empty
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    } else if (!symbol && firstTicker) {
      // Auto-select first ticker from rank data when no symbol is set
      setSymbol(firstTicker);
    }
  }, [selectedSymbol, firstTicker]);

  // Update view from settings
  useEffect(() => {
    if (settings?.stockDetail?.view) {
      setActiveView(settings.stockDetail.view);
    }
  }, [settings?.stockDetail?.view]);

  // Load fundamentals from backend when symbol changes (with caching)
  useEffect(() => {
    if (!symbol) return; // Skip if no symbol selected yet

    // Check WebSocket-pushed cache first (from static_update stream)
    const wsCache = getCachedProfile(symbol);
    if (wsCache) {
      setFundamentals(wsCache as StockFundamentals);
      profileCache.current.set(symbol, wsCache as StockFundamentals);
      setProfileError(null);
      return;
    }

    // Check local cache (from previous API fetches)
    const cached = profileCache.current.get(symbol);
    if (cached) {
      setFundamentals(cached);
      setProfileError(null);
      return;
    }

    const loadProfile = async () => {
      const fetchingSymbol = symbol; // Capture symbol at start
      setLoadingProfileFor(fetchingSymbol);
      setProfileError(null);

      const data = await fetchStockProfile(fetchingSymbol);
      if (data) {
        profileCache.current.set(fetchingSymbol, data); // Local cache
        setCachedProfile(fetchingSymbol, data); // Global persistent cache
        setFundamentals(data);

        // Also update RankList store with static data from profile
        patchStaticData(fetchingSymbol, {
          float: typeof data.float === 'number' ? data.float : undefined,
          marketCap: typeof data.marketCap === 'number' ? data.marketCap : undefined,
        });
      } else {
        setProfileError(`No profile data available for ${fetchingSymbol}`);
        setFundamentals(null);
      }
      // Only clear loading if still loading this symbol
      setLoadingProfileFor(prev => prev === fetchingSymbol ? null : prev);
    };

    loadProfile();
  }, [symbol]);

  // Load news from cache or fetch when switching to news view
  useEffect(() => {
    if (!symbol) return;
    if (activeView !== 'news') return;


    // Helper to normalize news format (handle both old and new cache formats)
    const normalizeNews = (articles: any[]): NewsArticle[] => {
      const normalized = articles.map((article, index) => ({
        id: article.id || `${symbol}-news-${index}`,
        title: article.title || '',
        source: article.source || article.sources || '',
        // Handle both publishedAt and published_time field names
        publishedAt: article.publishedAt || article.published_time || '',
        url: article.url || '',
        summary: article.summary || article.text || '',
        isNew: article.isNew || false,
      }));
      return sortNewsByPublishedAt(normalized);
    };

    // Check WebSocket push cache first (most recent)
    const wsNews = getCachedNews(symbol);
    if (wsNews && wsNews.length > 0) {
      setNews(normalizeNews(wsNews));
      return;
    }

    // Check local cache (from previous API fetches)
    const cached = newsCache.current.get(symbol);
    if (cached) {
      setNews(sortNewsByPublishedAt(cached));
      return;
    }

    // No cache - fetch news
    handleFetchNews(false);
  }, [symbol, activeView]);

  // Subscribe to real-time news article updates
  useEffect(() => {
    if (!symbol) return;

    const unsubscribe = subscribeNewsUpdates(({ symbol: updateSymbol, articles }) => {
      if (updateSymbol !== symbol) return;

      const sortedArticles = sortNewsByPublishedAt(articles);
      newsCache.current.set(updateSymbol, sortedArticles);
      if (activeView === 'news') {
        setNews(sortedArticles);
      }
      setLoadingNewsFor(prev => (prev === updateSymbol ? null : prev));
    });

    return () => {
      unsubscribe();
    };
  }, [symbol, activeView]);

  const handleFetchNews = useCallback(async (refresh: boolean = false) => {
    if (!symbol) return;
    const fetchingSymbol = symbol; // Capture symbol at start
    setLoadingNewsFor(fetchingSymbol);
    setDataStatus(fetchingSymbol, 'news', refresh ? 'pending' : 'loading');
    const count = parseInt(newsCount) || 10;
    const clampedCount = Math.min(Math.max(count, 1), 50);
    const { articles, queued } = await fetchStockNews(fetchingSymbol, clampedCount, refresh);
    if (!refresh) {
      const sortedArticles = sortNewsByPublishedAt(articles);
      newsCache.current.set(fetchingSymbol, sortedArticles); // Local cache
      setCachedNews(fetchingSymbol, sortedArticles); // Global persistent cache
      setNews(sortedArticles);
      setDataStatus(fetchingSymbol, 'news', queued ? 'pending' : 'ready');
    } else {
      // Refresh is async-only now; backend queues fetch and returns no articles
      setDataStatus(fetchingSymbol, 'news', 'pending');
    }

    // Also update RankList store with hasNews status (avoid overwriting on async refresh)
    if (!refresh && !queued) {
      patchStaticData(fetchingSymbol, {
        hasNews: articles.length > 0,
      });
    }

    // Only clear loading if still loading this symbol
    setLoadingNewsFor(prev => prev === fetchingSymbol ? null : prev);
  }, [symbol, newsCount]);

  const handleViewChange = (view: StockDetailView) => {
    setActiveView(view);
    onSettingsChange?.({ stockDetail: { view } });
    // Note: useEffect handles auto-fetch on view change with cache check
  };

  const handleSymbolChange = (newSymbol: string) => {
    setSymbol(newSymbol);
  };

  // Loading state
  if (isLoadingProfile) {
    return (
      <div className="h-full flex items-center justify-center bg-zinc-900 text-gray-500">
        <RefreshCw className="w-5 h-5 animate-spin mr-2" />
        Loading {symbol}...
      </div>
    );
  }

  // Error state - show message but allow symbol change
  if (profileError || !fundamentals) {
    return (
      <div className="h-full flex flex-col bg-zinc-900">
        {/* Header with search */}
        <div className="border-b border-zinc-800 p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 text-gray-400">
              <AlertCircle className="w-5 h-5 text-yellow-500" />
              <span>{profileError || 'No data available'}</span>
            </div>
            <div className="flex-shrink-0" style={{ width: '180px' }}>
              <SymbolSearch
                value={symbol}
                onChange={handleSymbolChange}
                availableSymbols={AVAILABLE_SYMBOLS}
                placeholder="Symbol..."
                useConfirmButton={true}
              />
            </div>
          </div>
        </div>
        <div className="flex-1 flex items-center justify-center text-gray-500">
          <p>Static data will appear once the ticker is processed by the backend</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header with Company Info */}
      <div className="border-b border-zinc-800 p-4">
        <div className="flex items-start gap-3">
          <Building2 className="w-5 h-5 text-blue-500 flex-shrink-0 mt-1" />
          <div className="flex-1 min-w-0">
            <h2 className="text-xl">{fundamentals.companyName}</h2>
            <div className="flex items-center gap-3 mt-1">
              <div className="text-sm text-gray-400">{fundamentals.symbol}</div>
              <div className="text-xs text-gray-500">Updated: {backendTimestamp}</div>
            </div>
          </div>
          {/* Search Bar - Right side with responsive width */}
          <div className="flex-shrink-0" style={{ width: '180px', minWidth: '140px', maxWidth: '280px' }}>
            <SymbolSearch
              value={symbol}
              onChange={handleSymbolChange}
              availableSymbols={AVAILABLE_SYMBOLS}
              placeholder="Symbol..."
              useConfirmButton={true}
            />
          </div>
        </div>
      </div>

      {/* Content Area */}
      <div className="flex-1 overflow-auto p-4">
        {activeView === 'fundamentals' && (
          <>
            {/* Company Info */}
            <div className="mb-6">
              <h3 className="text-sm text-gray-400 mb-3 flex items-center gap-2">
                <Globe className="w-4 h-4" />
                Company Information
              </h3>
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div>
                  <div className="text-gray-500 text-xs">Country</div>
                  <div>{fundamentals.country}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Sector</div>
                  <div>{fundamentals.sector}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Industry</div>
                  <div>{fundamentals.industry}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">ipoDate</div>
                  <div>{fundamentals.ipoDate || '-'}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">CEO</div>
                  <div>{fundamentals.ceo || '-'}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Employees</div>
                  <div>{fundamentals.fullTimeEmployees ? formatNumber(Number(fundamentals.fullTimeEmployees), 0) : '-'}</div>
                </div>
              </div>
            </div>

            {/* Market Data */}
            <div className="mb-6">
              <h3 className="text-sm text-gray-400 mb-3 flex items-center gap-2">
                <DollarSign className="w-4 h-4" />
                Market Data
              </h3>
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div>
                  <div className="text-gray-500 text-xs">Market Cap</div>
                  <div>{fundamentals.marketCap ? formatLargeNumber(Number(fundamentals.marketCap)) : '-'}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Exchange</div>
                  <div>{fundamentals.exchange || '-'}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Float Shares</div>
                  <div>{fundamentals.float ? formatVolume(Number(fundamentals.float)) : '-'}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Avg Volume</div>
                  <div>{fundamentals.averageVolume ? formatVolume(Number(fundamentals.averageVolume)) : '-'}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">52W Range</div>
                  <div>{fundamentals.range || '-'}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Location</div>
                  <div>{[fundamentals.city, fundamentals.state].filter(Boolean).join(', ') || '-'}</div>
                </div>
              </div>
            </div>

            {/* Description */}
            <div>
              <h3 className="text-sm text-gray-400 mb-2">About</h3>
              <p className="text-sm text-gray-300 leading-relaxed">
                {fundamentals.description}
              </p>
              <div className="mt-2">
                <a
                  href={`https://${fundamentals.website}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm text-blue-500 hover:text-blue-400"
                >
                  {fundamentals.website}
                </a>
              </div>
            </div>
          </>
        )}

        {activeView === 'news' && (
          <>
            {/* News Controls */}
            <div className="mb-4">
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  min="1"
                  max="50"
                  value={newsCount}
                  onChange={(e) => setNewsCount(e.target.value)}
                  placeholder="Number of articles"
                  className="w-24 px-3 py-1.5 bg-zinc-800 border border-zinc-700 text-sm focus:outline-none focus:border-zinc-600"
                />
                <button
                  onClick={() => handleFetchNews(false)}
                  disabled={isLoadingNews}
                  className="flex items-center gap-2 px-4 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 disabled:cursor-not-allowed transition-colors text-sm"
                >
                  {isLoadingNews ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <RefreshCw className="w-4 h-4" />
                  )}
                  {isLoadingNews ? 'Loading...' : 'Request'}
                </button>
                <button
                  onClick={() => handleFetchNews(true)}
                  disabled={isLoadingNews}
                  className="flex items-center gap-2 px-4 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 disabled:cursor-not-allowed transition-colors text-sm"
                  title="Trigger backend to refresh news asynchronously"
                >
                  <RefreshCw className="w-4 h-4" />
                  Refresh
                </button>
              </div>
            </div>

            {/* News List */}
            <div className="space-y-3">
              {news.length === 0 && !isLoadingNews && (
                <div className="text-center text-gray-500 text-sm py-8">
                  Click "Request News" to queue a fetch and stream results
                </div>
              )}

              {news.map((article) => (
                <div
                  key={article.id}
                  className="border border-zinc-800 bg-zinc-800/30 p-3 hover:bg-zinc-800/50 transition-colors"
                >
                  <div className="flex items-start justify-between gap-2 mb-2">
                    <h4 className="text-sm flex-1">{article.title}</h4>
                    {article.isNew && (
                      <span className="px-2 py-0.5 text-xs bg-blue-600 rounded shrink-0 flex items-center gap-1">
                        <Sparkles className="w-3 h-3" />
                        NEW
                      </span>
                    )}
                  </div>

                  <p className="text-xs text-gray-400 mb-2 line-clamp-2">
                    {article.summary}
                  </p>

                  <div className="flex items-start justify-between gap-2 text-xs">
                    <div className="flex flex-col gap-1 text-gray-500">
                      <div className="flex items-center gap-2">
                        <span className="text-gray-400">{article.source}</span>
                      </div>
                      <div>
                        {formatTimestampET(article.publishedAt)}
                      </div>
                    </div>
                    <a
                      href={article.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-1 text-blue-500 hover:text-blue-400 shrink-0"
                    >
                      <ExternalLink className="w-3 h-3" />
                    </a>
                  </div>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
