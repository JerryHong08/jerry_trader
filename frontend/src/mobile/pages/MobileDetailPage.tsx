import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import {
  Building2, Globe, TrendingUp, DollarSign, Users, Calendar,
  Newspaper, RefreshCw, ExternalLink, AlertCircle, Loader2, ChevronRight,
} from 'lucide-react';
import type { NewsArticle } from '../../types';
import { SymbolSearch } from '../../components/common/SymbolSearch';
import { useMarketDataStore } from '../../stores/marketDataStore';
import { getCachedProfile, getCachedNews, setCachedProfile, setCachedNews, subscribeNewsUpdates } from '../../hooks/useWebSocket';
import {
  fetchStockProfile, fetchStockNews, sortNewsByPublishedAt,
  type StockFundamentals,
} from '../../services/stockApi';
import { formatNumber, formatVolume, formatLargeNumber, formatTimestampET } from '../../utils/format';

// ---- Props -------------------------------------------------------------------

interface MobileDetailPageProps {
  symbol: string | null;
  onSelectSymbol?: (symbol: string) => void;
  onNavigateToChart?: () => void;
}

// ---- Component ---------------------------------------------------------------

export function MobileDetailPage({
  symbol,
  onSelectSymbol,
  onNavigateToChart,
}: MobileDetailPageProps) {
  const [currentSymbol, setCurrentSymbol] = useState(symbol || '');
  const [view, setView] = useState<'fundamentals' | 'news'>('fundamentals');
  const [fundamentals, setFundamentals] = useState<StockFundamentals | null>(null);
  const [news, setNews] = useState<NewsArticle[]>([]);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [loadingNews, setLoadingNews] = useState(false);
  const [profileError, setProfileError] = useState<string | null>(null);

  // Caches
  const profileCache = useRef<Map<string, StockFundamentals>>(new Map());
  const newsCache = useRef<Map<string, NewsArticle[]>>(new Map());

  // Available symbols
  const symbolsKey = useMarketDataStore((s) =>
    Array.from(s.entities.keys()).sort().join(',')
  );
  const availableSymbols = useMemo(() => (symbolsKey ? symbolsKey.split(',') : []), [symbolsKey]);

  // Sync symbol
  useEffect(() => {
    if (symbol) setCurrentSymbol(symbol);
  }, [symbol]);

  // -- Load profile --
  useEffect(() => {
    if (!currentSymbol) return;

    const wsCache = getCachedProfile(currentSymbol);
    if (wsCache) {
      setFundamentals(wsCache as StockFundamentals);
      profileCache.current.set(currentSymbol, wsCache as StockFundamentals);
      setProfileError(null);
      return;
    }

    const cached = profileCache.current.get(currentSymbol);
    if (cached) {
      setFundamentals(cached);
      setProfileError(null);
      return;
    }

    let cancelled = false;
    const load = async () => {
      setLoadingProfile(true);
      setProfileError(null);
      const data = await fetchStockProfile(currentSymbol);
      if (cancelled) return;
      if (data) {
        profileCache.current.set(currentSymbol, data);
        setCachedProfile(currentSymbol, data);
        setFundamentals(data);
      } else {
        setProfileError(`No profile for ${currentSymbol}`);
        setFundamentals(null);
      }
      setLoadingProfile(false);
    };
    load();
    return () => { cancelled = true; };
  }, [currentSymbol]);

  // -- Load news --
  const fetchNews = useCallback(async (refresh = false) => {
    if (!currentSymbol) return;
    setLoadingNews(true);
    const { articles } = await fetchStockNews(currentSymbol, 10, refresh);
    const sorted = sortNewsByPublishedAt(articles);
    newsCache.current.set(currentSymbol, sorted);
    setCachedNews(currentSymbol, sorted);
    setNews(sorted);
    setLoadingNews(false);
  }, [currentSymbol]);

  useEffect(() => {
    if (!currentSymbol || view !== 'news') return;

    const wsNews = getCachedNews(currentSymbol);
    if (wsNews && wsNews.length > 0) {
      setNews(sortNewsByPublishedAt(wsNews));
      return;
    }

    const cached = newsCache.current.get(currentSymbol);
    if (cached) {
      setNews(sortNewsByPublishedAt(cached));
      return;
    }

    fetchNews();
  }, [currentSymbol, view, fetchNews]);

  // Real-time news updates
  useEffect(() => {
    if (!currentSymbol) return;
    const unsub = subscribeNewsUpdates(({ symbol: s, articles }) => {
      if (s !== currentSymbol) return;
      const sorted = sortNewsByPublishedAt(articles);
      newsCache.current.set(currentSymbol, sorted);
      if (view === 'news') setNews(sorted);
    });
    return unsub;
  }, [currentSymbol, view]);

  // -- Loading profile --
  if (loadingProfile) {
    return (
      <div className="h-full flex items-center justify-center bg-zinc-900 text-zinc-500">
        <RefreshCw className="w-5 h-5 animate-spin mr-2" />
        Loading {currentSymbol}...
      </div>
    );
  }

  // -- Error state --
  if (profileError || !fundamentals) {
    return (
      <div className="h-full flex flex-col bg-zinc-900">
        <div className="flex-shrink-0 px-3 py-2 border-b border-zinc-800">
          <SymbolSearch
            value={currentSymbol}
            onChange={(s) => {
              setCurrentSymbol(s);
              onSelectSymbol?.(s);
            }}
            availableSymbols={availableSymbols}
            placeholder="Symbol..."
            useConfirmButton
          />
        </div>
        <div className="flex-1 flex items-center justify-center text-zinc-500">
          <div className="text-center">
            <AlertCircle className="w-8 h-8 text-yellow-500 mx-auto mb-2" />
            <p>{profileError || 'No data'}</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header */}
      <div className="flex-shrink-0 border-b border-zinc-800 px-3 py-2 space-y-2">
        {/* Search */}
        <div style={{ maxWidth: 180 }}>
          <SymbolSearch
            value={currentSymbol}
            onChange={(s) => {
              setCurrentSymbol(s);
              onSelectSymbol?.(s);
            }}
            availableSymbols={availableSymbols}
            placeholder="Symbol..."
            useConfirmButton
          />
        </div>

        {/* Company name */}
        <div className="flex items-center gap-2">
          <Building2 className="w-4 h-4 text-blue-500 flex-shrink-0" />
          <span className="text-sm font-medium truncate">{fundamentals.companyName}</span>
        </div>

        {/* View toggle */}
        <div className="flex bg-zinc-800 rounded p-0.5">
          {(['fundamentals', 'news'] as const).map((v) => (
            <button
              key={v}
              onClick={() => setView(v)}
              className={`flex-1 py-1 text-xs rounded transition-colors ${
                view === v ? 'bg-white text-black' : 'text-zinc-400'
              }`}
            >
              {v === 'fundamentals' ? 'Fundamentals' : 'News'}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto px-3 py-3">
        {view === 'fundamentals' && (
          <>
            {/* Company Info */}
            <div className="mb-4">
              <h3 className="text-xs text-zinc-400 mb-2 flex items-center gap-1.5">
                <Globe className="w-3.5 h-3.5" /> Company
              </h3>
              <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-xs">
                <div><span className="text-zinc-500">Country</span><br />{fundamentals.country}</div>
                <div><span className="text-zinc-500">Sector</span><br />{fundamentals.sector}</div>
                <div><span className="text-zinc-500">Industry</span><br />{fundamentals.industry}</div>
                <div><span className="text-zinc-500">IPO</span><br />{fundamentals.ipoDate || '-'}</div>
                <div><span className="text-zinc-500">CEO</span><br />{fundamentals.ceo || '-'}</div>
                <div><span className="text-zinc-500">Employees</span><br />{fundamentals.fullTimeEmployees ? formatNumber(Number(fundamentals.fullTimeEmployees), 0) : '-'}</div>
              </div>
            </div>

            {/* Market Data */}
            <div className="mb-4">
              <h3 className="text-xs text-zinc-400 mb-2 flex items-center gap-1.5">
                <DollarSign className="w-3.5 h-3.5" /> Market
              </h3>
              <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-xs">
                <div><span className="text-zinc-500">Market Cap</span><br />{fundamentals.marketCap ? formatLargeNumber(Number(fundamentals.marketCap)) : '-'}</div>
                <div><span className="text-zinc-500">Exchange</span><br />{fundamentals.exchange || '-'}</div>
                <div><span className="text-zinc-500">Float</span><br />{fundamentals.float ? formatVolume(Number(fundamentals.float)) : '-'}</div>
                <div><span className="text-zinc-500">Avg Vol</span><br />{fundamentals.averageVolume ? formatVolume(Number(fundamentals.averageVolume)) : '-'}</div>
                <div><span className="text-zinc-500">52W Range</span><br />{fundamentals.range || '-'}</div>
                <div><span className="text-zinc-500">Location</span><br />{[fundamentals.city, fundamentals.state].filter(Boolean).join(', ') || '-'}</div>
              </div>
            </div>

            {/* About */}
            <div>
              <h3 className="text-xs text-zinc-400 mb-1.5">About</h3>
              <p className="text-xs text-zinc-300 leading-relaxed">{fundamentals.description}</p>
              {fundamentals.website && (
                <a
                  href={`https://${fundamentals.website}`}
                  target="_blank" rel="noopener noreferrer"
                  className="text-xs text-blue-500 mt-1 inline-block"
                >
                  {fundamentals.website}
                </a>
              )}
            </div>
          </>
        )}

        {view === 'news' && (
          <>
            {/* News controls */}
            <div className="flex items-center gap-2 mb-3">
              <button
                onClick={() => fetchNews(false)}
                disabled={loadingNews}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 text-xs transition-colors"
              >
                {loadingNews ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <RefreshCw className="w-3.5 h-3.5" />}
                Request
              </button>
              <button
                onClick={() => fetchNews(true)}
                disabled={loadingNews}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 text-xs transition-colors"
              >
                <RefreshCw className="w-3.5 h-3.5" /> Refresh
              </button>
            </div>

            {news.length === 0 && !loadingNews && (
              <div className="text-center text-zinc-500 text-xs py-8">
                No news loaded — tap Request
              </div>
            )}

            <div className="space-y-2">
              {news.map((article) => (
                <div key={article.id} className="border border-zinc-800 bg-zinc-800/30 p-2.5">
                  <h4 className="text-xs mb-1.5">{article.title}</h4>
                  <p className="text-2xs text-zinc-400 mb-1.5 line-clamp-2">{article.summary}</p>
                  <div className="flex items-center justify-between text-3xs text-zinc-500">
                    <span>{article.source} · {formatTimestampET(article.publishedAt)}</span>
                    <a
                      href={article.url} target="_blank" rel="noopener noreferrer"
                      className="text-blue-500 flex items-center gap-0.5"
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

      {/* Footer */}
      {currentSymbol && onNavigateToChart && (
        <div className="flex-shrink-0 border-t border-zinc-800 px-3 py-2">
          <button
            onClick={onNavigateToChart}
            className="flex items-center justify-between w-full px-3 py-2 bg-zinc-800 hover:bg-zinc-700 rounded text-xs text-zinc-300 transition-colors"
          >
            <span>View {currentSymbol} Chart</span>
            <ChevronRight className="w-4 h-4 text-zinc-500" />
          </button>
        </div>
      )}
    </div>
  );
}
