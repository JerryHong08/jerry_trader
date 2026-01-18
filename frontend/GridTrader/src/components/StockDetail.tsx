import React, { useState, useEffect } from 'react';
import { Building2, Globe, TrendingUp, DollarSign, Users, Calendar, BarChart3, Newspaper, RefreshCw, ExternalLink, Sparkles } from 'lucide-react';
import type { ModuleProps, NewsArticle, StockDetailView } from '../types';
import { SymbolSearch } from './common/SymbolSearch';
import { useBackendTimestamp } from '../hooks/useBackendTimestamps';

interface StockFundamentals {
  symbol: string;
  companyName: string;
  country: string;
  sector: string;
  industry: string;
  floatShares: number;
  sharesOutstanding: number;
  marketCap: number;
  peRatio: number;
  eps: number;
  dividend: number;
  dividendYield: number;
  beta: number;
  fiftyTwoWeekHigh: number;
  fiftyTwoWeekLow: number;
  avgVolume: number;
  ipoDate: number;
  employees: number;
  ceo: string;
  website: string;
  description: string;
}

// Generate mock news articles
const generateMockNews = (symbol: string, count: number): NewsArticle[] => {
  const headlines = [
    'announces record quarterly earnings',
    'unveils new product line',
    'expands into emerging markets',
    'reports strong revenue growth',
    'faces regulatory challenges',
    'announces strategic partnership',
    'beats analyst expectations',
    'launches innovative technology',
    'stock surges on positive outlook',
    'investors remain optimistic',
    'market volatility impacts share price',
    'CEO addresses shareholder concerns',
    'plans major expansion',
    'receives analyst upgrade',
    'announces share buyback program',
  ];

  const sources = [
    'Bloomberg',
    'Reuters',
    'CNBC',
    'Wall Street Journal',
    'Financial Times',
    'MarketWatch',
    'Yahoo Finance',
    'Seeking Alpha',
    'The Motley Fool',
    'Barron\'s',
  ];

  const news: NewsArticle[] = [];
  const now = Date.now();

  for (let i = 0; i < count; i++) {
    const headlineTemplate = headlines[Math.floor(Math.random() * headlines.length)];
    const publishedAt = new Date(now - (i * 3600000) - Math.random() * 7200000); // Hours ago with randomness
    const isNew = i < Math.floor(count * 0.3); // 30% of articles are "new"

    news.push({
      id: `${symbol}-news-${i}`,
      title: `${symbol} ${headlineTemplate}`,
      source: sources[Math.floor(Math.random() * sources.length)],
      publishedAt: publishedAt.toISOString(),
      url: `https://news.example.com/${symbol.toLowerCase()}-${i}`,
      summary: `Latest developments regarding ${symbol}. This article provides in-depth analysis of the company's recent announcements and their potential impact on the stock price and market position.`,
      isNew,
    });
  }

  return news;
};

// Generate mock fundamental data
const generateFundamentals = (symbol: string): StockFundamentals => {
  const companies: Record<string, Partial<StockFundamentals>> = {
    'AAPL': { companyName: 'Apple Inc.', country: 'United States', sector: 'Technology', industry: 'Consumer Electronics', ceo: 'Tim Cook', ipoDate: 1976 },
    'TSLA': { companyName: 'Tesla, Inc.', country: 'United States', sector: 'Automotive', industry: 'Electric Vehicles', ceo: 'Elon Musk', ipoDate: 2003 },
    'NVDA': { companyName: 'NVIDIA Corporation', country: 'United States', sector: 'Technology', industry: 'Semiconductors', ceo: 'Jensen Huang', ipoDate: 1993 },
    'MSFT': { companyName: 'Microsoft Corporation', country: 'United States', sector: 'Technology', industry: 'Software', ceo: 'Satya Nadella', ipoDate: 1975 },
    'GOOGL': { companyName: 'Alphabet Inc.', country: 'United States', sector: 'Technology', industry: 'Internet Services', ceo: 'Sundar Pichai', ipoDate: 1998 },
    'AMZN': { companyName: 'Amazon.com, Inc.', country: 'United States', sector: 'Consumer Cyclical', industry: 'E-Commerce', ceo: 'Andy Jassy', ipoDate: 1994 },
    'META': { companyName: 'Meta Platforms, Inc.', country: 'United States', sector: 'Technology', industry: 'Social Media', ceo: 'Mark Zuckerberg', ipoDate: 2004 },
    'AMD': { companyName: 'Advanced Micro Devices', country: 'United States', sector: 'Technology', industry: 'Semiconductors', ceo: 'Lisa Su', ipoDate: 1969 },
  };

  const baseData = companies[symbol] || {
    companyName: `${symbol} Corporation`,
    country: 'United States',
    sector: 'Technology',
    industry: 'Software',
    ceo: 'John Doe',
    ipoDate: 2000,
  };

  return {
    symbol,
    companyName: baseData.companyName!,
    country: baseData.country!,
    sector: baseData.sector!,
    industry: baseData.industry!,
    floatShares: Math.random() * 5000000000 + 1000000000,
    sharesOutstanding: Math.random() * 6000000000 + 1000000000,
    marketCap: Math.random() * 2000000000000 + 100000000000,
    peRatio: Math.random() * 50 + 10,
    eps: Math.random() * 20 + 1,
    dividend: Math.random() * 5,
    dividendYield: Math.random() * 3,
    beta: Math.random() * 2 + 0.5,
    fiftyTwoWeekHigh: Math.random() * 300 + 100,
    fiftyTwoWeekLow: Math.random() * 100 + 20,
    avgVolume: Math.random() * 100000000 + 10000000,
    ipoDate: baseData.ipoDate!,
    employees: Math.floor(Math.random() * 150000 + 10000),
    ceo: baseData.ceo!,
    website: `www.${symbol.toLowerCase()}.com`,
    description: `${baseData.companyName} is a leading company in the ${baseData.industry} industry, operating primarily in ${baseData.country}. The company focuses on innovation and growth in the ${baseData.sector} sector.`,
  };
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
  const date = new Date(isoDate);
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

const AVAILABLE_SYMBOLS = [
  'AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'AMD',
  'NFLX', 'COIN', 'PLTR', 'RIVN', 'LCID', 'SOFI', 'BABA', 'NIO'
];

export function StockDetail({ onRemove, selectedSymbol, settings, onSettingsChange }: ModuleProps) {
  const [symbol, setSymbol] = useState(selectedSymbol || 'AAPL');
  const [fundamentals, setFundamentals] = useState<StockFundamentals | null>(null);
  const [news, setNews] = useState<NewsArticle[]>([]);
  const [newsCount, setNewsCount] = useState('10');
  const [isLoadingNews, setIsLoadingNews] = useState(false);
  const [activeView, setActiveView] = useState<StockDetailView>(settings?.stockDetail?.view || 'fundamentals');

  // Backend timestamp for stock detail domain
  const backendTimestamp = useBackendTimestamp('stock-detail');

  // Update symbol when selectedSymbol changes from sync
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    }
  }, [selectedSymbol]);

  // Update view from settings
  useEffect(() => {
    if (settings?.stockDetail?.view) {
      setActiveView(settings.stockDetail.view);
    }
  }, [settings?.stockDetail?.view]);

  // Load fundamentals when symbol changes
  useEffect(() => {
    const data = generateFundamentals(symbol);
    setFundamentals(data);
  }, [symbol]);

  // Auto-load news when symbol changes (only if on news view)
  useEffect(() => {
    if (activeView === 'news') {
      fetchNews();
    }
  }, [symbol]);

  const fetchNews = () => {
    setIsLoadingNews(true);

    // Simulate API call delay
    setTimeout(() => {
      const count = parseInt(newsCount) || 10;
      const clampedCount = Math.min(Math.max(count, 1), 50); // Limit between 1 and 50
      const articles = generateMockNews(symbol, clampedCount);
      setNews(articles);
      setIsLoadingNews(false);
    }, 500);
  };

  const handleViewChange = (view: StockDetailView) => {
    setActiveView(view);
    onSettingsChange?.({ stockDetail: { view } });

    // Auto-fetch news when switching to news view if not already loaded
    if (view === 'news' && news.length === 0) {
      fetchNews();
    }
  };

  const handleSymbolChange = (newSymbol: string) => {
    setSymbol(newSymbol);
    onSymbolSelect?.(newSymbol);
  };

  if (!fundamentals) {
    return (
      <div className="h-full flex items-center justify-center bg-zinc-900 text-gray-500">
        Loading...
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
                  <div>{fundamentals.ipoDate}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">CEO</div>
                  <div>{fundamentals.ceo}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Employees</div>
                  <div>{formatNumber(fundamentals.employees, 0)}</div>
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
                  <div>{formatLargeNumber(fundamentals.marketCap)}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Shares Outstanding</div>
                  <div>{formatVolume(fundamentals.sharesOutstanding)}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Float Shares</div>
                  <div>{formatVolume(fundamentals.floatShares)}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Avg Volume</div>
                  <div>{formatVolume(fundamentals.avgVolume)}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">52W High</div>
                  <div>${formatNumber(fundamentals.fiftyTwoWeekHigh)}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">52W Low</div>
                  <div>${formatNumber(fundamentals.fiftyTwoWeekLow)}</div>
                </div>
              </div>
            </div>

            {/* Financial Metrics */}
            <div className="mb-6">
              <h3 className="text-sm text-gray-400 mb-3 flex items-center gap-2">
                <BarChart3 className="w-4 h-4" />
                Financial Metrics
              </h3>
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div>
                  <div className="text-gray-500 text-xs">P/E Ratio</div>
                  <div>{formatNumber(fundamentals.peRatio)}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">EPS</div>
                  <div>${formatNumber(fundamentals.eps)}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Dividend</div>
                  <div>${formatNumber(fundamentals.dividend)}</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Dividend Yield</div>
                  <div>{formatNumber(fundamentals.dividendYield)}%</div>
                </div>
                <div>
                  <div className="text-gray-500 text-xs">Beta</div>
                  <div>{formatNumber(fundamentals.beta)}</div>
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
                  onClick={fetchNews}
                  disabled={isLoadingNews}
                  className="flex items-center gap-2 px-4 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 disabled:cursor-not-allowed transition-colors text-sm"
                >
                  <RefreshCw className={`w-4 h-4 ${isLoadingNews ? 'animate-spin' : ''}`} />
                  {isLoadingNews ? 'Loading...' : 'Fetch News'}
                </button>
              </div>
            </div>

            {/* News List */}
            <div className="space-y-3">
              {news.length === 0 && !isLoadingNews && (
                <div className="text-center text-gray-500 text-sm py-8">
                  Click "Fetch News" to load articles
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
