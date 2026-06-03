// Shared stock profile & news fetching — used by desktop StockDetail and mobile DetailPage.

import type { NewsArticle } from '../types';
import { IS_DEMO, MOCK_PROFILES, getMockNews } from '../data/mockData';

// ---- Config (port 5001 = JerryTraderBFF) ------------------------------------

const BFF_HTTP_URL =
  (typeof import.meta !== 'undefined' && import.meta.env?.VITE_BFF_URL)
    ? (import.meta.env.VITE_BFF_URL as string)
    : 'http://localhost:5001';
const BFF_URL = BFF_HTTP_URL || 'http://localhost:5001';

// ---- Types ------------------------------------------------------------------

export interface StockFundamentals {
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

// ---- Fetch functions --------------------------------------------------------

export async function fetchStockProfile(symbol: string): Promise<StockFundamentals | null> {
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
    if (responseData.error) {
      console.warn(`Profile error for ${symbol}: ${responseData.error}`);
      return null;
    }
    return responseData.profile as StockFundamentals;
  } catch (error) {
    console.error(`Error fetching profile for ${symbol}:`, error);
    return null;
  }
}

export async function fetchStockNews(
  symbol: string,
  limit: number = 10,
  refresh: boolean = false,
): Promise<{ articles: NewsArticle[]; queued: boolean }> {
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
    const articles: BackendNewsArticle[] = responseData.news || [];
    const mapped = articles.map((article, index) => ({
      id: `${symbol}-news-${index}`,
      title: article.title,
      source: article.sources,
      publishedAt: article.published_time,
      url: article.url,
      summary: article.text || '',
      isNew: false,
    }));
    return { articles: mapped, queued: Boolean(responseData.queued) };
  } catch (error) {
    console.error(`Error fetching news for ${symbol}:`, error);
    return { articles: [], queued: false };
  }
}

export function sortNewsByPublishedAt(articles: NewsArticle[]): NewsArticle[] {
  return [...articles].sort((a, b) => {
    const aTime = Date.parse(a.publishedAt || '') || 0;
    const bTime = Date.parse(b.publishedAt || '') || 0;
    return bTime - aTime;
  });
}
