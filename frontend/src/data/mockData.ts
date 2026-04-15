/**
 * Mock Data for GitHub Pages Demo Mode
 *
 * When no backend is configured (VITE_BFF_URL is empty), the app seeds
 * stores with this sample data so every component renders meaningfully.
 */

import type { RankItem, TickerState, NewsArticle } from '../types';
import type { LWSeriesData, RankEntity } from '../stores/marketDataStore';
import type { OrderStatusEventData, PortfolioSummaryResponse } from '../types/ibbot';
import type { PositionRow } from '../stores/ibbotStore';
import type { Quote, Trade } from '../stores/tickDataStore';

// ============================================================================
// Helpers
// ============================================================================

/** Whether we are running in demo mode (no backend configured). */
export const IS_DEMO = !import.meta.env.VITE_BFF_URL;

const STATES: TickerState[] = ['Best', 'Good', 'OnWatch', 'NotGood', 'Bad'];

function rand(min: number, max: number) {
  return Math.random() * (max - min) + min;
}

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

/** Generate a reproducible intraday series from 9:30→16:00 ET. */
function generateSeries(
  baseChange: number,
  volatility: number,
): LWSeriesData {
  const pts: { time: number; value: number }[] = [];
  const states: { time: number; state: TickerState }[] = [];

  // Use today's date for timestamps
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth();
  const day = now.getDate();

  // 9:30 ET → 16:00 ET  (390 minutes)
  const openET = new Date(year, month, day, 9, 30, 0);
  const open = Math.floor(openET.getTime() / 1000);

  let value = baseChange * 0.3; // start near zero and trend toward baseChange
  const step = (baseChange - value) / 390;

  for (let m = 0; m < 390; m++) {
    value += step + (Math.random() - 0.5) * volatility;
    pts.push({ time: open + m * 60, value: Math.round(value * 100) / 100 });
  }

  // 2-3 state transitions
  const stateForChange = (v: number): TickerState => {
    if (v > 15) return 'Best';
    if (v > 5) return 'Good';
    if (v > -2) return 'OnWatch';
    if (v > -8) return 'NotGood';
    return 'Bad';
  };

  states.push({ time: open, state: stateForChange(pts[0].value) });
  const mid = Math.floor(pts.length / 2);
  states.push({ time: pts[mid].time, state: stateForChange(pts[mid].value) });
  const last = pts[pts.length - 1];
  states.push({ time: last.time, state: stateForChange(last.value) });

  return { data: pts, states };
}

// ============================================================================
// Rank / Market Data
// ============================================================================

interface MockTicker {
  symbol: string;
  price: number;
  changePercent: number;
  volume: number;
  float: number;
  marketCap: number;
  sector: string;
  state: TickerState;
  stateReason: string;
}

const MOCK_TICKERS: MockTicker[] = [
  { symbol: 'NVDA', price: 142.50, changePercent: 18.5, volume: 85_200_000, float: 2_440_000_000, marketCap: 3_500_000_000_000, sector: 'Technology', state: 'Best', stateReason: 'Breakout +18% on earnings beat' },
  { symbol: 'TSLA', price: 278.30, changePercent: 12.3, volume: 67_400_000, float: 2_800_000_000, marketCap: 890_000_000_000, sector: 'Consumer Cyclical', state: 'Best', stateReason: 'Gapping up on delivery numbers' },
  { symbol: 'PLTR', price: 45.80, changePercent: 19.7, volume: 42_100_000, float: 1_900_000_000, marketCap: 101_000_000_000, sector: 'Technology', state: 'Good', stateReason: 'New gov contract announced' },
  { symbol: 'SMCI', price: 38.20, changePercent: 11.2, volume: 35_600_000, float: 480_000_000, marketCap: 22_000_000_000, sector: 'Technology', state: 'Good', stateReason: 'AI infrastructure demand' },
  { symbol: 'AMD', price: 168.90, changePercent: 20.1, volume: 28_900_000, float: 1_600_000_000, marketCap: 273_000_000_000, sector: 'Technology', state: 'Good', stateReason: 'Upgraded by analyst' },
  { symbol: 'AAPL', price: 232.10, changePercent: 10.4, volume: 51_200_000, float: 15_200_000_000, marketCap: 3_580_000_000_000, sector: 'Technology', state: 'OnWatch', stateReason: 'Steady uptrend' },
  { symbol: 'MARA', price: 22.40, changePercent: 22.8, volume: 18_700_000, float: 280_000_000, marketCap: 7_400_000_000, sector: 'Financial Services', state: 'OnWatch', stateReason: 'BTC correlation' },
  { symbol: 'SOFI', price: 14.60, changePercent: 20.5, volume: 22_300_000, float: 900_000_000, marketCap: 15_600_000_000, sector: 'Financial Services', state: 'OnWatch', stateReason: 'Holding support' },
  { symbol: 'RIVN', price: 13.80, changePercent: 16.1, volume: 12_400_000, float: 770_000_000, marketCap: 14_200_000_000, sector: 'Consumer Cyclical', state: 'OnWatch', stateReason: 'Range-bound' },
  { symbol: 'META', price: 585.40, changePercent: 16.2, volume: 15_800_000, float: 2_150_000_000, marketCap: 1_480_000_000_000, sector: 'Communication Services', state: 'NotGood', stateReason: 'Fading from highs' },
  { symbol: 'COIN', price: 205.60, changePercent: 8.5, volume: 9_800_000, float: 180_000_000, marketCap: 52_000_000_000, sector: 'Financial Services', state: 'NotGood', stateReason: 'Regulatory concerns' },
  { symbol: 'BABA', price: 88.90, changePercent: 19.8, volume: 14_200_000, float: 2_600_000_000, marketCap: 222_000_000_000, sector: 'Consumer Cyclical', state: 'NotGood', stateReason: 'China macro weakness' },
  { symbol: 'GME', price: 24.10, changePercent: 33.2, volume: 31_500_000, float: 260_000_000, marketCap: 10_200_000_000, sector: 'Consumer Cyclical', state: 'Bad', stateReason: 'Dilution announced' },
  { symbol: 'LCID', price: 2.85, changePercent: 16.1, volume: 28_900_000, float: 1_800_000_000, marketCap: 8_400_000_000, sector: 'Consumer Cyclical', state: 'Bad', stateReason: 'Cash burn concerns' },
  { symbol: 'SNAP', price: 11.20, changePercent: 31.4, volume: 19_600_000, float: 1_200_000_000, marketCap: 18_500_000_000, sector: 'Communication Services', state: 'Bad', stateReason: 'Ad revenue miss' },
];

export function getMockRankEntities(): Map<string, RankEntity> {
  const map = new Map<string, RankEntity>();
  MOCK_TICKERS.forEach((t, i) => {
    const entity: RankEntity = {
      symbol: t.symbol,
      price: t.price,
      changePercent: t.changePercent,
      volume: t.volume,
      vwap: t.price * (1 + (Math.random() - 0.5) * 0.005),
      state: t.state,
      stateReason: t.stateReason,
      relativeVolumeDaily: rand(0.8, 5.0),
      relativeVolume5min: rand(0.5, 8.0),
      rank: i + 1,
      marketCap: t.marketCap,
      float: t.float,
      hasNews: i < 8,
      latestNewsTime: i < 8 ? Date.now() - i * 3_600_000 : undefined,
      country: 'US',
      sector: t.sector,
      isSubscribed: true,
    };
    map.set(t.symbol, entity);
  });
  return map;
}

export function getMockSeriesData(): Record<string, LWSeriesData> {
  const result: Record<string, LWSeriesData> = {};
  for (const t of MOCK_TICKERS) {
    result[t.symbol] = generateSeries(t.changePercent, Math.abs(t.changePercent) * 0.05 + 0.03);
  }
  return result;
}

// ============================================================================
// IBBot / Portfolio Data
// ============================================================================

export function getMockOrders(): Record<number, OrderStatusEventData> {
  const orders: OrderStatusEventData[] = [
    { order_id: 1001, status: 'Filled', filled: 100, remaining: 0, avg_fill_price: 140.25, commission: 1.00, symbol: 'NVDA', action: 'BUY', quantity: 100, order_type: 'LMT', limit_price: 141.00, tif: 'DAY', outsideRth: false },
    { order_id: 1002, status: 'Filled', filled: 200, remaining: 0, avg_fill_price: 275.50, commission: 1.00, symbol: 'TSLA', action: 'BUY', quantity: 200, order_type: 'MKT', tif: 'DAY', outsideRth: false },
    { order_id: 1003, status: 'Filled', filled: 50, remaining: 0, avg_fill_price: 278.10, commission: 1.00, symbol: 'TSLA', action: 'SELL', quantity: 50, order_type: 'LMT', limit_price: 278.00, tif: 'DAY', outsideRth: false },
    { order_id: 1004, status: 'PreSubmitted', filled: 0, remaining: 500, symbol: 'PLTR', action: 'BUY', quantity: 500, order_type: 'LMT', limit_price: 44.50, tif: 'DAY', outsideRth: false },
    { order_id: 1005, status: 'PreSubmitted', filled: 0, remaining: 300, symbol: 'AMD', action: 'BUY', quantity: 300, order_type: 'LMT', limit_price: 166.00, tif: 'GTC', outsideRth: true },
    { order_id: 1006, status: 'Cancelled', filled: 0, remaining: 100, symbol: 'META', action: 'SELL', quantity: 100, order_type: 'LMT', limit_price: 590.00, tif: 'DAY', outsideRth: false },
    { order_id: 1007, status: 'Filled', filled: 1000, remaining: 0, avg_fill_price: 14.55, commission: 1.00, symbol: 'SOFI', action: 'BUY', quantity: 1000, order_type: 'MKT', tif: 'DAY', outsideRth: false },
  ];
  const map: Record<number, OrderStatusEventData> = {};
  for (const o of orders) map[o.order_id] = o;
  return map;
}

export function getMockPositions(): Record<string, PositionRow> {
  return {
    NVDA: { symbol: 'NVDA', position: 100, average_cost: 140.25, market_price: 142.50, market_value: 14_250, unrealized_pnl: 225, realized_pnl: 0 },
    TSLA: { symbol: 'TSLA', position: 150, average_cost: 275.50, market_price: 278.30, market_value: 41_745, unrealized_pnl: 420, realized_pnl: 130 },
    SOFI: { symbol: 'SOFI', position: 1000, average_cost: 14.55, market_price: 14.60, market_value: 14_600, unrealized_pnl: 50, realized_pnl: 0 },
    AAPL: { symbol: 'AAPL', position: 50, average_cost: 228.40, market_price: 232.10, market_value: 11_605, unrealized_pnl: 185, realized_pnl: 0 },
    AMD:  { symbol: 'AMD', position: 80, average_cost: 165.20, market_price: 168.90, market_value: 13_512, unrealized_pnl: 296, realized_pnl: 0 },
  };
}

export function getMockAccount(): Record<string, unknown> {
  return {
    NetLiquidation: 125_840.50,
    BuyingPower: 251_681.00,
    TotalCashValue: 30_128.50,
    EquityWithLoanValue: 125_840.50,
    AvailableFunds: 95_712.00,
    DayTradesRemaining: 3,
    InitMarginReq: 47_856.25,
    MaintMarginReq: 38_285.00,
    Cushion: 0.62,
  };
}

export function getMockPortfolioSummary(): PortfolioSummaryResponse {
  const positions = getMockPositions();
  const posArr = Object.values(positions).map((p) => ({
    symbol: p.symbol,
    quantity: p.position ?? 0,
    average_cost: p.average_cost ?? 0,
    market_value: p.market_value ?? 0,
    market_price: p.market_price,
    unrealized_pnl: p.unrealized_pnl,
    realized_pnl: p.realized_pnl,
  }));
  return {
    account: getMockAccount(),
    positions: posArr,
    total_market_value: posArr.reduce((s, p) => s + p.market_value, 0),
    position_count: posArr.length,
  };
}

// ============================================================================
// TickData (Quotes & Trades)
// ============================================================================

export function getMockQuote(symbol: string, price: number): Quote {
  const spread = price * 0.001;
  return {
    symbol,
    bid: Math.round((price - spread / 2) * 100) / 100,
    ask: Math.round((price + spread / 2) * 100) / 100,
    bid_size: Math.floor(rand(1, 50)) * 100,
    ask_size: Math.floor(rand(1, 50)) * 100,
    timestamp: Date.now(),
  };
}

export function getMockTrade(symbol: string, price: number): Trade {
  return {
    symbol,
    price: Math.round((price + (Math.random() - 0.5) * price * 0.002) * 100) / 100,
    size: Math.floor(rand(1, 20)) * 100,
    timestamp: Date.now(),
  };
}

// ============================================================================
// Stock Detail
// ============================================================================

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
  fullTimeEmployees: number;
  ceo: string;
  website: string;
  description: string;
  exchange: string;
  image?: string;
}

export const MOCK_PROFILES: Record<string, StockFundamentals> = {
  NVDA: { symbol: 'NVDA', companyName: 'NVIDIA Corporation', country: 'US', sector: 'Technology', industry: 'Semiconductors', float: 2_440_000_000, marketCap: 3_500_000_000_000, range: '47.32 - 153.13', averageVolume: 62_000_000, ipoDate: '1999-01-22', fullTimeEmployees: 32_600, ceo: 'Jensen Huang', website: 'https://www.nvidia.com', description: 'NVIDIA designs and sells graphics and compute processors for gaming, data center, automotive, and professional visualization markets.', exchange: 'NASDAQ' },
  TSLA: { symbol: 'TSLA', companyName: 'Tesla, Inc.', country: 'US', sector: 'Consumer Cyclical', industry: 'Auto Manufacturers', float: 2_800_000_000, marketCap: 890_000_000_000, range: '138.80 - 488.54', averageVolume: 55_000_000, ipoDate: '2010-06-29', fullTimeEmployees: 140_000, ceo: 'Elon Musk', website: 'https://www.tesla.com', description: 'Tesla designs, develops, manufactures, and sells fully electric vehicles, energy generation and storage systems.', exchange: 'NASDAQ' },
  AAPL: { symbol: 'AAPL', companyName: 'Apple Inc.', country: 'US', sector: 'Technology', industry: 'Consumer Electronics', float: 15_200_000_000, marketCap: 3_580_000_000_000, range: '164.08 - 260.10', averageVolume: 48_000_000, ipoDate: '1980-12-12', fullTimeEmployees: 164_000, ceo: 'Tim Cook', website: 'https://www.apple.com', description: 'Apple designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories.', exchange: 'NASDAQ' },
};

export function getMockNews(symbol: string): NewsArticle[] {
  const now = Date.now();
  return [
    { id: `${symbol}-1`, title: `${symbol} Surges on Strong Quarterly Earnings Beat`, source: 'Reuters', publishedAt: new Date(now - 1_800_000).toISOString(), url: '#', summary: `${symbol} reported quarterly earnings well above analyst expectations, driven by strong demand in core business segments.`, isNew: true },
    { id: `${symbol}-2`, title: `Analyst Upgrades ${symbol} to Overweight`, source: 'MarketWatch', publishedAt: new Date(now - 7_200_000).toISOString(), url: '#', summary: `Major Wall Street firm upgrades ${symbol} citing improved growth outlook and expanding market opportunity.`, isNew: true },
    { id: `${symbol}-3`, title: `${symbol} Announces New Product Line`, source: 'Bloomberg', publishedAt: new Date(now - 14_400_000).toISOString(), url: '#', summary: `${symbol} unveiled a new product line expected to drive significant revenue growth in 2026.`, isNew: false },
    { id: `${symbol}-4`, title: `Institutional Investors Increase ${symbol} Holdings`, source: 'CNBC', publishedAt: new Date(now - 28_800_000).toISOString(), url: '#', summary: `Several large institutional investors disclosed increased positions in ${symbol} during the latest filing period.`, isNew: false },
    { id: `${symbol}-5`, title: `${symbol} CEO Discusses Growth Strategy`, source: 'Yahoo Finance', publishedAt: new Date(now - 86_400_000).toISOString(), url: '#', summary: `In an exclusive interview, the CEO outlined the company's strategic priorities for the coming year.`, isNew: false },
  ];
}
