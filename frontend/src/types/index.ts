export type ModuleType = 'order-management' | 'rank-list' | 'chart' | 'overview-chart' | 'stock-detail' | 'portfolio' | 'news-room' | 'factor-chart';

export type ChartTimeframe = '10s' | '1m' | '5m' | '15m' | '30m' | '1h' | '4h' | '1D' | '1W' | '1M';

export interface Position {
  x: number;
  y: number;
}

export interface Size {
  width: number;
  height: number;
}

// Portfolio types
export interface PortfolioPosition {
  symbol: string;
  quantity: number;
  avgCost: number;
  marketPrice: number;
  marketValue: number;
  unrealizedPnL: number;
  realizedPnL: number;
}

export interface AccountInfo {
  netLiquidation: number;
  buyingPower: number;
  availableFunds: number;
  totalCashValue: number;
  equityWithLoanValue: number;
  dayTradesRemaining: number;
  initMarginReq: number;
  maintMarginReq: number;
  cushion: number;
}

export interface ChartModuleSettings {
  timeframe: ChartTimeframe;
}

export interface OverviewChartModuleSettings {
  selectedStates: TickerState[];
  focusMode?: boolean; // true = single ticker, false = all tickers
  timelineRange?: { start: number; end: number }; // For timeline sync
  timeRange?: string;
  topN?: number; // Number of top tickers to request from backend
}

export type RankListSortColumn = 'symbol' | 'state' | 'price' | 'change' | 'changePercent' | 'volume' | 'marketCap' | 'float' | 'relativeVolumeDaily' | 'relativeVolume5min' | 'news' | 'vwap';
export type RankListSortDirection = 'asc' | 'desc';

export interface RankListModuleSettings {
  sortColumn: RankListSortColumn;
  sortDirection: RankListSortDirection;
  visibleColumns?: RankListSortColumn[];
  columnWidths?: Record<string, number>;
  columnOrder?: RankListSortColumn[];
}

export type StockDetailView = 'fundamentals' | 'news';

export interface StockDetailModuleSettings {
  view: StockDetailView;
}

export type OrderManagementView = 'placement' | 'orders';

export interface OrderManagementModuleSettings {
  view: OrderManagementView;
}

export interface GridItemConfig {
  id: string;
  moduleType: ModuleType;
  position: Position;
  size: Size;
  syncGroup?: string | null;
  settings?: {
    chart?: ChartModuleSettings;
    overviewChart?: OverviewChartModuleSettings;
    rankList?: RankListModuleSettings;
    stockDetail?: StockDetailModuleSettings;
    orderManagement?: OrderManagementModuleSettings;
  };
}

export interface ModuleConfig {
  type: ModuleType;
  name: string;
  description: string;
  component: React.ComponentType<ModuleProps>;
  defaultSize: Size;
  supportSync?: boolean;
  supportSearch?: boolean;
}

export interface ModuleProps {
  moduleId: string;
  onRemove: () => void;
  syncGroup?: string | null;
  onSyncGroupChange?: (group: string | null) => void;
  selectedSymbol?: string | null;
  onSymbolSelect?: (symbol: string) => void;
  settings?: {
    chart?: ChartModuleSettings;
    overviewChart?: OverviewChartModuleSettings;
    rankList?: RankListModuleSettings;
    stockDetail?: StockDetailModuleSettings;
    orderManagement?: OrderManagementModuleSettings;
  };
  onSettingsChange?: (settings: any) => void;
}

export interface Order {
  symbol: string;
  side: 'buy' | 'sell';
  type: 'market' | 'limit';
  quantity: string;
  price?: string;
}

export type OrderStatus = 'filled' | 'pending' | 'cancelled' | 'rejected' | 'partial';
export type TimeInForce = 'day' | 'gtc' | 'ioc' | 'fok';

export interface HistoricalOrder {
  id: string;
  symbol: string;
  side: 'buy' | 'sell';
  orderType: 'market' | 'limit' | 'stop' | 'stop-limit';
  quantity: number;
  price: number;
  filledQuantity: number;
  status: OrderStatus;
  outsideRth: boolean;
  tif: TimeInForce;
  submittedAt: string;
  filledAt?: string;
}

export type TickerState = 'Best' | 'Good' | 'OnWatch' | 'NotGood' | 'Bad';

export interface RankItem {
  symbol: string;
  price: number;
  change: number;
  changePercent: number;
  volume: number;
  vwap: number;
  state: TickerState;
  stateReason?: string;  // Short description of why ticker is in current state
  relativeVolumeDaily: number;
  relativeVolume5min: number;
  latestNewsTime?: number; // Timestamp of latest news (milliseconds)
  isSubscribed?: boolean;  // Whether ticker is subscribed for overview chart display

  // Static fields (may be undefined until static data is fetched)
  marketCap?: number;
  float?: number;
  hasNews?: boolean;      // Whether ticker has news articles
  country?: string;       // Country of company (e.g., "US")
  sector?: string;        // Sector classification (e.g., "Technology")
}

export interface CandlestickData {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface NewsArticle {
  id: string;
  title: string;
  source: string;
  publishedAt: string;
  url: string;
  summary: string;
  isNew: boolean;
}

export interface NewsProcessorResult {
  id: string; // timestamp or unique id
  model: string;
  symbol: string;
  is_catalyst: boolean;
  classification: string; // "YES" or "NO"
  score: string; // e.g., "7/10"
  title: string;
  published_time: string;
  current_time: string;
  explanation: {
    raw?: string;
    [key: string]: any;
  };
  url: string;
  content_preview: string;
  sources: string; // JSON string
  source_from: string;
  timestamp: string;
}
