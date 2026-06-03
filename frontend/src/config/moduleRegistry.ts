import { OrderManagement } from '../components/OrderManagement';
import { RankList } from '../components/RankList';
import { OverviewChartModule } from '../components/OverviewChartModule';
import { StockDetail } from '../components/StockDetail';
import { PortfolioModule } from '../components/PortfolioModule';
import NewsRoom from '../components/NewsRoom';
import ChartPanelSystem from '../components/ChartPanelSystem';
import { BacktestChartModule } from '../components/BacktestChartModule';
import { BacktestConfigModule } from '../components/BacktestConfigModule';
import { BacktestResultsModule } from '../components/BacktestResultsModule';
import LabelingModule from '../components/LabelingModule';
import type { ModuleConfig, ModuleType } from '../types';

export const moduleRegistry: Record<ModuleType, ModuleConfig> = {
  'order-management': {
    type: 'order-management',
    name: 'Order Management',
    description: 'Create orders and view order history',
    component: OrderManagement,
    defaultSize: { width: 350, height: 450 },
    supportSync: true,
  },
  'rank-list': {
    type: 'rank-list',
    name: 'Top Gainers',
    description: 'Real-time top 20 gainers list',
    component: RankList,
    defaultSize: { width: 900, height: 450 },
    supportSync: true,
  },
  'chart': {
    type: 'chart',
    name: 'Chart',
    description: 'TradingView-style panel chart with overlays and indicators',
    component: ChartPanelSystem,
    defaultSize: { width: 600, height: 500 },
    supportSync: true,
    supportSearch: true,
  },
  'overview-chart': {
    type: 'overview-chart',
    name: 'Overview Chart',
    description: 'High-performance overview chart with Lightweight Charts',
    component: OverviewChartModule,
    defaultSize: { width: 800, height: 500 },
    supportSync: true,
  },
  'stock-detail': {
    type: 'stock-detail',
    name: 'Stock Detail',
    description: 'Stock fundamentals and company info',
    component: StockDetail,
    defaultSize: { width: 400, height: 600 },
    supportSync: true,
    supportSearch: true,
  },
  'portfolio': {
    type: 'portfolio',
    name: 'Portfolio',
    description: 'Account info and current positions',
    component: PortfolioModule,
    defaultSize: { width: 900, height: 550 },
    supportSync: true,
  },
  'news-room': {
    type: 'news-room',
    name: 'News Room',
    description: 'Real-time news processing results with filters',
    component: NewsRoom,
    defaultSize: { width: 800, height: 600 },
    supportSync: false,
  },
  'backtest-chart': {
    type: 'backtest-chart',
    name: 'Backtest Chart',
    description: 'Visualize backtest signals and factor analysis',
    component: BacktestChartModule,
    defaultSize: { width: 900, height: 600 },
    supportSync: false,
  },
  'backtest-config': {
    type: 'backtest-config',
    name: 'Backtest',
    description: 'Configure, run backtest, and view real-time progress',
    component: BacktestConfigModule,
    defaultSize: { width: 380, height: 700 },
    supportSync: false,
  },
  'backtest-results': {
    type: 'backtest-results',
    name: 'Results Browser',
    description: 'Multi-layer experiments/tickers browser with drill-down',
    component: BacktestResultsModule,
    defaultSize: { width: 500, height: 500 },
    supportSync: false,
  },
  'labeling': {
    type: 'labeling',
    name: 'MOMO Labeler',
    description: 'Label premarket momentum candidates with price chart, ignition markers, and keyboard shortcuts',
    component: LabelingModule,
    defaultSize: { width: 1200, height: 800 },
    supportSync: false,
  },
};
