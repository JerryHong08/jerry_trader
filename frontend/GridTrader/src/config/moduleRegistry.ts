import { OrderManagement } from '../components/OrderManagement';
import { RankList } from '../components/RankList';
import { ChartModule } from '../components/ChartModule';
import { OverviewChartModule } from '../components/OverviewChartModule';
import { StockDetail } from '../components/StockDetail';
import { PortfolioModule } from '../components/PortfolioModule';
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
    description: 'TradingView candlestick chart',
    component: ChartModule,
    defaultSize: { width: 600, height: 400 },
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
};
