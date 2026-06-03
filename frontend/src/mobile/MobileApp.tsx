import { useState, useEffect } from 'react';
import { Eye, EyeOff, Wifi, WifiOff } from 'lucide-react';
import {
  IS_DEMO,
  getMockRankEntities,
  getMockSeriesData,
  getMockOrders,
  getMockPositions,
  getMockAccount,
  getMockPortfolioSummary,
} from '../data/mockData';
import { useMarketDataStore } from '../stores/marketDataStore';
import { useIbbotStore } from '../stores/ibbotStore';
import { useTickDataStore } from '../stores/tickDataStore';
import { usePrivacyStore } from '../stores/privacyStore';
import { useWebSocketConnection } from '../hooks/useWebSocket';
import { TimelineClock } from '../components/TimelineClock';
import { MobileTabBar, loadTabConfig, saveTabConfig, type TabItem } from './MobileTabBar';
import { MobileMarketPage } from './pages/MobileMarketPage';
import { MobileChartPage } from './pages/MobileChartPage';
import { MobileStockChartPage } from './pages/MobileStockChartPage';
import { MobileDetailPage } from './pages/MobileDetailPage';
import { MobileNewsPage } from './pages/MobileNewsPage';
import { MobileTradePage } from './pages/MobileTradePage';
import { MobileHoldingsPage } from './pages/MobileHoldingsPage';

export type MobileTab = 'market' | 'overview' | 'chart' | 'trade' | 'holdings' | 'detail' | 'news';

const MOBILE_ACTIVE_TAB_KEY = 'jt_mobile_active_tab';
const MOBILE_SELECTED_SYMBOL_KEY = 'jt_mobile_selected_symbol';

function loadInitialTab(): MobileTab {
  try {
    const saved = localStorage.getItem(MOBILE_ACTIVE_TAB_KEY);
    if (saved && ['market','overview','chart','trade','holdings','detail','news'].includes(saved)) {
      return saved as MobileTab;
    }
  } catch {}
  return 'market';
}

function loadInitialSymbol(): string | null {
  try {
    return localStorage.getItem(MOBILE_SELECTED_SYMBOL_KEY) || null;
  } catch {}
  return null;
}

export function MobileApp() {
  const [activeTab, setActiveTab] = useState<MobileTab>(loadInitialTab);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(loadInitialSymbol);
  const [tabConfig, setTabConfig] = useState<TabItem[]>(() => loadTabConfig());

  const handleTabConfigChange = (next: TabItem[]) => {
    setTabConfig(next);
    saveTabConfig(next);
    // If active tab is hidden, switch to first visible
    const activeItem = next.find((t) => t.id === activeTab);
    if (!activeItem?.visible) {
      const firstVisible = next.find((t) => t.visible);
      if (firstVisible) setActiveTab(firstVisible.id as MobileTab);
    }
  };

  // Persist active tab and selected symbol across orientation switches
  useEffect(() => {
    try { localStorage.setItem(MOBILE_ACTIVE_TAB_KEY, activeTab); } catch {}
  }, [activeTab]);

  useEffect(() => {
    try {
      if (selectedSymbol) localStorage.setItem(MOBILE_SELECTED_SYMBOL_KEY, selectedSymbol);
      else localStorage.removeItem(MOBILE_SELECTED_SYMBOL_KEY);
    } catch {}
  }, [selectedSymbol]);

  // Initialize WebSocket singleton
  useWebSocketConnection();

  // Initialize stores (mirrors App.tsx)
  useEffect(() => {
    if (IS_DEMO) {
      const entities = getMockRankEntities();
      const seriesData = getMockSeriesData();
      const tickers = Array.from(entities.keys());

      useMarketDataStore.setState({
        entities,
        rankTimestamp: new Date().toISOString(),
        seriesData,
        chartTimestamp: new Date().toISOString(),
        connectionStatus: 'connected',
      });
      useMarketDataStore.getState().syncVisibility(tickers);

      useIbbotStore.setState({
        ordersById: getMockOrders(),
        positionsBySymbol: getMockPositions(),
        account: getMockAccount(),
        portfolioSummary: getMockPortfolioSummary(),
        wsStatus: 'connected',
        loading: false,
      });

      useTickDataStore.setState({ connected: true });
      return;
    }

    useTickDataStore.getState().init();
    useIbbotStore.getState().init();
    return () => {
      useTickDataStore.getState().dispose();
      useIbbotStore.getState().dispose();
    };
  }, []);

  // Connection status for unified top bar
  const marketConnected = useMarketDataStore((s) => s.connectionStatus === 'connected');
  const ibbotConnected = useIbbotStore((s) => s.wsStatus === 'connected');
  const privacyMode = usePrivacyStore((s) => s.privacyMode);
  const togglePrivacy = usePrivacyStore((s) => s.toggle);

  // Per-tab state
  const isDataTab = activeTab === 'market' || activeTab === 'overview' || activeTab === 'chart' || activeTab === 'detail';
  const isTradingTab = activeTab === 'trade' || activeTab === 'holdings';
  const tabConnected = isDataTab ? marketConnected : isTradingTab ? ibbotConnected : false;

  const handleSelectSymbol = (symbol: string) => {
    setSelectedSymbol(symbol);
    setActiveTab('chart');
  };

  const renderPage = () => {
    switch (activeTab) {
      case 'market':
        return <MobileMarketPage onSelectSymbol={handleSelectSymbol} />;
      case 'overview':
        return <MobileChartPage symbol={selectedSymbol} onSelectSymbol={setSelectedSymbol} />;
      case 'chart':
        return (
          <MobileStockChartPage
            symbol={selectedSymbol}
            onSelectSymbol={setSelectedSymbol}
            onNavigateToDetail={() => setActiveTab('detail')}
          />
        );
      case 'detail':
        return (
          <MobileDetailPage
            symbol={selectedSymbol}
            onSelectSymbol={setSelectedSymbol}
            onNavigateToChart={() => setActiveTab('chart')}
          />
        );
      case 'news':
        return <MobileNewsPage onSelectSymbol={handleSelectSymbol} />;
      case 'trade':
        return <MobileTradePage symbol={selectedSymbol} />;
      case 'holdings':
        return <MobileHoldingsPage />;
    }
  };

  return (
    <div className="h-dvh bg-black text-white flex flex-col">
      {/* Unified Top Bar */}
      <div className="flex-shrink-0 flex items-center justify-between px-2 py-1 border-b border-zinc-800 bg-zinc-900">
        <div className="w-8 flex items-center">
          {tabConnected ? (
            <Wifi className="w-3.5 h-3.5 text-emerald-400" />
          ) : (
            <WifiOff className="w-3.5 h-3.5 text-red-400" />
          )}
        </div>
        <TimelineClock compact />
        <div className="w-8 flex items-center justify-end">
          {isTradingTab && (
            <button
              onClick={togglePrivacy}
              className={`p-1 rounded transition-colors ${privacyMode ? 'text-amber-400 hover:bg-zinc-800' : 'text-zinc-500 hover:bg-zinc-800 hover:text-zinc-300'}`}
              title={privacyMode ? 'Show values' : 'Hide values'}
            >
              {privacyMode ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
            </button>
          )}
        </div>
      </div>
      <div className="flex-1 min-h-0 overflow-hidden">{renderPage()}</div>
      <MobileTabBar
        activeTab={activeTab}
        onTabChange={setActiveTab}
        config={tabConfig}
        onConfigChange={handleTabConfigChange}
      />
    </div>
  );
}
