import { useState } from 'react';
import {
  BarChart3, BarChart4, TrendingUp, ShoppingCart, Wallet,
  FileText, Newspaper,
  Settings, X, Eye, EyeOff, ChevronUp, ChevronDown,
} from 'lucide-react';

// ---- Tab registry ------------------------------------------------------------

const TAB_REGISTRY: Record<string, { label: string; icon: typeof BarChart3 }> = {
  market: { label: 'Market', icon: TrendingUp },
  overview: { label: 'Overview', icon: BarChart3 },
  chart: { label: 'Chart', icon: BarChart4 },
  trade: { label: 'Trade', icon: ShoppingCart },
  holdings: { label: 'Positions', icon: Wallet },
  detail: { label: 'Detail', icon: FileText },
  news: { label: 'News', icon: Newspaper },
};

export interface TabItem {
  id: string;
  visible: boolean;
}

export const DEFAULT_TABS: TabItem[] = [
  { id: 'market', visible: true },
  { id: 'overview', visible: true },
  { id: 'chart', visible: true },
  { id: 'trade', visible: true },
  { id: 'holdings', visible: true },
  { id: 'detail', visible: true },
  { id: 'news', visible: true },
];

const STORAGE_KEY = 'jt_mobile_tabs';

export function loadTabConfig(): TabItem[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length > 0) {
        // Validate: keep only known ids, preserve order & visibility
        const valid = parsed.filter((t: TabItem) => TAB_REGISTRY[t.id]);
        if (valid.length > 0) {
          // Merge in any new tabs from DEFAULT_TABS that aren't in saved config
          const existingIds = new Set(valid.map((t) => t.id));
          const merged = [...valid];
          for (const dt of DEFAULT_TABS) {
            if (!existingIds.has(dt.id)) {
              merged.push({ ...dt });
            }
          }
          return merged.length === valid.length ? valid : merged;
        }
      }
    }
  } catch { /* ignore */ }
  return DEFAULT_TABS;
}

export function saveTabConfig(config: TabItem[]) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(config));
  } catch { /* ignore */ }
}

// ---- Component ---------------------------------------------------------------

interface MobileTabBarProps {
  activeTab: string;
  onTabChange: (tab: string) => void;
  config: TabItem[];
  onConfigChange: (config: TabItem[]) => void;
}

export function MobileTabBar({ activeTab, onTabChange, config, onConfigChange }: MobileTabBarProps) {
  const [showConfig, setShowConfig] = useState(false);
  const visibleTabs = config.filter((t) => t.visible);

  const moveUp = (idx: number) => {
    if (idx === 0) return;
    const next = [...config];
    [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
    onConfigChange(next);
  };

  const moveDown = (idx: number) => {
    if (idx === config.length - 1) return;
    const next = [...config];
    [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
    onConfigChange(next);
  };

  const toggleVisible = (id: string) => {
    onConfigChange(config.map((t) => (t.id === id ? { ...t, visible: !t.visible } : t)));
  };

  const resetConfig = () => {
    onConfigChange(DEFAULT_TABS);
  };

  return (
    <>
      {/* Config panel */}
      {showConfig && (
        <div className="flex-shrink-0 border-t border-zinc-700 bg-zinc-900 px-3 pt-3 pb-2">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-medium text-zinc-300">Customize Tabs</span>
            <button
              onClick={() => setShowConfig(false)}
              className="p-1 text-zinc-500 hover:text-white"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
          <div className="space-y-1.5 mb-2">
            {config.map((tab, idx) => {
              const reg = TAB_REGISTRY[tab.id];
              if (!reg) return null;
              const Icon = reg.icon;
              return (
                <div
                  key={tab.id}
                  className="flex items-center gap-2 bg-zinc-800 rounded px-2 py-1.5"
                >
                  <Icon className="w-4 h-4 text-zinc-500 flex-shrink-0" />
                  <span className={`text-xs flex-1 ${tab.visible ? 'text-zinc-200' : 'text-zinc-600'}`}>
                    {reg.label}
                  </span>
                  <div className="flex items-center gap-0.5">
                    <button
                      onClick={() => moveUp(idx)}
                      disabled={idx === 0}
                      className="p-0.5 text-zinc-500 hover:text-white disabled:opacity-30"
                    >
                      <ChevronUp className="w-3.5 h-3.5" />
                    </button>
                    <button
                      onClick={() => moveDown(idx)}
                      disabled={idx === config.length - 1}
                      className="p-0.5 text-zinc-500 hover:text-white disabled:opacity-30"
                    >
                      <ChevronDown className="w-3.5 h-3.5" />
                    </button>
                  </div>
                  <button
                    onClick={() => toggleVisible(tab.id)}
                    className={`p-1 rounded ${tab.visible ? 'text-emerald-400' : 'text-zinc-600'}`}
                  >
                    {tab.visible ? <Eye className="w-3.5 h-3.5" /> : <EyeOff className="w-3.5 h-3.5" />}
                  </button>
                </div>
              );
            })}
          </div>
          <button
            onClick={resetConfig}
            className="text-2xs text-zinc-500 hover:text-zinc-300"
          >
            Reset to default
          </button>
        </div>
      )}

      {/* Tab bar */}
      <nav className="flex-shrink-0 border-t border-zinc-800 bg-zinc-900 pb-[env(safe-area-inset-bottom,0px)]">
        <div className="flex items-center h-14">
          <div className="flex items-center justify-around flex-1">
            {visibleTabs.map((tab) => {
              const reg = TAB_REGISTRY[tab.id];
              if (!reg) return null;
              const Icon = reg.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => onTabChange(tab.id)}
                  className={`flex flex-col items-center justify-center gap-0.5 w-full h-14 transition-colors ${
                    activeTab === tab.id ? 'text-blue-400' : 'text-zinc-500'
                  }`}
                >
                  <Icon className="w-5 h-5" />
                  <span className="text-2xs">{reg.label}</span>
                </button>
              );
            })}
          </div>
          {/* Customize button */}
          <button
            onClick={() => setShowConfig(!showConfig)}
            className={`flex flex-col items-center justify-center gap-0.5 px-3 h-14 transition-colors border-l border-zinc-800 ${
              showConfig ? 'text-blue-400' : 'text-zinc-600 hover:text-zinc-400'
            }`}
          >
            <Settings className="w-5 h-5" />
            <span className="text-2xs">More</span>
          </button>
        </div>
      </nav>
    </>
  );
}
