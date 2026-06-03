import React, { useState, useEffect, useRef } from 'react';
import { Plus, Undo2, Redo2, ZoomIn, ZoomOut, HelpCircle, Lock, Unlock, Settings, Layout } from 'lucide-react';
import { GridContainer, type GridContainerRef } from './components/GridContainer';
import { ModuleSidebar } from './components/ModuleSidebar';
import { HelpPanel, type TabType } from './components/HelpPanel';
import { TimelineClock } from './components/TimelineClock';
import { TouchGamepad } from './components/TouchGamepad';
import { moduleRegistry } from './config/moduleRegistry';
import { LAYOUT_TEMPLATES, DEFAULT_TEMPLATE_ID } from './config/layoutTemplates';
import { useIbbotStore } from './stores/ibbotStore';
import { useTickDataStore } from './stores/tickDataStore';
import { useMarketDataStore } from './stores/marketDataStore';
import { useLayoutStore } from './stores/layoutStore';
import { PinDialog } from './components/PinDialog';
import {
  IS_DEMO,
  getMockRankEntities,
  getMockSeriesData,
  getMockOrders,
  getMockPositions,
  getMockAccount,
  getMockPortfolioSummary,
} from './data/mockData';
import type { ModuleType, GridItemConfig } from './types';
import { checkCollision } from './utils/layoutUtils';

// Default values
const DEFAULT_GRID_GAP = 10;

// Find automatic position for new module
const findAutoPosition = (
  width: number,
  height: number,
  existingItems: GridItemConfig[]
): { x: number; y: number } => {
  const margin = 10;
  const gridStep = 10;

  // If no items, place at top-left
  if (existingItems.length === 0) {
    return { x: margin, y: margin };
  }

  // Sort items by position (top to bottom, left to right)
  const sortedItems = [...existingItems].sort((a, b) => {
    if (Math.abs(a.position.y - b.position.y) < 50) {
      return a.position.x - b.position.x;
    }
    return a.position.y - b.position.y;
  });

  // Try to place below or to the right of each existing item
  for (const item of sortedItems) {
    const candidates = [
      // Below the item (aligned left)
      { x: item.position.x, y: item.position.y + item.size.height },
      // To the right of the item (aligned top)
      { x: item.position.x + item.size.width, y: item.position.y },
      // Below the item (aligned right)
      { x: item.position.x + item.size.width - width, y: item.position.y + item.size.height },
      // To the right of the item (aligned bottom)
      { x: item.position.x + item.size.width, y: item.position.y + item.size.height - height },
    ];

    for (const pos of candidates) {
      // Ensure within bounds
      if (pos.x < 0 || pos.y < 0) continue;

      const testRect = {
        x: Math.max(margin, pos.x),
        y: Math.max(margin, pos.y),
        width,
        height,
      };

      // Check if this position collides with any existing item
      let hasCollision = false;
      for (const other of existingItems) {
        const otherRect = {
          x: other.position.x,
          y: other.position.y,
          width: other.size.width,
          height: other.size.height,
        };
        if (checkCollision(testRect, otherRect)) {
          hasCollision = true;
          break;
        }
      }

      if (!hasCollision) {
        return { x: testRect.x, y: testRect.y };
      }
    }
  }

  // If no valid position found, try grid search from top-left
  const maxY = Math.max(...existingItems.map(item => item.position.y + item.size.height)) + height;

  for (let y = margin; y < maxY + 500; y += gridStep * 5) {
    for (let x = margin; x < 2000; x += gridStep * 5) {
      const testRect = { x, y, width, height };

      let hasCollision = false;
      for (const other of existingItems) {
        const otherRect = {
          x: other.position.x,
          y: other.position.y,
          width: other.size.width,
          height: other.size.height,
        };
        if (checkCollision(testRect, otherRect)) {
          hasCollision = true;
          break;
        }
      }

      if (!hasCollision) {
        return { x, y };
      }
    }
  }

  // Fallback: stack diagonally
  return {
    x: margin + existingItems.length * 30,
    y: margin + existingItems.length * 30
  };
};

export default function App() {
  // Use layout store for undo/redo functionality
  const {
    items,
    setItems,
    addItem: addStoreItem,
    removeItem: removeStoreItem,
    updateItem: updateStoreItem,
    undo,
    redo,
    canUndo,
    canRedo,
    loadFromStorage
  } = useLayoutStore();

  const [gridGap, setGridGap] = useState(() => {
    const saved = localStorage.getItem('trading-system-gap');
    return saved ? Number(saved) : DEFAULT_GRID_GAP;
  });
  const [zoom, setZoom] = useState(() => {
    const saved = localStorage.getItem('trading-system-zoom');
    return saved ? Number(saved) : 1.0;
  });
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const [helpInitialTab, setHelpInitialTab] = useState<TabType>('shortcuts');
  const [isLocked, setIsLocked] = useState(() => {
    try { return localStorage.getItem('trading-system-locked') === 'true'; } catch { return false; }
  });
  const [currentTemplateId, setCurrentTemplateId] = useState<string>(() => {
    return localStorage.getItem('trading-system-template') || '';
  });
  const [selectedSymbols, setSelectedSymbols] = useState<Record<string, string>>({});
  const gridContainerRef = useRef<GridContainerRef>(null);

  // Load initial layout from localStorage on mount
  useEffect(() => {
    loadFromStorage();

    // Only apply default template + auto-focus on very first visit (no saved layout).
    // `items` may be stale here — check localStorage directly instead.
    const hasSavedLayout = !!localStorage.getItem('trading-system-layout');
    if (!hasSavedLayout) {
      const defaultTemplate = LAYOUT_TEMPLATES[DEFAULT_TEMPLATE_ID];
      if (defaultTemplate) {
        setItems(defaultTemplate.layout);
        setTimeout(() => {
          gridContainerRef.current?.focusToFit();
        }, 100);
      }
    }
    // On subsequent visits, saved zoom & pan restore from their own
    // localStorage keys — don't auto-focus, let the user's view persist.
  }, []);

  // Auto-save gap to localStorage
  useEffect(() => {
    localStorage.setItem('trading-system-gap', String(gridGap));
  }, [gridGap]);

  // Auto-save zoom to localStorage
  useEffect(() => {
    localStorage.setItem('trading-system-zoom', String(zoom));
  }, [zoom]);

  // Auto-save lock state to localStorage
  useEffect(() => {
    try { localStorage.setItem('trading-system-locked', String(isLocked)); } catch {}
  }, [isLocked]);

  // Keyboard shortcuts for undo/redo
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Undo: Ctrl+Z (Windows/Linux) or Cmd+Z (Mac)
      if ((e.ctrlKey || e.metaKey) && e.key === 'z' && !e.shiftKey) {
        e.preventDefault();
        if (canUndo()) {
          undo();
        }
      }
      // Redo: Ctrl+Y (Windows/Linux) or Cmd+Shift+Z (Mac)
      else if (((e.ctrlKey || e.metaKey) && e.key === 'y') ||
               ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'z')) {
        e.preventDefault();
        if (canRedo()) {
          redo();
        }
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [undo, redo, canUndo, canRedo]);

  // Keyboard shortcuts for help panel, settings, and layout
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      // Ignore if user is typing in an input/textarea
      const target = e.target as HTMLElement;
      if (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable) {
        return;
      }

      // Help: Ctrl+/ (toggle)
      if ((e.ctrlKey || e.metaKey) && e.key === '/') {
        e.preventDefault();
        setHelpInitialTab('shortcuts');
        setHelpOpen(v => !v);
      }

      // Settings: comma (open or switch to settings tab)
      if (e.key === ',' && !e.ctrlKey && !e.metaKey && !e.altKey) {
        e.preventDefault();
        setHelpInitialTab('settings');
        setHelpOpen(true);
      }

      // Layout: Shift+L (open or switch to layout tab)
      if ((e.key === 'l' || e.key === 'L') && e.shiftKey && !e.ctrlKey && !e.metaKey && !e.altKey) {
        e.preventDefault();
        setHelpInitialTab('layout');
        setHelpOpen(true);
      }
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, []);

  // Initialize backend stores on mount (or seed mock data in demo mode)
  useEffect(() => {
    if (IS_DEMO) {
      // Demo mode: seed all stores with mock data
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

      // Demo mode: show layout panel on first visit
      const hasSeenDemoLayout = localStorage.getItem('demo-layout-panel-shown');
      if (!hasSeenDemoLayout) {
        setHelpInitialTab('layout');
        setHelpOpen(true);
        localStorage.setItem('demo-layout-panel-shown', 'true');
      }

      console.log('[Demo] Seeded stores with mock data for', tickers.length, 'tickers');
      return;
    }

    useTickDataStore.getState().init();
    useIbbotStore.getState().init();
    return () => {
      useTickDataStore.getState().dispose();
      useIbbotStore.getState().dispose();
    };
  }, []);

  const addModule = (moduleType: ModuleType) => {
    const defaultSize = moduleRegistry[moduleType].defaultSize;
    const position = findAutoPosition(defaultSize.width, defaultSize.height, items);

    const newItem: GridItemConfig = {
      id: `${moduleType}-${Date.now()}`,
      moduleType,
      position,
      size: defaultSize,
      syncGroup: null,
    };
    addStoreItem(newItem);
    setSidebarOpen(false);
  };

  const removeModule = (id: string) => {
    removeStoreItem(id);
  };

  const updateItem = (id: string, updates: Partial<GridItemConfig>) => {
    updateStoreItem(id, updates);
  };

  const handleSymbolSelect = (syncGroup: string | null, symbol: string) => {
    if (syncGroup) {
      setSelectedSymbols(prev => ({
        ...prev,
        [syncGroup]: symbol,
      }));
    }
  };

  const handleSyncGroupChange = (id: string, syncGroup: string | null) => {
    updateItem(id, { syncGroup });
  };

  const handleImportLayout = (newItems: GridItemConfig[]) => {
    setItems(newItems);
  };

  const handleTemplateChange = (templateId: string) => {
    if (templateId === '') {
      // Clear template tracking
      setCurrentTemplateId('');
      localStorage.removeItem('trading-system-template');
      return;
    }

    const template = LAYOUT_TEMPLATES[templateId];
    if (template) {
      setItems(template.layout);
      setCurrentTemplateId(templateId);
      localStorage.setItem('trading-system-template', templateId);
      // Trigger focus-to-fit after layout change (next tick to ensure items are rendered)
      setTimeout(() => {
        gridContainerRef.current?.focusToFit();
      }, 50);
    }
  };

  // Open help panel with specific tab
  const openHelpTab = (tab: TabType) => {
    setHelpInitialTab(tab);
    setHelpOpen(true);
  };

  return (
    <div className="h-screen bg-black text-white flex">
      {/* Privacy PIN dialog (global) */}
      <PinDialog />

      {/* Help Panel (unified with tabs) */}
      <HelpPanel
        open={helpOpen}
        onClose={() => setHelpOpen(false)}
        initialTab={helpInitialTab}
        items={items}
        onImportLayout={handleImportLayout}
        currentTemplateId={currentTemplateId}
        onTemplateChange={handleTemplateChange}
        onFocusToFit={() => gridContainerRef.current?.focusToFit()}
      />

      {/* Sidebar */}
      <ModuleSidebar
        isOpen={sidebarOpen}
        onToggle={() => setSidebarOpen(!sidebarOpen)}
        onAddModule={addModule}
      />

      {/* Main Grid Area */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <div className="relative px-2 py-1 lg:px-4 lg:py-2 border-b border-zinc-800 flex items-center justify-between gap-1 lg:gap-3">
          {/* Left: title + undo/redo */}
          <div className="flex items-center gap-1 lg:gap-3">
            <h1 className="text-xs lg:text-xl font-semibold whitespace-nowrap">Trading System</h1>
            <div className="flex items-center gap-0.5">
              <button
                onClick={undo}
                disabled={!canUndo()}
                className={`p-1 lg:p-2 transition-colors ${
                  canUndo()
                    ? 'hover:bg-zinc-800 text-white'
                    : 'text-zinc-600 cursor-not-allowed'
                }`}
                title="Undo (Ctrl+Z)"
              >
                <Undo2 className="w-3.5 h-3.5 lg:w-4 lg:h-4" />
              </button>
              <button
                onClick={redo}
                disabled={!canRedo()}
                className={`p-1 lg:p-2 transition-colors ${
                  canRedo()
                    ? 'hover:bg-zinc-800 text-white'
                    : 'text-zinc-600 cursor-not-allowed'
                }`}
                title="Redo (Ctrl+Y)"
              >
                <Redo2 className="w-3.5 h-3.5 lg:w-4 lg:h-4" />
              </button>
            </div>
          </div>

          {/* Center — clock, absolute centered regardless of side widths */}
          <div className="absolute left-1/2 -translate-x-1/2 pointer-events-none">
            <TimelineClock compact />
          </div>

          {/* Spacer to balance left/right for absolute clock */}
          <div />

          {/* Right: zoom + help/layout/settings + add */}
          <div className="flex items-center gap-1 lg:gap-3 flex-shrink-0">
            {/* Zoom Controls */}
            <div className="flex items-center gap-0.5 lg:gap-1.5 px-1 lg:px-2.5 py-0.5 lg:py-1 bg-zinc-800 rounded">
              <button
                onClick={() => setIsLocked(v => !v)}
                className={`p-0.5 lg:p-1 transition-colors rounded ${
                  isLocked ? 'text-amber-400 hover:bg-zinc-700' : 'text-zinc-500 hover:bg-zinc-700 hover:text-zinc-300'
                }`}
                title={isLocked ? 'Zoom Locked (L to toggle)' : 'Zoom Unlocked (L to toggle)'}
              >
                {isLocked ? <Lock className="w-3 h-3 lg:w-3.5 lg:h-3.5" /> : <Unlock className="w-3 h-3 lg:w-3.5 lg:h-3.5" />}
              </button>
              <button
                onClick={() => setZoom(Math.max(0.25, +(zoom - 0.1).toFixed(2)))}
                disabled={zoom <= 0.25}
                className={`p-0.5 lg:p-1 transition-colors ${
                  zoom > 0.25 ? 'hover:bg-zinc-700 text-white' : 'text-zinc-600 cursor-not-allowed'
                }`}
                title="Zoom Out"
              >
                <ZoomOut className="w-3.5 h-3.5 lg:w-4 lg:h-4" />
              </button>
              <button
                onClick={() => setZoom(1.0)}
                className="text-2xs lg:text-sm text-zinc-400 min-w-[2rem] lg:min-w-[3rem] text-center hover:text-white transition-colors"
                title="Reset to 100%"
              >
                {Math.round(zoom * 100)}%
              </button>
              <button
                onClick={() => setZoom(Math.min(3.0, +(zoom + 0.1).toFixed(2)))}
                disabled={zoom >= 3.0}
                className={`p-0.5 lg:p-1 transition-colors ${
                  zoom < 3.0 ? 'hover:bg-zinc-700 text-white' : 'text-zinc-600 cursor-not-allowed'
                }`}
                title="Zoom In"
              >
                <ZoomIn className="w-3.5 h-3.5 lg:w-4 lg:h-4" />
              </button>
            </div>

            <div className="flex items-center gap-0.5 lg:gap-1.5">
              <button
                onClick={() => openHelpTab('shortcuts')}
                className="p-1 lg:p-2 hover:bg-zinc-700 transition-colors rounded"
                title="Help (Ctrl+/)"
              >
                <HelpCircle className="w-4 h-4 lg:w-5 lg:h-5" />
              </button>
              <button
                onClick={() => openHelpTab('layout')}
                className="p-1 lg:p-2 hover:bg-zinc-700 transition-colors rounded"
                title="Layout Templates (Shift+L)"
              >
                <Layout className="w-4 h-4 lg:w-5 lg:h-5" />
              </button>
              <button
                onClick={() => openHelpTab('settings')}
                className="p-1 lg:p-2 hover:bg-zinc-700 transition-colors rounded"
                title="Settings (,)"
              >
                <Settings className="w-4 h-4 lg:w-5 lg:h-5" />
              </button>
            </div>

            <button
              onClick={() => setSidebarOpen(true)}
              className="flex items-center gap-1 lg:gap-2 px-2 py-1 lg:px-4 lg:py-2 bg-white text-black hover:bg-zinc-200 transition-colors text-2xs lg:text-sm"
            >
              <Plus className="w-3 h-3 lg:w-4 lg:h-4" />
              <span className="hidden sm:inline">Add Module</span>
              <span className="sm:hidden">Add</span>
            </button>
          </div>
        </div>

        <GridContainer
          ref={gridContainerRef}
          items={items}
          onRemove={removeModule}
          onUpdate={updateItem}
          selectedSymbols={selectedSymbols}
          onSymbolSelect={handleSymbolSelect}
          onSyncGroupChange={handleSyncGroupChange}
          gridGap={gridGap}
          zoom={zoom}
          onZoomChange={setZoom}
          isLocked={isLocked}
          onLockChange={setIsLocked}
        />

        {/* Floating touch gamepad — only on touch devices */}
        {'ontouchstart' in window && (
          <TouchGamepad
            onPan={(dx, dy) => gridContainerRef.current?.panBy(dx, dy)}
            onZoomIn={() => {
              const z = Math.min(3.0, +(zoom + 0.05).toFixed(2));
              gridContainerRef.current?.zoomToward(window.innerWidth / 2, window.innerHeight / 2, z);
            }}
            onZoomOut={() => {
              const z = Math.max(0.25, +(zoom - 0.05).toFixed(2));
              gridContainerRef.current?.zoomToward(window.innerWidth / 2, window.innerHeight / 2, z);
            }}
            onFit={() => gridContainerRef.current?.focusToFit()}
          />
        )}
      </div>
    </div>
  );
}
