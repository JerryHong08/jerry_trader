import React, { useState, useEffect } from 'react';
import { Plus } from 'lucide-react';
import { GridContainer } from './components/GridContainer';
import { ModuleSidebar } from './components/ModuleSidebar';
import { SettingsMenu } from './components/SettingsMenu';
import { TimelineClock } from './components/TimelineClock';
import { moduleRegistry } from './config/moduleRegistry';
import { LAYOUT_TEMPLATES } from './config/layoutTemplates';
import { useIbbotStore } from './stores/ibbotStore';
import { useTickDataStore } from './stores/tickDataStore';
import { PinDialog } from './components/PinDialog';
import type { ModuleType, GridItemConfig } from './types';

// Default values
const DEFAULT_GRID_GAP = 10;

// Check if two rectangles collide
const checkCollision = (
  rect1: { x: number; y: number; width: number; height: number },
  rect2: { x: number; y: number; width: number; height: number }
): boolean => {
  return (
    rect1.x < rect2.x + rect2.width &&
    rect1.x + rect1.width > rect2.x &&
    rect1.y < rect2.y + rect2.height &&
    rect1.y + rect1.height > rect2.y
  );
};

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
  // Load from localStorage or use default layout
  const loadInitialLayout = (): GridItemConfig[] => {
    const saved = localStorage.getItem('trading-system-layout');
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        // Migrate old 'order-placement' to 'order-management'
        const migrated = parsed.map((item: GridItemConfig) => {
          if (item.moduleType === 'order-placement') {
            return {
              ...item,
              moduleType: 'order-management',
              settings: {
                ...item.settings,
                orderManagement: item.settings?.orderManagement || { view: 'placement' },
              },
            };
          }
          return item;
        });
        return migrated;
      } catch (e) {
        console.error('Failed to parse saved layout:', e);
      }
    }
    return LAYOUT_TEMPLATES['default-trading'].layout;
  };

  const loadInitialGap = (): number => {
    const saved = localStorage.getItem('trading-system-gap');
    if (saved) {
      return Number(saved);
    }
    return DEFAULT_GRID_GAP;
  };

  const [items, setItems] = useState<GridItemConfig[]>(loadInitialLayout);
  const [gridGap, setGridGap] = useState(loadInitialGap);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [currentTemplateId, setCurrentTemplateId] = useState<string>(() => {
    return localStorage.getItem('trading-system-template') || '';
  });
  const [selectedSymbols, setSelectedSymbols] = useState<Record<string, string>>({});

  // Auto-save to localStorage whenever items or gap changes
  useEffect(() => {
    localStorage.setItem('trading-system-layout', JSON.stringify(items));
  }, [items]);

  useEffect(() => {
    localStorage.setItem('trading-system-gap', String(gridGap));
  }, [gridGap]);

  // Initialize backend stores on mount
  useEffect(() => {
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
    setItems([...items, newItem]);
    setSidebarOpen(false);
  };

  const removeModule = (id: string) => {
    setItems(items.filter(item => item.id !== id));
  };

  const updateItem = (id: string, updates: Partial<GridItemConfig>) => {
    setItems(items.map(item =>
      item.id === id ? { ...item, ...updates } : item
    ));
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
    }
  };

  return (
    <div className="h-screen bg-black text-white flex">
      {/* Privacy PIN dialog (global) */}
      <PinDialog />

      {/* Sidebar */}
      <ModuleSidebar
        isOpen={sidebarOpen}
        onToggle={() => setSidebarOpen(!sidebarOpen)}
        onAddModule={addModule}
      />

      {/* Main Grid Area */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <div className="p-4 border-b border-zinc-800 flex items-center justify-between">
          <h1 className="text-xl">Trading System</h1>
          <div className="absolute left-1/2 -translate-x-1/2">
            <TimelineClock />
          </div>
          <div className="flex items-center gap-4">
            <SettingsMenu
              gridGap={gridGap}
              onGridGapChange={setGridGap}
              items={items}
              onImportLayout={handleImportLayout}
              onTemplateChange={handleTemplateChange}
              currentTemplateId={currentTemplateId}
            />
            <button
              onClick={() => setSidebarOpen(true)}
              className="flex items-center gap-2 px-4 py-2 bg-white text-black hover:bg-gray-200 transition-colors"
            >
              <Plus className="w-4 h-4" />
              Add Module
            </button>
          </div>
        </div>

        <GridContainer
          items={items}
          onRemove={removeModule}
          onUpdate={updateItem}
          selectedSymbols={selectedSymbols}
          onSymbolSelect={handleSymbolSelect}
          onSyncGroupChange={handleSyncGroupChange}
          gridGap={gridGap}
        />
      </div>
    </div>
  );
}
