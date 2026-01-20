import React, { useState, useRef, useEffect } from 'react';
import { Resizable } from 're-resizable';
import { GripVertical, X, Link, Unlink, ChevronDown } from 'lucide-react';
import { moduleRegistry } from '../config/moduleRegistry';
import type { GridItemConfig } from '../types';

interface GridItemProps {
  item: GridItemConfig;
  onRemove: () => void;
  onUpdate: (updates: Partial<GridItemConfig>) => void;
  allItems: GridItemConfig[];
  selectedSymbol?: string | null;
  onSymbolSelect?: (symbol: string) => void;
  onSyncGroupChange?: (group: string | null) => void;
  gridGap: number;
}

const GRID_SIZE = 10;
const SNAP_THRESHOLD = 20; // Distance to trigger magnetic snap

const snapToGrid = (value: number): number => {
  return Math.round(value / GRID_SIZE) * GRID_SIZE;
};

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

// Find magnetic snap position near other grids
const findMagneticSnap = (
  item: GridItemConfig,
  newX: number,
  newY: number,
  allItems: GridItemConfig[],
  gridGap: number
): { x: number; y: number } => {
  let finalX = newX;
  let finalY = newY;
  let snapApplied = false;

  for (const other of allItems) {
    if (other.id === item.id) continue;

    const otherRight = other.position.x + other.size.width + gridGap;
    const otherBottom = other.position.y + other.size.height + gridGap;
    const itemRight = newX + item.size.width;
    const itemBottom = newY + item.size.height;

    // Snap to left edge of other (with gap)
    if (Math.abs(itemRight - (other.position.x - gridGap)) < SNAP_THRESHOLD) {
      finalX = other.position.x - item.size.width - gridGap;
      snapApplied = true;
    }
    // Snap to right edge of other (with gap)
    else if (Math.abs(newX - otherRight) < SNAP_THRESHOLD) {
      finalX = otherRight;
      snapApplied = true;
    }

    // Snap to top edge of other (with gap)
    if (Math.abs(itemBottom - (other.position.y - gridGap)) < SNAP_THRESHOLD) {
      finalY = other.position.y - item.size.height - gridGap;
      snapApplied = true;
    }
    // Snap to bottom edge of other (with gap)
    else if (Math.abs(newY - otherBottom) < SNAP_THRESHOLD) {
      finalY = otherBottom;
      snapApplied = true;
    }

    // Check horizontal alignment for vertical snapping
    const horizontalOverlap =
      (newX < otherRight && itemRight > (other.position.x - gridGap));

    if (horizontalOverlap) {
      // Snap to bottom of other
      if (Math.abs(newY - otherBottom) < SNAP_THRESHOLD) {
        finalY = otherBottom;
        snapApplied = true;
      }
      // Snap to top of other
      else if (Math.abs(itemBottom - (other.position.y - gridGap)) < SNAP_THRESHOLD) {
        finalY = other.position.y - item.size.height - gridGap;
        snapApplied = true;
      }
    }

    // Check vertical alignment for horizontal snapping
    const verticalOverlap =
      (newY < otherBottom && itemBottom > (other.position.y - gridGap));

    if (verticalOverlap) {
      // Snap to right of other
      if (Math.abs(newX - otherRight) < SNAP_THRESHOLD) {
        finalX = otherRight;
        snapApplied = true;
      }
      // Snap to left of other
      else if (Math.abs(itemRight - (other.position.x - gridGap)) < SNAP_THRESHOLD) {
        finalX = other.position.x - item.size.width - gridGap;
        snapApplied = true;
      }
    }
  }

  return { x: finalX, y: finalY };
};

const SYNC_GROUPS = ['group-1', 'group-2', 'group-3', 'group-4', 'group-5'];

// Sync group colors - like indicator lights
const SYNC_GROUP_COLORS: Record<string, string> = {
  'group-1': '#3b82f6', // blue
  'group-2': '#10b981', // green
  'group-3': '#f59e0b', // amber
  'group-4': '#8b5cf6', // purple
  'group-5': '#ec4899', // pink
};

export function GridItem({
  item,
  onRemove,
  onUpdate,
  allItems,
  selectedSymbol,
  onSymbolSelect,
  onSyncGroupChange,
  gridGap,
}: GridItemProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [showSyncMenu, setShowSyncMenu] = useState(false);
  const [tempPosition, setTempPosition] = useState(item.position);
  const itemRef = useRef<HTMLDivElement>(null);
  const moduleConfig = moduleRegistry[item.moduleType];

  // Sync tempPosition with item.position when not dragging
  useEffect(() => {
    if (!isDragging) {
      setTempPosition(item.position);
    }
  }, [item.position, isDragging]);

  // Handle invalid module types gracefully
  if (!moduleConfig) {
    console.error(`Module type "${item.moduleType}" not found in registry`);
    return (
      <div
        style={{
          position: 'absolute',
          left: item.position.x,
          top: item.position.y,
          width: item.size.width,
          height: item.size.height,
        }}
        className="bg-red-900 border border-red-600 p-4 text-white"
      >
        <p className="text-sm">Error: Module type "{item.moduleType}" not found</p>
        <p className="text-xs text-gray-400 mt-2">Please remove this module</p>
        <button
          onClick={onRemove}
          className="mt-2 px-3 py-1 bg-red-600 hover:bg-red-700 text-sm"
        >
          Remove Module
        </button>
      </div>
    );
  }

  const handleMouseDown = (e: React.MouseEvent) => {
    if ((e.target as HTMLElement).closest('.no-drag')) {
      return;
    }

    // Get the scroll container
    const scrollContainer = (e.target as HTMLElement).closest('.overflow-y-scroll');
    const scrollTop = scrollContainer ? scrollContainer.scrollTop : 0;
    const scrollLeft = scrollContainer ? scrollContainer.scrollLeft : 0;

    setIsDragging(true);
    setDragOffset({
      x: e.clientX - item.position.x + scrollLeft,
      y: e.clientY - item.position.y + scrollTop,
    });
    e.preventDefault();
  };

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isDragging) return;

      // Get the scroll container and current scroll position
      const scrollContainer = document.querySelector('.overflow-y-scroll');
      const scrollTop = scrollContainer ? scrollContainer.scrollTop : 0;
      const scrollLeft = scrollContainer ? scrollContainer.scrollLeft : 0;

      let newX = e.clientX - dragOffset.x + scrollLeft;
      let newY = e.clientY - dragOffset.y + scrollTop;

      // Keep within bounds
      newX = Math.max(0, newX);
      newY = Math.max(0, newY);

      // Apply grid snap
      newX = snapToGrid(newX);
      newY = snapToGrid(newY);

      // Apply magnetic snap
      const snapped = findMagneticSnap(item, newX, newY, allItems, gridGap);

      // Allow dragging anywhere during move (remove collision check)
      // Collision will be checked only on mouseUp
      setTempPosition(snapped);
    };

    const handleMouseUp = () => {
      if (isDragging) {
        // Check for collision only when dropping
        const testRect = {
          x: tempPosition.x,
          y: tempPosition.y,
          width: item.size.width,
          height: item.size.height,
        };

        let hasCollision = false;
        for (const other of allItems) {
          if (other.id === item.id) continue;
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

        // Only update position if there's no collision
        if (!hasCollision) {
          onUpdate({ position: tempPosition });
        } else {
          // Reset to original position if collision detected
          setTempPosition(item.position);
        }
      }
      setIsDragging(false);
    };

    if (isDragging) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isDragging, dragOffset, onUpdate, allItems, item, tempPosition, gridGap]);

  const handleSyncToggle = (group: string) => {
    if (item.syncGroup === group) {
      onSyncGroupChange?.(null);
    } else {
      onSyncGroupChange?.(group);
    }
    setShowSyncMenu(false);
  };

  const currentPosition = isDragging ? tempPosition : item.position;
  const ModuleComponent = moduleRegistry[item.moduleType].component;

  // Determine view mode options for modules
  const getViewModeConfig = () => {
    switch (item.moduleType) {
      case 'stock-detail':
        return {
          hasViewMode: true,
          currentView: item.settings?.stockDetail?.view || 'fundamentals',
          options: [
            { value: 'fundamentals', label: 'Fundamentals' },
            { value: 'news', label: 'News' }
          ]
        };
      case 'order-management':
        return {
          hasViewMode: true,
          currentView: item.settings?.orderManagement?.view || 'placement',
          options: [
            { value: 'placement', label: 'Placement' },
            { value: 'orders', label: 'Orders' }
          ]
        };
      case 'overview-chart':
        return {
          hasViewMode: true,
          currentView: item.settings?.overviewChart?.focusMode ? 'focus' : 'overview',
          options: [
            { value: 'overview', label: 'Overview' },
            { value: 'focus', label: 'Focus' }
          ]
        };
      default:
        return { hasViewMode: false, currentView: null, options: [] };
    }
  };

  const viewModeConfig = getViewModeConfig();
  const [showViewOptions, setShowViewOptions] = useState(false);

  const handleViewChange = (value: string) => {
    if (item.moduleType === 'stock-detail') {
      onUpdate({ settings: { ...item.settings, stockDetail: { view: value as any } } });
    } else if (item.moduleType === 'order-management') {
      onUpdate({ settings: { ...item.settings, orderManagement: { view: value as any } } });
    } else if (item.moduleType === 'overview-chart' || item.moduleType === 'overview-chart') {
      const focusMode = value === 'focus';
      onUpdate({
        settings: {
          ...item.settings,
          overviewChart: {
            ...item.settings?.overviewChart,
            focusMode
          }
        }
      });
    }
    setShowViewOptions(false);
  };

  // Available symbols for search
  const AVAILABLE_SYMBOLS = ['AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'AMD', 'NFLX', 'COIN', 'PLTR', 'RIVN', 'LCID', 'SOFI', 'BABA', 'NIO'];

  return (
    <Resizable
      size={item.size}
      onResizeStop={(e, direction, ref, delta) => {
        const newWidth = item.size.width + delta.width;
        const newHeight = item.size.height + delta.height;
        const newSize = {
          width: Math.max(250, snapToGrid(newWidth)),
          height: Math.max(200, snapToGrid(newHeight)),
        };

        // Check for collision before resizing
        const testRect = {
          x: item.position.x,
          y: item.position.y,
          width: newSize.width,
          height: newSize.height,
        };

        let hasCollision = false;
        for (const other of allItems) {
          if (other.id === item.id) continue;
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
          onUpdate({ size: newSize });
        }
      }}
      minWidth={250}
      minHeight={200}
      style={{
        position: 'absolute',
        left: currentPosition.x,
        top: currentPosition.y,
        zIndex: isDragging ? 1000 : 1,
      }}
      enable={{
        top: false,
        right: true,
        bottom: true,
        left: false,
        topRight: false,
        bottomRight: true,
        bottomLeft: false,
        topLeft: false,
      }}
      grid={[GRID_SIZE, GRID_SIZE]}
    >
      <div
        ref={itemRef}
        className="h-full bg-zinc-900 border-2 border-zinc-700 flex flex-col shadow-xl"
        style={{ opacity: isDragging ? 0.7 : 1 }}
      >
        {/* Header with drag handle */}
        <div
          onMouseDown={handleMouseDown}
          className="flex items-center justify-between p-3 border-b border-zinc-800 cursor-move bg-zinc-800/50 select-none"
        >
          <div className="flex items-center gap-2">
            <GripVertical className="w-4 h-4 text-gray-500" />
            {/* Sync Group Indicator Dot */}
            {item.syncGroup && (
              <div
                className="w-2 h-2 rounded-full"
                style={{ backgroundColor: SYNC_GROUP_COLORS[item.syncGroup] }}
                title={item.syncGroup}
              />
            )}
            <span className="text-sm">{moduleConfig.name}</span>
            {/* View Mode Button with Dropdown */}
            {viewModeConfig.hasViewMode && (
              <div className="relative">
                <button
                  onClick={() => setShowViewOptions(!showViewOptions)}
                  onMouseEnter={() => setShowViewOptions(true)}
                  className="text-xs px-2 py-0.5 bg-zinc-700 hover:bg-zinc-600 transition-colors no-drag flex items-center gap-1"
                  title="Switch view mode"
                >
                  {viewModeConfig.options.find(opt => opt.value === viewModeConfig.currentView)?.label}
                  <ChevronDown className="w-3 h-3" />
                </button>

                {showViewOptions && (
                  <div
                    className="absolute left-0 top-full mt-1 bg-zinc-800 border border-zinc-700 shadow-xl z-50 min-w-[120px]"
                    onMouseLeave={() => setShowViewOptions(false)}
                  >
                    <div className="p-1">
                      {viewModeConfig.options.map((option) => (
                        <button
                          key={option.value}
                          onClick={() => handleViewChange(option.value)}
                          className={`w-full text-left px-2 py-1 hover:bg-zinc-700 text-xs ${
                            viewModeConfig.currentView === option.value ? 'bg-blue-600' : ''
                          }`}
                        >
                          {option.label}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="flex items-center gap-2 no-drag">
            {/* Controls */}
            {moduleConfig.supportSync && (
              <div className="relative">
                <button
                  onClick={() => setShowSyncMenu(!showSyncMenu)}
                  className={`p-1 hover:bg-zinc-700 transition-colors ${item.syncGroup ? 'text-blue-400' : ''}`}
                  title="Sync group"
                >
                  {item.syncGroup ? <Link className="w-4 h-4" /> : <Unlink className="w-4 h-4" />}
                </button>

                {showSyncMenu && (
                  <div className="absolute right-0 top-full mt-1 bg-zinc-800 border border-zinc-700 shadow-xl z-50 min-w-[120px]">
                    <div className="p-1">
                      <button
                        onClick={() => {
                          onSyncGroupChange?.(null);
                          setShowSyncMenu(false);
                        }}
                        className="w-full text-left px-2 py-1 hover:bg-zinc-700 text-xs"
                      >
                        No Sync
                      </button>
                      {SYNC_GROUPS.map((group) => (
                        <button
                          key={group}
                          onClick={() => handleSyncToggle(group)}
                          className={`w-full text-left px-2 py-1 hover:bg-zinc-700 text-xs flex items-center gap-2 ${
                            item.syncGroup === group ? 'bg-blue-600' : ''
                          }`}
                        >
                          <div
                            className="w-2 h-2 rounded-full"
                            style={{ backgroundColor: SYNC_GROUP_COLORS[group] }}
                          />
                          {group}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
            <button
              onClick={onRemove}
              className="p-1 hover:bg-zinc-700 transition-colors"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-hidden no-drag">
          <ModuleComponent
            onRemove={onRemove}
            syncGroup={item.syncGroup}
            selectedSymbol={selectedSymbol}
            onSymbolSelect={onSymbolSelect}
            settings={item.settings}
            onSettingsChange={(settings) => onUpdate({ settings: { ...item.settings, ...settings } })}
          />
        </div>
      </div>
    </Resizable>
  );
}
