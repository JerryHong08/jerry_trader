import React, { useState, useRef, useEffect, useMemo } from 'react';
import { Resizable } from 're-resizable';
import { GripVertical, X, Link, Unlink } from 'lucide-react';
import { moduleRegistry } from '../config/moduleRegistry';
import { SpatialHashGrid, hasCollisions as checkHasCollisions } from '../utils/layoutUtils';
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
  zoom: number;
  panOffset: { x: number; y: number };
  viewportRef: React.RefObject<HTMLDivElement | null>;
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
  zoom,
  panOffset,
  viewportRef,
}: GridItemProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [showSyncMenu, setShowSyncMenu] = useState(false);
  const [tempPosition, setTempPosition] = useState(item.position);
  const [snapGuides, setSnapGuides] = useState<{ x?: number; y?: number }>({});
  const [hasCollision, setHasCollision] = useState(false);
  const itemRef = useRef<HTMLDivElement>(null);
  const moduleConfig = moduleRegistry[item.moduleType];

  // Store zoom/panOffset in refs so mousemove handler always has latest values
  const zoomRef = useRef(zoom);
  const panOffsetRef = useRef(panOffset);
  useEffect(() => { zoomRef.current = zoom; }, [zoom]);
  useEffect(() => { panOffsetRef.current = panOffset; }, [panOffset]);

  // Create spatial hash grid for optimized collision detection
  const spatialGrid = useMemo(() => {
    if (allItems.length > 10) {
      const grid = new SpatialHashGrid(200);
      grid.build(allItems);
      return grid;
    }
    return undefined;
  }, [allItems]);

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
    if ((e.target as HTMLElement).closest('.no-drag')) return;

    const viewport = viewportRef.current;
    if (!viewport) return;

    const rect = viewport.getBoundingClientRect();
    // Convert screen coordinates to canvas coordinates
    const canvasX = (e.clientX - rect.left - panOffsetRef.current.x) / zoomRef.current;
    const canvasY = (e.clientY - rect.top - panOffsetRef.current.y) / zoomRef.current;

    // Offset = distance from canvas mouse point to item's top-left position
    setDragOffset({
      x: canvasX - item.position.x,
      y: canvasY - item.position.y,
    });

    setIsDragging(true);
    e.preventDefault();
  };

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isDragging) return;

      const viewport = viewportRef.current;
      if (!viewport) return;

      const rect = viewport.getBoundingClientRect();
      const currentZoom = zoomRef.current;
      const currentPan = panOffsetRef.current;

      // Convert screen coordinates to canvas coordinates
      const canvasX = (e.clientX - rect.left - currentPan.x) / currentZoom;
      const canvasY = (e.clientY - rect.top - currentPan.y) / currentZoom;

      let newX = canvasX - dragOffset.x;
      let newY = canvasY - dragOffset.y;

      // Apply grid snap
      newX = snapToGrid(newX);
      newY = snapToGrid(newY);

      // Apply magnetic snap and capture snap guide positions
      const snapped = findMagneticSnap(item, newX, newY, allItems, gridGap);

      // Detect snap guides for visual feedback
      const guides: { x?: number; y?: number } = {};
      for (const other of allItems) {
        if (other.id === item.id) continue;

        if (Math.abs(snapped.x - (other.position.x + other.size.width + gridGap)) < 2) {
          guides.x = other.position.x + other.size.width + gridGap;
        } else if (Math.abs(snapped.x + item.size.width - (other.position.x - gridGap)) < 2) {
          guides.x = other.position.x - gridGap;
        }

        if (Math.abs(snapped.y - (other.position.y + other.size.height + gridGap)) < 2) {
          guides.y = other.position.y + other.size.height + gridGap;
        } else if (Math.abs(snapped.y + item.size.height - (other.position.y - gridGap)) < 2) {
          guides.y = other.position.y - gridGap;
        }
      }

      setSnapGuides(guides);

      // Check for collision in real-time for visual feedback (optimized)
      const testRect = {
        x: snapped.x,
        y: snapped.y,
        width: item.size.width,
        height: item.size.height,
      };

      const collision = checkHasCollisions(testRect, allItems, item.id, spatialGrid);
      setHasCollision(collision);

      setTempPosition(snapped);
    };

    const handleMouseUp = () => {
      if (isDragging) {
        // Check for collision only when dropping (optimized)
        const testRect = {
          x: tempPosition.x,
          y: tempPosition.y,
          width: item.size.width,
          height: item.size.height,
        };

        const collision = checkHasCollisions(testRect, allItems, item.id, spatialGrid);
        if (!collision) {
          onUpdate({ position: tempPosition });
        } else {
          // Reset to original position if collision detected
          setTempPosition(item.position);
        }

        // Clear visual feedback
        setSnapGuides({});
        setHasCollision(false);
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

  const handleViewToggle = () => {
    const currentIndex = viewModeConfig.options.findIndex(opt => opt.value === viewModeConfig.currentView);
    const nextIndex = (currentIndex + 1) % viewModeConfig.options.length;
    const nextValue = viewModeConfig.options[nextIndex].value;

    if (item.moduleType === 'stock-detail') {
      onUpdate({ settings: { ...item.settings, stockDetail: { view: nextValue as any } } });
    } else if (item.moduleType === 'order-management') {
      onUpdate({ settings: { ...item.settings, orderManagement: { view: nextValue as any } } });
    } else if (item.moduleType === 'overview-chart') {
      const focusMode = nextValue === 'focus';
      const currentSettings = item.settings?.overviewChart;
      onUpdate({
        settings: {
          ...item.settings,
          overviewChart: {
            selectedStates: currentSettings?.selectedStates || [],
            focusMode,
            ...(currentSettings?.timelineRange && { timelineRange: currentSettings.timelineRange }),
            ...(currentSettings?.timeRange && { timeRange: currentSettings.timeRange }),
            ...(currentSettings?.topN && { topN: currentSettings.topN }),
          }
        }
      });
    }
  };

  // Available symbols for search
  const AVAILABLE_SYMBOLS = ['AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'AMD', 'NFLX', 'COIN', 'PLTR', 'RIVN', 'LCID', 'SOFI', 'BABA', 'NIO'];

  return (
    <>
      {/* Snap Guide Lines - positioned in canvas coordinates */}
      {isDragging && snapGuides.x !== undefined && (
        <div
          className="absolute w-0.5 bg-blue-400 pointer-events-none z-[999]"
          style={{
            left: snapGuides.x,
            top: -50000,
            height: 100000,
            opacity: 0.6,
          }}
        />
      )}
      {isDragging && snapGuides.y !== undefined && (
        <div
          className="absolute h-0.5 bg-blue-400 pointer-events-none z-[999]"
          style={{
            top: snapGuides.y,
            left: -50000,
            width: 100000,
            opacity: 0.6,
          }}
        />
      )}

      <Resizable
      size={item.size}
      onResizeStop={(e, direction, ref, delta) => {
        const newWidth = item.size.width + delta.width;
        const newHeight = item.size.height + delta.height;
        const newSize = {
          width: Math.max(250, snapToGrid(newWidth)),
          height: Math.max(200, snapToGrid(newHeight)),
        };

        // Check for collision before resizing (optimized)
        const testRect = {
          x: item.position.x,
          y: item.position.y,
          width: newSize.width,
          height: newSize.height,
        };

        const collision = checkHasCollisions(testRect, allItems, item.id, spatialGrid);

        if (!collision) {
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
        className={`h-full bg-zinc-900 border-2 flex flex-col shadow-xl transition-colors ${
          hasCollision && isDragging
            ? 'border-red-500'
            : 'border-zinc-700'
        }`}
        style={{
          opacity: isDragging ? 0.7 : 1,
          transition: isDragging ? 'none' : 'all 0.2s ease',
        }}
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
            {/* View Mode Toggle Button */}
            {viewModeConfig.hasViewMode && (
              <button
                onClick={handleViewToggle}
                className="text-xs px-2 py-0.5 bg-zinc-700 hover:bg-zinc-600 transition-colors no-drag"
                title="Click to switch view mode"
              >
                {viewModeConfig.options.find(opt => opt.value === viewModeConfig.currentView)?.label}
              </button>
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
            moduleId={item.id}
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
    </>
  );
}
