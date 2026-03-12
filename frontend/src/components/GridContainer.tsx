import React, { useState, useRef, useEffect, useCallback } from 'react';
import { GridItem } from './GridItem';
import type { GridItemConfig } from '../types';

interface GridContainerProps {
  items: GridItemConfig[];
  onRemove: (id: string) => void;
  onUpdate: (id: string, updates: Partial<GridItemConfig>) => void;
  selectedSymbols: Record<string, string>;
  onSymbolSelect: (syncGroup: string | null, symbol: string) => void;
  onSyncGroupChange: (id: string, syncGroup: string | null) => void;
  gridGap: number;
  zoom: number;
  onZoomChange: (zoom: number) => void;
  isLocked: boolean;
  onLockChange: (locked: boolean) => void;
}

const MIN_ZOOM = 0.25;
const MAX_ZOOM = 3.0;
const FOCUS_PADDING = 20; // px padding around focused area
const ANIMATE_DURATION = 400; // ms for smooth focus transition

export function GridContainer({
  items,
  onRemove,
  onUpdate,
  selectedSymbols,
  onSymbolSelect,
  onSyncGroupChange,
  gridGap,
  zoom,
  onZoomChange,
  isLocked,
  onLockChange,
}: GridContainerProps) {
  const [panOffset, setPanOffset] = useState(() => {
    try {
      const saved = localStorage.getItem('trading-system-pan');
      if (saved) return JSON.parse(saved);
    } catch {}
    return { x: 0, y: 0 };
  });
  const [isPanning, setIsPanning] = useState(false);
  const panStartRef = useRef({ x: 0, y: 0 });
  const panOffsetStartRef = useRef({ x: 0, y: 0 });
  const containerRef = useRef<HTMLDivElement>(null);

  // Selection rectangle state (left-click drag on canvas)
  const [isSelecting, setIsSelecting] = useState(false);
  const [selectionStart, setSelectionStart] = useState({ x: 0, y: 0 });
  const [selectionEnd, setSelectionEnd] = useState({ x: 0, y: 0 });

  // Smooth transition state
  const [isAnimating, setIsAnimating] = useState(false);

  // Focused grid: clicking a grid sets it focused so scroll goes to its content
  const [focusedGridId, setFocusedGridId] = useState<string | null>(null);

  // Lock ref for stable closures

  // === All refs for stable closures (no listener churn) ===
  const zoomRef = useRef(zoom);
  const panOffsetRef = useRef(panOffset);
  const onZoomChangeRef = useRef(onZoomChange);
  const focusedGridIdRef = useRef(focusedGridId);
  const isLockedRef = useRef(isLocked);
  // Sync refs on every render (no useEffect needed — refs are synchronous)
  zoomRef.current = zoom;
  panOffsetRef.current = panOffset;
  onZoomChangeRef.current = onZoomChange;
  focusedGridIdRef.current = focusedGridId;
  isLockedRef.current = isLocked;

  // Persist pan offset
  useEffect(() => {
    localStorage.setItem('trading-system-pan', JSON.stringify(panOffset));
  }, [panOffset]);

  // Handle wheel — single stable useEffect, reads everything from refs
  // Never re-registers → no glitches from listener churn
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const onWheel = (e: WheelEvent) => {
      const curZoom = zoomRef.current;
      const curPan = panOffsetRef.current;

      if (e.ctrlKey || e.metaKey) {
        // Zoom toward cursor (blocked when locked)
        e.preventDefault();
        if (isLockedRef.current) return;
        const rect = container.getBoundingClientRect();
        const mx = e.clientX - rect.left;
        const my = e.clientY - rect.top;

        const canvasX = (mx - curPan.x) / curZoom;
        const canvasY = (my - curPan.y) / curZoom;

        // Exponential zoom — consistent feel, faster response
        const factor = Math.pow(0.995, e.deltaY);
        const nz = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, curZoom * factor));

        const np = { x: mx - canvasX * nz, y: my - canvasY * nz };
        zoomRef.current = nz;
        panOffsetRef.current = np;
        setPanOffset(np);
        onZoomChangeRef.current(nz);
        return;
      }

      // Plain scroll — check if over focused grid content
      const target = e.target as HTMLElement;
      const gridEl = target.closest('[data-grid-id]');
      if (gridEl) {
        const gridId = gridEl.getAttribute('data-grid-id');
        if (gridId === focusedGridIdRef.current && target.closest('.no-drag')) {
          return; // let native scroll handle it
        }
      }

      // Canvas pan
      e.preventDefault();
      const np = { x: curPan.x - e.deltaX, y: curPan.y - e.deltaY };
      panOffsetRef.current = np;
      setPanOffset(np);
    };

    container.addEventListener('wheel', onWheel, { passive: false });
    return () => container.removeEventListener('wheel', onWheel);
  }, []); // stable — never re-registers

  // Mouse down: left-click selection on canvas, focus detection on grids, panning
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (e.button === 0) {
      const target = e.target as HTMLElement;
      const gridEl = target.closest('[data-grid-id]');

      if (gridEl) {
        // Clicked on a grid → focus it (GridItem handles its own drag)
        setFocusedGridId(gridEl.getAttribute('data-grid-id'));
      } else {
        // Clicked empty canvas → start selection rectangle + clear focus
        e.preventDefault(); // prevent native text selection during drag
        setFocusedGridId(null);
        const rect = containerRef.current!.getBoundingClientRect();
        const sx = e.clientX - rect.left;
        const sy = e.clientY - rect.top;
        setSelectionStart({ x: sx, y: sy });
        setSelectionEnd({ x: sx, y: sy });
        setIsSelecting(true);
      }
      return;
    }

    // Middle-click → pan
    if (e.button === 1) {
      setIsPanning(true);
      panStartRef.current = { x: e.clientX, y: e.clientY };
      panOffsetStartRef.current = { ...panOffsetRef.current };
      e.preventDefault();
    }
  }, []); // stable — reads from refs

  useEffect(() => {
    if (!isPanning) return;

    const handleMouseMove = (e: MouseEvent) => {
      setPanOffset({
        x: panOffsetStartRef.current.x + (e.clientX - panStartRef.current.x),
        y: panOffsetStartRef.current.y + (e.clientY - panStartRef.current.y),
      });
    };

    const handleMouseUp = () => setIsPanning(false);

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isPanning]);

  // Focus-to-fit: animate pan/zoom so a canvas bounding box is centered in viewport
  const focusOnBounds = useCallback((
    canvasMinX: number,
    canvasMinY: number,
    canvasMaxX: number,
    canvasMaxY: number,
  ) => {
    const container = containerRef.current;
    if (!container) return;

    const viewW = container.clientWidth;
    const viewH = container.clientHeight;
    const boundsW = canvasMaxX - canvasMinX;
    const boundsH = canvasMaxY - canvasMinY;

    if (boundsW <= 0 || boundsH <= 0) return;

    // Fit with padding
    const fitZoom = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM,
      Math.min(
        (viewW - FOCUS_PADDING * 2) / boundsW,
        (viewH - FOCUS_PADDING * 2) / boundsH,
      )
    ));

    // Center the bounds in the viewport
    const centerX = (canvasMinX + canvasMaxX) / 2;
    const centerY = (canvasMinY + canvasMaxY) / 2;
    const newPanX = viewW / 2 - centerX * fitZoom;
    const newPanY = viewH / 2 - centerY * fitZoom;

    // Animate the transition
    setIsAnimating(true);
    onZoomChange(fitZoom);
    setPanOffset({ x: newPanX, y: newPanY });
    setTimeout(() => setIsAnimating(false), ANIMATE_DURATION);
  }, [onZoomChange]);

  // Left-click drag: selection rectangle on canvas
  useEffect(() => {
    if (!isSelecting) return;

    const handleMouseMove = (e: MouseEvent) => {
      const rect = containerRef.current!.getBoundingClientRect();
      setSelectionEnd({
        x: e.clientX - rect.left,
        y: e.clientY - rect.top,
      });
    };

    const handleMouseUp = (e: MouseEvent) => {
      if (e.button !== 0) return;
      setIsSelecting(false);

      const rect = containerRef.current!.getBoundingClientRect();
      const endX = e.clientX - rect.left;
      const endY = e.clientY - rect.top;

      // Read latest values from refs (no stale closure)
      const curPan = panOffsetRef.current;
      const curZoom = zoomRef.current;

      // Convert selection rectangle from screen to canvas coords
      const canvasX1 = (Math.min(selectionStart.x, endX) - curPan.x) / curZoom;
      const canvasY1 = (Math.min(selectionStart.y, endY) - curPan.y) / curZoom;
      const canvasX2 = (Math.max(selectionStart.x, endX) - curPan.x) / curZoom;
      const canvasY2 = (Math.max(selectionStart.y, endY) - curPan.y) / curZoom;

      const selW = canvasX2 - canvasX1;
      const selH = canvasY2 - canvasY1;

      // Ignore tiny selections (accidental clicks)
      if (selW < 20 && selH < 20) return;

      // Find items that intersect the selection
      const selectedItems = items.filter(item => {
        const ix = item.position.x;
        const iy = item.position.y;
        const iw = item.size.width;
        const ih = item.size.height;
        return ix + iw > canvasX1 && ix < canvasX2 && iy + ih > canvasY1 && iy < canvasY2;
      });

      if (selectedItems.length > 0) {
        const minX = Math.min(...selectedItems.map(i => i.position.x));
        const minY = Math.min(...selectedItems.map(i => i.position.y));
        const maxX = Math.max(...selectedItems.map(i => i.position.x + i.size.width));
        const maxY = Math.max(...selectedItems.map(i => i.position.y + i.size.height));
        focusOnBounds(minX, minY, maxX, maxY);
      } else {
        focusOnBounds(canvasX1, canvasY1, canvasX2, canvasY2);
      }
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isSelecting, selectionStart, items, focusOnBounds]);

  // Keyboard shortcut: F to focus all, L to lock, Alt+L to unlock
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.target as HTMLElement).closest('input, textarea, [contenteditable]')) return;

      if (e.code === 'KeyF' && !e.ctrlKey && !e.metaKey && !e.shiftKey) {
        e.preventDefault();
        onLockChange(false); // F cancels lock
        if (items.length === 0) return;
        const minX = Math.min(...items.map(i => i.position.x));
        const minY = Math.min(...items.map(i => i.position.y));
        const maxX = Math.max(...items.map(i => i.position.x + i.size.width));
        const maxY = Math.max(...items.map(i => i.position.y + i.size.height));
        focusOnBounds(minX, minY, maxX, maxY);
      }

      if (e.code === 'KeyL' && !e.ctrlKey && !e.metaKey && !e.shiftKey) {
        e.preventDefault();
        if (e.altKey) {
          onLockChange(false); // Alt+L unlocks
        } else {
          onLockChange(true); // L locks
        }
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [items, focusOnBounds, onLockChange]);

  // Compute selection rect in screen pixels for the overlay
  const selRect = isSelecting ? {
    left: Math.min(selectionStart.x, selectionEnd.x),
    top: Math.min(selectionStart.y, selectionEnd.y),
    width: Math.abs(selectionEnd.x - selectionStart.x),
    height: Math.abs(selectionEnd.y - selectionStart.y),
  } : null;

  return (
    <div
      ref={containerRef}
      className="flex-1 relative bg-zinc-950 overflow-hidden"
      style={{ cursor: isSelecting ? 'crosshair' : isPanning ? 'grabbing' : 'default' }}
      onMouseDown={handleMouseDown}
    >
      {/* Infinite grid background - moves with pan/zoom */}
      <div
        className="absolute inset-0 pointer-events-none opacity-10"
        style={{
          backgroundImage: `
            linear-gradient(to right, #27272a 1px, transparent 1px),
            linear-gradient(to bottom, #27272a 1px, transparent 1px)
          `,
          backgroundSize: `${20 * zoom}px ${20 * zoom}px`,
          backgroundPosition: `${panOffset.x}px ${panOffset.y}px`,
        }}
      />

      {/* Canvas content layer */}
      <div
        className="absolute"
        style={{
          transform: `translate(${panOffset.x}px, ${panOffset.y}px) scale(${zoom})`,
          transformOrigin: '0 0',
          transition: isAnimating ? `transform ${ANIMATE_DURATION}ms cubic-bezier(0.4, 0, 0.2, 1)` : 'none',
        }}
      >
        {items.map((item) => (
          <div key={item.id} data-grid-id={item.id}>
            <GridItem
              item={item}
              onRemove={() => onRemove(item.id)}
              onUpdate={(updates) => onUpdate(item.id, updates)}
              allItems={items}
              selectedSymbol={item.syncGroup ? selectedSymbols[item.syncGroup] : null}
              onSymbolSelect={(symbol) => onSymbolSelect(item.syncGroup ?? null, symbol)}
              onSyncGroupChange={(group) => onSyncGroupChange(item.id, group)}
              gridGap={gridGap}
              zoom={zoom}
              panOffset={panOffset}
              viewportRef={containerRef}
            />
          </div>
        ))}
      </div>

      {/* Selection rectangle overlay */}
      {selRect && (
        <div
          className="absolute pointer-events-none border-2 border-blue-400 bg-blue-400/10 z-[2000]"
          style={{
            left: selRect.left,
            top: selRect.top,
            width: selRect.width,
            height: selRect.height,
          }}
        />
      )}


    </div>
  );
}
