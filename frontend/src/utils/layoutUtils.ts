/**
 * Utilities for responsive scaling and optimized layout calculations
 */

import type { GridItemConfig, Position, Size } from '../types';

// Screen breakpoints
export const BREAKPOINTS = {
  sm: 640,
  md: 1024,
  lg: 1440,
  xl: 1920,
  xxl: 2560,
} as const;

// Base design width for scaling calculations
const BASE_WIDTH = 1920;

/**
 * Calculate scale factor based on current viewport width
 * Clamps between 0.5 and 2.0 to prevent extreme scaling
 */
export const getScaleFactor = (viewportWidth: number): number => {
  const factor = viewportWidth / BASE_WIDTH;
  return Math.max(0.5, Math.min(2.0, factor));
};

/**
 * Get current screen breakpoint
 */
export const getCurrentBreakpoint = (width: number): keyof typeof BREAKPOINTS => {
  if (width >= BREAKPOINTS.xxl) return 'xxl';
  if (width >= BREAKPOINTS.xl) return 'xl';
  if (width >= BREAKPOINTS.lg) return 'lg';
  if (width >= BREAKPOINTS.md) return 'md';
  return 'sm';
};

/**
 * Scale a position based on viewport width
 */
export const scalePosition = (position: Position, scaleFactor: number): Position => {
  return {
    x: Math.round(position.x * scaleFactor),
    y: Math.round(position.y * scaleFactor),
  };
};

/**
 * Scale a size based on viewport width
 */
export const scaleSize = (size: Size, scaleFactor: number, minWidth = 250, minHeight = 200): Size => {
  return {
    width: Math.max(minWidth, Math.round(size.width * scaleFactor)),
    height: Math.max(minHeight, Math.round(size.height * scaleFactor)),
  };
};

/**
 * Scale entire layout for responsive display
 */
export const scaleLayout = (items: GridItemConfig[], scaleFactor: number): GridItemConfig[] => {
  return items.map(item => ({
    ...item,
    position: scalePosition(item.position, scaleFactor),
    size: scaleSize(item.size, scaleFactor),
  }));
};

/**
 * Spatial hash grid for optimized collision detection
 * Divides space into cells for O(1) neighbor lookups
 */
export class SpatialHashGrid {
  private cellSize: number;
  private grid: Map<string, GridItemConfig[]>;

  constructor(cellSize: number = 200) {
    this.cellSize = cellSize;
    this.grid = new Map();
  }

  /**
   * Get cell key for position
   */
  private getCellKey(x: number, y: number): string {
    const cellX = Math.floor(x / this.cellSize);
    const cellY = Math.floor(y / this.cellSize);
    return `${cellX},${cellY}`;
  }

  /**
   * Get all cell keys that a rectangle overlaps
   */
  private getCellKeys(x: number, y: number, width: number, height: number): string[] {
    const keys: string[] = [];
    const startX = Math.floor(x / this.cellSize);
    const endX = Math.floor((x + width) / this.cellSize);
    const startY = Math.floor(y / this.cellSize);
    const endY = Math.floor((y + height) / this.cellSize);

    for (let cellX = startX; cellX <= endX; cellX++) {
      for (let cellY = startY; cellY <= endY; cellY++) {
        keys.push(`${cellX},${cellY}`);
      }
    }
    return keys;
  }

  /**
   * Clear and rebuild the grid
   */
  build(items: GridItemConfig[]): void {
    this.grid.clear();

    for (const item of items) {
      const keys = this.getCellKeys(
        item.position.x,
        item.position.y,
        item.size.width,
        item.size.height
      );

      for (const key of keys) {
        if (!this.grid.has(key)) {
          this.grid.set(key, []);
        }
        this.grid.get(key)!.push(item);
      }
    }
  }

  /**
   * Get potential collision candidates for a rectangle
   * Returns only items in nearby cells instead of all items
   */
  getNearby(x: number, y: number, width: number, height: number): GridItemConfig[] {
    const keys = this.getCellKeys(x, y, width, height);
    const nearby = new Set<GridItemConfig>();

    for (const key of keys) {
      const items = this.grid.get(key);
      if (items) {
        items.forEach(item => nearby.add(item));
      }
    }

    return Array.from(nearby);
  }
}

/**
 * Check if two rectangles collide
 */
export const checkCollision = (
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

/**
 * Check if a rectangle has any collisions with items
 * Uses spatial hash grid for optimization when many items exist
 */
export const hasCollisions = (
  testRect: { x: number; y: number; width: number; height: number },
  items: GridItemConfig[],
  excludeId?: string,
  spatialGrid?: SpatialHashGrid
): boolean => {
  // Use spatial grid if provided and there are many items
  const checkItems = spatialGrid && items.length > 10
    ? spatialGrid.getNearby(testRect.x, testRect.y, testRect.width, testRect.height)
    : items;

  for (const item of checkItems) {
    if (excludeId && item.id === excludeId) continue;

    const itemRect = {
      x: item.position.x,
      y: item.position.y,
      width: item.size.width,
      height: item.size.height,
    };

    if (checkCollision(testRect, itemRect)) {
      return true;
    }
  }

  return false;
};

/**
 * Debounce function for performance optimization
 */
export const debounce = <T extends (...args: any[]) => any>(
  func: T,
  wait: number
): ((...args: Parameters<T>) => void) => {
  let timeout: ReturnType<typeof setTimeout> | null = null;

  return (...args: Parameters<T>) => {
    if (timeout) clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
};

/**
 * Throttle function for performance optimization
 */
export const throttle = <T extends (...args: any[]) => any>(
  func: T,
  limit: number
): ((...args: Parameters<T>) => void) => {
  let inThrottle: boolean = false;

  return (...args: Parameters<T>) => {
    if (!inThrottle) {
      func(...args);
      inThrottle = true;
      setTimeout(() => (inThrottle = false), limit);
    }
  };
};
