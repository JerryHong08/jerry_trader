import { describe, it, expect, vi } from 'vitest';
import {
  getScaleFactor,
  getCurrentBreakpoint,
  scalePosition,
  scaleSize,
  scaleLayout,
  SpatialHashGrid,
  checkCollision,
  hasCollisions,
  debounce,
  throttle,
} from '../../utils/layoutUtils';
import type { GridItemConfig } from '../../types';

// ── Helpers ──────────────────────────────────────────────────────────────────

function makeItem(id: string, x: number, y: number, w = 200, h = 150): GridItemConfig {
  return {
    id,
    moduleType: 'chart',
    position: { x, y },
    size: { width: w, height: h },
    syncGroup: null,
  };
}

// ── Scale & Breakpoint ──────────────────────────────────────────────────────

describe('getScaleFactor', () => {
  it('returns 1.0 at base width 1920', () => {
    expect(getScaleFactor(1920)).toBe(1.0);
  });

  it('clamps to min 0.5', () => {
    expect(getScaleFactor(100)).toBe(0.5);
  });

  it('clamps to max 2.0', () => {
    expect(getScaleFactor(5000)).toBe(2.0);
  });
});

describe('getCurrentBreakpoint', () => {
  it('returns sm for smallest widths', () => {
    expect(getCurrentBreakpoint(100)).toBe('sm');
    expect(getCurrentBreakpoint(639)).toBe('sm');
  });

  it('returns md at 1024', () => {
    expect(getCurrentBreakpoint(1024)).toBe('md');
  });

  it('returns xl at 1920', () => {
    expect(getCurrentBreakpoint(1920)).toBe('xl');
  });

  it('returns xxl at 2560', () => {
    expect(getCurrentBreakpoint(2560)).toBe('xxl');
  });
});

describe('scalePosition', () => {
  it('scales both coordinates', () => {
    expect(scalePosition({ x: 100, y: 200 }, 1.5)).toEqual({ x: 150, y: 300 });
  });

  it('rounds to integers', () => {
    expect(scalePosition({ x: 10, y: 10 }, 0.33)).toEqual({ x: 3, y: 3 });
  });
});

describe('scaleSize', () => {
  it('scales width and height', () => {
    expect(scaleSize({ width: 200, height: 100 }, 1.5)).toEqual({ width: 300, height: 200 });
  });

  it('clamps to minimums', () => {
    expect(scaleSize({ width: 10, height: 10 }, 1.0)).toEqual({ width: 250, height: 200 });
  });
});

describe('scaleLayout', () => {
  it('scales all items in a layout', () => {
    const items = [makeItem('a', 100, 200, 300, 400)];
    const result = scaleLayout(items, 2.0);
    expect(result[0].position).toEqual({ x: 200, y: 400 });
    expect(result[0].size).toEqual({ width: 600, height: 800 });
  });
});

// ── SpatialHashGrid ─────────────────────────────────────────────────────────

describe('SpatialHashGrid', () => {
  it('builds grid and returns nearby items', () => {
    const grid = new SpatialHashGrid(200);
    const items = [
      makeItem('a', 0, 0, 100, 100),
      makeItem('b', 50, 50, 100, 100),  // overlaps 'a'
      makeItem('c', 500, 500, 100, 100), // far away
    ];
    grid.build(items);

    // Query around item 'a' — should find both 'a' and 'b', but not 'c'
    const nearby = grid.getNearby(0, 0, 100, 100);
    const ids = nearby.map(i => i.id).sort();
    expect(ids).toEqual(['a', 'b']);
  });

  it('returns empty for empty grid', () => {
    const grid = new SpatialHashGrid();
    expect(grid.getNearby(0, 0, 100, 100)).toEqual([]);
  });

  it('handles item spanning multiple cells', () => {
    const grid = new SpatialHashGrid(50);
    // 200x200 item at (0,0) spans 4x4=16 cells with cellSize=50
    const items = [makeItem('big', 0, 0, 200, 200)];
    grid.build(items);

    // Query each corner cell — all should contain the big item
    expect(grid.getNearby(0, 0, 1, 1).map(i => i.id)).toEqual(['big']);
    expect(grid.getNearby(190, 190, 1, 1).map(i => i.id)).toEqual(['big']);
  });
});

// ── Collision Detection ─────────────────────────────────────────────────────

describe('checkCollision', () => {
  const r = (x: number, y: number, w = 100, h = 100) => ({ x, y, width: w, height: h });

  it('detects overlapping rectangles', () => {
    expect(checkCollision(r(0, 0), r(50, 50))).toBe(true);
  });

  it('detects contained rectangle', () => {
    expect(checkCollision(r(0, 0, 200, 200), r(50, 50, 50, 50))).toBe(true);
  });

  it('returns false for non-overlapping', () => {
    expect(checkCollision(r(0, 0), r(200, 200))).toBe(false);
  });

  it('returns false for edge-adjacent (not overlapping)', () => {
    // rect1 ends at x=100, rect2 starts at x=100
    expect(checkCollision(r(0, 0, 100, 100), r(100, 0, 100, 100))).toBe(false);
  });
});

describe('hasCollisions', () => {
  const r = (x: number, y: number, w = 100, h = 100) => ({ x, y, width: w, height: h });

  it('returns true when collision exists', () => {
    const items = [makeItem('a', 0, 0, 100, 100)];
    expect(hasCollisions(r(50, 50, 100, 100), items)).toBe(true);
  });

  it('returns false when no collision', () => {
    const items = [makeItem('a', 0, 0, 100, 100)];
    expect(hasCollisions(r(500, 500, 100, 100), items)).toBe(false);
  });

  it('excludes item by ID', () => {
    const items = [makeItem('self', 0, 0, 100, 100)];
    // The test rect IS the item — should be no collision when excluding itself
    expect(hasCollisions(r(0, 0, 100, 100), items, 'self')).toBe(false);
  });

  it('uses spatial grid when >10 items', () => {
    const items = Array.from({ length: 15 }, (_, i) =>
      makeItem(`item-${i}`, i * 10, i * 10, 5, 5)
    );
    const grid = new SpatialHashGrid(200);
    grid.build(items);

    // Query far from all items — no collision, but uses spatial grid path
    expect(hasCollisions(r(5000, 5000, 10, 10), items, undefined, grid)).toBe(false);
  });
});

// ── Utilities ───────────────────────────────────────────────────────────────

describe('debounce', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('delays execution', () => {
    const fn = vi.fn();
    const debounced = debounce(fn, 300);

    debounced();
    expect(fn).not.toHaveBeenCalled();

    vi.advanceTimersByTime(299);
    expect(fn).not.toHaveBeenCalled();

    vi.advanceTimersByTime(1);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('resets timer on repeated calls', () => {
    const fn = vi.fn();
    const debounced = debounce(fn, 300);

    debounced();
    vi.advanceTimersByTime(200);
    debounced();
    vi.advanceTimersByTime(200);
    expect(fn).not.toHaveBeenCalled();

    vi.advanceTimersByTime(100);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('passes arguments to wrapped function', () => {
    const fn = vi.fn();
    const debounced = debounce(fn, 100);

    debounced('hello', 42);
    vi.advanceTimersByTime(100);

    expect(fn).toHaveBeenCalledWith('hello', 42);
  });
});

describe('throttle', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('executes immediately on first call', () => {
    const fn = vi.fn();
    const throttled = throttle(fn, 300);

    throttled();
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('ignores calls within limit window', () => {
    const fn = vi.fn();
    const throttled = throttle(fn, 300);

    throttled();
    throttled();
    throttled();
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('allows calls after limit window', () => {
    const fn = vi.fn();
    const throttled = throttle(fn, 300);

    throttled();
    expect(fn).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(300);
    throttled();
    expect(fn).toHaveBeenCalledTimes(2);
  });
});
