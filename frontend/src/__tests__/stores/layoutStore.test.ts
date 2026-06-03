import { describe, it, expect, beforeEach, vi } from 'vitest';

// Mock localStorage
const storage = new Map<string, string>();
vi.stubGlobal('localStorage', {
  getItem: (key: string) => storage.get(key) ?? null,
  setItem: (key: string, value: string) => { storage.set(key, value); },
  removeItem: (key: string) => { storage.delete(key); },
});

import { useLayoutStore } from '../../stores/layoutStore';
import type { GridItemConfig } from '../../types';

function makeItem(id: string): GridItemConfig {
  return {
    id,
    moduleType: 'chart',
    position: { x: 0, y: 0 },
    size: { width: 600, height: 500 },
    syncGroup: null,
  };
}

describe('layoutStore', () => {
  beforeEach(() => {
    storage.clear();
    useLayoutStore.setState({ items: [], history: [], historyIndex: -1 });
  });

  describe('addItem / removeItem', () => {
    it('adds an item', () => {
      useLayoutStore.getState().addItem(makeItem('a1'));
      expect(useLayoutStore.getState().items).toHaveLength(1);
      expect(useLayoutStore.getState().items[0].id).toBe('a1');
    });

    it('removes an item', () => {
      const store = useLayoutStore.getState();
      store.addItem(makeItem('a1'));
      store.addItem(makeItem('a2'));
      store.removeItem('a1');
      expect(useLayoutStore.getState().items).toHaveLength(1);
      expect(useLayoutStore.getState().items[0].id).toBe('a2');
    });
  });

  describe('updateItem', () => {
    it('partial-updates an item', () => {
      const store = useLayoutStore.getState();
      store.addItem(makeItem('a1'));
      store.updateItem('a1', { position: { x: 100, y: 200 } });
      expect(useLayoutStore.getState().items[0].position).toEqual({ x: 100, y: 200 });
    });
  });

  describe('undo / redo', () => {
    it('undoes last change', () => {
      useLayoutStore.getState().addItem(makeItem('a1'));
      useLayoutStore.getState().addItem(makeItem('a2'));
      expect(useLayoutStore.getState().items).toHaveLength(2);

      useLayoutStore.getState().undo();
      expect(useLayoutStore.getState().items).toHaveLength(1);
      expect(useLayoutStore.getState().canUndo()).toBe(false); // only 1 entry left
    });

    it('cannot undo with empty or single-state history', () => {
      expect(useLayoutStore.getState().canUndo()).toBe(false); // empty
      useLayoutStore.getState().addItem(makeItem('a1'));
      expect(useLayoutStore.getState().canUndo()).toBe(false); // only 1 state
    });

    it('redoes after undo', () => {
      useLayoutStore.getState().addItem(makeItem('a1'));
      useLayoutStore.getState().addItem(makeItem('a2'));
      useLayoutStore.getState().undo();
      expect(useLayoutStore.getState().items).toHaveLength(1);
      expect(useLayoutStore.getState().canRedo()).toBe(true);

      useLayoutStore.getState().redo();
      expect(useLayoutStore.getState().items).toHaveLength(2);
    });
  });

  describe('persistence', () => {
    it('saves to localStorage on changes', () => {
      useLayoutStore.getState().addItem(makeItem('a1'));
      const saved = storage.get('trading-system-layout');
      expect(saved).toBeTruthy();
      expect(JSON.parse(saved!)).toHaveLength(1);
    });

    it('loads from localStorage', () => {
      const items = [makeItem('saved')];
      storage.set('trading-system-layout', JSON.stringify(items));

      useLayoutStore.getState().loadFromStorage();
      expect(useLayoutStore.getState().items).toHaveLength(1);
      expect(useLayoutStore.getState().items[0].id).toBe('saved');
    });

    it('handles empty localStorage gracefully', () => {
      useLayoutStore.getState().loadFromStorage();
      expect(useLayoutStore.getState().items).toEqual([]);
    });
  });
});
