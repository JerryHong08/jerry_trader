import { create } from 'zustand';
import type { GridItemConfig } from '../types';

const MAX_HISTORY = 50;

interface LayoutState {
  items: GridItemConfig[];
  history: GridItemConfig[][];
  historyIndex: number;

  // Actions
  setItems: (items: GridItemConfig[]) => void;
  addItem: (item: GridItemConfig) => void;
  removeItem: (id: string) => void;
  updateItem: (id: string, updates: Partial<GridItemConfig>) => void;

  // History actions
  undo: () => void;
  redo: () => void;
  canUndo: () => boolean;
  canRedo: () => boolean;

  // Utility
  clearHistory: () => void;
  loadFromStorage: () => void;
  saveToStorage: () => void;
}

export const useLayoutStore = create<LayoutState>((set, get) => ({
  items: [],
  history: [],
  historyIndex: -1,

  setItems: (items) => {
    const state = get();
    const newHistory = [...state.history.slice(0, state.historyIndex + 1), items];

    // Limit history size
    const trimmedHistory = newHistory.length > MAX_HISTORY
      ? newHistory.slice(newHistory.length - MAX_HISTORY)
      : newHistory;

    set({
      items,
      history: trimmedHistory,
      historyIndex: trimmedHistory.length - 1,
    });

    // Auto-save to localStorage
    get().saveToStorage();
  },

  addItem: (item) => {
    const state = get();
    const newItems = [...state.items, item];
    get().setItems(newItems);
  },

  removeItem: (id) => {
    const state = get();
    const newItems = state.items.filter(item => item.id !== id);
    get().setItems(newItems);
  },

  updateItem: (id, updates) => {
    const state = get();
    const newItems = state.items.map(item =>
      item.id === id ? { ...item, ...updates } : item
    );
    get().setItems(newItems);
  },

  undo: () => {
    const state = get();
    if (state.historyIndex > 0) {
      const newIndex = state.historyIndex - 1;
      set({
        items: state.history[newIndex],
        historyIndex: newIndex,
      });
      get().saveToStorage();
    }
  },

  redo: () => {
    const state = get();
    if (state.historyIndex < state.history.length - 1) {
      const newIndex = state.historyIndex + 1;
      set({
        items: state.history[newIndex],
        historyIndex: newIndex,
      });
      get().saveToStorage();
    }
  },

  canUndo: () => {
    const state = get();
    return state.historyIndex > 0;
  },

  canRedo: () => {
    const state = get();
    return state.historyIndex < state.history.length - 1;
  },

  clearHistory: () => {
    const state = get();
    set({
      history: [state.items],
      historyIndex: 0,
    });
  },

  loadFromStorage: () => {
    try {
      const saved = localStorage.getItem('trading-system-layout');
      if (saved) {
        const items = JSON.parse(saved);
        set({
          items,
          history: [items],
          historyIndex: 0,
        });
      }
    } catch (e) {
      console.error('Failed to load layout from storage:', e);
    }
  },

  saveToStorage: () => {
    try {
      const state = get();
      localStorage.setItem('trading-system-layout', JSON.stringify(state.items));
    } catch (e) {
      console.error('Failed to save layout to storage:', e);
    }
  },
}));
