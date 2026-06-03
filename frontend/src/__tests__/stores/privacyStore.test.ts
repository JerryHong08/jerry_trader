import { describe, it, expect, beforeEach, vi } from 'vitest';

// Mock localStorage before any import that reads it
const storage = new Map<string, string>();
vi.stubGlobal('localStorage', {
  getItem: (key: string) => storage.get(key) ?? null,
  setItem: (key: string, value: string) => { storage.set(key, value); },
  removeItem: (key: string) => { storage.delete(key); },
});

// Mock import.meta.env at module level
vi.stubGlobal('import', { meta: { env: { VITE_PRIVACY_PIN: '0808' } } });

import { usePrivacyStore } from '../../stores/privacyStore';

describe('privacyStore', () => {
  beforeEach(() => {
    storage.clear();
    usePrivacyStore.setState({ privacyMode: false, pinDialogOpen: false, pinError: '' });
  });

  describe('toggle', () => {
    it('turns privacy ON instantly', () => {
      const store = usePrivacyStore.getState();
      store.toggle();
      expect(usePrivacyStore.getState().privacyMode).toBe(true);
      expect(storage.get('JerryTrader-privacy-mode')).toBe('true');
    });

    it('opens PIN dialog when turning OFF', () => {
      usePrivacyStore.setState({ privacyMode: true });
      usePrivacyStore.getState().toggle();
      const state = usePrivacyStore.getState();
      expect(state.privacyMode).toBe(true); // still on until PIN entered
      expect(state.pinDialogOpen).toBe(true);
      expect(state.pinError).toBe('');
    });
  });

  describe('attemptUnlock', () => {
    it('unlocks with correct PIN', () => {
      usePrivacyStore.setState({ privacyMode: true, pinDialogOpen: true });
      const result = usePrivacyStore.getState().attemptUnlock('0808');
      expect(result).toBe(true);
      const state = usePrivacyStore.getState();
      expect(state.privacyMode).toBe(false);
      expect(state.pinDialogOpen).toBe(false);
    });

    it('rejects wrong PIN', () => {
      usePrivacyStore.setState({ privacyMode: true, pinDialogOpen: true });
      const result = usePrivacyStore.getState().attemptUnlock('1234');
      expect(result).toBe(false);
      const state = usePrivacyStore.getState();
      expect(state.pinDialogOpen).toBe(true);
      expect(state.pinError).toBe('Incorrect PIN');
    });
  });

  describe('closePinDialog', () => {
    it('closes dialog without changing privacy mode', () => {
      usePrivacyStore.setState({ privacyMode: true, pinDialogOpen: true, pinError: 'x' });
      usePrivacyStore.getState().closePinDialog();
      const state = usePrivacyStore.getState();
      expect(state.pinDialogOpen).toBe(false);
      expect(state.pinError).toBe('');
      expect(state.privacyMode).toBe(true);
    });
  });

  describe('mask', () => {
    it('returns dots when privacy is on', () => {
      usePrivacyStore.setState({ privacyMode: true });
      expect(usePrivacyStore.getState().mask('100.50')).toBe('••••');
      expect(usePrivacyStore.getState().mask(0)).toBe('••••');
    });

    it('returns string value when privacy is off', () => {
      usePrivacyStore.setState({ privacyMode: false });
      expect(usePrivacyStore.getState().mask('100.50')).toBe('100.50');
      expect(usePrivacyStore.getState().mask(0)).toBe('0');
    });

    it('returns fallback for null/undefined when privacy is off', () => {
      usePrivacyStore.setState({ privacyMode: false });
      expect(usePrivacyStore.getState().mask(null, 'N/A')).toBe('N/A');
      expect(usePrivacyStore.getState().mask(undefined)).toBe('—');
    });
  });
});
