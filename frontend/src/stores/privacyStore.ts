/**
 * Privacy Store — global toggle to mask all sensitive financial numbers.
 *
 * When enabled, monetary values (buying power, quantities, fills,
 * avg prices, commissions, P&L, account values, position sizes, etc.)
 * are replaced with "••••" across all modules.
 *
 * Turning privacy ON is free. Turning it OFF (revealing numbers)
 * requires a simple frontend PIN via a styled modal dialog.
 *
 * Persisted to localStorage.
 */

import { create } from 'zustand';

const STORAGE_KEY = 'gridtrader-privacy-mode';
const UNLOCK_PIN = (import.meta.env.VITE_PRIVACY_PIN ?? '1234').trim();

interface PrivacyState {
  privacyMode: boolean;
  /** Whether the PIN dialog is currently open */
  pinDialogOpen: boolean;
  /** Error message shown in the PIN dialog */
  pinError: string;
  /** Toggle privacy. Turning ON is instant. Turning OFF opens the PIN dialog. */
  toggle: () => void;
  /** Called from the PIN dialog to attempt unlock */
  attemptUnlock: (pin: string) => boolean;
  /** Close the PIN dialog without unlocking */
  closePinDialog: () => void;
  /** Mask a numeric / currency string when privacy is on */
  mask: (value: string | number | null | undefined, fallback?: string) => string;
}

export const usePrivacyStore = create<PrivacyState>()((set, get) => ({
  privacyMode: (() => {
    try {
      return localStorage.getItem(STORAGE_KEY) === 'true';
    } catch {
      return false;
    }
  })(),

  pinDialogOpen: false,
  pinError: '',

  toggle: () => {
    const current = get().privacyMode;

    // Turning ON is always allowed
    if (!current) {
      try { localStorage.setItem(STORAGE_KEY, 'true'); } catch {}
      set({ privacyMode: true });
      return;
    }

    // Turning OFF opens the PIN dialog
    set({ pinDialogOpen: true, pinError: '' });
  },

  attemptUnlock: (pin: string) => {
    if (pin !== UNLOCK_PIN) {
      set({ pinError: 'Incorrect PIN' });
      return false;
    }
    try { localStorage.setItem(STORAGE_KEY, 'false'); } catch {}
    set({ privacyMode: false, pinDialogOpen: false, pinError: '' });
    return true;
  },

  closePinDialog: () => {
    set({ pinDialogOpen: false, pinError: '' });
  },

  mask: (value, fallback = '—') => {
    if (get().privacyMode) return '••••';
    if (value == null) return fallback;
    return String(value);
  },
}));
