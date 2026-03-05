/**
 * PinDialog — centered modal for unlocking privacy mode.
 *
 * Renders as a dark overlay with a zinc-styled dialog in the center.
 * Matches the GridTrader design language (bg-zinc-900, border-zinc-700, etc.).
 */

import React, { useState, useEffect, useRef } from 'react';
import { Lock, X } from 'lucide-react';
import { usePrivacyStore } from '../stores/privacyStore';

export function PinDialog() {
  const open = usePrivacyStore((s) => s.pinDialogOpen);
  const pinError = usePrivacyStore((s) => s.pinError);
  const attemptUnlock = usePrivacyStore((s) => s.attemptUnlock);
  const closePinDialog = usePrivacyStore((s) => s.closePinDialog);

  const [pin, setPin] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  // Focus input and reset when dialog opens
  useEffect(() => {
    if (open) {
      setPin('');
      setTimeout(() => inputRef.current?.focus(), 50);
    }
  }, [open]);

  if (!open) return null;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    attemptUnlock(pin.trim());
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') closePinDialog();
  };

  return (
    <div
      className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/60 backdrop-blur-sm"
      onClick={closePinDialog}
      onKeyDown={handleKeyDown}
    >
      <div
        className="bg-zinc-900 border border-zinc-700 shadow-2xl shadow-black/50 w-72 rounded-lg overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800">
          <div className="flex items-center gap-2 text-sm text-gray-300">
            <Lock className="w-4 h-4 text-yellow-500" />
            Unlock Privacy
          </div>
          <button
            onClick={closePinDialog}
            className="p-0.5 hover:bg-zinc-700 rounded transition-colors text-gray-500 hover:text-gray-300"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Body */}
        <form onSubmit={handleSubmit} className="p-4 space-y-3">
          <div>
            <label className="block text-xs text-gray-400 mb-1">Enter PIN to reveal numbers</label>
            <input
              ref={inputRef}
              type="password"
              value={pin}
              onChange={(e) => setPin(e.target.value)}
              className="w-full bg-black border border-zinc-700 px-3 py-2 text-sm text-center tracking-[0.3em]
                focus:outline-none focus:border-zinc-500 transition-colors"
              placeholder="••••"
              autoComplete="off"
            />
          </div>

          {pinError && (
            <div className="text-xs text-red-400 text-center">{pinError}</div>
          )}

          <div className="flex gap-2">
            <button
              type="button"
              onClick={closePinDialog}
              className="flex-1 py-2 text-xs bg-zinc-800 hover:bg-zinc-700 text-gray-400 transition-colors rounded"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="flex-1 py-2 text-xs bg-yellow-600 hover:bg-yellow-500 text-white transition-colors rounded"
            >
              Unlock
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
