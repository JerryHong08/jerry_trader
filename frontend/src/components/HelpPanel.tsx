import React, { useEffect, useRef } from 'react';
import { X } from 'lucide-react';

interface HelpPanelProps {
  open: boolean;
  onClose: () => void;
}

const shortcuts: { category: string; items: { keys: string[]; description: string }[] }[] = [
  {
    category: 'Navigation',
    items: [
      { keys: ['Scroll'], description: 'Pan the canvas' },
      { keys: ['Ctrl', 'Scroll'], description: 'Zoom in / out toward cursor' },
      { keys: ['Middle-click', 'Drag'], description: 'Pan the canvas' },
      { keys: ['L'], description: 'Lock zoom' },
      { keys: ['Alt', 'L'], description: 'Unlock zoom' },
    ],
  },
  {
    category: 'Selection & Focus',
    items: [
      { keys: ['Drag canvas'], description: 'Selection rectangle → focus-to-fit' },
      { keys: ['Click grid'], description: 'Focus grid (scroll controls its content)' },
      { keys: ['Click canvas'], description: 'Unfocus grid (scroll returns to pan)' },
      { keys: ['F'], description: 'Focus all items to viewport' },
    ],
  },
  {
    category: 'Layout',
    items: [
      { keys: ['Ctrl', 'Z'], description: 'Undo' },
      { keys: ['Ctrl', 'Y'], description: 'Redo' },
    ],
  },
  {
    category: 'General',
    items: [
      { keys: ['Ctrl', '/'], description: 'Toggle this help panel' },
    ],
  },
];

export function HelpPanel({ open, onClose }: HelpPanelProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.code === 'Escape') {
        e.preventDefault();
        onClose();
      }
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [open, onClose]);

  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    const timer = setTimeout(() => document.addEventListener('mousedown', handleClick), 0);
    return () => {
      clearTimeout(timer);
      document.removeEventListener('mousedown', handleClick);
    };
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[9999] flex items-center justify-center">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60" />

      {/* Panel */}
      <div
        ref={panelRef}
        className="relative bg-zinc-900 border-2 border-zinc-700 shadow-xl max-h-[70vh] overflow-auto"
        style={{ width: 560 }}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800 bg-zinc-800/50">
          <span className="text-sm font-medium text-zinc-200">Shortcuts</span>
          <button
            onClick={onClose}
            className="p-0.5 hover:bg-zinc-800 rounded transition-colors"
          >
            <X className="w-4 h-4 text-zinc-500" />
          </button>
        </div>

        {/* Content */}
        <div className="px-4 py-3 divide-y divide-zinc-800">
          {shortcuts.map((group) => (
            <div key={group.category} className="py-3 first:pt-0 last:pb-0">
              <div className="text-[10px] font-semibold uppercase tracking-widest text-zinc-600 mb-1.5">
                {group.category}
              </div>
              <div className="space-y-0">
                {group.items.map((item, i) => (
                  <div
                    key={i}
                    className="flex items-center justify-between py-1.5"
                  >
                    <span className="text-[13px] text-zinc-400">{item.description}</span>
                    <div className="flex items-center gap-0.5 shrink-0 ml-3">
                      {item.keys.map((key, j) => (
                        <React.Fragment key={j}>
                          {j > 0 && <span className="text-zinc-700 text-[10px] mx-0.5">+</span>}
                          <kbd className="px-1.5 py-[1px] bg-zinc-900 border border-zinc-700 rounded-[3px] text-[11px] text-zinc-400 font-mono leading-relaxed">
                            {key}
                          </kbd>
                        </React.Fragment>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className="px-4 py-2 border-t border-zinc-800 flex justify-end">
          <span className="text-[11px] text-zinc-600">
            <kbd className="px-1 py-[1px] bg-zinc-900 border border-zinc-700 rounded-[3px] text-[10px] text-zinc-400 font-mono">Esc</kbd>
            <span className="ml-1">to close</span>
          </span>
        </div>
      </div>
    </div>
  );
}
