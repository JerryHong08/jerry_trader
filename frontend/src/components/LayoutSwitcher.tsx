import React, { useState, useEffect, useRef } from 'react';
import { Layout, Check, X } from 'lucide-react';
import { getAllTemplates } from '../config/layoutTemplates';

interface LayoutSwitcherProps {
  currentTemplateId?: string;
  onTemplateChange: (templateId: string) => void;
  isOpen?: boolean;
  onOpenChange?: (open: boolean) => void;
}

export function LayoutSwitcher({ currentTemplateId, onTemplateChange, isOpen: controlledIsOpen, onOpenChange }: LayoutSwitcherProps) {
  const [internalIsOpen, setInternalIsOpen] = useState(false);

  // Support both controlled and uncontrolled modes
  const isOpen = controlledIsOpen !== undefined ? controlledIsOpen : internalIsOpen;
  const setIsOpen = (open: boolean) => {
    setInternalIsOpen(open);
    onOpenChange?.(open);
  };
  const panelRef = useRef<HTMLDivElement>(null);

  // Close panel when clicking outside
  useEffect(() => {
    if (!isOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    const timer = setTimeout(() => document.addEventListener('mousedown', handleClick), 0);
    return () => {
      clearTimeout(timer);
      document.removeEventListener('mousedown', handleClick);
    };
  }, [isOpen]);

  // Close on Escape key
  useEffect(() => {
    if (!isOpen) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.code === 'Escape') {
        e.preventDefault();
        setIsOpen(false);
      }
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [isOpen]);

  const handleTemplateSelect = (templateId: string) => {
    onTemplateChange(templateId);
    setIsOpen(false);
  };

  return (
    <div className="relative">
      {/* Toggle Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className={`p-2 transition-colors ${
          isOpen ? 'bg-zinc-700 text-white' : 'hover:bg-zinc-800 text-zinc-400 hover:text-white'
        }`}
        title="Layout Templates (Shift+L)"
      >
        <Layout className="w-5 h-5" />
      </button>

      {/* Dropdown Panel */}
      {isOpen && (
        <div
          ref={panelRef}
          className="absolute right-0 top-full mt-2 w-72 bg-zinc-900 border border-zinc-700 shadow-xl z-[9999]"
        >
          {/* Header */}
          <div className="flex items-center justify-between px-3 py-2 border-b border-zinc-800 bg-zinc-800/50">
            <div className="flex items-center gap-2">
              <Layout className="w-4 h-4 text-zinc-400" />
              <span className="text-sm font-medium text-zinc-200">Layout Templates</span>
            </div>
            <button
              onClick={() => setIsOpen(false)}
              className="p-0.5 hover:bg-zinc-800 rounded transition-colors"
            >
              <X className="w-3.5 h-3.5 text-zinc-500" />
            </button>
          </div>

          {/* Template List */}
          <div className="p-2 max-h-[60vh] overflow-auto">
            <div className="space-y-1">
              {getAllTemplates().map(template => (
                <button
                  key={template.id}
                  onClick={() => handleTemplateSelect(template.id)}
                  className={`w-full text-left p-2.5 border transition-colors ${
                    currentTemplateId === template.id
                      ? 'bg-blue-600/20 border-blue-500/50'
                      : 'bg-zinc-800/50 border-zinc-800 hover:bg-zinc-800 hover:border-zinc-700'
                  }`}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex-1 min-w-0">
                      <div className={`text-sm mb-0.5 ${
                        currentTemplateId === template.id ? 'text-blue-400' : 'text-zinc-300'
                      }`}>
                        {template.name}
                      </div>
                      <div className="text-xs text-zinc-500 truncate">{template.description}</div>
                    </div>
                    {currentTemplateId === template.id && (
                      <Check className="w-4 h-4 text-blue-400 shrink-0 mt-0.5" />
                    )}
                  </div>
                </button>
              ))}
            </div>
          </div>

          {/* Footer */}
          <div className="px-3 py-2 border-t border-zinc-800 bg-zinc-800/30">
            <div className="flex items-center justify-between text-xs text-zinc-500">
              <span>Current layout will be replaced</span>
              <kbd className="px-1.5 py-0.5 bg-zinc-900 border border-zinc-700 rounded text-[10px] text-zinc-400 font-mono">
                Esc
              </kbd>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
