import React, { useEffect, useRef, useState } from 'react';
import { X, Command, Layout, Settings, Copy, Check, Upload, Trash2, AlertCircle } from 'lucide-react';
import { getAllTemplates } from '../config/layoutTemplates';
import { clearAllCaches, getCacheStats } from '../hooks/useWebSocket';
import type { GridItemConfig } from '../types';

export type TabType = 'shortcuts' | 'layout' | 'settings';

interface HelpPanelProps {
  open: boolean;
  onClose: () => void;
  initialTab?: TabType;
  items?: GridItemConfig[];
  onImportLayout?: (items: GridItemConfig[]) => void;
  currentTemplateId?: string;
  onTemplateChange?: (templateId: string) => void;
  onFocusToFit?: () => void;
}

const shortcuts = [
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
      { keys: ['Ctrl', '/'], description: 'Toggle this help panel (Shortcuts)' },
      { keys: [','], description: 'Open Settings' },
      { keys: ['Shift', 'L'], description: 'Open Layout Templates' },
    ],
  },
];

const tabs: { id: TabType; label: string; icon: React.ElementType }[] = [
  { id: 'shortcuts', label: 'Shortcuts', icon: Command },
  { id: 'layout', label: 'Layouts', icon: Layout },
  { id: 'settings', label: 'Settings', icon: Settings },
];

export function HelpPanel({
  open,
  onClose,
  initialTab = 'shortcuts',
  items = [],
  onImportLayout,
  currentTemplateId,
  onTemplateChange,
  onFocusToFit,
}: HelpPanelProps) {
  const [activeTab, setActiveTab] = useState<TabType>(initialTab);
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open) {
      setActiveTab(initialTab);
    }
  }, [open, initialTab]);

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
      <div className="absolute inset-0 bg-black/60" />

      <div
        ref={panelRef}
        className="relative bg-zinc-900 border border-zinc-800 shadow-2xl overflow-hidden flex flex-col"
        style={{ width: 640, maxHeight: '85vh' }}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800 bg-zinc-900">
          <div className="flex items-center gap-0.5">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.id;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`flex items-center gap-2 px-3 py-1.5 text-[13px] font-medium transition-all ${
                    isActive
                      ? 'bg-zinc-800 text-zinc-100'
                      : 'text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800/50'
                  }`}
                >
                  <Icon className="w-3.5 h-3.5" />
                  {tab.label}
                </button>
              );
            })}
          </div>
          <button
            onClick={onClose}
            className="p-1 hover:bg-zinc-800 transition-colors"
          >
            <X className="w-4 h-4 text-zinc-500" />
          </button>
        </div>

        {/* Content */}
        <div className="overflow-auto" style={{ height: '600px' }}>
          {activeTab === 'shortcuts' && <ShortcutsTab />}
          {activeTab === 'layout' && (
            <LayoutTab
              currentTemplateId={currentTemplateId}
              onTemplateChange={onTemplateChange}
            />
          )}
          {activeTab === 'settings' && (
            <SettingsTab
              items={items}
              onImportLayout={onImportLayout}
              onFocusToFit={onFocusToFit}
            />
          )}
        </div>

        {/* Footer */}
        <div className="px-4 py-2.5 border-t border-zinc-800 flex justify-end bg-zinc-900">
          <span className="text-[11px] text-zinc-600 flex items-center gap-1">
            Press
            <kbd className="px-1.5 py-0.5 bg-zinc-800 text-zinc-400 text-[10px] font-mono">Esc</kbd>
            to close
          </span>
        </div>
      </div>
    </div>
  );
}

// Shortcuts Tab - Reference Style
function ShortcutsTab() {
  return (
    <div className="px-4 py-3">
      {shortcuts.map((group, groupIndex) => (
        <div key={group.category} className={`py-3 ${groupIndex !== 0 ? 'border-t border-zinc-800/30' : ''}`}>
          <div className="text-[10px] font-semibold uppercase tracking-widest text-zinc-600 mb-2">
            {group.category}
          </div>
          <div className="space-y-0">
            {group.items.map((item, i) => (
              <div key={i} className="flex items-center justify-between py-1.5">
                <span className="text-[13px] text-zinc-400">{item.description}</span>
                <div className="flex items-center gap-0.5 shrink-0 ml-3">
                  {item.keys.map((key, j) => (
                    <React.Fragment key={j}>
                      {j > 0 && <span className="text-zinc-700 text-[10px] mx-0.5">+</span>}
                      <kbd className="px-1.5 py-[1px] bg-zinc-800 border border-zinc-700 text-[11px] text-zinc-400 font-mono">
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
  );
}

// Layout Templates Tab - Unified List Style
interface LayoutTabProps {
  currentTemplateId?: string;
  onTemplateChange?: (templateId: string) => void;
}

function LayoutTab({ currentTemplateId, onTemplateChange }: LayoutTabProps) {
  const handleSelect = (templateId: string) => {
    onTemplateChange?.(templateId);
  };

  const templates = getAllTemplates();

  return (
    <div className="px-4 py-3">
      <div className="text-[10px] font-semibold uppercase tracking-widest text-zinc-600 mb-2">
        Available Templates
      </div>
      <div>
        {templates.map((template, index) => {
          const isActive = currentTemplateId === template.id;
          return (
            <button
              key={template.id}
              onClick={() => handleSelect(template.id)}
              className={`w-full flex items-start justify-between py-2.5 px-3 text-left transition-all border-t border-zinc-800/30 ${
                isActive
                  ? 'bg-zinc-800/60 text-zinc-200'
                  : 'hover:bg-zinc-800/30 text-zinc-400 hover:text-zinc-300'
              } ${index === 0 ? 'rounded-t' : ''} ${index === templates.length - 1 ? 'rounded-b' : ''}`}
            >
              <div className="flex-1 min-w-0">
                <div className={`text-[13px] font-medium flex items-center gap-2 ${isActive ? 'text-green-400' : 'text-zinc-300'}`}>
                  {template.name}
                </div>
                <div
                  className="text-[11px] mt-0.5 leading-relaxed"
                  style={{ color: isActive ? '#4ade80' : undefined }}
                >
                  {template.description}
                </div>
              </div>
              <div className="shrink-0 ml-3 mt-1">
                <div className={`w-2 h-2 rounded-full transition-colors ${
                  isActive ? 'bg-white' : 'bg-zinc-600'
                }`} />
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// Settings Tab - Unified Row Style
interface SettingsTabProps {
  items: GridItemConfig[];
  onImportLayout?: (items: GridItemConfig[]) => void;
  onFocusToFit?: () => void;
}

function SettingsTab({ items, onImportLayout, onFocusToFit }: SettingsTabProps) {
  const [copied, setCopied] = useState(false);
  const [importText, setImportText] = useState('');
  const [showImport, setShowImport] = useState(false);

  const handleExport = () => {
    const layoutConfig = JSON.stringify(items, null, 2);
    navigator.clipboard.writeText(layoutConfig);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleImport = () => {
    try {
      const parsed = JSON.parse(importText);
      if (Array.isArray(parsed)) {
        onImportLayout?.(parsed);
        setImportText('');
        setShowImport(false);
        setTimeout(() => {
          onFocusToFit?.();
        }, 50);
      } else {
        alert('Invalid format');
      }
    } catch (e) {
      alert('Invalid JSON');
    }
  };

  const handleClearStorage = () => {
    localStorage.removeItem('trading-system-layout');
    localStorage.removeItem('trading-system-gap');
    alert('Layout cleared');
  };

  const handleClearDataCache = () => {
    clearAllCaches();
    alert('Cache cleared');
  };

  const stats = getCacheStats();

  return (
    <div className="px-4 py-3 space-y-4">
      {/* Layout Section */}
      <div>
        <div className="text-[10px] font-semibold uppercase tracking-widest text-zinc-600 mb-2">
          Layout
        </div>
        <div className="divide-y divide-zinc-800/30 border-t border-zinc-800/30">
          {/* Export */}
          <div className="flex items-center justify-between py-2.5 px-3 hover:bg-zinc-800/30 transition-colors">
            <div>
              <div className="text-[13px] font-medium text-zinc-300">Export Layout</div>
              <div className="text-[11px] text-zinc-500 mt-1">
                Copy current layout as JSON
              </div>
            </div>
            <button
              onClick={handleExport}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-zinc-800 hover:bg-zinc-700 text-zinc-300 text-[12px] transition-colors"
            >
              {copied ? <Check className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
              {copied ? 'Copied' : 'Copy'}
            </button>
          </div>

          {/* Import Toggle */}
          <div className={`transition-colors ${showImport ? 'bg-zinc-800/30' : 'hover:bg-zinc-800/30'}`}>
            <div className="flex items-center justify-between py-2.5 px-3">
              <div>
                <div className="text-[13px] font-medium text-zinc-300">Import Layout</div>
                <div className="text-[11px] text-zinc-500 mt-1">
                  Restore layout from JSON
                </div>
              </div>
              <button
                onClick={() => setShowImport(!showImport)}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-zinc-800 hover:bg-zinc-700 text-zinc-300 text-[12px] transition-colors"
              >
                <Upload className="w-3.5 h-3.5" />
                {showImport ? 'Hide' : 'Show'}
              </button>
            </div>

            {showImport && (
              <div className="px-3 pb-3 space-y-2">
                <textarea
                  value={importText}
                  onChange={(e) => setImportText(e.target.value)}
                  placeholder="Paste JSON here..."
                  className="w-full bg-zinc-800/50 border border-zinc-700 p-2.5 text-[12px] font-mono text-zinc-300 placeholder:text-zinc-600 resize-none focus:outline-none focus:border-zinc-600"
                  style={{ height: '120px' }}
                />
                <div className="flex gap-2">
                  <button
                    onClick={handleImport}
                    disabled={!importText.trim()}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-green-600/20 hover:bg-green-600/30 disabled:opacity-50 disabled:hover:bg-green-600/20 text-green-400 text-[12px] transition-colors border border-green-600/30"
                  >
                    <Upload className="w-3.5 h-3.5" />
                    Import
                  </button>
                  <button
                    onClick={() => setImportText('')}
                    className="px-3 py-1.5 bg-zinc-800 hover:bg-zinc-700 text-zinc-400 text-[12px] transition-colors"
                  >
                    Clear
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Storage Section */}
      <div className="mt-6 pt-4 border-t border-zinc-800">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-zinc-600 mb-2">
          Storage & Cache
        </div>
        <div className="divide-y divide-zinc-800/30 border-t border-zinc-800/30">
          {/* Data Cache */}
          <div className="flex items-center justify-between py-2.5 px-3 hover:bg-zinc-800/30 transition-colors">
            <div>
              <div className="text-[13px] font-medium text-zinc-300">Data Cache</div>
              <div className="text-[11px] text-zinc-500 mt-1">
                {stats.profiles} profiles, {stats.news} articles
              </div>
            </div>
            <button
              onClick={handleClearDataCache}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-zinc-800 hover:bg-zinc-700 text-zinc-400 hover:text-zinc-300 text-[12px] transition-colors"
            >
              <Trash2 className="w-3.5 h-3.5" />
              Clear
            </button>
          </div>

          {/* Local Storage */}
          <div className="flex items-center justify-between py-2.5 px-3 hover:bg-zinc-800/30 transition-colors">
            <div>
              <div className="text-[13px] font-medium text-zinc-300">Saved Layout</div>
              <div className="text-[11px] text-zinc-500 mt-1">
                Clear browser storage
              </div>
            </div>
            <button
              onClick={handleClearStorage}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-red-600/10 hover:bg-red-600/20 text-red-400 text-[12px] transition-colors"
            >
              <AlertCircle className="w-3.5 h-3.5" />
              Clear
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
