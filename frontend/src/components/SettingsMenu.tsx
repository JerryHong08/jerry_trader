import React, { useState } from 'react';
import { Settings, X, Copy, Check, Upload, Download, Layout, Trash2 } from 'lucide-react';
import { getAllTemplates } from '../config/layoutTemplates';
import { clearAllCaches, getCacheStats } from '../hooks/useWebSocket';
import type { GridItemConfig } from '../types';

interface SettingsMenuProps {
  gridGap: number;
  onGridGapChange: (gap: number) => void;
  items: GridItemConfig[];
  onImportLayout: (items: GridItemConfig[]) => void;
  currentTemplateId?: string;
  onTemplateChange: (templateId: string) => void;
}

export function SettingsMenu({ gridGap, onGridGapChange, items, onImportLayout, currentTemplateId, onTemplateChange }: SettingsMenuProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const [importText, setImportText] = useState('');

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
        onImportLayout(parsed);
        setImportText('');
        alert('Layout imported successfully!');
      } else {
        alert('Invalid layout format. Must be an array.');
      }
    } catch (e) {
      alert('Invalid JSON format. Please check your input.');
    }
  };

  const handleClearStorage = () => {
    localStorage.removeItem('trading-system-layout');
    localStorage.removeItem('trading-system-gap');
    alert('Layout cleared from browser storage.');
    // if (confirm('Are you sure you want to clear saved layout?')) {
    // }
  };

  const handleClearDataCache = () => {
    const stats = getCacheStats();
    clearAllCaches();
    alert('Data cache cleared successfully.');
    // if (confirm(`Clear all cached data?\n\nThis will remove:\n- ${stats.profiles} cached profiles\n- ${stats.news} cached news articles\n\nData will be re-fetched from the server.`)) {
    // }
  };

  if (!isOpen) {
    return (
      <button
        onClick={() => setIsOpen(true)}
        className="p-2 hover:bg-zinc-800 transition-colors"
        title="Settings"
      >
        <Settings className="w-5 h-5" />
      </button>
    );
  }

  return (
    <div className="fixed inset-0 bg-black/50 z-[9999] flex items-center justify-center">
      <div className="bg-zinc-900 border border-zinc-700 w-[600px] max-h-[80vh] overflow-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-zinc-800">
          <div className="flex items-center gap-2">
            <Settings className="w-5 h-5" />
            <h2 className="text-lg">Settings</h2>
          </div>
          <button
            onClick={() => setIsOpen(false)}
            className="p-1 hover:bg-zinc-800 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-4 space-y-6">
          {/* Grid Gap Control */}
          <div>
            <label className="block text-sm mb-2">
              Grid Gap: <span className="text-blue-400">{gridGap}px</span>
            </label>
            <input
              type="range"
              min="0"
              max="50"
              step="5"
              value={gridGap}
              onChange={(e) => onGridGapChange(Number(e.target.value))}
              className="w-full"
            />
            <div className="flex justify-between text-xs text-gray-500 mt-1">
              <span>0px (Tight)</span>
              <span>50px (Spacious)</span>
            </div>
          </div>

          {/* Export Layout */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-sm">Export Layout</label>
              <button
                onClick={handleExport}
                className="flex items-center gap-2 px-3 py-1 bg-blue-600 hover:bg-blue-700 transition-colors text-sm"
              >
                {copied ? (
                  <>
                    <Check className="w-4 h-4" />
                    Copied!
                  </>
                ) : (
                  <>
                    <Copy className="w-4 h-4" />
                    Copy to Clipboard
                  </>
                )}
              </button>
            </div>
            <textarea
              readOnly
              value={JSON.stringify(items, null, 2)}
              className="w-full h-32 bg-zinc-800 border border-zinc-700 p-2 text-xs font-mono"
              onClick={(e) => e.currentTarget.select()}
            />
            <p className="text-xs text-gray-500 mt-1">
              Copy this configuration to save your layout. You can paste it in the import section below or set it as default in the code.
            </p>
          </div>

          {/* Import Layout */}
          <div>
            <label className="block text-sm mb-2">Import Layout</label>
            <textarea
              value={importText}
              onChange={(e) => setImportText(e.target.value)}
              placeholder="Paste layout configuration JSON here..."
              className="w-full h-32 bg-zinc-800 border border-zinc-700 p-2 text-xs font-mono"
            />
            <div className="flex gap-2 mt-2">
              <button
                onClick={handleImport}
                className="flex items-center gap-2 px-3 py-1 bg-green-600 hover:bg-green-700 transition-colors text-sm"
                disabled={!importText.trim()}
              >
                <Upload className="w-4 h-4" />
                Import
              </button>
              <button
                onClick={() => setImportText('')}
                className="px-3 py-1 bg-zinc-700 hover:bg-zinc-600 transition-colors text-sm"
              >
                Clear
              </button>
            </div>
          </div>

          {/* Layout Templates */}
          <div className="border-t border-zinc-800 pt-4">
            <div className="flex items-center gap-2 mb-3">
              <Layout className="w-4 h-4" />
              <label className="text-sm">Layout Templates</label>
            </div>
            <div className="space-y-2">
              {getAllTemplates().map(template => (
                <button
                  key={template.id}
                  onClick={() => {
                    onTemplateChange(template.id);
                    setIsOpen(false);
                    // if (confirm(`Load "${template.name}" template? This will replace your current layout.`)) {
                    // }
                  }}
                  className={`w-full text-left p-3 border transition-colors ${
                    currentTemplateId === template.id
                      ? 'bg-blue-600 border-blue-500'
                      : 'bg-zinc-800 border-zinc-700 hover:bg-zinc-700'
                  }`}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex-1">
                      <div className="text-sm mb-1">{template.name}</div>
                      <div className="text-xs text-gray-400">{template.description}</div>
                    </div>
                    {currentTemplateId === template.id && (
                      <Check className="w-4 h-4 text-white shrink-0" />
                    )}
                  </div>
                </button>
              ))}
            </div>
            <p className="text-xs text-gray-500 mt-2">
              Select a template to instantly apply a predefined layout. Your current layout will be replaced.
            </p>
          </div>

          {/* Data Cache Management */}
          <div className="border-t border-zinc-800 pt-4">
            <div className="flex items-center justify-between mb-2">
              <div>
                <label className="text-sm">Data Cache</label>
                <p className="text-xs text-gray-500 mt-1">
                  {(() => {
                    const stats = getCacheStats();
                    return `${stats.profiles} profiles, ${stats.news} news articles cached`;
                  })()}
                </p>
              </div>
              <button
                onClick={handleClearDataCache}
                className="flex items-center gap-2 px-3 py-1 bg-orange-600 hover:bg-orange-700 transition-colors text-sm"
              >
                <Trash2 className="w-4 h-4" />
                Clear Data Cache
              </button>
            </div>
            <p className="text-xs text-gray-500">
              Cached profile and news data persists across page refreshes. Clear to force re-fetch from server.
            </p>
          </div>

          {/* Local Storage Info */}
          <div className="border-t border-zinc-800 pt-4">
            <div className="flex items-center justify-between mb-2">
              <label className="text-sm">Browser Storage</label>
              <button
                onClick={handleClearStorage}
                className="px-3 py-1 bg-red-600 hover:bg-red-700 transition-colors text-sm"
              >
                Clear Saved Layout
              </button>
            </div>
            <p className="text-xs text-gray-500">
              Your layout is automatically saved to browser storage. Click "Clear Saved Layout" to reset.
            </p>
          </div>

          {/* Instructions */}
          <div className="border-t border-zinc-800 pt-4">
            <h3 className="text-sm mb-2">How to Add Custom Template</h3>
            <ol className="text-xs text-gray-400 space-y-1 list-decimal list-inside">
              <li>Arrange your modules in the perfect layout</li>
              <li>Click "Copy to Clipboard" to export the configuration</li>
              <li>Create a new <code className="bg-zinc-800 px-1 py-0.5">.json</code> file in <code className="bg-zinc-800 px-1 py-0.5">src/config/templates/</code></li>
              <li>Paste the layout array into the JSON with id, name, description, and layout fields</li>
              <li>The template will be auto-registered on next build/reload</li>
            </ol>
          </div>
        </div>
      </div>
    </div>
  );
}
