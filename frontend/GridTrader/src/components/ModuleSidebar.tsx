import React from 'react';
import { X } from 'lucide-react';
import { moduleRegistry } from '../config/moduleRegistry';
import type { ModuleType } from '../types';

interface ModuleSidebarProps {
  isOpen: boolean;
  onToggle: () => void;
  onAddModule: (moduleType: ModuleType) => void;
}

export function ModuleSidebar({ isOpen, onToggle, onAddModule }: ModuleSidebarProps) {
  return (
    <>
      {/* Overlay */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40"
          onClick={onToggle}
        />
      )}

      {/* Sidebar */}
      <div
        className={`fixed left-0 top-0 h-full w-80 bg-zinc-900 border-r border-zinc-800 z-50 transform transition-transform duration-300 ${
          isOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="p-4 border-b border-zinc-800 flex items-center justify-between">
          <h2 className="text-lg">Add Module</h2>
          <button
            onClick={onToggle}
            className="p-1 hover:bg-zinc-800 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-4 space-y-3">
          {Object.values(moduleRegistry).map((module) => (
            <button
              key={module.type}
              onClick={() => onAddModule(module.type)}
              className="w-full p-4 bg-zinc-800 hover:bg-zinc-700 transition-colors text-left border border-zinc-700"
            >
              <div className="mb-1">{module.name}</div>
              <div className="text-sm text-gray-400">{module.description}</div>
            </button>
          ))}
        </div>
      </div>
    </>
  );
}
