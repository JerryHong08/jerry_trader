/**
 * ChartPanelHeader - Collapsible/closable panel header for chart panels.
 *
 * Features:
 * - Collapse button (−) to minimize panel
 * - Close button (×) to remove panel
 * - Title display
 * - Drag handle for reordering (future)
 */

import React from 'react';
import { ChevronDown, ChevronRight, X } from 'lucide-react';

interface ChartPanelHeaderProps {
  title: string;
  collapsed: boolean;
  canClose?: boolean;
  onToggleCollapse: () => void;
  onClose?: () => void;
  rightContent?: React.ReactNode;
}

export function ChartPanelHeader({
  title,
  collapsed,
  canClose = true,
  onToggleCollapse,
  onClose,
  rightContent,
}: ChartPanelHeaderProps) {
  return (
    <div className="flex items-center justify-between px-2 py-1 bg-zinc-800 border-b border-zinc-700 select-none">
      {/* Left: Collapse toggle + Title */}
      <div className="flex items-center gap-1">
        <button
          onClick={onToggleCollapse}
          className="p-0.5 hover:bg-zinc-700 rounded transition-colors"
          title={collapsed ? 'Expand' : 'Collapse'}
        >
          {collapsed ? (
            <ChevronRight className="w-3.5 h-3.5 text-zinc-400" />
          ) : (
            <ChevronDown className="w-3.5 h-3.5 text-zinc-400" />
          )}
        </button>
        <span className="text-xs font-medium text-zinc-200">{title}</span>
      </div>

      {/* Right: Optional content + Close button */}
      <div className="flex items-center gap-1">
        {rightContent}
        {canClose && onClose && (
          <button
            onClick={onClose}
            className="p-0.5 hover:bg-zinc-700 rounded transition-colors"
            title="Close panel"
          >
            <X className="w-3.5 h-3.5 text-zinc-500 hover:text-red-400" />
          </button>
        )}
      </div>
    </div>
  );
}

/**
 * ChartPanelWrapper - Wrapper that adds collapse/close behavior to any panel.
 *
 * Renders header + children (or just header if collapsed).
 */
interface ChartPanelWrapperProps {
  id: string;
  title: string;
  collapsed: boolean;
  visible: boolean;
  canClose?: boolean;
  onToggleCollapse: () => void;
  onClose?: () => void;
  headerRight?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}

export function ChartPanelWrapper({
  id,
  title,
  collapsed,
  visible,
  canClose = true,
  onToggleCollapse,
  onClose,
  headerRight,
  children,
  className = '',
}: ChartPanelWrapperProps) {
  if (!visible) return null;

  return (
    <div className={`flex flex-col ${className}`}>
      <ChartPanelHeader
        title={title}
        collapsed={collapsed}
        canClose={canClose}
        onToggleCollapse={onToggleCollapse}
        onClose={onClose}
        rightContent={headerRight}
      />
      {!collapsed && (
        <div className="flex-1 min-h-0">
          {children}
        </div>
      )}
    </div>
  );
}
