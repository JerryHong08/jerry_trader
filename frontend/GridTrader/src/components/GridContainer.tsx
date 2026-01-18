import React from 'react';
import { GridItem } from './GridItem';
import type { GridItemConfig } from '../types';

interface GridContainerProps {
  items: GridItemConfig[];
  onRemove: (id: string) => void;
  onUpdate: (id: string, updates: Partial<GridItemConfig>) => void;
  selectedSymbols: Record<string, string>;
  onSymbolSelect: (syncGroup: string | null, symbol: string) => void;
  onSyncGroupChange: (id: string, syncGroup: string | null) => void;
  gridGap: number;
}

export function GridContainer({
  items,
  onRemove,
  onUpdate,
  selectedSymbols,
  onSymbolSelect,
  onSyncGroupChange,
  gridGap
}: GridContainerProps) {
  return (
    <div className="flex-1 overflow-y-scroll overflow-x-auto p-4 relative bg-zinc-950">
      {/* Grid background pattern */}
      <div
        className="absolute inset-0 pointer-events-none opacity-10"
        style={{
          backgroundImage: `
            linear-gradient(to right, #27272a 1px, transparent 1px),
            linear-gradient(to bottom, #27272a 1px, transparent 1px)
          `,
          backgroundSize: '20px 20px',
        }}
      />

      <div className="relative min-h-full" style={{ minWidth: '100%', minHeight: 'calc(100vh - 100px)' }}>
        {items.map((item) => (
          <GridItem
            key={item.id}
            item={item}
            onRemove={() => onRemove(item.id)}
            onUpdate={(updates) => onUpdate(item.id, updates)}
            allItems={items}
            selectedSymbol={item.syncGroup ? selectedSymbols[item.syncGroup] : null}
            onSymbolSelect={(symbol) => onSymbolSelect(item.syncGroup, symbol)}
            onSyncGroupChange={(group) => onSyncGroupChange(item.id, group)}
            gridGap={gridGap}
          />
        ))}
      </div>
    </div>
  );
}
