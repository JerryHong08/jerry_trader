import React from 'react';

export interface TableColumn<T> {
  key: string;
  label: string;
  width?: number;
  align?: 'left' | 'center' | 'right';
  sortable?: boolean;
  render: (item: T, index: number) => React.ReactNode;
}

interface DataTableProps<T> {
  data: T[];
  columns: TableColumn<T>[];
  onRowClick?: (item: T, index: number) => void;
  getRowKey: (item: T, index: number) => string;
  selectedKey?: string | null;
  sortColumn?: string;
  sortDirection?: 'asc' | 'desc';
  onSort?: (column: string) => void;
  showIndex?: boolean;
  indexWidth?: number;
  hoverable?: boolean;
  className?: string;
}

export function DataTable<T>({
  data,
  columns,
  onRowClick,
  getRowKey,
  selectedKey,
  sortColumn,
  sortDirection,
  onSort,
  showIndex = false,
  indexWidth = 50,
  hoverable = true,
  className = '',
}: DataTableProps<T>) {
  const handleSort = (column: TableColumn<T>) => {
    if (column.sortable && onSort) {
      onSort(column.key);
    }
  };

  return (
    <div className={`overflow-x-auto ${className}`}>
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-zinc-800 border-b border-zinc-700 z-10">
          <tr>
            {showIndex && (
              <th
                className="px-3 py-2 text-left text-xs text-gray-400"
                style={{ width: indexWidth }}
              >
                #
              </th>
            )}
            {columns.map((column) => (
              <th
                key={column.key}
                className={`px-3 py-2 text-xs text-gray-400 ${
                  column.align === 'center' ? 'text-center' :
                  column.align === 'right' ? 'text-right' :
                  'text-left'
                } ${
                  column.sortable ? 'cursor-pointer hover:bg-zinc-700 transition-colors' : ''
                }`}
                style={column.width ? { width: column.width } : undefined}
                onClick={() => handleSort(column)}
              >
                {column.label}
                {column.sortable && sortColumn === column.key && (
                  <span className="ml-1">
                    {sortDirection === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((item, index) => {
            const rowKey = getRowKey(item, index);
            const isSelected = selectedKey === rowKey;
            const isClickable = !!onRowClick;

            return (
              <tr
                key={rowKey}
                onClick={() => onRowClick?.(item, index)}
                className={`border-b border-zinc-800 transition-colors ${
                  hoverable ? 'hover:bg-zinc-800/50' : ''
                } ${
                  isClickable ? 'cursor-pointer' : ''
                } ${
                  isSelected ? 'bg-blue-600/20' : ''
                }`}
              >
                {showIndex && (
                  <td className="px-3 py-2 text-gray-500">{index + 1}</td>
                )}
                {columns.map((column) => (
                  <td
                    key={column.key}
                    className={`px-3 py-2 ${
                      column.align === 'center' ? 'text-center' :
                      column.align === 'right' ? 'text-right' :
                      'text-left'
                    }`}
                  >
                    {column.render(item, index)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
