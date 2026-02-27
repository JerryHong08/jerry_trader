/**
 * PortfolioModule – Real IB positions & account summary
 *
 * Powered by ibbotStore (REST + WS from OrderManagement backend port 8888).
 * Features:
 *  - Account summary (Net Liquidation, Buying Power, Cash, …)
 *  - Positions table with symbol click → sync group
 *  - Refresh portfolio button (triggers IB reqPositions under the hood)
 *  - All mock data replaced with live backend data
 */

import React, { useState } from 'react';
import { TrendingUp, TrendingDown, RefreshCw, Wifi, WifiOff, Eye, EyeOff } from 'lucide-react';
import type { ModuleProps } from '../types';
import { DataTable } from './common/DataTable';
import { useIbbotStore, type PositionRow } from '../stores/ibbotStore';
import { usePrivacyStore } from '../stores/privacyStore';

// ============================================================================
// Helpers
// ============================================================================

const formatCurrency = (value: number | undefined | null, masked = false) => {
  if (masked) return '••••';
  if (value == null) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
};

const formatNumber = (value: number | undefined | null, decimals = 2, masked = false) => {
  if (masked) return '••••';
  if (value == null) return '—';
  return value.toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
};

// ============================================================================
// Types
// ============================================================================

type SortColumn = 'symbol' | 'position' | 'average_cost' | 'market_price' | 'market_value' | 'unrealized_pnl' | 'realized_pnl';
type SortDirection = 'asc' | 'desc';

// ============================================================================
// Component
// ============================================================================

export function PortfolioModule({
  onRemove,
  syncGroup,
  onSyncGroupChange,
  selectedSymbol,
  onSymbolSelect,
}: ModuleProps) {
  // Store
  const wsStatus = useIbbotStore((s) => s.wsStatus);
  const loading = useIbbotStore((s) => s.loading);
  const error = useIbbotStore((s) => s.error);
  const positionsList = useIbbotStore((s) => s.positionsList);
  const account = useIbbotStore((s) => s.account);
  const portfolioSummary = useIbbotStore((s) => s.portfolioSummary);
  const doRefreshPortfolio = useIbbotStore((s) => s.doRefreshPortfolio);

  // Privacy
  const privacyMode = usePrivacyStore((s) => s.privacyMode);
  const togglePrivacy = usePrivacyStore((s) => s.toggle);
  const m = privacyMode; // shorthand for masked param

  // Local sort state
  const [sortColumn, setSortColumn] = useState<SortColumn>('market_value');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  const handleSort = (column: SortColumn) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('desc');
    }
  };

  const positions = positionsList();

  const sortedPositions = [...positions].sort((a, b) => {
    const aVal = a[sortColumn];
    const bVal = b[sortColumn];
    const mult = sortDirection === 'asc' ? 1 : -1;
    if (aVal == null && bVal == null) return 0;
    if (aVal == null) return 1;
    if (bVal == null) return -1;
    if (typeof aVal === 'string' && typeof bVal === 'string')
      return aVal.localeCompare(bVal) * mult;
    return ((aVal as number) - (bVal as number)) * mult;
  });

  const handleRowClick = (symbol: string) => {
    if (syncGroup && onSymbolSelect) {
      onSymbolSelect(symbol);
    }
  };

  // Account tags from WS (live patching) or portfolio summary
  const acct = account ?? {};
  const netLiq = Number(acct['NetLiquidation'] ?? acct['NetLiquidationByCurrency'] ?? 0);
  const buyingPower = Number(acct['BuyingPower'] ?? 0);
  const cash = Number(acct['TotalCashValue'] ?? acct['CashBalance'] ?? 0);
  const availableFunds = Number(acct['AvailableFunds'] ?? 0);
  const initMargin = Number(acct['InitMarginReq'] ?? acct['FullInitMarginReq'] ?? 0);
  const maintMargin = Number(acct['MaintMarginReq'] ?? acct['FullMaintMarginReq'] ?? 0);
  const cushion = Number(acct['Cushion'] ?? 0);

  const totalMarketValue = portfolioSummary?.total_market_value ??
    positions.reduce((s, p) => s + (p.market_value ?? 0), 0);
  const totalUnrealizedPnL = positions.reduce((s, p) => s + (p.unrealized_pnl ?? 0), 0);
  const totalRealizedPnL = positions.reduce((s, p) => s + (p.realized_pnl ?? 0), 0);

  const wsConnected = wsStatus === 'connected';

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Header bar */}
      <div className="px-3 py-1.5 border-b border-zinc-800 flex items-center justify-between text-xs text-gray-400">
        <div className="flex items-center gap-2">
          {wsConnected ? <Wifi className="w-3 h-3 text-green-500" /> : <WifiOff className="w-3 h-3 text-red-500" />}
          <span>Positions: {positions.length}</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={togglePrivacy}
            className="p-0.5 hover:bg-zinc-700 rounded transition-colors"
            title={privacyMode ? 'Show numbers' : 'Hide numbers'}
          >
            {privacyMode ? <EyeOff className="w-3.5 h-3.5 text-yellow-500" /> : <Eye className="w-3.5 h-3.5" />}
          </button>
          <button
            onClick={() => doRefreshPortfolio()}
            disabled={loading}
            className="flex items-center gap-1 px-2 py-0.5 text-xs bg-blue-600 hover:bg-blue-700 disabled:opacity-50"
          >
            <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="px-3 py-1.5 bg-red-900/30 border-b border-red-800 text-xs text-red-300">
          {error}
        </div>
      )}

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {/* Account Summary */}
        {(netLiq > 0 || buyingPower > 0) && (
          <div className="p-3 border-b border-zinc-800 bg-zinc-900/50">
            <div className="text-xs text-gray-400 mb-2">Account Summary</div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div>
                <div className="text-xs text-gray-500">Net Liquidation</div>
                <div className="text-sm mt-0.5">{formatCurrency(netLiq, m)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Buying Power</div>
                <div className="text-sm mt-0.5">{formatCurrency(buyingPower, m)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Cash</div>
                <div className="text-sm mt-0.5">{formatCurrency(cash, m)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Available Funds</div>
                <div className="text-sm mt-0.5">{formatCurrency(availableFunds, m)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Init Margin</div>
                <div className="text-sm mt-0.5">{formatCurrency(initMargin, m)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Maint Margin</div>
                <div className="text-sm mt-0.5">{formatCurrency(maintMargin, m)}</div>
              </div>
              {cushion > 0 && (
                <div>
                  <div className="text-xs text-gray-500">Cushion</div>
                  <div className="text-sm mt-0.5">{m ? '••••' : `${(cushion * 100).toFixed(1)}%`}</div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Portfolio Summary */}
        <div className="p-3 border-b border-zinc-800 bg-zinc-900/30">
          <div className="grid grid-cols-3 gap-4">
            <div>
              <div className="text-xs text-gray-500">Total Market Value</div>
              <div className="text-sm mt-0.5">{formatCurrency(totalMarketValue, m)}</div>
            </div>
            <div>
              <div className="text-xs text-gray-500">Unrealized P&L</div>
              <div className={`text-sm mt-0.5 ${totalUnrealizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                {m ? '••••' : `${totalUnrealizedPnL >= 0 ? '+' : ''}${formatCurrency(totalUnrealizedPnL)}`}
              </div>
            </div>
            <div>
              <div className="text-xs text-gray-500">Realized P&L</div>
              <div className={`text-sm mt-0.5 ${totalRealizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                {m ? '••••' : `${totalRealizedPnL >= 0 ? '+' : ''}${formatCurrency(totalRealizedPnL)}`}
              </div>
            </div>
          </div>
        </div>

        {/* Positions Table */}
        <DataTable<PositionRow>
          data={sortedPositions}
          columns={[
            {
              key: 'symbol',
              label: 'Symbol',
              sortable: true,
              render: (p) => (
                <div className="flex items-center gap-2">
                  {p.symbol}
                  {syncGroup && selectedSymbol === p.symbol && (
                    <div className="w-1.5 h-1.5 bg-blue-500 rounded-full" />
                  )}
                </div>
              ),
            },
            {
              key: 'position',
              label: 'Qty',
              align: 'right' as const,
              sortable: true,
              render: (p) => formatNumber(p.position, 0, m),
            },
            {
              key: 'average_cost',
              label: 'Avg Cost',
              align: 'right' as const,
              sortable: true,
              render: (p) => formatCurrency(p.average_cost, m),
            },
            {
              key: 'market_price',
              label: 'Mkt Price',
              align: 'right' as const,
              sortable: true,
              render: (p) => formatCurrency(p.market_price, m),
            },
            {
              key: 'market_value',
              label: 'Mkt Value',
              align: 'right' as const,
              sortable: true,
              render: (p) => formatCurrency(p.market_value, m),
            },
            {
              key: 'unrealized_pnl',
              label: 'Unrealized P&L',
              align: 'right' as const,
              sortable: true,
              render: (p) => (
                <div className={`flex items-center justify-end gap-1 ${(p.unrealized_pnl ?? 0) >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                  {(p.unrealized_pnl ?? 0) >= 0 ? (
                    <TrendingUp className="w-3 h-3" />
                  ) : (
                    <TrendingDown className="w-3 h-3" />
                  )}
                  {m ? '••••' : `${(p.unrealized_pnl ?? 0) >= 0 ? '+' : ''}${formatCurrency(p.unrealized_pnl)}`}
                </div>
              ),
            },
            {
              key: 'realized_pnl',
              label: 'Realized P&L',
              align: 'right' as const,
              sortable: true,
              render: (p) => (
                <span className={(p.realized_pnl ?? 0) >= 0 ? 'text-green-500' : 'text-red-500'}>
                  {m ? '••••' : `${(p.realized_pnl ?? 0) >= 0 ? '+' : ''}${formatCurrency(p.realized_pnl)}`}
                </span>
              ),
            },
          ]}
          onRowClick={(p) => handleRowClick(p.symbol)}
          getRowKey={(p) => p.symbol}
          selectedKey={selectedSymbol}
          sortColumn={sortColumn}
          sortDirection={sortDirection}
          onSort={(col) => handleSort(col as SortColumn)}
        />
      </div>
    </div>
  );
}
