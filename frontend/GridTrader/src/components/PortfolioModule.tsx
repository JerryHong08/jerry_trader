import React, { useState, useEffect } from 'react';
import { TrendingUp, TrendingDown } from 'lucide-react';
import type { ModuleProps, PortfolioPosition, AccountInfo } from '../types';
import { DataTable, TableColumn } from './common/DataTable';
import { useBackendTimestamp } from '../hooks/useBackendTimestamps';

// Generate mock portfolio data
const generateMockPortfolio = (): PortfolioPosition[] => {
  const symbols = ['AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'META', 'AMD', 'NFLX'];

  return symbols.map((symbol) => {
    const quantity = Math.floor(Math.random() * 500) + 10;
    const avgCost = Math.random() * 300 + 50;
    const priceChange = (Math.random() - 0.5) * 0.15; // +/- 15%
    const marketPrice = avgCost * (1 + priceChange);
    const marketValue = marketPrice * quantity;
    const unrealizedPnL = (marketPrice - avgCost) * quantity;
    const realizedPnL = (Math.random() - 0.3) * 5000; // Some realized gains/losses

    return {
      symbol,
      quantity,
      avgCost,
      marketPrice,
      marketValue,
      unrealizedPnL,
      realizedPnL,
    };
  });
};

// Generate mock account info
const generateMockAccountInfo = (positions: PortfolioPosition[]): AccountInfo => {
  const totalMarketValue = positions.reduce((sum, p) => sum + p.marketValue, 0);
  const totalUnrealizedPnL = positions.reduce((sum, p) => sum + p.unrealizedPnL, 0);
  const cash = 1022728.72;
  const netLiquidation = totalMarketValue + cash;

  return {
    netLiquidation,
    buyingPower: netLiquidation * 4, // 4x leverage
    availableFunds: netLiquidation - totalMarketValue * 0.25,
    totalCashValue: cash,
    equityWithLoanValue: netLiquidation,
    dayTradesRemaining: 3,
    initMarginReq: totalMarketValue * 0.25,
    maintMarginReq: totalMarketValue * 0.25 * 0.9,
    cushion: 1,
  };
};

type SortColumn = 'symbol' | 'quantity' | 'avgCost' | 'marketPrice' | 'marketValue' | 'unrealizedPnL' | 'realizedPnL';
type SortDirection = 'asc' | 'desc';

export function PortfolioModule({
  onRemove,
  syncGroup,
  onSyncGroupChange,
  selectedSymbol,
  onSymbolSelect,
}: ModuleProps) {
  const [positions, setPositions] = useState<PortfolioPosition[]>([]);
  const [accountInfo, setAccountInfo] = useState<AccountInfo | null>(null);
  const [sortColumn, setSortColumn] = useState<SortColumn>('marketValue');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  // Backend timestamp for portfolio domain
  const backendTimestamp = useBackendTimestamp('portfolio');

  // Initialize data
  useEffect(() => {
    const mockPositions = generateMockPortfolio();
    const mockAccountInfo = generateMockAccountInfo(mockPositions);
    setPositions(mockPositions);
    setAccountInfo(mockAccountInfo);

    // Update prices periodically
    const interval = setInterval(() => {
      setPositions(prev => prev.map(pos => {
        const priceChange = (Math.random() - 0.5) * 0.02; // +/- 2% change
        const newMarketPrice = pos.marketPrice * (1 + priceChange);
        const newMarketValue = newMarketPrice * pos.quantity;
        const newUnrealizedPnL = (newMarketPrice - pos.avgCost) * pos.quantity;

        return {
          ...pos,
          marketPrice: newMarketPrice,
          marketValue: newMarketValue,
          unrealizedPnL: newUnrealizedPnL,
        };
      }));
    }, 2000);

    return () => clearInterval(interval);
  }, []);

  // Update account info when positions change
  useEffect(() => {
    if (positions.length > 0) {
      const mockAccountInfo = generateMockAccountInfo(positions);
      setAccountInfo(mockAccountInfo);
    }
  }, [positions]);

  const handleSort = (column: SortColumn) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('desc');
    }
  };

  const sortedPositions = [...positions].sort((a, b) => {
    const aVal = a[sortColumn];
    const bVal = b[sortColumn];
    const multiplier = sortDirection === 'asc' ? 1 : -1;

    if (typeof aVal === 'string' && typeof bVal === 'string') {
      return aVal.localeCompare(bVal) * multiplier;
    }
    return ((aVal as number) - (bVal as number)) * multiplier;
  });

  const handleRowClick = (symbol: string) => {
    if (syncGroup && onSymbolSelect) {
      onSymbolSelect(symbol);
    }
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  const formatNumber = (value: number, decimals: number = 2) => {
    return value.toLocaleString('en-US', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  };

  const totalMarketValue = positions.reduce((sum, p) => sum + p.marketValue, 0);
  const totalUnrealizedPnL = positions.reduce((sum, p) => sum + p.unrealizedPnL, 0);
  const totalRealizedPnL = positions.reduce((sum, p) => sum + p.realizedPnL, 0);

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Content */}
      <div className="flex-1 overflow-auto">
        {/* Account Summary */}
        {accountInfo && (
          <div className="p-3 border-b border-zinc-800 bg-zinc-900/50">
            <div className="text-xs text-gray-400 mb-2">Account Summary</div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div>
                <div className="text-xs text-gray-500">Net Liquidation</div>
                <div className="text-sm mt-0.5">{formatCurrency(accountInfo.netLiquidation)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Buying Power</div>
                <div className="text-sm mt-0.5">{formatCurrency(accountInfo.buyingPower)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Cash</div>
                <div className="text-sm mt-0.5">{formatCurrency(accountInfo.totalCashValue)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Available Funds</div>
                <div className="text-sm mt-0.5">{formatCurrency(accountInfo.availableFunds)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Day Trades Left</div>
                <div className="text-sm mt-0.5">
                  {accountInfo.dayTradesRemaining === -1 ? 'Unlimited' : accountInfo.dayTradesRemaining}
                </div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Init Margin</div>
                <div className="text-sm mt-0.5">{formatCurrency(accountInfo.initMarginReq)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Maint Margin</div>
                <div className="text-sm mt-0.5">{formatCurrency(accountInfo.maintMarginReq)}</div>
              </div>
              <div>
                <div className="text-xs text-gray-500">Cushion</div>
                <div className="text-sm mt-0.5">{(accountInfo.cushion * 100).toFixed(1)}%</div>
              </div>
            </div>
          </div>
        )}

        {/* Portfolio Summary */}
        <div className="p-3 border-b border-zinc-800 bg-zinc-900/30">
          <div className="flex items-center justify-between mb-2">
            <div className="text-xs text-gray-500">Updated: {backendTimestamp}</div>
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div>
              <div className="text-xs text-gray-500">Total Market Value</div>
              <div className="text-sm mt-0.5">{formatCurrency(totalMarketValue)}</div>
            </div>
            <div>
              <div className="text-xs text-gray-500">Unrealized P&L</div>
              <div className={`text-sm mt-0.5 ${totalUnrealizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                {totalUnrealizedPnL >= 0 ? '+' : ''}{formatCurrency(totalUnrealizedPnL)}
              </div>
            </div>
            <div>
              <div className="text-xs text-gray-500">Realized P&L</div>
              <div className={`text-sm mt-0.5 ${totalRealizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                {totalRealizedPnL >= 0 ? '+' : ''}{formatCurrency(totalRealizedPnL)}
              </div>
            </div>
          </div>
        </div>

        {/* Positions Table */}
        <DataTable
          data={sortedPositions}
          columns={[
            {
              key: 'symbol',
              label: 'Symbol',
              sortable: true,
              render: (position) => (
                <div className="flex items-center gap-2">
                  {position.symbol}
                  {syncGroup && selectedSymbol === position.symbol && (
                    <div className="w-1.5 h-1.5 bg-blue-500 rounded-full" />
                  )}
                </div>
              ),
            },
            {
              key: 'quantity',
              label: 'Qty',
              align: 'right' as const,
              sortable: true,
              render: (position) => formatNumber(position.quantity, 0),
            },
            {
              key: 'avgCost',
              label: 'Avg Cost',
              align: 'right' as const,
              sortable: true,
              render: (position) => formatCurrency(position.avgCost),
            },
            {
              key: 'marketPrice',
              label: 'Market Price',
              align: 'right' as const,
              sortable: true,
              render: (position) => formatCurrency(position.marketPrice),
            },
            {
              key: 'marketValue',
              label: 'Market Value',
              align: 'right' as const,
              sortable: true,
              render: (position) => formatCurrency(position.marketValue),
            },
            {
              key: 'unrealizedPnL',
              label: 'Unrealized P&L',
              align: 'right' as const,
              sortable: true,
              render: (position) => (
                <div className={`flex items-center justify-end gap-1 ${position.unrealizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                  {position.unrealizedPnL >= 0 ? (
                    <TrendingUp className="w-3 h-3" />
                  ) : (
                    <TrendingDown className="w-3 h-3" />
                  )}
                  {position.unrealizedPnL >= 0 ? '+' : ''}{formatCurrency(position.unrealizedPnL)}
                </div>
              ),
            },
            {
              key: 'realizedPnL',
              label: 'Realized P&L',
              align: 'right' as const,
              sortable: true,
              render: (position) => (
                <span className={position.realizedPnL >= 0 ? 'text-green-500' : 'text-red-500'}>
                  {position.realizedPnL >= 0 ? '+' : ''}{formatCurrency(position.realizedPnL)}
                </span>
              ),
            },
          ]}
          onRowClick={(position) => handleRowClick(position.symbol)}
          getRowKey={(position) => position.symbol}
          selectedKey={selectedSymbol}
          sortColumn={sortColumn}
          sortDirection={sortDirection}
          onSort={(column) => handleSort(column as SortColumn)}
        />
      </div>
    </div>
  );
}
