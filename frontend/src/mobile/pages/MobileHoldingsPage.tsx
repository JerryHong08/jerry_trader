import { useMemo } from 'react';
import { useIbbotStore } from '../../stores/ibbotStore';
import { usePrivacyStore } from '../../stores/privacyStore';
import { RefreshCw } from 'lucide-react';

export function MobileHoldingsPage() {
  const account = useIbbotStore((s) => s.account);
  const portfolioSummary = useIbbotStore((s) => s.portfolioSummary);
  const positionsBySymbol = useIbbotStore((s) => s.positionsBySymbol);
  const positions = useMemo(
    () => Object.values(positionsBySymbol).sort((a, b) => a.symbol.localeCompare(b.symbol)),
    [positionsBySymbol],
  );
  const loading = useIbbotStore((s) => s.loading);
  const doRefreshPortfolio = useIbbotStore((s) => s.doRefreshPortfolio);
  const mask = usePrivacyStore((s) => s.mask);

  const summaryItems = [
    { label: 'Net Liq', value: account?.['NetLiquidation'] },
    { label: 'Buying Power', value: account?.['BuyingPower'] },
    { label: 'Cash', value: account?.['TotalCashValue'] },
    { label: 'Available', value: account?.['AvailableFunds'] },
  ];

  return (
    <div className="h-full flex flex-col">
      {/* Thin controls */}
      <div className="flex-shrink-0 flex items-center justify-between px-2 py-1 bg-zinc-900">
        <span className="text-2xs text-zinc-500">{positions.length} stocks</span>
        <button
          onClick={doRefreshPortfolio}
          disabled={loading}
          className="text-2xs px-2 py-0.5 bg-zinc-800 rounded hover:bg-zinc-700 transition-colors disabled:opacity-50 flex items-center gap-1 text-zinc-400"
        >
          <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      <div className="flex-1 overflow-y-auto overscroll-contain">
        {/* Account Summary */}
        <div className="p-3 border-b border-zinc-800">
          <div className="grid grid-cols-2 gap-2">
            {summaryItems.map(({ label, value }) => (
              <div key={label} className="bg-zinc-900 rounded p-2">
                <div className="text-2xs text-zinc-500">{label}</div>
                <div className="text-sm font-mono tabular-nums">
                  {mask(typeof value === 'number' ? `$${value.toLocaleString()}` : undefined)}
                </div>
              </div>
            ))}
          </div>

          {portfolioSummary && (
            <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
              <div className="flex justify-between">
                <span className="text-zinc-500">Mkt Value</span>
                <span className="tabular-nums">
                  {mask(`$${portfolioSummary.total_market_value?.toLocaleString() ?? '—'}`)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-zinc-500">Unreal. P&L</span>
                <span className={`tabular-nums ${(portfolioSummary.total_unrealized_pnl ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {mask(
                    portfolioSummary.total_unrealized_pnl != null
                      ? `$${portfolioSummary.total_unrealized_pnl.toLocaleString()}`
                      : undefined
                  )}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-zinc-500">Real. P&L</span>
                <span className={`tabular-nums ${(portfolioSummary.total_realized_pnl ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {mask(
                    portfolioSummary.total_realized_pnl != null
                      ? `$${portfolioSummary.total_realized_pnl.toLocaleString()}`
                      : undefined
                  )}
                </span>
              </div>
            </div>
          )}
        </div>

        {/* Positions */}
        {positions.length === 0 ? (
          <div className="text-center text-zinc-500 py-10 text-sm">No positions</div>
        ) : (
          <div className="p-3 space-y-2">
            {positions.map((pos) => (
              <div key={pos.symbol} className="bg-zinc-900 rounded p-3 border border-zinc-800">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-semibold">{pos.symbol}</span>
                  <span className={`text-xs tabular-nums ${(pos.unrealized_pnl ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {mask(
                      pos.unrealized_pnl != null
                        ? `${pos.unrealized_pnl >= 0 ? '+' : ''}$${pos.unrealized_pnl.toLocaleString()}`
                        : undefined
                    )}
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Qty</span>
                    <span className="tabular-nums">{mask(pos.position)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Avg Cost</span>
                    <span className="tabular-nums">
                      {mask(pos.average_cost != null ? `$${pos.average_cost.toFixed(2)}` : undefined)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Mkt Price</span>
                    <span className="tabular-nums">
                      {mask(pos.market_price != null ? `$${pos.market_price.toFixed(2)}` : undefined)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Mkt Value</span>
                    <span className="tabular-nums">
                      {mask(pos.market_value != null ? `$${pos.market_value.toLocaleString()}` : undefined)}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
