import { describe, it, expect, beforeEach, vi } from 'vitest';

vi.stubGlobal('import', { meta: { env: { VITE_CHART_BFF_URL: 'http://localhost:8000' } } });

import {
  useChartDataStore,
  chartStoreKey,
  type OHLCVBar,
} from '../../stores/chartDataStore';

/** Build a 5-bar OHLCV series starting at baseTimeSec (must be a multiple of barDurationSec) */
function seedBars(
  moduleId: string,
  ticker: string,
  timeframe: string,
  barDurationSec: number,
  baseTimeSec: number = 0,
): string {
  const key = chartStoreKey(moduleId, ticker.toUpperCase());
  const bars: OHLCVBar[] = [];
  for (let i = 0; i < 5; i++) {
    bars.push({
      time: baseTimeSec + i * barDurationSec,
      open: 100 + i,
      high: 102 + i,
      low: 99 + i,
      close: 101 + i,
      volume: 1000 * (i + 1),
    });
  }
  useChartDataStore.setState((s) => ({
    symbolBars: {
      ...s.symbolBars,
      [key]: {
        bars,
        timeframe: timeframe as any,
        barDurationSec,
        loading: false,
        error: null,
        source: 'test',
        lastFetchTime: Date.now(),
        requestId: 'test-request',
      },
    },
  }));
  return key;
}

describe('chartStoreKey', () => {
  it('builds composite key', () => {
    expect(chartStoreKey('chart-123', 'AAPL')).toBe('chart-123::AAPL');
    expect(chartStoreKey('abc', 'tsla')).toBe('abc::TSLA');
  });
});

describe('chartDataStore', () => {
  beforeEach(() => {
    useChartDataStore.setState({ symbolBars: {}, fetchTriggers: {} });
  });

  // ======================================================================
  // updateFromTrade — intraday (barDuration < 3600)
  // ======================================================================

  describe('updateFromTrade — current bar (intraday)', () => {
    it('updates OHLCV when trade falls within current bar', () => {
      // Bars at 0, 60, 120, 180, 240. Last bar: i=4 → open=104, high=106, low=103, close=105, vol=5000
      // Trade at 270000ms=270s → boundary = floor(270/60)*60 = 240 → same bar
      const key = seedBars('m1', 'AAPL', '1m', 60, 0);
      useChartDataStore.getState().updateFromTrade('m1', 'AAPL', 108, 500, 270_000);

      const bars = useChartDataStore.getState().symbolBars[key].bars;
      const last = bars[bars.length - 1];
      expect(last.high).toBe(108); // max(106, 108)
      expect(last.low).toBe(103);  // min(103, 108)
      expect(last.close).toBe(108);
      expect(last.volume).toBe(5500); // 5000 + 500
    });

    it('creates new bar when trade crosses boundary', () => {
      // Trade at 310s → boundary = floor(310/60)*60 = 300 > 240 → new bar
      const key = seedBars('m1', 'AAPL', '1m', 60, 0);
      useChartDataStore.getState().updateFromTrade('m1', 'AAPL', 110, 300, 310_000);

      const bars = useChartDataStore.getState().symbolBars[key].bars;
      expect(bars.length).toBe(6);
      expect(bars[5].time).toBe(300);
      expect(bars[5].open).toBe(110);
      expect(bars[5].close).toBe(110);
      expect(bars[5].volume).toBe(300);
    });

    it('ignores stale trade before current bar', () => {
      const key = seedBars('m1', 'AAPL', '1m', 60, 0);
      useChartDataStore.getState().updateFromTrade('m1', 'AAPL', 50, 100, 0); // before epoch → stale
      const bars = useChartDataStore.getState().symbolBars[key].bars;
      expect(bars.length).toBe(5); // unchanged
    });
  });

  describe('updateFromTrade — 1h+ bars (session-aligned)', () => {
    it('updates current bar when trade within range', () => {
      // Bars at 36000, 39600, ..., 50400 (5 bars, 1h each)
      // Trade at 52000s < 50400 + 3600 = 54000 → within current bar
      const key = seedBars('m1', 'AAPL', '1h', 3600, 36000);
      useChartDataStore.getState().updateFromTrade('m1', 'AAPL', 150, 200, 52000_000);

      const bars = useChartDataStore.getState().symbolBars[key].bars;
      expect(bars.length).toBe(5); // still 5 bars
      // Last bar i=4: open=104, high=106, low=103. price=150 → new high
      expect(bars[4].high).toBe(150);
      expect(bars[4].close).toBe(150);
    });

    it('creates new bar when trade past range', () => {
      // Trade at 58000s > 50400 + 3600 = 54000 → new bar at 50400 + 3600 = 54000
      const key = seedBars('m1', 'AAPL', '1h', 3600, 36000);
      useChartDataStore.getState().updateFromTrade('m1', 'AAPL', 150, 200, 58000_000);

      const bars = useChartDataStore.getState().symbolBars[key].bars;
      expect(bars.length).toBe(6);
      expect(bars[5].time).toBe(54000);
    });

    it('ignores stale trade before first bar', () => {
      const key = seedBars('m1', 'AAPL', '1h', 3600, 36000);
      useChartDataStore.getState().updateFromTrade('m1', 'AAPL', 50, 100, 0); // before first bar
      const bars = useChartDataStore.getState().symbolBars[key].bars;
      expect(bars.length).toBe(5);
    });
  });

  // ======================================================================
  // applyBarUpdate (server-pushed completed bar)
  // ======================================================================

  describe('applyBarUpdate', () => {
    it('overwrites bar with same time', () => {
      const key = seedBars('m1', 'AAPL', '1m', 60, 0);
      const completedBar: OHLCVBar = {
        time: 240, open: 104, high: 112, low: 102, close: 110, volume: 7000,
      };
      useChartDataStore.getState().applyBarUpdate('m1', 'AAPL', '1m', completedBar);

      const bars = useChartDataStore.getState().symbolBars[key].bars;
      expect(bars[4].high).toBe(112);
      expect(bars[4].low).toBe(102);
      expect(bars[4].volume).toBe(7000);
    });

    it('appends bar with new time', () => {
      const key = seedBars('m1', 'AAPL', '1m', 60, 0);
      const newBar: OHLCVBar = {
        time: 300, open: 110, high: 115, low: 108, close: 112, volume: 3000,
      };
      useChartDataStore.getState().applyBarUpdate('m1', 'AAPL', '1m', newBar);

      const bars = useChartDataStore.getState().symbolBars[key].bars;
      expect(bars.length).toBe(6);
      expect(bars[5].time).toBe(300);
    });

    it('ignores bar with wrong timeframe', () => {
      const key = seedBars('m1', 'AAPL', '5m', 300, 0);
      const bar: OHLCVBar = { time: 600, open: 107, high: 110, low: 106, close: 109, volume: 100 };
      useChartDataStore.getState().applyBarUpdate('m1', 'AAPL', '1m', bar); // timeframe mismatch

      const bars = useChartDataStore.getState().symbolBars[key].bars;
      expect(bars.length).toBe(5);
    });
  });

  // ======================================================================
  // broadcastBarUpdate
  // ======================================================================

  describe('broadcastBarUpdate', () => {
    it('broadcasts to all modules showing the same ticker+timeframe', () => {
      seedBars('m1', 'AAPL', '1m', 60, 0);
      seedBars('m2', 'AAPL', '1m', 60, 0);
      seedBars('m3', 'TSLA', '1m', 60, 0); // different ticker

      const bar: OHLCVBar = { time: 300, open: 107, high: 110, low: 106, close: 109, volume: 100 };
      useChartDataStore.getState().broadcastBarUpdate('AAPL', '1m', bar);

      const state = useChartDataStore.getState();
      expect(state.symbolBars['m1::AAPL'].bars.length).toBe(6);
      expect(state.symbolBars['m2::AAPL'].bars.length).toBe(6);
      expect(state.symbolBars['m3::TSLA'].bars.length).toBe(5); // unchanged
    });
  });

  // ======================================================================
  // clearSymbol / clearBarsForTicker
  // ======================================================================

  describe('clearSymbol', () => {
    it('removes a specific module+ticker entry', () => {
      seedBars('m1', 'AAPL', '1m', 60);
      seedBars('m2', 'AAPL', '1m', 60);

      useChartDataStore.getState().clearSymbol('m1', 'AAPL');
      const state = useChartDataStore.getState();
      expect(state.symbolBars['m1::AAPL']).toBeUndefined();
      expect(state.symbolBars['m2::AAPL']).toBeDefined();
    });
  });

  describe('clearBarsForTicker', () => {
    it('removes all entries for a ticker across all modules', () => {
      seedBars('m1', 'AAPL', '1m', 60);
      seedBars('m2', 'AAPL', '1m', 60);
      seedBars('m3', 'TSLA', '1m', 60);

      useChartDataStore.getState().clearBarsForTicker('AAPL');
      const state = useChartDataStore.getState();
      expect(state.symbolBars['m1::AAPL']).toBeUndefined();
      expect(state.symbolBars['m2::AAPL']).toBeUndefined();
      expect(state.symbolBars['m3::TSLA']).toBeDefined();
    });
  });

  describe('reset', () => {
    it('clears all bars', () => {
      seedBars('m1', 'AAPL', '1m', 60);
      useChartDataStore.getState().reset();
      expect(Object.keys(useChartDataStore.getState().symbolBars)).toHaveLength(0);
    });
  });
});
