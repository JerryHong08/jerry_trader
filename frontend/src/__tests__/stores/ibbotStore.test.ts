import { describe, it, expect, beforeEach, vi } from 'vitest';

// Mock IS_DEMO to true so no WebSocket/REST calls are made
vi.mock('../../data/mockData', () => ({ IS_DEMO: true }));

import { useIbbotStore } from '../../stores/ibbotStore';
import type { OrderStatusEventData } from '../../types/ibbot';

describe('ibbotStore', () => {
  beforeEach(() => {
    useIbbotStore.setState({
      ordersById: {},
      positionsBySymbol: {},
      account: {},
      portfolioSummary: null,
      loading: false,
      error: '',
    });
  });

  // ======================================================================
  // ordersList — sorted newest first
  // ======================================================================

  describe('ordersList', () => {
    it('returns orders sorted by order_id descending', () => {
      const o1: OrderStatusEventData = {
        order_id: 1, symbol: 'AAPL', action: 'BUY', order_type: 'MKT',
        total_quantity: 100, status: 'Filled',
      } as any;
      const o2: OrderStatusEventData = {
        order_id: 2, symbol: 'TSLA', action: 'SELL', order_type: 'LMT',
        total_quantity: 50, status: 'Submitted',
      } as any;

      useIbbotStore.setState({ ordersById: { 1: o1, 2: o2 } });
      const list = useIbbotStore.getState().ordersList();
      expect(list).toHaveLength(2);
      expect(list[0].order_id).toBe(2);
      expect(list[1].order_id).toBe(1);
    });

    it('returns empty array with no orders', () => {
      expect(useIbbotStore.getState().ordersList()).toEqual([]);
    });
  });

  // ======================================================================
  // positionsList — sorted by symbol
  // ======================================================================

  describe('positionsList', () => {
    it('returns positions sorted by symbol alphabetically', () => {
      useIbbotStore.setState({
        positionsBySymbol: {
          'TSLA': { symbol: 'TSLA', position: 100 },
          'AAPL': { symbol: 'AAPL', position: 200 },
        },
      });
      const list = useIbbotStore.getState().positionsList();
      expect(list).toHaveLength(2);
      expect(list[0].symbol).toBe('AAPL');
      expect(list[1].symbol).toBe('TSLA');
    });
  });

  // ======================================================================
  // buyingPower — derived from portfolioSummary
  // ======================================================================

  describe('buyingPower', () => {
    it('returns buying power from portfolio summary', () => {
      useIbbotStore.setState({
        portfolioSummary: { account: { BuyingPower: 100000 } } as any,
      });
      expect(useIbbotStore.getState().buyingPower()).toBe(100000);
    });

    it('returns null when no buying power', () => {
      useIbbotStore.setState({ portfolioSummary: null });
      expect(useIbbotStore.getState().buyingPower()).toBeNull();
    });
  });

  // ======================================================================
  // WebSocket order update merges
  // ======================================================================

  describe('init (demo mode)', () => {
    it('sets wsStatus to connected without network calls', () => {
      useIbbotStore.getState().init();
      expect(useIbbotStore.getState().wsStatus).toBe('connected');
      expect(useIbbotStore.getState().loading).toBe(false);
    });
  });
});
