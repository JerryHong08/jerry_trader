import { ColorType, CrosshairMode } from 'lightweight-charts';
import type { DeepPartial, ChartOptions } from 'lightweight-charts';

/**
 * Unified dark chart theme — single source of truth for all TradingView charts.
 *
 * Colors use the project Zinc palette:
 *   zinc-900 (#18181b) background / zinc-800 (#27272a) grid
 *   zinc-700 (#3f3f46) border / zinc-600 (#52525b) crosshair
 *   zinc-400 (#a1a1aa) text
 */
export const CHART_THEME: DeepPartial<ChartOptions> = {
  layout: {
    background: { type: ColorType.Solid, color: '#18181b' },
    textColor: '#a1a1aa',
  },
  grid: {
    vertLines: { color: '#27272a' },
    horzLines: { color: '#27272a' },
  },
  crosshair: {
    mode: CrosshairMode.Normal,
    vertLine: { color: '#52525b', width: 1, style: 2, labelBackgroundColor: '#3f3f46' },
    horzLine: { color: '#52525b', width: 1, style: 2, labelBackgroundColor: '#3f3f46' },
  },
  timeScale: {
    borderColor: '#27272a',
    timeVisible: true,
    secondsVisible: false,
  },
  rightPriceScale: {
    borderColor: '#27272a',
  },
};
