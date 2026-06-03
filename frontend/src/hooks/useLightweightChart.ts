import { useEffect, useRef, useState, type RefObject } from 'react';
import { createChart, type IChartApi, type ChartOptions, type DeepPartial } from 'lightweight-charts';
import { CHART_THEME } from '../components/common/chartTheme';

interface UseLightweightChartOptions {
  /** Container element ref or direct DOM element */
  container: RefObject<HTMLDivElement | null>;
  /** Chart options merged on top of CHART_THEME */
  options?: DeepPartial<ChartOptions>;
  /** Use requestAnimationFrame for resize (panel-based layouts) */
  animateResize?: boolean;
  /** Don't create chart until this is true */
  enabled?: boolean;
}

/**
 * Manages the full lifecycle of a Lightweight Charts instance.
 *
 * - Creates chart on mount with CHART_THEME base + custom overrides
 * - Sets up ResizeObserver for responsive width/height
 * - Cleans up (disconnect observer, remove chart) on unmount
 *
 * Returns mutable refs so callers can add series & subscribe to events
 * in their own useEffect hooks without triggering re-renders.
 */
export function useLightweightChart({
  container,
  options,
  animateResize = false,
  enabled = true,
}: UseLightweightChartOptions) {
  const chartRef = useRef<IChartApi | null>(null);
  const [chartReady, setChartReady] = useState(false);

  useEffect(() => {
    const el = container.current;
    if (!el || !enabled) return;

    const chart = createChart(el, {
      width: el.clientWidth,
      height: el.clientHeight,
      ...CHART_THEME,
      ...options,
      // Deep-merge nested objects so callers can override a single timeScale
      // property (e.g. secondsVisible) without losing theme defaults.
      timeScale: { ...CHART_THEME.timeScale, ...options?.timeScale },
      rightPriceScale: { ...CHART_THEME.rightPriceScale, ...options?.rightPriceScale },
      layout: { ...CHART_THEME.layout, ...options?.layout },
      grid: { ...CHART_THEME.grid, ...options?.grid },
      crosshair: { ...CHART_THEME.crosshair, ...options?.crosshair },
    });

    chartRef.current = chart;
    setChartReady(true);

    const handleResize = () => {
      // Re-read container in case ref updated
      const currentEl = container.current;
      if (currentEl && chartRef.current) {
        const w = currentEl.clientWidth;
        const h = currentEl.clientHeight;
        if (w > 0 && h > 0) {
          chartRef.current.applyOptions({
            width: Math.max(100, w),
            height: Math.max(100, h),
          });
        }
      }
    };

    const ro = new ResizeObserver(() => {
      if (animateResize) {
        requestAnimationFrame(handleResize);
      } else {
        handleResize();
      }
    });
    ro.observe(el);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      setChartReady(false);
    };
    // Only create/destroy on mount/unmount or enabled toggle.
    // Options changes after creation should use chart.applyOptions() directly.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled]);

  return { chartRef, chartReady } as const;
}
