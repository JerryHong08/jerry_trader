import { useEffect, useRef } from 'react'
import { createChart, ColorType, LineSeries } from 'lightweight-charts'
import type { IChartApi, ISeriesApi, LineData, Time } from 'lightweight-charts'

interface Trade {
  symbol: string
  price: number
  size: number
  timestamp: number
}

interface ChartPanelProps {
  symbol: string | null
  latestTrade: Trade | null
}

export default function ChartPanel({ symbol, latestTrade }: ChartPanelProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ISeriesApi<'Line'> | null>(null)
  const dataRef = useRef<LineData<Time>[]>([])
  const currentSymbolRef = useRef<string | null>(null)

  // Initialize chart
  useEffect(() => {
    if (!chartContainerRef.current) return

    // Create chart
    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 400,
      layout: {
        background: { type: ColorType.Solid, color: '#1e1e1e' },
        textColor: '#d1d4dc',
      },
      grid: {
        vertLines: { color: '#2b2b43' },
        horzLines: { color: '#2b2b43' },
      },
      timeScale: {
        borderColor: '#485c7b',
        timeVisible: true,
        secondsVisible: true,
      },
      rightPriceScale: {
        borderColor: '#485c7b',
      },
      crosshair: {
        mode: 0,
      },
    })

    chartRef.current = chart

    // Add line series for trades
    const series = chart.addSeries(LineSeries, {
      color: '#2962FF',
      lineWidth: 2,
      priceLineVisible: true,
      lastValueVisible: true,
      crosshairMarkerVisible: true,
      crosshairMarkerRadius: 4,
    })

    seriesRef.current = series

    // Handle resize
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        })
      }
    }

    const resizeObserver = new ResizeObserver(handleResize)
    resizeObserver.observe(chartContainerRef.current)

    return () => {
      resizeObserver.disconnect()
      chart.remove()
      chartRef.current = null
      seriesRef.current = null
      dataRef.current = []
    }
  }, [])

  // Reset data when symbol changes
  useEffect(() => {
    if (!seriesRef.current) return

    if (symbol !== currentSymbolRef.current) {
      // Symbol changed, clear data
      dataRef.current = []
      seriesRef.current.setData([])
      currentSymbolRef.current = symbol
    }
  }, [symbol])

  // Handle real-time trade updates
  useEffect(() => {
    if (!seriesRef.current || !latestTrade || latestTrade.symbol !== symbol) return
    if (typeof latestTrade.price !== 'number' || typeof latestTrade.timestamp !== 'number') return

    const newTime = Math.floor(latestTrade.timestamp / 1000) as Time

    // Check if this is an exact duplicate (same time and value)
    const lastPoint = dataRef.current[dataRef.current.length - 1]
    if (lastPoint && lastPoint.time === newTime && lastPoint.value === latestTrade.price) {
      return // Skip exact duplicate
    }

    // If same timestamp but different value, update the existing point instead of adding
    if (lastPoint && lastPoint.time === newTime) {
      // Update the last point's value (use latest price for that second)
      lastPoint.value = latestTrade.price
      try {
        seriesRef.current.update(lastPoint)
      } catch {
        // Ignore update errors
      }
      return
    }

    const newPoint: LineData<Time> = {
      time: newTime,
      value: latestTrade.price,
    }

    // Use update for real-time data
    try {
      seriesRef.current.update(newPoint)
      dataRef.current.push(newPoint)

      // Keep only last 1000 points to prevent memory issues
      if (dataRef.current.length > 1000) {
        dataRef.current = dataRef.current.slice(-1000)
        seriesRef.current.setData(dataRef.current)
      }
    } catch {
      // If update fails (e.g., out of order), deduplicate and rebuild
      dataRef.current.push(newPoint)
      // Sort by time and deduplicate (keep last value for each timestamp)
      const deduped = new Map<number, LineData<Time>>()
      for (const pt of dataRef.current) {
        deduped.set(pt.time as number, pt)
      }
      dataRef.current = Array.from(deduped.values()).sort((a, b) => (a.time as number) - (b.time as number))
      seriesRef.current.setData(dataRef.current)
    }
  }, [latestTrade, symbol])

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <span style={{ fontSize: '14px', color: '#d1d4dc' }}>
          {symbol ? `${symbol} - Real-time Trades` : 'No Symbol Selected'}
        </span>
        {latestTrade && latestTrade.symbol === symbol && typeof latestTrade.price === 'number' && (
          <span style={{ fontSize: '12px', color: '#26a69a' }}>
            Last: ${latestTrade.price.toFixed(2)} ({latestTrade.size} shares)
          </span>
        )}
      </div>
      <div ref={chartContainerRef} />
      {!symbol && (
        <div style={{ textAlign: 'center', padding: '40px', color: '#888' }}>
          Add a symbol to see real-time trade data
        </div>
      )}
      <div style={{ marginTop: '12px', fontSize: '12px', color: '#888' }}>
        Real-time trade chart using TradingView lightweight-charts
      </div>
    </div>
  )
}
