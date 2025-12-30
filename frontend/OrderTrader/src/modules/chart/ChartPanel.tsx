import { useEffect, useRef } from 'react'
import { createChart, ColorType, CandlestickSeries } from 'lightweight-charts'
import type { IChartApi, CandlestickSeriesPartialOptions } from 'lightweight-charts'

export default function ChartPanel() {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)

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
      },
    })

    chartRef.current = chart

    // Add candlestick series with sample data
    const seriesOptions: CandlestickSeriesPartialOptions = {
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
    }

    const candlestickSeries = chart.addSeries(CandlestickSeries, seriesOptions)

    // Generate sample data
    const sampleData = generateSampleData()
    candlestickSeries.setData(sampleData)

    // Fit content
    chart.timeScale().fitContent()

    // Handle resize
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        })
      }
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.remove()
    }
  }, [])

  return (
    <div>
      <div ref={chartContainerRef} />
      <div style={{ marginTop: '12px', fontSize: '12px', color: '#888' }}>
        Sample candlestick chart using TradingView lightweight-charts
      </div>
    </div>
  )
}

// Generate sample candlestick data
function generateSampleData() {
  const data = []
  const basePrice = 100
  const startTime = Math.floor(Date.now() / 1000) - 86400 * 30 // 30 days ago

  let lastClose = basePrice

  for (let i = 0; i < 100; i++) {
    const time = startTime + i * 86400
    const change = (Math.random() - 0.5) * 4
    const open = lastClose
    const close = open + change
    const high = Math.max(open, close) + Math.random() * 2
    const low = Math.min(open, close) - Math.random() * 2

    data.push({
      time: time as any,  // unix ms
      open: open,  // float
      high: high,  // float
      low: low,  // float
      close: close,  // float
    })

    lastClose = close
  }

  return data
}
