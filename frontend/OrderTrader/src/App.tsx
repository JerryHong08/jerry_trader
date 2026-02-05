import { useEffect, useRef, useState } from 'react'
import './App.css'

import IbbotPanel from './modules/ibbot/IbbotPanel'
import OrderBookDashboard from './modules/orderbook/OrderBookDashboard'
import ChartPanel from './modules/chart/ChartPanel'

interface Quote {
  symbol: string
  bid: number
  ask: number
  bid_size: number
  ask_size: number
  timestamp: number
}

interface Trade {
  symbol: string
  price: number
  size: number
  timestamp: number
}

type SymbolData = Record<string, Partial<Record<"Q" | "T", Quote | Trade>>>

export default function App() {
  // Lifted WebSocket state to share between OrderBookDashboard and ChartPanel
  const [symbols, setSymbols] = useState<string[]>(() => {
    const saved = localStorage.getItem("symbols")
    return saved ? JSON.parse(saved) : []
  })

  const [symbolData, setSymbolData] = useState<SymbolData>({})
  const [perSymbolEvents, setPerSymbolEvents] = useState<Record<string, string[]>>(() => {
    const saved = localStorage.getItem("perSymbolEvents")
    return saved ? JSON.parse(saved) : {}
  })

  const ws = useRef<WebSocket | null>(null)

  // Track the last added symbol for the chart
  const [chartSymbol, setChartSymbol] = useState<string | null>(() => {
    const saved = localStorage.getItem("symbols")
    const syms = saved ? JSON.parse(saved) : []
    return syms.length > 0 ? syms[syms.length - 1] : null
  })

  useEffect(() => {
    ws.current = new WebSocket("ws://localhost:8000/ws/tickdata")

    ws.current.onopen = () => {
      console.log("✅ Connected to backend")
      if (symbols.length > 0) {
        const subs = symbols.map((s) => ({
          symbol: s,
          events: perSymbolEvents[s] || ["Q", "T"],
        }))
        ws.current?.send(JSON.stringify({ subscriptions: subs }))
      }
    }

    ws.current.onmessage = (event) => {
      console.log("📩 Message from server:", event.data)
      const data = JSON.parse(event.data)
      const ev = data.event_type as "Q" | "T"
      const sym = data.symbol

      setSymbolData((prev) => {
        const prevSym = prev[sym] || {}
        return {
          ...prev,
          [sym]: { ...prevSym, [ev]: data },
        }
      })
    }

    ws.current.onclose = () => console.log("❌ Disconnected")

    return () => ws.current?.close()
  }, [])

  const addSymbol = (newSyms: string[], events: string[]) => {
    const newSymbols = Array.from(new Set([...symbols, ...newSyms]))
    setSymbols(newSymbols)

    const newPerSymbolEvents = { ...perSymbolEvents }
    newSyms.forEach((s) => {
      newPerSymbolEvents[s] = events
    })
    setPerSymbolEvents(newPerSymbolEvents)
    localStorage.setItem("symbols", JSON.stringify(newSymbols))
    localStorage.setItem("perSymbolEvents", JSON.stringify(newPerSymbolEvents))

    const subs = newSyms.map((s) => ({ symbol: s, events }))
    if (subs.length > 0) ws.current?.send(JSON.stringify({ subscriptions: subs }))

    // Update chart symbol to last added
    if (newSyms.length > 0) {
      setChartSymbol(newSyms[newSyms.length - 1])
    }
  }

  const removeSymbol = (symbol: string) => {
    const updated = symbols.filter((s) => s !== symbol)
    setSymbols(updated)
    localStorage.setItem("symbols", JSON.stringify(updated))

    const events = perSymbolEvents[symbol] || ["Q"]
    ws.current?.send(JSON.stringify({ action: "unsubscribe", symbol, events }))

    const newPer = { ...perSymbolEvents }
    delete newPer[symbol]
    setPerSymbolEvents(newPer)
    localStorage.setItem("perSymbolEvents", JSON.stringify(newPer))

    setSymbolData((prev) => {
      const newData = { ...prev }
      delete newData[symbol]
      return newData
    })

    // Update chart symbol if removed
    if (chartSymbol === symbol) {
      setChartSymbol(updated.length > 0 ? updated[updated.length - 1] : null)
    }
  }

  // Get latest trade for chart symbol
  const latestTrade = chartSymbol ? (symbolData[chartSymbol]?.T as Trade | undefined) ?? null : null

  return (
    <div className="page">
      <header className="header">
        <div>
          <h1 className="title">Trading Console</h1>
          <div className="subtitle">OrderBook + IBBot in one frontend</div>
        </div>
      </header>

      <main style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <section className="card">
          <div className="cardHeader">
            <div>
              <div className="cardTitle">Trading Chart</div>
              <div className="cardMeta">TradingView lightweight-charts • Real-time trades</div>
            </div>
            {/* Symbol selector for chart */}
            {symbols.length > 0 && (
              <select
                value={chartSymbol || ''}
                onChange={(e) => setChartSymbol(e.target.value || null)}
                style={{
                  padding: '4px 8px',
                  borderRadius: '4px',
                  border: '1px solid #444',
                  background: '#2a2a2a',
                  color: '#d1d4dc',
                }}
              >
                {symbols.map((s) => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            )}
          </div>
          <div className="cardBody">
            <ChartPanel symbol={chartSymbol} latestTrade={latestTrade} />
          </div>
        </section>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
          <section className="card">
            <div className="cardHeader">
              <div>
                <div className="cardTitle">OrderBook</div>
                <div className="cardMeta">Market data demo</div>
              </div>
            </div>
            <div className="cardBody">
              <OrderBookDashboard
                symbols={symbols}
                symbolData={symbolData}
                perSymbolEvents={perSymbolEvents}
                onAddSymbol={addSymbol}
                onRemoveSymbol={removeSymbol}
              />
            </div>
          </section>

          <section className="card">
            <div className="cardHeader">
              <div>
                <div className="cardTitle">IBBot</div>
                <div className="cardMeta">Orders + Portfolio + WS status</div>
              </div>
            </div>
            <div className="cardBody">
              <IbbotPanel
                chartSymbol={chartSymbol}
                lastTradePrice={latestTrade?.price ?? null}
              />
            </div>
          </section>
        </div>
      </main>
    </div>
  )
}
