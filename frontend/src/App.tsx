import './App.css'

import IbbotPanel from './modules/ibbot/IbbotPanel'
import OrderBookDashboard from './modules/orderbook/OrderBookDashboard'

export default function App() {
  return (
    <div className="page">
      <header className="header">
        <div>
          <h1 className="title">Trading Console</h1>
          <div className="subtitle">OrderBook + IBBot in one frontend</div>
        </div>
      </header>

      <main style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, alignItems: 'start' }}>
        <section className="card">
          <div className="cardHeader">
            <div>
              <div className="cardTitle">OrderBook</div>
              <div className="cardMeta">Market data demo</div>
            </div>
          </div>
          <div className="cardBody">
            <OrderBookDashboard />
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
            <IbbotPanel />
          </div>
        </section>
      </main>
    </div>
  )
}
