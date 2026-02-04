import { useState } from "react";

interface Quote {
  symbol: string;
  bid: number;
  ask: number;
  bid_size: number;
  ask_size: number;
  timestamp: number;
}

interface Trade {
  symbol: string;
  price: number;
  size: number;
  timestamp: number;
}

interface OrderBookDashboardProps {
  symbols: string[];
  symbolData: Record<string, Partial<Record<"Q" | "T", Quote | Trade>>>;
  perSymbolEvents: Record<string, string[]>;
  onAddSymbol: (symbols: string[], events: string[]) => void;
  onRemoveSymbol: (symbol: string) => void;
}

export default function OrderBookDashboard({
  symbols,
  symbolData,
  onAddSymbol,
  onRemoveSymbol,
}: OrderBookDashboardProps) {
  const [input, setInput] = useState("");
  // selected events for new subscriptions (Q = quotes, T = trades)
  const [selectedEvents, setSelectedEvents] = useState<string[]>(["Q", "T"]);

  const addSymbol = () => {
    const newSyms = input
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
    if (newSyms.length > 0) {
      onAddSymbol(newSyms, selectedEvents);
    }
    setInput("");
  };

  const removeSymbol = (symbol: string) => {
    onRemoveSymbol(symbol);
  };

  return (
    <div className="p-4" style={{ textAlign: "left" }}>
      <div className="flex gap-2 mb-4">
        <input
          className="border p-2 flex-1"
          placeholder="Enter tickers, e.g. AAPL, NVDA"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && addSymbol()}
        />
        <button
          onClick={addSymbol}
          className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600"
        >
          Add
        </button>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {symbols.map((symbol) => {
          const data = symbolData[symbol] || {};
          const quote = data["Q"];
          const trade = data["T"];
          return (
            <div
              key={symbol}
              className="border p-4 rounded-lg shadow-sm bg-gray-50"
            >
              <div className="flex justify-between items-start mb-3">
                <h3 className="text-lg font-bold text-gray-800">{symbol}</h3>
                <button
                  className="text-red-500 hover:text-red-700 text-xl leading-none"
                  onClick={() => removeSymbol(symbol)}
                  title={`Remove ${symbol}`}
                >
                  ✕
                </button>
              </div>

              <div className="space-y-3">
                {/* Quote panel */}
                <div className="bg-white p-3 rounded border">
                  <div className="flex justify-between mb-1">
                    <span className="text-sm text-gray-600">Bid</span>
                    <span className="font-mono text-green-600 font-semibold">
                      {quote ? `$${(quote as Quote).bid}` : "—"}
                    </span>
                  </div>

                  <div className="flex justify-between mb-1">
                    <span className="text-sm text-gray-600">Ask</span>
                    <span className="font-mono text-red-600 font-semibold">
                      {quote ? `$${(quote as Quote).ask}` : "—"}
                    </span>
                  </div>

                  <div className="flex justify-between text-xs text-gray-500">
                    <span>Sizes</span>
                    <span>
                      {quote
                        ? `B:${(quote as Quote).bid_size} / A:${(quote as Quote).ask_size}`
                        : "—"}
                    </span>
                  </div>
                  <div className="flex justify-between text-xs text-gray-400 mt-1">
                    <span>Updated</span>
                    <span>
                      {quote
                        ? new Date((quote as Quote).timestamp).toLocaleTimeString()
                        : "—"}
                    </span>
                  </div>
                </div>

                {/* Trade panel */}
                <div className="bg-white p-3 rounded border">
                  <div className="flex justify-between mb-1">
                    <span className="text-sm text-gray-600">Last Trade</span>
                    <span className="font-mono text-gray-800 font-semibold">
                      {trade ? `$${(trade as Trade).price}` : "—"}
                    </span>
                  </div>
                  <div className="flex justify-between text-xs text-gray-500">
                    <span>Size</span>
                    <span>{trade ? (trade as Trade).size : "—"}</span>
                  </div>
                  <div className="flex justify-between text-xs text-gray-400 mt-1">
                    <span>Time</span>
                    <span>
                      {trade
                        ? new Date((trade as Trade).timestamp).toLocaleTimeString()
                        : "—"}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {symbols.length === 0 && (
        <div className="text-center py-12 text-gray-500">
          <p className="text-xl">No symbols added yet</p>
          <p>Add some stock symbols to start monitoring quotes</p>
        </div>
      )}

      {/* Event selection UI: global selection for new subscriptions */}
      <div className="mt-4 flex items-center gap-4">
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={selectedEvents.includes("Q")}
            onChange={(e) => {
              if (e.target.checked)
                setSelectedEvents((s) => Array.from(new Set([...s, "Q"])));
              else setSelectedEvents((s) => s.filter((x) => x !== "Q"));
            }}
          />
          Quotes (Q)
        </label>

        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={selectedEvents.includes("T")}
            onChange={(e) => {
              if (e.target.checked)
                setSelectedEvents((s) => Array.from(new Set([...s, "T"])));
              else setSelectedEvents((s) => s.filter((x) => x !== "T"));
            }}
          />
          Trades (T)
        </label>
      </div>
    </div>
  );
}
