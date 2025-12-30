import { useEffect, useRef, useState } from "react";

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

export default function OrderBookDashboard() {
  // local storage for symbols
  const [symbols, setSymbols] = useState<string[]>(() => {
    const saved = localStorage.getItem("symbols");
    return saved ? JSON.parse(saved) : [];
  });

  const [input, setInput] = useState("");
  const ws = useRef<WebSocket | null>(null);
  // data per symbol, grouping events by event_type: { [symbol]: { Q?: Quote, T?: Trade } }
  const [symbolData, setSymbolData] = useState<
    Record<string, Partial<Record<"Q" | "T", Quote | Trade>>>
  >({});
  // per-symbol events that were subscribed (persisted)
  const [perSymbolEvents, setPerSymbolEvents] = useState<
    Record<string, string[]>
  >(() => {
    const saved = localStorage.getItem("perSymbolEvents");
    return saved ? JSON.parse(saved) : {};
  });
  // selected events for new subscriptions (Q = quotes, T = trades)
  const [selectedEvents, setSelectedEvents] = useState<string[]>(["Q", "T"]);

  useEffect(() => {
    ws.current = new WebSocket("ws://localhost:8000/ws/tickdata");

    ws.current.onopen = () => {
      console.log("✅ Connected to backend");
      // on connect, re-subscribe stored symbols with their events
      if (symbols.length > 0) {
        // build subscriptions array
        const subs = symbols.map((s) => ({
          symbol: s,
          events: perSymbolEvents[s] || ["Q"],
        }));
        ws.current?.send(JSON.stringify({ subscriptions: subs }));
      }
    };

    ws.current.onmessage = (event) => {
      // messages include an event_type field now
      console.log("📩 Message from server:", event.data);
      const data = JSON.parse(event.data);
      const ev = data.event_type;
      const sym = data.symbol;

      setSymbolData((prev) => {
        const prevSym = prev[sym] || {};
        return {
          ...prev,
          [sym]: { ...prevSym, [ev]: data },
        };
      });
    };

    ws.current.onclose = () => console.log("❌ Disconnected");

    return () => ws.current?.close();
  }, []);

  const addSymbol = () => {
    const newSyms = input
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
    const newSymbols = Array.from(new Set([...symbols, ...newSyms]));
    setSymbols(newSymbols);
    // save per-symbol events for each newly added symbol
    const newPerSymbolEvents = { ...perSymbolEvents };
    newSyms.forEach((s) => {
      newPerSymbolEvents[s] = selectedEvents;
    });
    setPerSymbolEvents(newPerSymbolEvents);
    localStorage.setItem("symbols", JSON.stringify(newSymbols));
    localStorage.setItem("perSymbolEvents", JSON.stringify(newPerSymbolEvents));

    // send subscriptions to server in the shape: { subscriptions: [{symbol, events}, ...] }
    const subs = newSyms.map((s) => ({ symbol: s, events: selectedEvents }));
    if (subs.length > 0) ws.current?.send(JSON.stringify({ subscriptions: subs }));
    setInput("");
  };

  const removeSymbol = (symbol: string) => {
    const updated = symbols.filter((s) => s !== symbol);
    setSymbols(updated);
    localStorage.setItem("symbols", JSON.stringify(updated));
    // send unsubscribe with events for this symbol if we have them
    const events = perSymbolEvents[symbol] || ["Q"];
    ws.current?.send(JSON.stringify({ action: "unsubscribe", symbol, events }));
    console.log("unsubscribe:", symbol);

    // remove saved per-symbol events
    const newPer = { ...perSymbolEvents };
    delete newPer[symbol];
    setPerSymbolEvents(newPer);
    localStorage.setItem("perSymbolEvents", JSON.stringify(newPer));

    // clear its data
    setSymbolData((prev) => {
      const newData = { ...prev };
      delete newData[symbol];
      return newData;
    });
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
