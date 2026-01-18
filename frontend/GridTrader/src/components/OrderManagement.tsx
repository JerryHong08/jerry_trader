import React, { useState, useEffect } from "react";
import { RefreshCw, Calendar } from "lucide-react";
import type {
  ModuleProps,
  Order,
  HistoricalOrder,
  OrderManagementView,
  OrderStatus,
  TimeInForce,
} from "../types";
import { useBackendTimestamp } from '../hooks/useBackendTimestamps';

// Generate mock historical orders
const generateMockOrders = (
  date: string,
): HistoricalOrder[] => {
  const symbols = [
    "AAPL",
    "TSLA",
    "NVDA",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "AMD",
  ];
  const statuses: OrderStatus[] = [
    "filled",
    "pending",
    "cancelled",
    "rejected",
    "partial",
  ];
  const tifs: TimeInForce[] = ["day", "gtc", "ioc", "fok"];
  const sides: Array<"buy" | "sell"> = ["buy", "sell"];
  const orderTypes: Array<
    "market" | "limit" | "stop" | "stop-limit"
  > = ["market", "limit", "stop", "stop-limit"];

  const orders: HistoricalOrder[] = [];
  const baseDate = new Date(date);

  for (let i = 0; i < 15; i++) {
    const symbol =
      symbols[Math.floor(Math.random() * symbols.length)];
    const side =
      sides[Math.floor(Math.random() * sides.length)];
    const orderType =
      orderTypes[Math.floor(Math.random() * orderTypes.length)];
    const quantity = Math.floor(Math.random() * 500) + 10;
    const price = Math.random() * 500 + 50;
    const status =
      statuses[Math.floor(Math.random() * statuses.length)];
    const filledQuantity =
      status === "filled"
        ? quantity
        : status === "partial"
          ? Math.floor(quantity * 0.5)
          : 0;
    const outsideRth = Math.random() > 0.7;
    const tif = tifs[Math.floor(Math.random() * tifs.length)];

    const submittedAt = new Date(
      baseDate.getTime() +
        i * 3600000 +
        Math.random() * 3600000,
    );
    const filledAt =
      status === "filled" || status === "partial"
        ? new Date(
            submittedAt.getTime() + Math.random() * 600000,
          )
        : undefined;

    orders.push({
      id: `order-${date}-${i}`,
      symbol,
      side,
      orderType,
      quantity,
      price,
      filledQuantity,
      status,
      outsideRth,
      tif,
      submittedAt: submittedAt.toISOString(),
      filledAt: filledAt?.toISOString(),
    });
  }

  return orders.sort(
    (a, b) =>
      new Date(b.submittedAt).getTime() -
      new Date(a.submittedAt).getTime(),
  );
};

const formatTimestampET = (isoDate: string): string => {
  const date = new Date(isoDate);
  const formatted = date.toLocaleString("en-US", {
    timeZone: "America/New_York",
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  return `${formatted} ET`;
};

const getStatusColor = (status: OrderStatus): string => {
  switch (status) {
    case "filled":
      return "bg-green-600";
    case "pending":
      return "bg-blue-600";
    case "cancelled":
      return "bg-gray-600";
    case "rejected":
      return "bg-red-600";
    case "partial":
      return "bg-yellow-600";
    default:
      return "bg-gray-600";
  }
};

export function OrderManagement({
  onRemove,
  selectedSymbol,
  settings,
  onSettingsChange,
}: ModuleProps) {
  const [order, setOrder] = useState<Order>({
    symbol: "",
    side: "buy",
    type: "limit",
    quantity: "",
    price: "",
  });

  const [orders, setOrders] = useState<HistoricalOrder[]>([]);
  const [selectedDate, setSelectedDate] = useState(() => {
    const today = new Date();
    return today.toISOString().split("T")[0];
  });
  const [isLoadingOrders, setIsLoadingOrders] = useState(false);
  const [activeView, setActiveView] =
    useState<OrderManagementView>(
      settings?.orderManagement?.view || "placement",
    );
  const [outsideRth, setOutsideRth] = useState(false);
  const [tif, setTif] = useState<TimeInForce>("day");

  // Backend timestamp for orders domain
  const backendTimestamp = useBackendTimestamp('orders');

  // Update symbol when selectedSymbol changes from sync
  useEffect(() => {
    if (selectedSymbol) {
      setOrder((prev) => ({ ...prev, symbol: selectedSymbol }));
    }
  }, [selectedSymbol]);

  // Update view from settings
  useEffect(() => {
    if (settings?.orderManagement?.view) {
      setActiveView(settings.orderManagement.view);
    }
  }, [settings?.orderManagement?.view]);

  // Auto-load orders when switching to orders view
  useEffect(() => {
    if (activeView === "orders" && orders.length === 0) {
      fetchOrders();
    }
  }, [activeView]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    console.log("Order submitted:", {
      ...order,
      outsideRth,
      tif,
    });
    // Handle order submission logic here
    alert(
      `Order submitted: ${order.side.toUpperCase()} ${order.quantity} ${order.symbol} at ${order.type === "limit" ? "$" + order.price : "MARKET"}`,
    );
  };

  const updateOrder = (updates: Partial<Order>) => {
    setOrder({ ...order, ...updates });
  };

  const handleViewChange = (view: OrderManagementView) => {
    setActiveView(view);
    onSettingsChange?.({ orderManagement: { view } });

    // Auto-fetch orders when switching to orders view if not already loaded
    if (view === "orders" && orders.length === 0) {
      fetchOrders();
    }
  };

  const fetchOrders = () => {
    setIsLoadingOrders(true);

    // Simulate API call to local database
    setTimeout(() => {
      const mockOrders = generateMockOrders(selectedDate);
      setOrders(mockOrders);
      setIsLoadingOrders(false);
    }, 500);
  };

  const handleDateChange = (
    e: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setSelectedDate(e.target.value);
  };

  return (
    <div className="h-full flex flex-col bg-zinc-900">
      {/* Content Area */}
      <div className="flex-1 overflow-auto p-4">
        {activeView === "placement" && (
          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Symbol */}
            <div>
              <label className="block text-sm text-gray-400 mb-1">
                Symbol
              </label>
              <input
                type="text"
                value={order.symbol}
                onChange={(e) =>
                  updateOrder({
                    symbol: e.target.value.toUpperCase(),
                  })
                }
                placeholder="AAPL"
                className="w-full bg-black border border-zinc-700 px-3 py-2 text-sm focus:outline-none focus:border-white transition-colors"
              />
            </div>

            {/* Side */}
            <div>
              <label className="block text-sm text-gray-400 mb-1">
                Side
              </label>
              <div className="grid grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={() => updateOrder({ side: "buy" })}
                  className={`py-2 text-sm transition-colors ${
                    order.side === "buy"
                      ? "bg-green-600 text-white"
                      : "bg-zinc-800 text-gray-400 hover:bg-zinc-700"
                  }`}
                >
                  Buy
                </button>
                <button
                  type="button"
                  onClick={() => updateOrder({ side: "sell" })}
                  className={`py-2 text-sm transition-colors ${
                    order.side === "sell"
                      ? "bg-red-600 text-white"
                      : "bg-zinc-800 text-gray-400 hover:bg-zinc-700"
                  }`}
                >
                  Sell
                </button>
              </div>
            </div>

            {/* Type */}
            <div>
              <label className="block text-sm text-gray-400 mb-1">
                Order Type
              </label>
              <select
                value={order.type}
                onChange={(e) =>
                  updateOrder({
                    type: e.target.value as "market" | "limit",
                  })
                }
                className="w-full bg-black border border-zinc-700 px-3 py-2 text-sm focus:outline-none focus:border-white transition-colors"
              >
                <option value="market">Market</option>
                <option value="limit">Limit</option>
              </select>
            </div>

            {/* Quantity */}
            <div>
              <label className="block text-sm text-gray-400 mb-1">
                Quantity
              </label>
              <input
                type="number"
                value={order.quantity}
                onChange={(e) =>
                  updateOrder({ quantity: e.target.value })
                }
                placeholder="100"
                className="w-full bg-black border border-zinc-700 px-3 py-2 text-sm focus:outline-none focus:border-white transition-colors"
              />
            </div>

            {/* Price (only for limit orders) */}
            {order.type === "limit" && (
              <div>
                <label className="block text-sm text-gray-400 mb-1">
                  Price
                </label>
                <input
                  type="number"
                  step="0.01"
                  value={order.price}
                  onChange={(e) =>
                    updateOrder({ price: e.target.value })
                  }
                  placeholder="150.00"
                  className="w-full bg-black border border-zinc-700 px-3 py-2 text-sm focus:outline-none focus:border-white transition-colors"
                />
              </div>
            )}

            {/* Time In Force */}
            <div>
              <label className="block text-sm text-gray-400 mb-1">
                Time In Force (TIF)
              </label>
              <select
                value={tif}
                onChange={(e) =>
                  setTif(e.target.value as TimeInForce)
                }
                className="w-full bg-black border border-zinc-700 px-3 py-2 text-sm focus:outline-none focus:border-white transition-colors"
              >
                <option value="day">Day</option>
                <option value="gtc">
                  Good 'Til Canceled (GTC)
                </option>
                <option value="ioc">
                  Immediate or Cancel (IOC)
                </option>
                <option value="fok">Fill or Kill (FOK)</option>
              </select>
            </div>

            {/* Outside RTH */}
            <div>
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={outsideRth}
                  onChange={(e) =>
                    setOutsideRth(e.target.checked)
                  }
                  className="w-4 h-4"
                />
                <span className="text-sm text-gray-400">
                  Allow Outside Regular Trading Hours
                </span>
              </label>
            </div>

            {/* Submit Button */}
            <button
              type="submit"
              className={`w-full py-3 text-sm transition-colors ${
                order.side === "buy"
                  ? "bg-green-600 hover:bg-green-700"
                  : "bg-red-600 hover:bg-red-700"
              } text-white`}
            >
              Place {order.side === "buy" ? "Buy" : "Sell"}{" "}
              Order
            </button>
          </form>
        )}

        {activeView === "orders" && (
          <>
            {/* Orders Controls */}
            <div className="mb-4">
              <div className="flex items-center justify-between mb-2">
                <div className="text-xs text-gray-500">
                  Updated: {backendTimestamp}
                </div>
              </div>
              <div className="flex items-center gap-2">
                <div className="flex items-center gap-2 flex-1">
                  <Calendar className="w-4 h-4 text-gray-500" />
                  <input
                    type="date"
                    value={selectedDate}
                    onChange={handleDateChange}
                    className="px-3 py-1.5 bg-zinc-800 border border-zinc-700 text-sm focus:outline-none focus:border-zinc-600"
                  />
                </div>
                <button
                  onClick={fetchOrders}
                  disabled={isLoadingOrders}
                  className="flex items-center gap-2 px-4 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-zinc-700 disabled:cursor-not-allowed transition-colors text-sm"
                >
                  <RefreshCw
                    className={`w-4 h-4 ${isLoadingOrders ? "animate-spin" : ""}`}
                  />
                  {isLoadingOrders ? "Loading..." : "Refresh"}
                </button>
              </div>
            </div>

            {/* Orders Table */}
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-zinc-800 border-b border-zinc-700">
                  <tr>
                    <th className="text-left p-2 text-gray-400">
                      Symbol
                    </th>
                    <th className="text-left p-2 text-gray-400">
                      Side
                    </th>
                    <th className="text-left p-2 text-gray-400">
                      Type
                    </th>
                    <th className="text-right p-2 text-gray-400">
                      Qty
                    </th>
                    <th className="text-right p-2 text-gray-400">
                      Filled
                    </th>
                    <th className="text-right p-2 text-gray-400">
                      Price
                    </th>
                    <th className="text-left p-2 text-gray-400">
                      Status
                    </th>
                    <th className="text-center p-2 text-gray-400">
                      RTH
                    </th>
                    <th className="text-left p-2 text-gray-400">
                      TIF
                    </th>
                    <th className="text-left p-2 text-gray-400">
                      Submitted
                    </th>
                    <th className="text-left p-2 text-gray-400">
                      Filled At
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {orders.length === 0 && !isLoadingOrders && (
                    <tr>
                      <td
                        colSpan={11}
                        className="text-center text-gray-500 py-8"
                      >
                        No orders found for {selectedDate}
                      </td>
                    </tr>
                  )}

                  {orders.map((order) => (
                    <tr
                      key={order.id}
                      className="border-b border-zinc-800 hover:bg-zinc-800/50 transition-colors"
                    >
                      <td className="p-2">{order.symbol}</td>
                      <td className="p-2">
                        <span
                          className={
                            order.side === "buy"
                              ? "text-green-500"
                              : "text-red-500"
                          }
                        >
                          {order.side.toUpperCase()}
                        </span>
                      </td>
                      <td className="p-2 text-gray-400">
                        {order.orderType}
                      </td>
                      <td className="p-2 text-right">
                        {order.quantity}
                      </td>
                      <td className="p-2 text-right">
                        {order.filledQuantity > 0
                          ? order.filledQuantity
                          : "-"}
                      </td>
                      <td className="p-2 text-right">
                        ${order.price.toFixed(2)}
                      </td>
                      <td className="p-2">
                        <span
                          className={`px-2 py-0.5 ${getStatusColor(order.status)} rounded text-xs`}
                        >
                          {order.status}
                        </span>
                      </td>
                      <td className="p-2 text-center">
                        {order.outsideRth ? "✓" : "✗"}
                      </td>
                      <td className="p-2 text-gray-400">
                        {order.tif.toUpperCase()}
                      </td>
                      <td className="p-2 text-gray-400 text-xs whitespace-nowrap">
                        {formatTimestampET(order.submittedAt)}
                      </td>
                      <td className="p-2 text-gray-400 text-xs whitespace-nowrap">
                        {order.filledAt
                          ? formatTimestampET(order.filledAt)
                          : "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
