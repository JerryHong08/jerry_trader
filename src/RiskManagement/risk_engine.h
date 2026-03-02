/**
 * RiskEngine - Real-time PnL calculation engine
 *
 * Thread-safe C++ engine that tracks filled positions and computes
 * real-time unrealized/realized PnL as market prices update.
 *
 * Usage (from Python via ctypes bridge):
 *   1. on_fill()        — called when an order is filled
 *   2. update_price()   — called each time a tick arrives from tickDataManager
 *   3. get_pnl()        — query current unrealized PnL for a symbol
 */

#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace risk {

/** Per-symbol position state */
struct Position {
    double avg_fill_price = 0.0;  ///< Weighted-average fill price
    long   quantity       = 0;    ///< Net shares: positive = long, negative = short
    double current_price  = 0.0;  ///< Latest market price from tick feed
    double realized_pnl   = 0.0;  ///< Cumulative realized PnL for this symbol
};

/**
 * RiskEngine
 *
 * Responsibilities:
 *   - Maintain a map of Position records keyed by symbol
 *   - Update average fill price on each fill event
 *   - Compute realized PnL when a position is reduced/closed
 *   - Recompute unrealized PnL whenever a market price tick arrives
 */
class RiskEngine {
public:
    RiskEngine() = default;
    ~RiskEngine() = default;

    // Non-copyable, non-movable (owns a mutex)
    RiskEngine(const RiskEngine&)            = delete;
    RiskEngine& operator=(const RiskEngine&) = delete;

    /**
     * Record a fill event.
     *
     * @param symbol     Ticker symbol (e.g. "AAPL")
     * @param side       "BUY" or "SELL" (case-insensitive)
     * @param quantity   Number of shares filled (always positive)
     * @param fill_price Execution price
     */
    void on_fill(const std::string& symbol,
                 const std::string& side,
                 long               quantity,
                 double             fill_price);

    /**
     * Update the latest market price for a symbol (called on each tick).
     *
     * @param symbol Ticker symbol
     * @param price  Latest bid/ask mid or last trade price
     */
    void update_price(const std::string& symbol, double price);

    /**
     * Get unrealized PnL for a single symbol.
     * Returns 0.0 if the symbol is not tracked.
     */
    double get_unrealized_pnl(const std::string& symbol) const;

    /**
     * Get realized PnL for a single symbol.
     * Returns 0.0 if the symbol is not tracked.
     */
    double get_realized_pnl(const std::string& symbol) const;

    /** Sum of unrealized PnL across all open positions. */
    double get_total_unrealized_pnl() const;

    /** Sum of realized PnL across all symbols. */
    double get_total_realized_pnl() const;

    /**
     * Copy the Position record for a symbol.
     * Returns a zeroed Position if the symbol is unknown.
     */
    Position get_position(const std::string& symbol) const;

    /** List all tracked symbol names. */
    std::vector<std::string> get_symbols() const;

    /** Remove all positions (for testing / reset). */
    void reset();

private:
    mutable std::mutex                        mutex_;
    std::unordered_map<std::string, Position> positions_;

    /** Compute unrealized PnL from position data (no lock required). */
    static double calc_unrealized(const Position& p) noexcept;
};

}  // namespace risk
