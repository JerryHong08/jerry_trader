/**
 * RiskEngine implementation
 */

#include "risk_engine.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace risk {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return s;
}

/*static*/ double RiskEngine::calc_unrealized(const Position& p) noexcept {
    if (p.quantity == 0 || p.current_price <= 0.0) {
        return 0.0;
    }
    return (p.current_price - p.avg_fill_price) * static_cast<double>(p.quantity);
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void RiskEngine::on_fill(const std::string& symbol,
                         const std::string& side,
                         long               quantity,
                         double             fill_price) {
    if (quantity <= 0) {
        throw std::invalid_argument("quantity must be positive");
    }
    if (fill_price <= 0.0) {
        throw std::invalid_argument("fill_price must be positive");
    }

    const std::string side_upper = to_upper(side);
    const bool is_buy  = (side_upper == "BUY"  || side_upper == "BOT");
    const bool is_sell = (side_upper == "SELL" || side_upper == "SLD");

    if (!is_buy && !is_sell) {
        throw std::invalid_argument("side must be BUY/BOT or SELL/SLD");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    Position& pos = positions_[symbol];  // creates zeroed entry if absent

    if (is_buy) {
        // Weighted-average fill price for the long side
        long   new_qty        = pos.quantity + quantity;
        double total_cost     = pos.avg_fill_price * static_cast<double>(pos.quantity)
                              + fill_price * static_cast<double>(quantity);
        pos.avg_fill_price    = (new_qty > 0) ? total_cost / static_cast<double>(new_qty) : fill_price;
        pos.quantity          = new_qty;
    } else {
        // SELL: realize PnL on the portion being sold
        long sold = std::min(quantity, std::abs(pos.quantity));
        if (pos.quantity > 0 && sold > 0) {
            // Closing (part of) a long position
            pos.realized_pnl += (fill_price - pos.avg_fill_price) * static_cast<double>(sold);
        }
        pos.quantity -= quantity;
        // If now net short, reset avg_fill_price to current sell price
        if (pos.quantity < 0) {
            pos.avg_fill_price = fill_price;
        } else if (pos.quantity == 0) {
            pos.avg_fill_price = 0.0;
        }
    }
}

void RiskEngine::update_price(const std::string& symbol, double price) {
    if (price <= 0.0) {
        return;  // Ignore invalid prices silently
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = positions_.find(symbol);
    if (it != positions_.end()) {
        it->second.current_price = price;
    }
    // If symbol not yet tracked, don't create a phantom entry;
    // it will be added by on_fill() when the first order is filled.
}

double RiskEngine::get_unrealized_pnl(const std::string& symbol) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = positions_.find(symbol);
    if (it == positions_.end()) {
        return 0.0;
    }
    return calc_unrealized(it->second);
}

double RiskEngine::get_realized_pnl(const std::string& symbol) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = positions_.find(symbol);
    if (it == positions_.end()) {
        return 0.0;
    }
    return it->second.realized_pnl;
}

double RiskEngine::get_total_unrealized_pnl() const {
    std::lock_guard<std::mutex> lock(mutex_);
    double total = 0.0;
    for (const auto& kv : positions_) {
        total += calc_unrealized(kv.second);
    }
    return total;
}

double RiskEngine::get_total_realized_pnl() const {
    std::lock_guard<std::mutex> lock(mutex_);
    double total = 0.0;
    for (const auto& kv : positions_) {
        total += kv.second.realized_pnl;
    }
    return total;
}

Position RiskEngine::get_position(const std::string& symbol) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = positions_.find(symbol);
    if (it == positions_.end()) {
        return Position{};
    }
    return it->second;
}

std::vector<std::string> RiskEngine::get_symbols() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    result.reserve(positions_.size());
    for (const auto& kv : positions_) {
        result.push_back(kv.first);
    }
    return result;
}

void RiskEngine::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    positions_.clear();
}

}  // namespace risk
