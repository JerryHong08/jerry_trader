/**
 * C API implementation — thin wrappers around risk::RiskEngine.
 */

#include "risk_engine_c_api.h"
#include "risk_engine.h"

#include <cstring>

// Helpers to safely cast the opaque handle
static risk::RiskEngine* to_engine(void* p) {
    return static_cast<risk::RiskEngine*>(p);
}

// ---------------------------------------------------------------------------

void* risk_engine_create(void) {
    return new risk::RiskEngine();
}

void risk_engine_destroy(void* engine) {
    delete to_engine(engine);
}

int risk_engine_on_fill(void*       engine,
                        const char* symbol,
                        const char* side,
                        long        quantity,
                        double      fill_price) {
    if (!engine || !symbol || !side) {
        return -1;
    }
    try {
        to_engine(engine)->on_fill(symbol, side, quantity, fill_price);
        return 0;
    } catch (...) {
        return -1;
    }
}

void risk_engine_update_price(void* engine, const char* symbol, double price) {
    if (!engine || !symbol) {
        return;
    }
    to_engine(engine)->update_price(symbol, price);
}

double risk_engine_get_unrealized_pnl(void* engine, const char* symbol) {
    if (!engine || !symbol) {
        return 0.0;
    }
    return to_engine(engine)->get_unrealized_pnl(symbol);
}

double risk_engine_get_realized_pnl(void* engine, const char* symbol) {
    if (!engine || !symbol) {
        return 0.0;
    }
    return to_engine(engine)->get_realized_pnl(symbol);
}

double risk_engine_get_total_unrealized_pnl(void* engine) {
    if (!engine) {
        return 0.0;
    }
    return to_engine(engine)->get_total_unrealized_pnl();
}

double risk_engine_get_total_realized_pnl(void* engine) {
    if (!engine) {
        return 0.0;
    }
    return to_engine(engine)->get_total_realized_pnl();
}

int risk_engine_get_position(void*       engine,
                             const char* symbol,
                             double*     out_avg_price,
                             long*       out_quantity,
                             double*     out_current_price,
                             double*     out_realized_pnl) {
    if (!engine || !symbol) {
        return 0;
    }
    risk::Position p = to_engine(engine)->get_position(symbol);
    if (p.quantity == 0 && p.avg_fill_price == 0.0 && p.realized_pnl == 0.0) {
        // Check if the symbol is actually tracked (it may have 0 quantity after a close)
        auto syms = to_engine(engine)->get_symbols();
        bool found = false;
        for (const auto& s : syms) {
            if (s == symbol) { found = true; break; }
        }
        if (!found) {
            return 0;
        }
    }
    if (out_avg_price)     *out_avg_price     = p.avg_fill_price;
    if (out_quantity)      *out_quantity      = p.quantity;
    if (out_current_price) *out_current_price = p.current_price;
    if (out_realized_pnl)  *out_realized_pnl  = p.realized_pnl;
    return 1;
}

void risk_engine_reset(void* engine) {
    if (engine) {
        to_engine(engine)->reset();
    }
}
