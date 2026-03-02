/**
 * C-compatible API for Python ctypes binding.
 *
 * All functions use only C-compatible types so that Python ctypes can call
 * them directly from the compiled shared library without any additional
 * binding framework.
 *
 * Naming convention:  risk_engine_<action>
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/** Create a new RiskEngine instance and return an opaque handle. */
void* risk_engine_create(void);

/** Destroy a RiskEngine instance created by risk_engine_create(). */
void risk_engine_destroy(void* engine);

/**
 * Record a fill event.
 *
 * @param engine     Opaque handle returned by risk_engine_create()
 * @param symbol     Null-terminated ticker string (e.g. "AAPL")
 * @param side       "BUY" or "SELL"
 * @param quantity   Shares filled (must be > 0)
 * @param fill_price Execution price (must be > 0)
 * @return  0 on success, -1 on error (bad arguments)
 */
int risk_engine_on_fill(void*       engine,
                        const char* symbol,
                        const char* side,
                        long        quantity,
                        double      fill_price);

/**
 * Feed a real-time market price tick into the engine.
 *
 * @param engine  Opaque handle
 * @param symbol  Null-terminated ticker string
 * @param price   Latest market price (ignored if <= 0)
 */
void risk_engine_update_price(void* engine, const char* symbol, double price);

/** @return Unrealized PnL for symbol, or 0.0 if not tracked. */
double risk_engine_get_unrealized_pnl(void* engine, const char* symbol);

/** @return Realized PnL for symbol, or 0.0 if not tracked. */
double risk_engine_get_realized_pnl(void* engine, const char* symbol);

/** @return Sum of unrealized PnL across all open positions. */
double risk_engine_get_total_unrealized_pnl(void* engine);

/** @return Sum of realized PnL across all positions. */
double risk_engine_get_total_realized_pnl(void* engine);

/**
 * Fill a PositionInfo struct for the given symbol.
 *
 * @param out_avg_price    Output: weighted-average fill price
 * @param out_quantity     Output: net quantity (+ long, - short)
 * @param out_current_price Output: latest market price
 * @param out_realized_pnl Output: cumulative realized PnL
 * @return 1 if the symbol is tracked, 0 otherwise
 */
int risk_engine_get_position(void*       engine,
                             const char* symbol,
                             double*     out_avg_price,
                             long*       out_quantity,
                             double*     out_current_price,
                             double*     out_realized_pnl);

/** Reset all positions. */
void risk_engine_reset(void* engine);

#ifdef __cplusplus
}  // extern "C"
#endif
