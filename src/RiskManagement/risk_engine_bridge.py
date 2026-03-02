"""
RiskEngine Python bridge

Tries to load the compiled C++ shared library (librisk_engine.so) via ctypes.
If the library is not found or fails to load, falls back to a pure-Python
implementation that mirrors the C++ logic exactly.

Public interface (same for both backends):
    bridge = load_risk_engine()

    bridge.on_fill(symbol, side, quantity, fill_price)
    bridge.update_price(symbol, price)
    bridge.get_unrealized_pnl(symbol)  -> float
    bridge.get_realized_pnl(symbol)    -> float
    bridge.get_total_unrealized_pnl()  -> float
    bridge.get_total_realized_pnl()    -> float
    bridge.get_position(symbol)        -> dict | None
    bridge.reset()
"""

from __future__ import annotations

import ctypes
import logging
import os
import threading
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# C++ backend (ctypes)
# ---------------------------------------------------------------------------

_LIB_NAME = "librisk_engine.so"
_LIB_DIR = os.path.dirname(os.path.abspath(__file__))


def _try_load_lib() -> Optional[ctypes.CDLL]:
    """Attempt to load the compiled shared library."""
    lib_path = os.path.join(_LIB_DIR, _LIB_NAME)
    if not os.path.isfile(lib_path):
        return None
    try:
        lib = ctypes.CDLL(lib_path)
        _configure_lib(lib)
        return lib
    except OSError as exc:
        logger.warning("RiskEngine: failed to load %s: %s", lib_path, exc)
        return None


def _configure_lib(lib: ctypes.CDLL) -> None:
    """Set argtypes / restype for every C API function."""
    c_void_p = ctypes.c_void_p
    c_char_p = ctypes.c_char_p
    c_int    = ctypes.c_int
    c_long   = ctypes.c_long
    c_double = ctypes.c_double

    lib.risk_engine_create.argtypes = []
    lib.risk_engine_create.restype  = c_void_p

    lib.risk_engine_destroy.argtypes = [c_void_p]
    lib.risk_engine_destroy.restype  = None

    lib.risk_engine_on_fill.argtypes = [c_void_p, c_char_p, c_char_p, c_long, c_double]
    lib.risk_engine_on_fill.restype  = c_int

    lib.risk_engine_update_price.argtypes = [c_void_p, c_char_p, c_double]
    lib.risk_engine_update_price.restype  = None

    lib.risk_engine_get_unrealized_pnl.argtypes = [c_void_p, c_char_p]
    lib.risk_engine_get_unrealized_pnl.restype  = c_double

    lib.risk_engine_get_realized_pnl.argtypes = [c_void_p, c_char_p]
    lib.risk_engine_get_realized_pnl.restype  = c_double

    lib.risk_engine_get_total_unrealized_pnl.argtypes = [c_void_p]
    lib.risk_engine_get_total_unrealized_pnl.restype  = c_double

    lib.risk_engine_get_total_realized_pnl.argtypes = [c_void_p]
    lib.risk_engine_get_total_realized_pnl.restype  = c_double

    lib.risk_engine_get_position.argtypes = [
        c_void_p, c_char_p,
        ctypes.POINTER(c_double),  # out_avg_price
        ctypes.POINTER(c_long),    # out_quantity
        ctypes.POINTER(c_double),  # out_current_price
        ctypes.POINTER(c_double),  # out_realized_pnl
    ]
    lib.risk_engine_get_position.restype = c_int

    lib.risk_engine_reset.argtypes = [c_void_p]
    lib.risk_engine_reset.restype  = None


class _CppRiskEngine:
    """Thin Python wrapper around the compiled librisk_engine.so."""

    def __init__(self, lib: ctypes.CDLL):
        self._lib    = lib
        self._handle = lib.risk_engine_create()
        if not self._handle:
            raise RuntimeError("risk_engine_create() returned NULL")
        logger.info("RiskEngine: using C++ backend (%s)", _LIB_NAME)

    def __del__(self):
        if self._handle and self._lib:
            self._lib.risk_engine_destroy(self._handle)
            self._handle = None

    def on_fill(self, symbol: str, side: str, quantity: int, fill_price: float) -> None:
        rc = self._lib.risk_engine_on_fill(
            self._handle,
            symbol.encode(),
            side.encode(),
            int(quantity),
            float(fill_price),
        )
        if rc != 0:
            raise ValueError(
                f"risk_engine_on_fill failed for {symbol} {side} qty={quantity} price={fill_price}"
            )

    def update_price(self, symbol: str, price: float) -> None:
        self._lib.risk_engine_update_price(self._handle, symbol.encode(), float(price))

    def get_unrealized_pnl(self, symbol: str) -> float:
        return self._lib.risk_engine_get_unrealized_pnl(self._handle, symbol.encode())

    def get_realized_pnl(self, symbol: str) -> float:
        return self._lib.risk_engine_get_realized_pnl(self._handle, symbol.encode())

    def get_total_unrealized_pnl(self) -> float:
        return self._lib.risk_engine_get_total_unrealized_pnl(self._handle)

    def get_total_realized_pnl(self) -> float:
        return self._lib.risk_engine_get_total_realized_pnl(self._handle)

    def get_position(self, symbol: str) -> Optional[dict]:
        avg_price    = ctypes.c_double(0.0)
        quantity     = ctypes.c_long(0)
        current_price = ctypes.c_double(0.0)
        realized_pnl  = ctypes.c_double(0.0)
        found = self._lib.risk_engine_get_position(
            self._handle,
            symbol.encode(),
            ctypes.byref(avg_price),
            ctypes.byref(quantity),
            ctypes.byref(current_price),
            ctypes.byref(realized_pnl),
        )
        if not found:
            return None
        qty = quantity.value
        # Delegate unrealized PnL computation to the C++ engine (single source of truth)
        upnl = self._lib.risk_engine_get_unrealized_pnl(self._handle, symbol.encode())
        return {
            "symbol":          symbol,
            "quantity":        qty,
            "avg_fill_price":  avg_price.value,
            "current_price":   current_price.value,
            "unrealized_pnl":  upnl,
            "realized_pnl":    realized_pnl.value,
        }

    def reset(self) -> None:
        self._lib.risk_engine_reset(self._handle)


# ---------------------------------------------------------------------------
# Pure-Python fallback
# ---------------------------------------------------------------------------

class _PyPosition:
    __slots__ = ("avg_fill_price", "quantity", "current_price", "realized_pnl")

    def __init__(self):
        self.avg_fill_price: float = 0.0
        self.quantity:       int   = 0
        self.current_price:  float = 0.0
        self.realized_pnl:   float = 0.0


class _PyRiskEngine:
    """Pure-Python risk engine — same semantics as the C++ RiskEngine."""

    def __init__(self):
        self._lock:      threading.Lock              = threading.Lock()
        self._positions: Dict[str, _PyPosition]     = {}
        logger.info("RiskEngine: using pure-Python fallback (C++ library not found)")

    def on_fill(self, symbol: str, side: str, quantity: int, fill_price: float) -> None:
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        if fill_price <= 0.0:
            raise ValueError("fill_price must be positive")
        side_up = side.upper()
        is_buy  = side_up in ("BUY", "BOT")
        is_sell = side_up in ("SELL", "SLD")
        if not is_buy and not is_sell:
            raise ValueError("side must be BUY/BOT or SELL/SLD")

        with self._lock:
            if symbol not in self._positions:
                self._positions[symbol] = _PyPosition()
            pos = self._positions[symbol]

            if is_buy:
                new_qty   = pos.quantity + quantity
                total_cost = pos.avg_fill_price * pos.quantity + fill_price * quantity
                pos.avg_fill_price = total_cost / new_qty if new_qty > 0 else fill_price
                pos.quantity = new_qty
            else:
                sold = min(quantity, abs(pos.quantity))
                if pos.quantity > 0 and sold > 0:
                    pos.realized_pnl += (fill_price - pos.avg_fill_price) * sold
                pos.quantity -= quantity
                if pos.quantity < 0:
                    pos.avg_fill_price = fill_price
                elif pos.quantity == 0:
                    pos.avg_fill_price = 0.0

    def update_price(self, symbol: str, price: float) -> None:
        if price <= 0.0:
            return
        with self._lock:
            if symbol in self._positions:
                self._positions[symbol].current_price = price

    def _unrealized(self, pos: _PyPosition) -> float:
        if pos.quantity == 0 or pos.current_price <= 0.0:
            return 0.0
        return (pos.current_price - pos.avg_fill_price) * pos.quantity

    def get_unrealized_pnl(self, symbol: str) -> float:
        with self._lock:
            pos = self._positions.get(symbol)
            return self._unrealized(pos) if pos else 0.0

    def get_realized_pnl(self, symbol: str) -> float:
        with self._lock:
            pos = self._positions.get(symbol)
            return pos.realized_pnl if pos else 0.0

    def get_total_unrealized_pnl(self) -> float:
        with self._lock:
            return sum(self._unrealized(p) for p in self._positions.values())

    def get_total_realized_pnl(self) -> float:
        with self._lock:
            return sum(p.realized_pnl for p in self._positions.values())

    def get_position(self, symbol: str) -> Optional[dict]:
        with self._lock:
            pos = self._positions.get(symbol)
            if pos is None:
                return None
            upnl = self._unrealized(pos)
            return {
                "symbol":         symbol,
                "quantity":       pos.quantity,
                "avg_fill_price": pos.avg_fill_price,
                "current_price":  pos.current_price,
                "unrealized_pnl": upnl,
                "realized_pnl":   pos.realized_pnl,
            }

    def get_symbols(self) -> List[str]:
        with self._lock:
            return list(self._positions.keys())

    def reset(self) -> None:
        with self._lock:
            self._positions.clear()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def load_risk_engine():
    """
    Return the best available RiskEngine implementation.

    Prefers the compiled C++ library; falls back to pure Python.
    """
    lib = _try_load_lib()
    if lib is not None:
        return _CppRiskEngine(lib)
    return _PyRiskEngine()
