"""Tests for domain/market/tick.py.

Covers:
  - Tick construction and __post_init__ validation
  - Trade (Tick subclass) behavior
  - Quote construction and validation (bid/ask prices, sizes, cross)
  - Quote properties (spread, mid_price)
  - Immutability (frozen=True)
  - Edge cases: zero price, zero size, large values, empty conditions
"""

from __future__ import annotations

import pytest

from jerry_trader.domain.market.tick import Quote, Tick, Trade

# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════


def _tick(**overrides) -> Tick:
    defaults = dict(
        symbol="TEST",
        timestamp_ns=1_715_000_000_000_000_000,
        price=100.0,
        size=100,
        exchange="N",
        conditions=("@", "T"),
    )
    defaults.update(overrides)
    return Tick(**defaults)


def _trade(**overrides) -> Trade:
    defaults = dict(
        symbol="TEST",
        timestamp_ns=1_715_000_000_000_000_000,
        price=100.0,
        size=100,
        exchange="N",
        conditions=(),
    )
    defaults.update(overrides)
    return Trade(**defaults)


def _quote(**overrides) -> Quote:
    defaults = dict(
        symbol="TEST",
        timestamp_ns=1_715_000_000_000_000_000,
        bid_price=99.0,
        bid_size=500,
        ask_price=101.0,
        ask_size=300,
        exchange="N",
        conditions=(),
    )
    defaults.update(overrides)
    return Quote(**defaults)


# ══════════════════════════════════════════════════════════════════════
# Tick construction and validation
# ══════════════════════════════════════════════════════════════════════


class TestTickConstruction:
    def test_valid_tick_constructs(self):
        tick = _tick()
        assert tick.symbol == "TEST"
        assert tick.price == 100.0
        assert tick.size == 100
        assert tick.exchange == "N"

    def test_minimal_valid_tick(self):
        tick = _tick(price=0.01, size=0, conditions=())
        assert tick.price == 0.01
        assert tick.size == 0
        assert tick.conditions == ()

    def test_large_values(self):
        tick = _tick(
            price=999_999.99, size=10_000_000, timestamp_ns=9_999_999_999_999_999_999
        )
        assert tick.price == 999_999.99
        assert tick.size == 10_000_000

    def test_zero_conditions(self):
        tick = _tick(conditions=())
        assert tick.conditions == ()
        assert len(tick.conditions) == 0

    def test_many_conditions(self):
        tick = _tick(conditions=("@", "T", "I", "X", " "))
        assert len(tick.conditions) == 5

    def test_timestamp_ns_type(self):
        tick = _tick(timestamp_ns=0)
        assert tick.timestamp_ns == 0


class TestTickValidation:
    def test_negative_price_raises(self):
        with pytest.raises(ValueError, match="Price must be positive"):
            _tick(price=-1.0)

    def test_zero_price_raises(self):
        with pytest.raises(ValueError, match="Price must be positive"):
            _tick(price=0.0)

    def test_negative_size_raises(self):
        with pytest.raises(ValueError, match="Size must be non-negative"):
            _tick(size=-1)

    def test_zero_size_valid(self):
        tick = _tick(size=0)
        assert tick.size == 0


# ══════════════════════════════════════════════════════════════════════
# Trade (Tick subclass)
# ══════════════════════════════════════════════════════════════════════


class TestTrade:
    def test_trade_is_tick_subclass(self):
        trade = _trade()
        assert isinstance(trade, Tick)
        assert isinstance(trade, Trade)

    def test_trade_inherits_validation(self):
        with pytest.raises(ValueError, match="Price must be positive"):
            _trade(price=0.0)

    def test_trade_default_conditions(self):
        trade = _trade()
        assert trade.conditions == ()

    def test_trade_with_conditions(self):
        trade = _trade(conditions=("@", "T"))
        assert trade.conditions == ("@", "T")

    def test_trade_size_negative_raises(self):
        with pytest.raises(ValueError, match="Size must be non-negative"):
            _trade(size=-5)


# ══════════════════════════════════════════════════════════════════════
# Quote construction and validation
# ══════════════════════════════════════════════════════════════════════


class TestQuoteConstruction:
    def test_valid_quote_constructs(self):
        quote = _quote()
        assert quote.symbol == "TEST"
        assert quote.bid_price == 99.0
        assert quote.ask_price == 101.0
        assert quote.bid_size == 500
        assert quote.ask_size == 300

    def test_default_conditions_empty_tuple(self):
        quote = _quote()
        assert quote.conditions == ()

    def test_custom_conditions(self):
        quote = _quote(conditions=("R",))
        assert quote.conditions == ("R",)


class TestQuoteValidation:
    def test_negative_bid_price_raises(self):
        with pytest.raises(ValueError, match="Bid price must be non-negative"):
            _quote(bid_price=-0.01)

    def test_negative_ask_price_raises(self):
        with pytest.raises(ValueError, match="Ask price must be non-negative"):
            _quote(ask_price=-0.01)

    def test_negative_bid_size_raises(self):
        with pytest.raises(ValueError, match="Bid size must be non-negative"):
            _quote(bid_size=-1)

    def test_negative_ask_size_raises(self):
        with pytest.raises(ValueError, match="Ask size must be non-negative"):
            _quote(ask_size=-1)

    def test_bid_exceeds_ask_raises(self):
        with pytest.raises(ValueError, match="Bid price .* cannot exceed ask"):
            _quote(bid_price=102.0, ask_price=101.0)

    def test_bid_equals_ask_valid(self):
        """Bid == ask is a valid locked market."""
        quote = _quote(bid_price=100.0, ask_price=100.0)
        assert quote.bid_price == 100.0
        assert quote.ask_price == 100.0
        assert quote.spread == 0.0

    def test_zero_bid_price_valid(self):
        """Zero bid is allowed (no bid)."""
        quote = _quote(bid_price=0.0, ask_price=101.0)
        assert quote.bid_price == 0.0

    def test_zero_ask_price_valid(self):
        """Zero ask is allowed (no ask)."""
        quote = _quote(bid_price=99.0, ask_price=0.0)
        assert quote.ask_price == 0.0

    def test_both_zero_prices_valid(self):
        """Both prices zero skips cross check (both > 0 check fails)."""
        quote = _quote(bid_price=0.0, ask_price=0.0)
        assert quote.spread == 0.0

    def test_bid_zero_ask_positive_no_cross_check(self):
        """If bid is zero, cross check is skipped (bid_price > 0 is False)."""
        # bid=0, ask=100 is valid — cross check requires both > 0
        quote = _quote(bid_price=0.0, ask_price=100.0)
        assert quote.spread == 100.0

    def test_zero_sizes_valid(self):
        quote = _quote(bid_size=0, ask_size=0)
        assert quote.bid_size == 0
        assert quote.ask_size == 0


# ══════════════════════════════════════════════════════════════════════
# Quote properties
# ══════════════════════════════════════════════════════════════════════


class TestQuoteProperties:
    def test_spread(self):
        quote = _quote(bid_price=99.0, ask_price=101.0)
        assert quote.spread == 2.0

    def test_spread_zero_locked_market(self):
        quote = _quote(bid_price=100.0, ask_price=100.0)
        assert quote.spread == 0.0

    def test_spread_wide(self):
        quote = _quote(bid_price=10.0, ask_price=100.0)
        assert quote.spread == 90.0

    def test_mid_price(self):
        quote = _quote(bid_price=99.0, ask_price=101.0)
        assert quote.mid_price == 100.0

    def test_mid_price_locked_market(self):
        quote = _quote(bid_price=100.0, ask_price=100.0)
        assert quote.mid_price == 100.0

    def test_mid_price_one_sided_bid_zero(self):
        """Mid price with bid=0 returns half the ask."""
        quote = _quote(bid_price=0.0, ask_price=100.0)
        assert quote.mid_price == 50.0


# ══════════════════════════════════════════════════════════════════════
# Immutability
# ══════════════════════════════════════════════════════════════════════


class TestImmutability:
    def test_tick_frozen(self):
        tick = _tick()
        with pytest.raises(Exception):
            tick.price = 999.0  # type: ignore[misc]

    def test_trade_frozen(self):
        trade = _trade()
        with pytest.raises(Exception):
            trade.price = 999.0  # type: ignore[misc]

    def test_quote_frozen(self):
        quote = _quote()
        with pytest.raises(Exception):
            quote.bid_price = 999.0  # type: ignore[misc]

    def test_conditions_immutable(self):
        """Conditions is a tuple so it can't be mutated."""
        tick = _tick(conditions=("@", "T"))
        with pytest.raises(Exception):
            tick.conditions[0] = "X"  # type: ignore[index]

    def test_quote_conditions_immutable(self):
        quote = _quote(conditions=("R",))
        with pytest.raises(Exception):
            quote.conditions[0] = "X"  # type: ignore[index]


# ══════════════════════════════════════════════════════════════════════
# Hashability (frozen dataclasses are hashable)
# ══════════════════════════════════════════════════════════════════════


class TestHashability:
    def test_tick_hashable(self):
        tick = _tick()
        assert hash(tick) is not None

    def test_trade_hashable(self):
        trade = _trade()
        assert hash(trade) is not None

    def test_quote_hashable(self):
        quote = _quote()
        assert hash(quote) is not None

    def test_equal_ticks_have_equal_hash(self):
        a = _tick()
        b = _tick()
        assert hash(a) == hash(b)
        assert a == b

    def test_different_ticks_different_hash(self):
        a = _tick(price=100.0)
        b = _tick(price=101.0)
        assert a != b
