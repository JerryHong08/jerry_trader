"""Integration tests for Rust BarBuilder (#[pyclass]).

Tests cover:
  - Construction (default + subset timeframes)
  - Trade ingestion → completed bar emission
  - Partial bar retrieval
  - Session-aware bar boundary truncation
  - Multi-ticker / multi-timeframe
  - VWAP calculation
  - flush() and remove_ticker()
  - Closed-session tick rejection
"""

import pytest

from jerry_trader._rust import BarBuilder

# ── Constants ────────────────────────────────────────────────────────

# 2025-01-06 (Monday) 00:00 UTC epoch ms
BASE_DAY = 1_736_121_600_000


# Helper: build epoch ms for a given hour:min:sec on that day
def _ts(h: int, m: int = 0, s: int = 0) -> int:
    return BASE_DAY + h * 3_600_000 + m * 60_000 + s * 1_000


# All 8 default timeframes
ALL_TFS = ["10s", "1m", "5m", "15m", "1h", "4h", "1d", "1w"]


# ── Construction ─────────────────────────────────────────────────────


class TestBarBuilderConstruction:
    def test_default_timeframes(self):
        b = BarBuilder()
        assert "BarBuilder" in repr(b)
        assert "tickers=0" in repr(b)

    def test_subset_timeframes(self):
        b = BarBuilder(["1m", "5m"])
        r = repr(b)
        assert '"1m"' in r
        assert '"5m"' in r
        assert '"15m"' not in r

    def test_invalid_timeframe_raises(self):
        with pytest.raises(ValueError, match="Unknown timeframe"):
            BarBuilder(["1m", "invalid"])

    def test_empty_list_uses_all(self):
        b = BarBuilder([])
        r = repr(b)
        for tf in ALL_TFS:
            assert f'"{tf}"' in r


# ── Basic trade ingestion ────────────────────────────────────────────


class TestTradeIngestion:
    def test_first_trade_no_completed(self):
        b = BarBuilder(["1m"])
        result = b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        assert result == []

    def test_same_bar_updates(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.ingest_trade("AAPL", 152.0, 200, _ts(9, 30, 15))
        b.ingest_trade("AAPL", 149.0, 50, _ts(9, 30, 30))

        bar = b.get_current_bar("AAPL", "1m")
        assert bar is not None
        assert bar["open"] == 150.0
        assert bar["high"] == 152.0
        assert bar["low"] == 149.0
        assert bar["close"] == 149.0
        assert bar["volume"] == 350.0
        assert bar["trade_count"] == 3

    def test_bar_completion(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.ingest_trade("AAPL", 151.0, 200, _ts(9, 30, 30))

        # Next minute → completes the bar
        completed = b.ingest_trade("AAPL", 152.0, 50, _ts(9, 31, 5))
        assert len(completed) == 1

        bar = completed[0]
        assert bar["ticker"] == "AAPL"
        assert bar["timeframe"] == "1m"
        assert bar["open"] == 150.0
        assert bar["high"] == 151.0
        assert bar["low"] == 150.0
        assert bar["close"] == 151.0
        assert bar["volume"] == 300.0
        assert bar["trade_count"] == 2
        assert bar["session"] == "regular"

    def test_multi_bar_completions(self):
        """Skipping a whole bar: completes the original bar + forward-fills the gap."""
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 100.0, 50, _ts(9, 30, 5))

        # Jump ahead 2 minutes → completes 09:30 bar + forward-fills 09:31 bar
        completed = b.ingest_trade("AAPL", 105.0, 30, _ts(9, 32, 5))
        assert len(completed) == 2
        # First: the real completed bar (09:30)
        assert completed[0]["open"] == 100.0
        assert completed[0]["volume"] == 50.0
        assert completed[0]["trade_count"] == 1
        # Second: the forward-filled bar (09:31) — OHLC = prev close, V=0
        assert completed[1]["open"] == 100.0
        assert completed[1]["close"] == 100.0
        assert completed[1]["volume"] == 0.0
        assert completed[1]["trade_count"] == 0


# ── VWAP ─────────────────────────────────────────────────────────────


class TestVWAP:
    def test_vwap_single_trade(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        bar = b.get_current_bar("AAPL", "1m")
        assert abs(bar["vwap"] - 150.0) < 1e-9

    def test_vwap_multiple_trades(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 100.0, 50, _ts(9, 30, 5))
        b.ingest_trade("AAPL", 110.0, 30, _ts(9, 30, 10))
        b.ingest_trade("AAPL", 90.0, 20, _ts(9, 30, 15))

        bar = b.get_current_bar("AAPL", "1m")
        expected_vwap = (100 * 50 + 110 * 30 + 90 * 20) / (50 + 30 + 20)
        assert abs(bar["vwap"] - expected_vwap) < 1e-9


# ── Session awareness ────────────────────────────────────────────────


class TestSessionAwareness:
    def test_premarket_session(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(4, 30, 5))
        bar = b.get_current_bar("AAPL", "1m")
        assert bar["session"] == "premarket"

    def test_afterhours_session(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(17, 0, 5))
        bar = b.get_current_bar("AAPL", "1m")
        assert bar["session"] == "afterhours"

    def test_closed_session_drops_tick(self):
        b = BarBuilder(["1m"])
        result = b.ingest_trade("AAPL", 150.0, 100, _ts(2, 0, 0))
        assert result == []
        assert b.get_current_bar("AAPL", "1m") is None

    def test_bar_truncated_at_session_boundary(self):
        """A 5m bar starting at 09:28 should close at 09:30 (premarket→regular)."""
        b = BarBuilder(["5m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 28, 0))

        bar = b.get_current_bar("AAPL", "5m")
        assert bar is not None
        # bar_end should be 09:30:00 (session boundary), not 09:33:00 (natural end)
        expected_end = _ts(9, 30, 0)
        assert bar["bar_end"] == expected_end

    def test_session_boundary_completes_bar(self):
        """Trade at 09:30:01 should complete the premarket bar starting at 09:28."""
        b = BarBuilder(["5m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 28, 0))

        # Next trade is in regular session
        completed = b.ingest_trade("AAPL", 151.0, 200, _ts(9, 30, 1))
        assert len(completed) == 1
        assert completed[0]["session"] == "premarket"
        assert completed[0]["close"] == 150.0

        # The new partial bar is in regular session
        bar = b.get_current_bar("AAPL", "5m")
        assert bar["session"] == "regular"
        assert bar["open"] == 151.0


# ── Multi-ticker ─────────────────────────────────────────────────────


class TestMultiTicker:
    def test_independent_tickers(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.ingest_trade("TSLA", 200.0, 50, _ts(9, 30, 10))

        assert b.ticker_count() == 2
        assert set(b.active_tickers()) == {"AAPL", "TSLA"}

        aapl = b.get_current_bar("AAPL", "1m")
        tsla = b.get_current_bar("TSLA", "1m")
        assert aapl["open"] == 150.0
        assert tsla["open"] == 200.0

    def test_completion_independent(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.ingest_trade("TSLA", 200.0, 50, _ts(9, 30, 10))

        # Only AAPL advances to next bar
        completed = b.ingest_trade("AAPL", 152.0, 30, _ts(9, 31, 5))
        assert len(completed) == 1
        assert completed[0]["ticker"] == "AAPL"

        # TSLA is still partial
        assert b.get_current_bar("TSLA", "1m")["close"] == 200.0


# ── Multi-timeframe ──────────────────────────────────────────────────


class TestMultiTimeframe:
    def test_multi_tf_partial(self):
        b = BarBuilder(["10s", "1m", "5m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))

        for tf in ["10s", "1m", "5m"]:
            bar = b.get_current_bar("AAPL", tf)
            assert bar is not None
            assert bar["timeframe"] == tf
            assert bar["open"] == 150.0

    def test_short_tf_completes_while_long_partial(self):
        """10s bar completes while 5m bar is still partial."""
        b = BarBuilder(["10s", "5m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 1))

        # 15 seconds later → 10s bar completes, 5m still partial
        completed = b.ingest_trade("AAPL", 151.0, 50, _ts(9, 30, 16))
        tfs = [c["timeframe"] for c in completed]
        assert "10s" in tfs
        assert "5m" not in tfs


# ── flush() and remove_ticker() ─────────────────────────────────────


class TestFlushAndRemove:
    def test_flush_returns_all_open_bars(self):
        b = BarBuilder(["1m", "5m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.ingest_trade("TSLA", 200.0, 50, _ts(9, 30, 10))

        flushed = b.flush()
        # 2 tickers × 2 timeframes = 4 bars
        assert len(flushed) == 4
        assert b.ticker_count() == 0

    def test_remove_ticker(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.ingest_trade("TSLA", 200.0, 50, _ts(9, 30, 10))

        removed = b.remove_ticker("AAPL")
        assert len(removed) == 1
        assert removed[0]["ticker"] == "AAPL"
        assert b.ticker_count() == 1

    def test_remove_nonexistent_ticker(self):
        b = BarBuilder(["1m"])
        removed = b.remove_ticker("NOPE")
        assert removed == []


# ── get_current_bar edge cases ───────────────────────────────────────


class TestGetCurrentBar:
    def test_unknown_ticker(self):
        b = BarBuilder(["1m"])
        assert b.get_current_bar("NOPE", "1m") is None

    def test_invalid_timeframe_raises(self):
        b = BarBuilder(["1m"])
        with pytest.raises(ValueError, match="Unknown timeframe"):
            b.get_current_bar("AAPL", "invalid")


# ── Bar dict fields completeness ────────────────────────────────────


class TestBarDictFields:
    EXPECTED_KEYS = {
        "ticker",
        "timeframe",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
        "bar_start",
        "bar_end",
        "session",
    }

    def test_completed_bar_has_all_fields(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        completed = b.ingest_trade("AAPL", 151.0, 50, _ts(9, 31, 5))
        assert len(completed) == 1
        assert set(completed[0].keys()) == self.EXPECTED_KEYS

    def test_partial_bar_has_all_fields(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        partial = b.get_current_bar("AAPL", "1m")
        assert set(partial.keys()) == self.EXPECTED_KEYS

    def test_flushed_bar_has_all_fields(self):
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        flushed = b.flush()
        assert len(flushed) == 1
        assert set(flushed[0].keys()) == self.EXPECTED_KEYS


# ── advance (wall-time bar completion) ───────────────────────────────


class TestAdvance:
    """Test BarBuilder.advance() — wall-time driven bar closure."""

    @staticmethod
    def _builder(timeframes):
        b = BarBuilder(timeframes)
        b.configure_watermark(late_arrival_ms=0, idle_close_ms=1)
        return b

    def test_no_bars_returns_empty(self):
        b = self._builder(["1m"])
        assert b.advance(_ts(10, 0, 0)) == []

    def test_unexpired_bar_not_returned(self):
        b = self._builder(["1m"])
        # Trade at 09:30:05 → 1m bar from 09:30 to 09:31
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        # At 09:30:30, bar hasn't ended yet
        expired = b.advance(_ts(9, 30, 30))
        assert expired == []
        # Bar should still be in-progress
        assert b.get_current_bar("AAPL", "1m") is not None

    def test_expired_bar_returned(self):
        b = self._builder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        # At 09:31:00, the 1m bar (09:30–09:31) is expired
        expired = b.advance(_ts(9, 31, 0))
        assert len(expired) == 1
        bar = expired[0]
        assert bar["ticker"] == "AAPL"
        assert bar["timeframe"] == "1m"
        assert bar["open"] == 150.0
        assert bar["bar_start"] == _ts(9, 30, 0)
        assert bar["bar_end"] == _ts(9, 31, 0)

    def test_expired_bar_removed_from_state(self):
        b = self._builder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.advance(_ts(9, 31, 0))
        # Bar should be gone
        assert b.get_current_bar("AAPL", "1m") is None
        # Second call should return empty
        assert b.advance(_ts(9, 31, 0)) == []

    def test_mixed_expiry_across_timeframes(self):
        b = self._builder(["1m", "5m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        # At 09:31, only 1m bar is expired (5m ends at 09:35)
        expired = b.advance(_ts(9, 31, 0))
        assert len(expired) == 1
        assert expired[0]["timeframe"] == "1m"
        # 5m bar still in progress
        assert b.get_current_bar("AAPL", "5m") is not None

    def test_all_timeframes_expire_at_boundary(self):
        b = self._builder(["1m", "5m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        # At 09:35, both 1m (09:30–09:31) and 5m (09:30–09:35) are expired
        expired = b.advance(_ts(9, 35, 0))
        assert len(expired) == 2
        tfs = {bar["timeframe"] for bar in expired}
        assert tfs == {"1m", "5m"}

    def test_multi_ticker_expiry(self):
        b = self._builder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.ingest_trade("MSFT", 300.0, 50, _ts(9, 30, 10))
        # At 09:31, both tickers' 1m bars are expired
        expired = b.advance(_ts(9, 31, 0))
        assert len(expired) == 2
        tickers = {bar["ticker"] for bar in expired}
        assert tickers == {"AAPL", "MSFT"}

    def test_ingest_after_advance_starts_new_bar(self):
        b = self._builder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        b.advance(_ts(9, 31, 0))
        # New trade at 09:31:10 starts a fresh bar
        completed = b.ingest_trade("AAPL", 152.0, 200, _ts(9, 31, 10))
        assert completed == []  # no bar completed by this trade
        bar = b.get_current_bar("AAPL", "1m")
        assert bar is not None
        assert bar["open"] == 152.0
        assert bar["bar_start"] == _ts(9, 31, 0)

    def test_advance_bar_has_all_fields(self):
        b = self._builder(["1m"])
        b.ingest_trade("AAPL", 150.0, 100, _ts(9, 30, 5))
        expired = b.advance(_ts(9, 31, 0))
        assert len(expired) == 1
        assert set(expired[0].keys()) == TestBarDictFields.EXPECTED_KEYS


# ── Forward-Fill ───────────────────────────────────────────────────


class TestForwardFill:
    """Tests for forward-fill of empty bar periods after first trade."""

    def test_no_fill_before_first_trade(self):
        """No forward-fill happens before any trade (no reference price)."""
        b = BarBuilder(["1m"])
        # First trade at 9:32 — no fill before this
        completed = b.ingest_trade("AAPL", 100.0, 50, _ts(9, 32, 5))
        assert len(completed) == 0

    def test_fill_one_gap_bar(self):
        """One missing bar period gets forward-filled."""
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 100.0, 50, _ts(9, 30, 5))

        # Trade 2 minutes later → 09:30 bar completes + 09:31 fill
        completed = b.ingest_trade("AAPL", 105.0, 30, _ts(9, 32, 5))
        assert len(completed) == 2
        # First: real bar (09:30)
        assert completed[0]["volume"] == 50.0
        # Second: forward-filled (09:31)
        assert completed[1]["open"] == 100.0
        assert completed[1]["close"] == 100.0
        assert completed[1]["high"] == 100.0
        assert completed[1]["low"] == 100.0
        assert completed[1]["volume"] == 0.0
        assert completed[1]["trade_count"] == 0

    def test_fill_multiple_gap_bars(self):
        """Multiple missing bar periods all get forward-filled."""
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 100.0, 50, _ts(9, 30, 5))

        # Trade 5 minutes later → 1 real + 4 fills
        completed = b.ingest_trade("AAPL", 105.0, 30, _ts(9, 35, 5))
        assert len(completed) == 5
        # First: real bar (09:30)
        assert completed[0]["volume"] == 50.0
        # Rest: forward-filled bars (09:31, 09:32, 09:33, 09:34)
        for i in range(1, 5):
            assert completed[i]["open"] == 100.0
            assert completed[i]["close"] == 100.0
            assert completed[i]["volume"] == 0.0
            assert completed[i]["trade_count"] == 0

    def test_fill_uses_latest_close_price(self):
        """Forward-fill uses the most recent close price, not the first."""
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 100.0, 50, _ts(9, 30, 5))
        # Second trade in same bar → updates close to 110
        b.ingest_trade("AAPL", 110.0, 30, _ts(9, 30, 30))

        # Trade 2 minutes later → fills at 110
        completed = b.ingest_trade("AAPL", 115.0, 20, _ts(9, 32, 5))
        assert len(completed) == 2
        # Fill bar uses 110 as reference
        assert completed[1]["open"] == 110.0
        assert completed[1]["close"] == 110.0

    def test_no_fill_when_no_gap(self):
        """No forward-fill when trades arrive in adjacent bars."""
        b = BarBuilder(["1m"])
        b.ingest_trade("AAPL", 100.0, 50, _ts(9, 30, 5))
        completed = b.ingest_trade("AAPL", 105.0, 30, _ts(9, 31, 5))
        # Only the real bar (09:30) completes, no fill needed
        assert len(completed) == 1
