"""Tests for services/signal/engine.py — SignalEngine.

Covers:
  - Construction and parameter handling
  - load_events() from events.yaml
  - _process_message() JSON parsing
  - _evaluate_events() matching, dedup, cooldown
  - _is_replay_mode() static method
  - trigger_count, events, anti_patterns properties
  - _on_trigger() logging and storage
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from jerry_trader.domain.event import Event, EventAction
from jerry_trader.services.signal.engine import SignalEngine


@pytest.fixture
def mock_redis():
    """Mock Redis client — never connects to a real server."""
    return MagicMock()


@pytest.fixture
def engine(mock_redis):
    """Minimal SignalEngine with defaults."""
    return SignalEngine(redis_client=mock_redis, symbols=["TEST"])


# ══════════════════════════════════════════════════════════════════════
# Construction
# ══════════════════════════════════════════════════════════════════════


class TestConstruction:
    def test_defaults(self, mock_redis):
        engine = SignalEngine(redis_client=mock_redis)
        assert engine.symbols == []
        assert "tick" in engine.timeframes
        assert engine.trigger_count == 0

    def test_symbols_uppercased(self, mock_redis):
        engine = SignalEngine(redis_client=mock_redis, symbols=["aapl", "tsla"])
        assert engine.symbols == ["AAPL", "TSLA"]

    def test_custom_timeframes(self, mock_redis):
        engine = SignalEngine(redis_client=mock_redis, timeframes=["1m", "5m"])
        assert engine.timeframes == ["1m", "5m"]

    def test_return_fill_interval_default(self, mock_redis):
        engine = SignalEngine(redis_client=mock_redis)
        assert engine._return_fill_interval_sec == 120.0

    def test_custom_return_fill_interval(self, mock_redis):
        engine = SignalEngine(redis_client=mock_redis, return_fill_interval_sec=300.0)
        assert engine._return_fill_interval_sec == 300.0


# ══════════════════════════════════════════════════════════════════════
# _is_replay_mode
# ══════════════════════════════════════════════════════════════════════


class TestIsReplayMode:
    def test_not_replay_by_default(self, mock_redis):
        engine = SignalEngine(redis_client=mock_redis)
        assert engine._is_replay_mode() is False

    @patch("jerry_trader.clock.is_replay")
    def test_is_replay_when_clock_says_so(self, mock_is_replay, mock_redis):
        mock_is_replay.return_value = True
        engine = SignalEngine(redis_client=mock_redis)
        assert engine._is_replay_mode() is True

    @patch("jerry_trader.clock.is_replay")
    def test_is_not_replay_when_clock_says_no(self, mock_is_replay, mock_redis):
        mock_is_replay.return_value = False
        engine = SignalEngine(redis_client=mock_redis)
        assert engine._is_replay_mode() is False


# ══════════════════════════════════════════════════════════════════════
# Properties (before load)
# ══════════════════════════════════════════════════════════════════════


class TestPropertiesBeforeLoad:
    def test_events_empty_before_load(self, engine):
        assert engine.events == []

    def test_anti_patterns_empty_before_load(self, engine):
        assert engine.anti_patterns == []

    def test_trigger_count_zero_before_load(self, engine):
        assert engine.trigger_count == 0


# ══════════════════════════════════════════════════════════════════════
# load_events
# ══════════════════════════════════════════════════════════════════════


class TestLoadEvents:
    def test_loads_from_real_yaml(self, engine):
        count = engine.load_events()
        assert count > 0
        assert engine._evaluator is not None
        assert len(engine.events) == count
        assert len(engine.anti_patterns) > 0

    def test_events_are_event_objects(self, engine):
        engine.load_events()
        for event in engine.events:
            assert isinstance(event, Event)

    def test_anti_patterns_have_avoid_action(self, engine):
        engine.load_events()
        for ap in engine.anti_patterns:
            assert ap.action == EventAction.AVOID

    def test_load_twice_overwrites(self, engine):
        first = engine.load_events()
        second = engine.load_events()
        assert first == second


# ══════════════════════════════════════════════════════════════════════
# _process_message
# ══════════════════════════════════════════════════════════════════════


class TestProcessMessage:
    def test_normal_message_calls_evaluate(self, engine):
        engine.load_events()
        called = {}

        def fake_evaluate(symbol, timeframe, factors, ts_ms, price):
            called.update(
                symbol=symbol,
                timeframe=timeframe,
                factors=factors,
                ts_ms=ts_ms,
                price=price,
            )

        engine._evaluate_events = fake_evaluate

        msg = {
            "type": "message",
            "data": json.dumps(
                {
                    "symbol": "AAPL",
                    "timeframe": "1m",
                    "factors": {"trade_rate": 50, "rel_vol_20": 3.0},
                    "timestamp_ms": 1700000000000,
                    "price": 150.25,
                }
            ),
        }
        engine._process_message(msg)

        assert called["symbol"] == "AAPL"
        assert called["timeframe"] == "1m"
        assert called["factors"] == {"trade_rate": 50, "rel_vol_20": 3.0}
        assert called["ts_ms"] == 1700000000000
        assert called["price"] == 150.25

    def test_message_without_symbol_skipped(self, engine):
        engine.load_events()
        called = False

        def fake_evaluate(*args, **kwargs):
            nonlocal called
            called = True

        engine._evaluate_events = fake_evaluate

        msg = {
            "type": "message",
            "data": json.dumps({"timeframe": "1m", "factors": {"x": 1}}),
        }
        engine._process_message(msg)
        assert not called

    def test_message_without_factors_skipped(self, engine):
        engine.load_events()
        called = False

        def fake_evaluate(*args, **kwargs):
            nonlocal called
            called = True

        engine._evaluate_events = fake_evaluate

        msg = {
            "type": "message",
            "data": json.dumps({"symbol": "AAPL", "factors": {}}),
        }
        engine._process_message(msg)
        assert not called

    def test_message_without_price(self, engine):
        engine.load_events()
        called = {}

        def fake_evaluate(symbol, timeframe, factors, ts_ms, price):
            called["price"] = price

        engine._evaluate_events = fake_evaluate

        msg = {
            "type": "message",
            "data": json.dumps(
                {
                    "symbol": "AAPL",
                    "timeframe": "1m",
                    "factors": {"trade_rate": 50},
                    "timestamp_ms": 1700000000000,
                }
            ),
        }
        engine._process_message(msg)
        assert called["price"] is None

    def test_invalid_json_skipped(self, engine):
        engine.load_events()
        called = False

        def fake_evaluate(*args, **kwargs):
            nonlocal called
            called = True

        engine._evaluate_events = fake_evaluate

        engine._process_message({"type": "message", "data": "not-json"})
        assert not called

    def test_missing_data_key_raises_keyerror(self, engine):
        """Production code uses message["data"] directly — KeyError is not caught."""
        engine.load_events()
        with pytest.raises(KeyError):
            engine._process_message({"type": "message"})

    def test_symbol_lowercased_to_upper(self, engine):
        engine.load_events()
        called = {}

        def fake_evaluate(symbol, timeframe, factors, ts_ms, price):
            called["symbol"] = symbol

        engine._evaluate_events = fake_evaluate

        msg = {
            "type": "message",
            "data": json.dumps(
                {
                    "symbol": "aapl",
                    "factors": {"trade_rate": 50},
                    "timestamp_ms": 1700000000000,
                }
            ),
        }
        engine._process_message(msg)
        assert called["symbol"] == "AAPL"

    def test_pmessage_type_also_processed(self, engine):
        """pmessage is the pattern-subscribe message type."""
        engine.load_events()
        called = {}

        def fake_evaluate(symbol, timeframe, factors, ts_ms, price):
            called["symbol"] = symbol

        engine._evaluate_events = fake_evaluate

        msg = {
            "type": "pmessage",
            "data": json.dumps(
                {
                    "symbol": "TSLA",
                    "factors": {"trade_rate": 100},
                    "timestamp_ms": 1700000000000,
                }
            ),
        }
        engine._process_message(msg)
        assert called["symbol"] == "TSLA"


# ══════════════════════════════════════════════════════════════════════
# _evaluate_events
# ══════════════════════════════════════════════════════════════════════


def _mock_evaluator(engine, return_event=None, return_ml_result=None):
    """Set up a mocked evaluator on the engine that returns the given results."""
    mock_eval = MagicMock()
    mock_eval.match_signal_with_ml.return_value = (return_event, return_ml_result)
    mock_eval.events = engine._evaluator.events if engine._evaluator else []
    mock_eval.anti_patterns = (
        engine._evaluator.anti_patterns if engine._evaluator else []
    )
    engine._evaluator = mock_eval
    return mock_eval


class TestEvaluateEvents:
    def test_no_evaluator_skips(self, engine):
        """When _evaluator is None, nothing happens."""
        engine._evaluate_events("AAPL", "1m", {"trade_rate": 50}, 1000, 150.0)
        assert engine.trigger_count == 0

    def test_no_match_returns_early(self, engine):
        """When no event matches, trigger count stays 0."""
        engine.load_events()
        _mock_evaluator(engine, return_event=None)
        engine._evaluate_events("AAPL", "1m", {"trade_rate": 50}, 1000, 150.0)
        assert engine.trigger_count == 0

    def test_matching_event_increments_trigger_count(self, engine):
        """A signal matching an event fires a trigger."""
        engine.load_events()
        event = engine._evaluator.events[0]
        _mock_evaluator(engine, return_event=event)

        engine._evaluate_events(
            "AAPL", "1m", {"trade_rate": 50.0, "rel_vol_20": 3.0}, 1000, 150.0
        )
        assert engine.trigger_count == 1

    def test_trigger_cooldown_prevents_duplicate(self, engine):
        """Same (event, symbol) within cooldown period fires only once."""
        engine.load_events()
        event = engine._evaluator.events[0]
        _mock_evaluator(engine, return_event=event)

        factors = {"trade_rate": 50.0, "rel_vol_20": 3.0}

        # First trigger
        engine._evaluate_events("AAPL", "1m", factors, 1000, 150.0)
        assert engine.trigger_count == 1

        # Second trigger — same event, same symbol, within cooldown
        engine._evaluate_events("AAPL", "1m", factors, 1001, 150.0)
        assert engine.trigger_count == 1  # Deduped

    def test_different_symbol_resets_cooldown(self, engine):
        """Different symbols trigger independently."""
        engine.load_events()
        event = engine._evaluator.events[0]
        _mock_evaluator(engine, return_event=event)

        factors = {"trade_rate": 50.0, "rel_vol_20": 3.0}

        engine._evaluate_events("AAPL", "1m", factors, 1000, 150.0)
        engine._evaluate_events("TSLA", "1m", factors, 1000, 150.0)
        assert engine.trigger_count == 2

    def test_cooldown_expires_after_interval(self, engine):
        """After cooldown period, same (event, symbol) fires again."""
        engine.load_events()
        event = engine._evaluator.events[0]
        _mock_evaluator(engine, return_event=event)

        factors = {"trade_rate": 50.0, "rel_vol_20": 3.0}

        # First trigger
        engine._evaluate_events("AAPL", "1m", factors, 1000, 150.0)
        assert engine.trigger_count == 1

        # Manually set last trigger far in the past to simulate expiry
        engine._last_trigger[(event.name, "AAPL")] = 0
        engine._evaluate_events("AAPL", "1m", factors, 1000, 150.0)
        assert engine.trigger_count == 2

    def test_anti_pattern_blocks_trigger(self, engine):
        """When evaluator returns None (anti-pattern hit), no trigger fires."""
        engine.load_events()
        _mock_evaluator(engine, return_event=None)

        engine._evaluate_events(
            "AAPL", "1m", {"trade_rate": 1.0, "rel_vol_20": 5.0}, 1000, 150.0
        )
        assert engine.trigger_count == 0

    def test_ml_event_with_result(self, engine):
        """ML-based event matching with MLEvaluationResult."""
        engine.load_events()
        from jerry_trader.services.backtest.event_evaluator import MLEvaluationResult

        event = engine._evaluator.events[0]
        ml_result = MLEvaluationResult(
            should_enter=True,
            expected_return=0.05,
            confidence=0.8,
            event=event,
            signal={"trade_rate": 50.0},
            reason="test",
        )
        _mock_evaluator(engine, return_event=event, return_ml_result=ml_result)

        engine._evaluate_events("AAPL", "1m", {"trade_rate": 50.0}, 1000, 150.0)
        assert engine.trigger_count == 1

    def test_different_event_names_no_cooldown(self, engine):
        """Two different events for the same symbol both trigger."""
        engine.load_events()
        events = engine._evaluator.events[:2]
        assert len(events) >= 2

        mock_eval = MagicMock()
        mock_eval.events = engine._evaluator.events
        mock_eval.anti_patterns = engine._evaluator.anti_patterns
        # Return different events on successive calls
        mock_eval.match_signal_with_ml.side_effect = [
            (events[0], None),
            (events[1], None),
        ]
        engine._evaluator = mock_eval

        factors = {"trade_rate": 50.0, "rel_vol_20": 3.0}

        engine._evaluate_events("AAPL", "1m", factors, 1000, 150.0)
        engine._evaluate_events("AAPL", "1m", factors, 1001, 150.0)
        # Both should fire because different event names bypass cooldown
        assert engine.trigger_count == 2


# ══════════════════════════════════════════════════════════════════════
# _on_trigger
# ══════════════════════════════════════════════════════════════════════


class TestOnTrigger:
    def test_on_trigger_logs_and_stores(self, engine):
        """_on_trigger writes to storage when configured."""
        engine.load_events()
        mock_storage = MagicMock()
        engine._storage = mock_storage

        event = engine.events[0]
        engine._on_trigger(
            event=event,
            symbol="AAPL",
            timeframe="1m",
            factors={"trade_rate": 50.0},
            timestamp_ms=1000,
            price=150.0,
        )

        mock_storage.write_signal_event.assert_called_once()
        call_args = mock_storage.write_signal_event.call_args[1]
        assert call_args["symbol"] == "AAPL"
        assert call_args["timeframe"] == "1m"

    def test_on_trigger_without_storage_does_not_crash(self, engine):
        """No storage configured — just log."""
        engine.load_events()
        event = engine.events[0]
        # Should not raise
        engine._on_trigger(
            event=event,
            symbol="AAPL",
            timeframe="1m",
            factors={"trade_rate": 50.0},
            timestamp_ms=1000,
            price=150.0,
        )

    def test_on_trigger_with_ml_result(self, engine):
        """Logging includes ML info when available."""
        engine.load_events()
        mock_storage = MagicMock()
        engine._storage = mock_storage

        from jerry_trader.services.backtest.event_evaluator import MLEvaluationResult

        event = engine.events[0]
        ml_result = MLEvaluationResult(
            should_enter=True,
            expected_return=0.05,
            confidence=0.8,
            event=event,
            signal={"trade_rate": 50.0},
            reason="test",
        )

        engine._on_trigger(
            event=event,
            symbol="AAPL",
            timeframe="1m",
            factors={"trade_rate": 50.0},
            timestamp_ms=1000,
            price=150.0,
            ml_result=ml_result,
        )
        mock_storage.write_signal_event.assert_called_once()
