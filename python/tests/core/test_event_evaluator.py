"""Tests for services/backtest/event_evaluator.py.

Covers:
  - compute_signal_density() — rolling window signal counting
  - add_signal_density_to_factor_ts() — density factor injection
  - TickerState — state transitions
  - EventEvaluator construction (from events list, missing config raises)
  - EventEvaluator.match_signal() — boolean match, anti-pattern rejection
  - EventEvaluator.filter_signals() — batch filtering
  - EventEvaluator.classify_signals() — grouping by event name
  - EventEvaluator.get_rejected_signals() — anti-pattern matching
  - EventEvaluator.match_signal_with_ml() — boolean path
  - EventEvaluator.evaluate_ticker() — FIRST_ENTRY and CONTINUOUS trigger modes
  - EventEvaluator._check_pre_condition() — pre-condition logic
  - EventEvaluator._find_closest_timestamp()
  - WATCH_TIMEOUT_MS behavior
"""

import pytest

from jerry_trader.domain.event import (
    Condition,
    Event,
    EventAction,
    EventStage,
    TriggerType,
)
from jerry_trader.services.backtest.event_evaluator import (
    EventEvaluator,
    TickerState,
    add_signal_density_to_factor_ts,
    compute_signal_density,
)

# ── Helpers ──────────────────────────────────────────────────────────


def _make_event(
    name: str = "test_event",
    conditions: list[Condition] | None = None,
    stage: EventStage = EventStage.ENTRY,
    trigger: TriggerType = TriggerType.CONTINUOUS,
    pre_condition: str | None = None,
    action: EventAction = EventAction.ACCEPT,
) -> Event:
    return Event(
        name=name,
        semantic=name,
        stage=stage,
        trigger=trigger,
        pre_condition=pre_condition,
        conditions=conditions or [],
        action=action,
    )


# ══════════════════════════════════════════════════════════════════════
# compute_signal_density
# ══════════════════════════════════════════════════════════════════════


class TestComputeSignalDensity:
    def test_empty_input(self):
        result = compute_signal_density({})
        assert result == {}

    def test_no_threshold_breaches(self):
        factor_ts = {
            1000: {"trade_rate": 50.0},
            2000: {"trade_rate": 60.0},
            3000: {"trade_rate": 40.0},
        }
        result = compute_signal_density(factor_ts, threshold=150.0)
        assert result[1000] == 0
        assert result[2000] == 0
        assert result[3000] == 0

    def test_all_above_threshold(self):
        factor_ts = {
            1000: {"trade_rate": 200.0},
            2000: {"trade_rate": 250.0},
        }
        result = compute_signal_density(factor_ts, threshold=150.0, window_minutes=5)
        assert result[1000] == 1
        assert result[2000] == 2

    def test_rolling_window_expiry(self):
        """Signals outside the rolling window are pruned."""
        window_ms = 5 * 60 * 1000  # 5 min
        base = 1_000_000
        factor_ts = {
            base: {"trade_rate": 200.0},
            base + window_ms + 1000: {"trade_rate": 200.0},  # outside window
        }
        result = compute_signal_density(factor_ts, threshold=150.0, window_minutes=5)
        # At the second timestamp the first signal is outside the window
        # so density = 1 (only this timestamp's signal counts)
        assert result[base + window_ms + 1000] == 1

    def test_missing_factor_defaults_zero(self):
        factor_ts = {1000: {"other_factor": 999}}
        result = compute_signal_density(factor_ts, threshold=150.0)
        assert result[1000] == 0

    def test_custom_base_factor(self):
        factor_ts = {
            1000: {"custom_factor": 200.0},
            2000: {"custom_factor": 200.0},
        }
        result = compute_signal_density(
            factor_ts, base_factor="custom_factor", threshold=150.0, window_minutes=5
        )
        assert result[2000] == 2


# ══════════════════════════════════════════════════════════════════════
# add_signal_density_to_factor_ts
# ══════════════════════════════════════════════════════════════════════


class TestAddSignalDensityToFactorTs:
    def test_default_configs_add_two_variants(self):
        factor_ts = {
            1000: {"trade_rate": 200.0},
            2000: {"trade_rate": 200.0},
        }
        result = add_signal_density_to_factor_ts(factor_ts)
        assert "signal_density_5m" in result[1000]
        assert "signal_density_3m" in result[2000]

    def test_custom_config(self):
        factor_ts = {1000: {"trade_rate": 200.0}}
        configs = [
            {
                "base_factor": "trade_rate",
                "threshold": 150.0,
                "window_minutes": 1,
                "name": "my_density",
            }
        ]
        result = add_signal_density_to_factor_ts(factor_ts, configs=configs)
        assert "my_density" in result[1000]

    def test_preserves_existing_factors(self):
        factor_ts = {1000: {"trade_rate": 200.0, "ema_20": 150.0}}
        result = add_signal_density_to_factor_ts(factor_ts)
        assert result[1000]["trade_rate"] == 200.0
        assert result[1000]["ema_20"] == 150.0


# ══════════════════════════════════════════════════════════════════════
# TickerState
# ══════════════════════════════════════════════════════════════════════


class TestTickerState:
    def test_default_state(self):
        ts = TickerState(symbol="AAPL")
        assert ts.symbol == "AAPL"
        assert ts.watch_triggered is False
        assert ts.entry_triggered is False
        assert ts.has_position is False
        assert ts.abandoned is False
        assert ts.triggered_events == {}

    def test_watch_then_entry_flow(self):
        ts = TickerState(symbol="AAPL")
        # WATCH triggers
        ts.watch_triggered = True
        ts.watch_time_ms = 1000
        ts.watch_event_name = "gap_up_watch"
        ts.triggered_events["gap_up_watch"] = 1000

        # ENTRY triggers
        ts.entry_triggered = True
        ts.entry_time_ms = 5000
        ts.entry_event_name = "momentum_entry"
        ts.has_position = True
        ts.entry_price = 105.50
        ts.triggered_events["momentum_entry"] = 5000

        assert ts.triggered_events == {
            "gap_up_watch": 1000,
            "momentum_entry": 5000,
        }


# ══════════════════════════════════════════════════════════════════════
# EventEvaluator construction
# ══════════════════════════════════════════════════════════════════════


class TestEventEvaluatorConstruction:
    def test_construct_with_events_list(self):
        events = [_make_event(name="e1")]
        evaluator = EventEvaluator(events=events, anti_patterns=[])
        assert len(evaluator.events) == 1
        assert evaluator.events[0].name == "e1"

    def test_construct_without_events_raises(self):
        with pytest.raises(ValueError, match="Either config_path or events"):
            EventEvaluator()

    def test_construct_with_events_and_default_anti_patterns(self):
        evaluator = EventEvaluator(events=[_make_event()])
        assert evaluator.anti_patterns == []

    def test_construct_with_anti_patterns(self):
        anti = [_make_event(name="bad", action=EventAction.AVOID)]
        evaluator = EventEvaluator(events=[_make_event()], anti_patterns=anti)
        assert len(evaluator.anti_patterns) == 1


# ══════════════════════════════════════════════════════════════════════
# EventEvaluator.match_signal()
# ══════════════════════════════════════════════════════════════════════


class TestMatchSignal:
    def test_matches_valid_event(self):
        event = _make_event(
            name="momentum",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        result = evaluator.match_signal({"trade_rate": 150})
        assert result is not None
        assert result.name == "momentum"

    def test_no_match_returns_none(self):
        event = _make_event(
            name="momentum",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        assert evaluator.match_signal({"trade_rate": 50}) is None

    def test_anti_pattern_rejects(self):
        anti = _make_event(
            name="low_volume",
            action=EventAction.AVOID,
            conditions=[Condition(factor="trade_rate", op="lt", value=10)],
        )
        valid = _make_event(
            name="valid",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[valid], anti_patterns=[anti])
        # Signal matches both valid and anti → anti wins
        assert evaluator.match_signal({"trade_rate": 5}) is None

    def test_first_matching_event_returned(self):
        e1 = _make_event(
            name="first",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        e2 = _make_event(
            name="second",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[e1, e2])
        result = evaluator.match_signal({"trade_rate": 150})
        assert result.name == "first"

    def test_session_phase_auto_computed(self):
        """When trigger_time_ns is present but session_phase is not,
        it should be auto-computed."""
        event = _make_event(
            name="mid_only",
            conditions=[Condition(factor="session_phase", op="eq", value="mid")],
        )
        evaluator = EventEvaluator(events=[event])
        # 2026-03-06 12:00 UTC = 07:00 ET → "mid" phase
        # epoch seconds for 2026-03-06 12:00:00 UTC
        import calendar as _cal

        ns = _cal.timegm((2026, 3, 6, 12, 0, 0)) * 1_000_000_000
        result = evaluator.match_signal({"trade_rate": 150, "trigger_time_ns": ns})
        assert result is not None
        assert result.name == "mid_only"


class TestFilterSignals:
    def test_filters_to_matching_only(self):
        event = _make_event(
            name="high_rate",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        signals = [
            {"trade_rate": 150},
            {"trade_rate": 50},
            {"trade_rate": 200},
        ]
        matched = evaluator.filter_signals(signals)
        assert len(matched) == 2
        for signal, ev in matched:
            assert ev.name == "high_rate"
            assert signal["trade_rate"] > 100

    def test_empty_list(self):
        evaluator = EventEvaluator(events=[_make_event()])
        assert evaluator.filter_signals([]) == []

    def test_anti_pattern_not_included(self):
        anti = _make_event(
            name="avoid_me",
            action=EventAction.AVOID,
            conditions=[Condition(factor="x", op="gt", value=0)],
        )
        evaluator = EventEvaluator(events=[], anti_patterns=[anti])
        # Signal matches anti → should be excluded from filter_signals
        # (anti is not in events list, so filter_signals won't match it anyway)
        # But if it were an event with AVOID action, it would be filtered
        avoid_event = _make_event(
            name="bad_event",
            action=EventAction.AVOID,
            conditions=[Condition(factor="x", op="gt", value=0)],
        )
        evaluator2 = EventEvaluator(events=[avoid_event])
        matched = evaluator2.filter_signals([{"x": 10}])
        assert len(matched) == 0


class TestClassifySignals:
    def test_groups_by_event_name(self):
        e1 = _make_event(
            name="type_a",
            conditions=[Condition(factor="f", op="gt", value=0)],
        )
        e2 = _make_event(
            name="type_b",
            conditions=[Condition(factor="f", op="lt", value=0)],
        )
        evaluator = EventEvaluator(events=[e1, e2])
        signals = [{"f": 5}, {"f": -3}, {"f": 10}]
        classified = evaluator.classify_signals(signals)
        assert len(classified["type_a"]) == 2
        assert len(classified["type_b"]) == 1

    def test_no_matches_returns_empty(self):
        evaluator = EventEvaluator(events=[_make_event()])
        assert evaluator.classify_signals([]) == {}


class TestGetRejectedSignals:
    def test_rejected_by_anti_pattern(self):
        anti = _make_event(
            name="bad",
            action=EventAction.AVOID,
            conditions=[Condition(factor="x", op="lt", value=0)],
        )
        evaluator = EventEvaluator(events=[], anti_patterns=[anti])
        rejected = evaluator.get_rejected_signals([{"x": -5}, {"x": 10}])
        assert len(rejected) == 1
        assert rejected[0]["x"] == -5

    def test_none_rejected(self):
        evaluator = EventEvaluator(events=[_make_event()])
        assert evaluator.get_rejected_signals([{"x": 1}]) == []


# ══════════════════════════════════════════════════════════════════════
# EventEvaluator._find_closest_timestamp
# ══════════════════════════════════════════════════════════════════════


class TestFindClosestTimestamp:
    def test_finds_first_geq(self):
        evaluator = EventEvaluator(events=[_make_event()])
        factor_ts = {1000: {}, 2000: {}, 3000: {}}
        result = evaluator._find_closest_timestamp(factor_ts, 1500)
        assert result == 2000

    def test_exact_match(self):
        evaluator = EventEvaluator(events=[_make_event()])
        factor_ts = {1000: {}, 2000: {}}
        assert evaluator._find_closest_timestamp(factor_ts, 2000) == 2000

    def test_target_after_all_returns_last(self):
        evaluator = EventEvaluator(events=[_make_event()])
        factor_ts = {1000: {}, 2000: {}}
        assert evaluator._find_closest_timestamp(factor_ts, 5000) == 2000

    def test_empty_returns_none(self):
        evaluator = EventEvaluator(events=[_make_event()])
        assert evaluator._find_closest_timestamp({}, 1000) is None


# ══════════════════════════════════════════════════════════════════════
# EventEvaluator._check_pre_condition
# ══════════════════════════════════════════════════════════════════════


class TestCheckPreCondition:
    def test_no_pre_condition_always_true(self):
        evaluator = EventEvaluator(events=[_make_event()])
        event = _make_event(pre_condition=None)
        state = TickerState(symbol="AAPL")
        assert evaluator._check_pre_condition(event, state) is True

    def test_has_position_pre_condition(self):
        evaluator = EventEvaluator(events=[_make_event()])
        event = _make_event(stage=EventStage.EXIT, pre_condition="has_position")
        state = TickerState(symbol="AAPL")
        assert evaluator._check_pre_condition(event, state) is False
        state.has_position = True
        assert evaluator._check_pre_condition(event, state) is True

    def test_specific_event_pre_condition(self):
        evaluator = EventEvaluator(events=[_make_event()])
        event = _make_event(pre_condition="gap_up_watch")
        state = TickerState(symbol="AAPL")
        assert evaluator._check_pre_condition(event, state) is False
        state.triggered_events["gap_up_watch"] = 1000
        assert evaluator._check_pre_condition(event, state) is True

    def test_entry_stage_any_watch_satisfies(self):
        evaluator = EventEvaluator(events=[_make_event()])
        event = _make_event(stage=EventStage.ENTRY, pre_condition="specific_watch")
        state = TickerState(symbol="AAPL")
        state.watch_triggered = True
        # Even though specific_watch is not in triggered_events,
        # any watch_triggered satisfies ENTRY pre-condition
        assert evaluator._check_pre_condition(event, state) is True


# ══════════════════════════════════════════════════════════════════════
# EventEvaluator.evaluate_ticker() — FIRST_ENTRY trigger
# ══════════════════════════════════════════════════════════════════════


class TestEvaluateTickerFirstEntry:
    def test_no_first_entry_returns_empty(self):
        event = _make_event(
            name="watch_event",
            stage=EventStage.WATCH,
            trigger=TriggerType.FIRST_ENTRY,
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        factor_ts = {1000: {"trade_rate": 150}}
        signals, state = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999,
            first_entry_ms=None,  # No first entry → skip
        )
        assert signals == []

    def test_matches_at_first_entry(self):
        event = _make_event(
            name="watch_event",
            stage=EventStage.WATCH,
            trigger=TriggerType.FIRST_ENTRY,
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        factor_ts = {2000: {"trade_rate": 150}}
        signals, state = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999,
            first_entry_ms=1990,  # Just before ts 2000
        )
        assert len(signals) == 1
        assert signals[0]["event_name"] == "watch_event"
        assert signals[0]["event_stage"] == "watch"
        assert state.watch_triggered is True
        assert state.watch_event_name == "watch_event"

    def test_condition_not_met_at_first_entry(self):
        event = _make_event(
            name="watch_event",
            stage=EventStage.WATCH,
            trigger=TriggerType.FIRST_ENTRY,
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        factor_ts = {2000: {"trade_rate": 50}}  # below threshold
        signals, state = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999,
            first_entry_ms=1990,
        )
        assert signals == []

    def test_already_watch_triggered_skip(self):
        event = _make_event(
            name="watch_event",
            stage=EventStage.WATCH,
            trigger=TriggerType.FIRST_ENTRY,
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        state = TickerState(symbol="AAPL", watch_triggered=True)
        factor_ts = {2000: {"trade_rate": 150}}
        signals, updated_state = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999,
            first_entry_ms=1990,
            ticker_state=state,
        )
        assert signals == []


# ══════════════════════════════════════════════════════════════════════
# EventEvaluator.evaluate_ticker() — CONTINUOUS trigger (ENTRY stage)
# ══════════════════════════════════════════════════════════════════════


class TestEvaluateTickerContinuousEntry:
    def test_entry_already_triggered_skip(self):
        event = _make_event(
            name="entry_event",
            stage=EventStage.ENTRY,
            trigger=TriggerType.CONTINUOUS,
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        state = TickerState(symbol="AAPL", entry_triggered=True)
        factor_ts = {1000: {"trade_rate": 150}}
        signals, _ = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999,
            ticker_state=state,
        )
        assert signals == []

    def test_match_in_continuous_mode(self):
        event = _make_event(
            name="entry_event",
            stage=EventStage.ENTRY,
            trigger=TriggerType.CONTINUOUS,
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        factor_ts = {
            1000: {"trade_rate": 50},
            2000: {"trade_rate": 150},
            3000: {"trade_rate": 200},
        }
        signals, state = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999,
        )
        # Only 1 signal: the guard at line 657 stops after first ENTRY match
        assert len(signals) == 1
        assert signals[0]["trigger_time_ms"] == 2000
        assert state.entry_triggered is True

    def test_outside_session_window_filtered(self):
        event = _make_event(
            name="entry_event",
            stage=EventStage.ENTRY,
            trigger=TriggerType.CONTINUOUS,
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        factor_ts = {
            1000: {"trade_rate": 150},  # < session_start, filtered
            5000: {"trade_rate": 200},  # in range
        }
        signals, _ = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=3000,
            session_end_ms=99999,
        )
        assert len(signals) == 1
        assert signals[0]["trigger_time_ms"] == 5000

    def test_watch_timeout_abandons(self):
        """After 30min without entry, TickerState.abandoned is set."""
        event = _make_event(
            name="entry_event",
            stage=EventStage.ENTRY,
            trigger=TriggerType.CONTINUOUS,
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        evaluator = EventEvaluator(events=[event])
        state = TickerState(
            symbol="AAPL",
            watch_triggered=True,
            watch_time_ms=0,
        )
        # Factor ts at 31 min → should trigger timeout and abandon
        factor_ts = {31 * 60 * 1000 + 1000: {"trade_rate": 150}}
        signals, updated_state = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999999,
            ticker_state=state,
        )
        assert updated_state.abandoned is True


# ══════════════════════════════════════════════════════════════════════
# EventEvaluator.evaluate_ticker() — EXIT stage
# ══════════════════════════════════════════════════════════════════════


class TestEvaluateTickerExit:
    def test_exit_without_position_skips(self):
        event = _make_event(
            name="take_profit",
            stage=EventStage.EXIT,
            trigger=TriggerType.CONTINUOUS,
            conditions=[Condition(factor="trade_rate", op="lt", value=50)],
        )
        evaluator = EventEvaluator(events=[event])
        state = TickerState(symbol="AAPL")  # has_position=False
        factor_ts = {1000: {"trade_rate": 30}}
        signals, _ = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999,
            ticker_state=state,
        )
        assert signals == []

    def test_exit_with_position_matches(self):
        event = _make_event(
            name="take_profit",
            stage=EventStage.EXIT,
            trigger=TriggerType.CONTINUOUS,
            conditions=[Condition(factor="trade_rate", op="lt", value=50)],
        )
        evaluator = EventEvaluator(events=[event])
        state = TickerState(
            symbol="AAPL",
            has_position=True,
            entry_triggered=True,
            entry_price=100.0,
        )
        factor_ts = {1000: {"trade_rate": 30}}
        signals, _ = evaluator.evaluate_ticker(
            event=event,
            symbol="AAPL",
            factor_ts=factor_ts,
            session_start_ms=0,
            session_end_ms=99999,
            ticker_state=state,
        )
        assert len(signals) == 1
        assert signals[0]["event_name"] == "take_profit"
        assert signals[0]["event_stage"] == "exit"
