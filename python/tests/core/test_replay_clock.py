"""Tests for ReplayClock (Rust PyO3) and clock.py Python singleton.

Tests cover:
  - ReplayClock construction and basic time queries
  - now_ns / now_ms consistency
  - Speed multiplier (1×, 2×, 5×, 10×)
  - Pause / resume (frozen time, no-count pause duration)
  - Jump (seek to arbitrary time, jump while paused)
  - set_speed preserves position
  - Idempotent pause/resume
  - clock.py singleton: live mode, replay mode, mode switching
  - clock.py control functions: jump_to, set_speed, pause, resume
  - Error handling: control functions in live mode raise RuntimeError
  - Drift accuracy (sustained replay, measure cumulative drift)
"""

import time

import pytest

from jerry_trader import clock
from jerry_trader._rust import ReplayClock

# ── Constants ────────────────────────────────────────────────────────

# Arbitrary epoch ns — 2023-11-14 22:13:20 UTC
START_NS = 1_700_000_000_000_000_000
START_MS = START_NS // 1_000_000

# Jan 15 2026, 09:30 ET (epoch ns)
JAN15_0930_NS = 1_768_487_400_000_000_000
JAN15_0930_MS = JAN15_0930_NS // 1_000_000


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def reset_clock_after_test():
    """Ensure clock.py is in live mode after every test."""
    yield
    clock.set_live_mode()


# ══════════════════════════════════════════════════════════════════════
#  Part 1: ReplayClock (Rust #[pyclass]) direct tests
# ══════════════════════════════════════════════════════════════════════


class TestReplayClockConstruction:
    def test_basic_construction(self):
        c = ReplayClock(START_NS, 1.0)
        assert "ReplayClock(" in repr(c)
        assert "running" in repr(c)

    def test_default_speed(self):
        c = ReplayClock(START_NS)
        assert c.speed == 1.0

    def test_custom_speed(self):
        c = ReplayClock(START_NS, 2.5)
        assert c.speed == 2.5

    def test_initial_now_close_to_start(self):
        c = ReplayClock(START_NS, 1.0)
        # Immediately after construction, now_ns should be very close to START_NS
        diff = abs(c.now_ns() - START_NS)
        assert diff < 10_000_000, f"Expected <10ms diff, got {diff}ns"


class TestReplayClockTimeQueries:
    def test_now_advances(self):
        c = ReplayClock(START_NS, 1.0)
        t0 = c.now_ns()
        time.sleep(0.02)  # 20ms
        t1 = c.now_ns()
        delta = t1 - t0
        assert delta > 10_000_000, f"Expected > 10ms advance, got {delta}ns"
        assert delta < 100_000_000, f"Expected < 100ms advance, got {delta}ns"

    def test_now_ms_equals_now_ns_div_1e6(self):
        c = ReplayClock(START_NS, 1.0)
        ns = c.now_ns()
        ms = c.now_ms()
        assert ms == ns // 1_000_000

    def test_elapsed_ns(self):
        c = ReplayClock(START_NS, 1.0)
        time.sleep(0.02)
        e = c.elapsed_ns()
        assert e > 10_000_000
        assert e < 100_000_000

    def test_data_start_ts_ns_property(self):
        c = ReplayClock(START_NS, 1.0)
        assert c.data_start_ts_ns == START_NS


class TestReplayClockSpeed:
    def test_2x_speed(self):
        c = ReplayClock(START_NS, 2.0)
        time.sleep(0.05)  # 50ms wall
        elapsed = c.now_ns() - START_NS
        # At 2×, ~50ms wall → ~100ms data
        assert elapsed > 60_000_000, f"2× speed: expected >60ms data, got {elapsed}ns"
        assert elapsed < 250_000_000, f"2× speed: expected <250ms data, got {elapsed}ns"

    def test_5x_speed(self):
        c = ReplayClock(START_NS, 5.0)
        time.sleep(0.05)  # 50ms wall
        elapsed = c.now_ns() - START_NS
        # At 5×, ~50ms wall → ~250ms data
        assert elapsed > 150_000_000, f"5× speed: expected >150ms data, got {elapsed}ns"
        assert elapsed < 500_000_000, f"5× speed: expected <500ms data, got {elapsed}ns"

    def test_half_speed(self):
        c = ReplayClock(START_NS, 0.5)
        time.sleep(0.05)  # 50ms wall
        elapsed = c.now_ns() - START_NS
        # At 0.5×, ~50ms wall → ~25ms data
        assert elapsed > 10_000_000, f"0.5× speed: expected >10ms data, got {elapsed}ns"
        assert elapsed < 80_000_000, f"0.5× speed: expected <80ms data, got {elapsed}ns"


class TestReplayClockPauseResume:
    def test_pause_freezes_time(self):
        c = ReplayClock(START_NS, 1.0)
        time.sleep(0.01)
        c.pause()
        assert c.is_paused
        frozen = c.now_ns()
        time.sleep(0.05)
        assert c.now_ns() == frozen, "Time should be frozen while paused"

    def test_resume_continues(self):
        c = ReplayClock(START_NS, 1.0)
        time.sleep(0.01)
        c.pause()
        frozen = c.now_ns()
        time.sleep(0.05)
        c.resume()
        assert not c.is_paused
        time.sleep(0.01)
        after = c.now_ns()
        assert after > frozen, "Time should advance after resume"

    def test_paused_time_not_counted(self):
        c = ReplayClock(START_NS, 1.0)
        time.sleep(0.01)  # ~10ms of real elapsed
        c.pause()
        time.sleep(0.1)  # 100ms paused — should NOT count
        c.resume()
        time.sleep(0.01)  # ~10ms more
        total = c.now_ns() - START_NS
        # Should be ~20ms, not ~120ms
        assert total < 80_000_000, f"Paused time should not count: total={total}ns"

    def test_double_pause_is_idempotent(self):
        c = ReplayClock(START_NS, 1.0)
        c.pause()
        t1 = c.now_ns()
        c.pause()  # second pause
        t2 = c.now_ns()
        assert t1 == t2

    def test_double_resume_is_idempotent(self):
        c = ReplayClock(START_NS, 1.0)
        c.pause()
        c.resume()
        c.resume()  # second resume — no panic
        _ = c.now_ns()


class TestReplayClockJump:
    def test_jump_to_future(self):
        c = ReplayClock(START_NS, 1.0)
        target = START_NS + 3_600_000_000_000  # +1 hour
        c.jump_to(target)
        diff = abs(c.now_ns() - target)
        assert diff < 5_000_000, f"After jump, should be near target: diff={diff}ns"

    def test_jump_to_past(self):
        c = ReplayClock(START_NS, 1.0)
        time.sleep(0.01)
        c.jump_to(START_NS)  # back to start
        diff = abs(c.now_ns() - START_NS)
        assert diff < 5_000_000, f"After jump back, should be near start: diff={diff}ns"

    def test_jump_while_paused(self):
        c = ReplayClock(START_NS, 1.0)
        c.pause()
        target = START_NS + 7_200_000_000_000  # +2 hours
        c.jump_to(target)
        assert c.is_paused, "Should remain paused after jump"
        assert c.now_ns() == target, "Should land exactly on target when paused"
        time.sleep(0.02)
        assert c.now_ns() == target, "Should stay frozen at target"

    def test_jump_resets_elapsed(self):
        c = ReplayClock(START_NS, 1.0)
        time.sleep(0.05)
        target = START_NS + 1_000_000_000_000
        c.jump_to(target)
        elapsed = c.elapsed_ns()
        assert elapsed < 5_000_000, f"Elapsed should reset after jump: {elapsed}ns"


class TestReplayClockSetSpeed:
    def test_set_speed_preserves_position(self):
        c = ReplayClock(START_NS, 1.0)
        time.sleep(0.02)
        before = c.now_ns()
        c.set_speed(5.0)
        after = c.now_ns()
        diff = abs(after - before)
        assert diff < 5_000_000, f"Position should be preserved: diff={diff}ns"
        assert c.speed == 5.0

    def test_speed_change_affects_rate(self):
        c = ReplayClock(START_NS, 1.0)
        time.sleep(0.02)
        c.set_speed(10.0)
        t0 = c.now_ns()
        time.sleep(0.05)  # 50ms wall at 10× → ~500ms data
        t1 = c.now_ns()
        delta = t1 - t0
        assert delta > 300_000_000, f"10× speed: expected >300ms data, got {delta}ns"


class TestReplayClockRepr:
    def test_repr_running(self):
        c = ReplayClock(START_NS, 1.0)
        r = repr(c)
        assert "running" in r
        assert "1.0x" in r

    def test_repr_paused(self):
        c = ReplayClock(START_NS, 2.0)
        c.pause()
        r = repr(c)
        assert "paused" in r
        assert "2.0x" in r


# ══════════════════════════════════════════════════════════════════════
#  Part 2: clock.py Python singleton tests
# ══════════════════════════════════════════════════════════════════════


class TestClockSingletonLiveMode:
    def test_default_is_live(self):
        assert not clock.is_replay()
        assert clock.get_clock() is None

    def test_now_ms_matches_wall_clock(self):
        clk = clock.now_ms()
        wall = int(time.time() * 1000)
        assert abs(clk - wall) < 100, f"Live mode drift: {abs(clk - wall)}ms"

    def test_now_ns_matches_wall_clock(self):
        clk = clock.now_ns()
        wall = int(time.time() * 1_000_000_000)
        assert abs(clk - wall) < 100_000_000  # <100ms

    def test_now_s_matches_wall_clock(self):
        clk = clock.now_s()
        wall = time.time()
        assert abs(clk - wall) < 0.1

    def test_now_datetime_reasonable(self):
        dt = clock.now_datetime()
        assert dt.year >= 2025
        assert dt.tzinfo is not None  # should be ET


class TestClockSingletonReplayMode:
    def test_init_replay(self):
        c = clock.init_replay(JAN15_0930_NS, speed=1.0)
        assert clock.is_replay()
        assert clock.get_clock() is c
        assert isinstance(c, ReplayClock)

    def test_now_ms_returns_replay_time(self):
        clock.init_replay(JAN15_0930_NS, speed=1.0)
        ms = clock.now_ms()
        assert abs(ms - JAN15_0930_MS) < 1000  # within 1 second

    def test_now_datetime_returns_replay_time(self):
        clock.init_replay(JAN15_0930_NS, speed=1.0)
        dt = clock.now_datetime()
        assert dt.year == 2026
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 9
        assert dt.minute == 30

    def test_now_s_returns_replay_time(self):
        clock.init_replay(JAN15_0930_NS, speed=1.0)
        s = clock.now_s()
        expected_s = JAN15_0930_MS / 1000.0
        assert abs(s - expected_s) < 1.0


class TestClockSingletonModeSwitching:
    def test_live_to_replay_to_live(self):
        # Live
        assert not clock.is_replay()
        wall = clock.now_ms()

        # Switch to replay
        clock.init_replay(JAN15_0930_NS, speed=1.0)
        assert clock.is_replay()
        replay = clock.now_ms()
        assert abs(replay - JAN15_0930_MS) < 1000

        # Back to live
        clock.set_live_mode()
        assert not clock.is_replay()
        live = clock.now_ms()
        assert abs(live - int(time.time() * 1000)) < 100

    def test_multiple_init_replay_replaces(self):
        c1 = clock.init_replay(START_NS)
        c2 = clock.init_replay(JAN15_0930_NS)
        assert clock.get_clock() is c2
        assert clock.now_ms() != START_MS  # should be at Jan15


class TestClockSingletonControl:
    def test_jump(self):
        clock.init_replay(JAN15_0930_NS, speed=1.0)
        target_ns = JAN15_0930_NS + 3_600_000_000_000  # +1 hour
        clock.jump_to(target_ns)
        dt = clock.now_datetime()
        assert dt.hour == 10 and dt.minute == 30

    def test_set_speed(self):
        clock.init_replay(JAN15_0930_NS, speed=1.0)
        clock.set_speed(5.0)
        assert clock.get_clock().speed == 5.0

    def test_pause_resume(self):
        clock.init_replay(JAN15_0930_NS, speed=1.0)
        clock.pause()
        frozen = clock.now_ms()
        time.sleep(0.05)
        assert clock.now_ms() == frozen
        clock.resume()
        time.sleep(0.01)
        assert clock.now_ms() > frozen


class TestClockSingletonErrors:
    """Control functions should raise RuntimeError in live mode."""

    def test_jump_in_live_mode(self):
        with pytest.raises(RuntimeError, match="replay mode"):
            clock.jump_to(START_NS)

    def test_set_speed_in_live_mode(self):
        with pytest.raises(RuntimeError, match="replay mode"):
            clock.set_speed(2.0)

    def test_pause_in_live_mode(self):
        with pytest.raises(RuntimeError, match="replay mode"):
            clock.pause()

    def test_resume_in_live_mode(self):
        with pytest.raises(RuntimeError, match="replay mode"):
            clock.resume()


# ══════════════════════════════════════════════════════════════════════
#  Part 3: Drift / accuracy tests
# ══════════════════════════════════════════════════════════════════════


class TestReplayClockDrift:
    """Measure drift over a sustained period to verify Instant-anchored precision."""

    def test_1s_drift_at_1x(self):
        """Over 1 second of wall time at 1× speed, drift should be < 5ms."""
        c = ReplayClock(START_NS, 1.0)
        wall_start = time.monotonic_ns()
        time.sleep(1.0)
        wall_elapsed_ns = time.monotonic_ns() - wall_start
        data_elapsed_ns = c.now_ns() - START_NS
        drift_ms = abs(data_elapsed_ns - wall_elapsed_ns) / 1_000_000
        assert drift_ms < 5.0, f"1s drift at 1×: {drift_ms:.2f}ms (expected <5ms)"

    def test_1s_drift_at_2x(self):
        """Over 1 second at 2× speed, data should advance ~2s with < 10ms drift."""
        c = ReplayClock(START_NS, 2.0)
        wall_start = time.monotonic_ns()
        time.sleep(1.0)
        wall_elapsed_ns = time.monotonic_ns() - wall_start
        data_elapsed_ns = c.now_ns() - START_NS
        expected_data_ns = int(wall_elapsed_ns * 2.0)
        drift_ms = abs(data_elapsed_ns - expected_data_ns) / 1_000_000
        assert drift_ms < 10.0, f"1s drift at 2×: {drift_ms:.2f}ms (expected <10ms)"

    def test_rapid_queries_no_regression(self):
        """Rapidly querying now_ns() should never go backward."""
        c = ReplayClock(START_NS, 1.0)
        prev = c.now_ns()
        regressions = 0
        for _ in range(10_000):
            curr = c.now_ns()
            if curr < prev:
                regressions += 1
            prev = curr
        assert regressions == 0, f"Clock went backward {regressions} times"

    def test_rapid_queries_during_pause_resume(self):
        """Clock should never go backward across pause/resume cycles."""
        c = ReplayClock(START_NS, 1.0)
        prev = c.now_ns()
        regressions = 0
        for i in range(100):
            if i % 10 == 0:
                c.pause()
            if i % 10 == 5:
                c.resume()
            curr = c.now_ns()
            if curr < prev:
                regressions += 1
            prev = curr
        # Make sure to leave it running
        if c.is_paused:
            c.resume()
        assert regressions == 0, f"Clock went backward {regressions} times"
