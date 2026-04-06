"""Tests for ReturnFiller — offline return backfill for signal_events."""

import math
from unittest.mock import MagicMock

from jerry_trader.services.signal.return_fill import ReturnFiller


class TestFindCloseAt:
    """Tests for _find_close_at static method."""

    def test_exact_bar_match(self):
        bars = [
            (1000, 10.0),
            (1060, 10.5),
            (1120, 11.0),
        ]
        # target 1050 falls in bar starting at 1000
        assert ReturnFiller._find_close_at(bars, 1050) == 10.0

    def test_target_at_bar_start(self):
        bars = [
            (1000, 10.0),
            (1060, 10.5),
        ]
        assert ReturnFiller._find_close_at(bars, 1060) == 10.5

    def test_target_before_first_bar(self):
        bars = [(1_700_000_000_000, 10.0)]  # bar at epoch ms
        # target 500ms is way before first bar
        result = ReturnFiller._find_close_at(bars, 500)
        assert result is None  # too far away (> 2min before bar)

    def test_target_after_last_bar(self):
        bars = [(1_700_000_000_000, 10.0)]
        # target well after the bar ends (bar covers [t, t+60000))
        target = 1_700_000_200_000  # 200s after bar start, way past bar + fallback
        result = ReturnFiller._find_close_at(bars, target)
        assert result is None

    def test_empty_bars(self):
        assert ReturnFiller._find_close_at([], 1000) is None

    def test_fallback_nearby_bar(self):
        bars = [(1000, 10.0), (1060, 10.5)]
        # target 1015 is within bar at 1000
        assert ReturnFiller._find_close_at(bars, 1015) == 10.0


class TestReturnFillerRun:
    """Tests for ReturnFiller.run()."""

    def _make_filler(self) -> tuple[ReturnFiller, MagicMock]:
        mock_ch = MagicMock()
        filler = ReturnFiller(mock_ch)
        return filler, mock_ch

    def test_no_pending_events(self):
        filler, mock_ch = self._make_filler()
        mock_ch.query.return_value.result_rows = []
        count = filler.run()
        assert count == 0

    def test_single_event_with_bars(self):
        filler, mock_ch = self._make_filler()

        # Mock: fetch pending events
        events_result = MagicMock()
        events_result.result_rows = [
            ("uuid-1", "AAPL", 1772805736116000000, 150.0),
        ]

        # Mock: fetch bars for AAPL
        bars_result = MagicMock()
        bars_result.result_rows = [
            (1772805736000, 150.0),  # bar at trigger time
            (1772805796000, 151.5),  # +60s (1m)
            (1772806036000, 153.0),  # +300s (5m)
            (1772806636000, 155.0),  # +900s (15m)
        ]

        mock_ch.query.side_effect = [events_result, bars_result]
        mock_ch.command.return_value = None

        count = filler.run()
        assert count == 1
        # Verify UPDATE was called
        mock_ch.command.assert_called_once()
        call_sql = mock_ch.command.call_args[0][0]
        assert "ALTER TABLE signal_events" in call_sql
        assert "UPDATE" in call_sql

    def test_no_bars_for_ticker(self):
        filler, mock_ch = self._make_filler()

        events_result = MagicMock()
        events_result.result_rows = [
            ("uuid-1", "UNKNOWN", 1772805736116000000, 100.0),
        ]

        bars_result = MagicMock()
        bars_result.result_rows = []

        mock_ch.query.side_effect = [events_result, bars_result]
        count = filler.run()
        assert count == 0

    def test_no_client(self):
        filler = ReturnFiller(None)
        count = filler.run()
        assert count == 0

    def test_return_calculation(self):
        """Verify return = (exit - entry) / entry."""
        filler, mock_ch = self._make_filler()

        events_result = MagicMock()
        events_result.result_rows = [
            ("uuid-1", "AAPL", 1_000_000_000_000, 100.0),  # trigger_price=100
        ]

        bars_result = MagicMock()
        bars_result.result_rows = [
            (1_000_000, 100.0),  # entry bar
            (1_060_000, 101.0),  # +1m: close=101
            (1_300_000, 105.0),  # +5m: close=105
            (1_900_000, 110.0),  # +15m: close=110
        ]

        mock_ch.query.side_effect = [events_result, bars_result]
        mock_ch.command.return_value = None

        filler.run()

        # Check the UPDATE parameters
        call_kwargs = mock_ch.command.call_args
        params = call_kwargs[1].get("parameters") or call_kwargs.kwargs.get(
            "parameters", {}
        )

        # return_1m = (101-100)/100 = 0.01
        # return_5m = (105-100)/100 = 0.05
        # return_15m = (110-100)/100 = 0.10
        assert math.isclose(params.get("return_1m", 0), 0.01, rel_tol=1e-6)
        assert math.isclose(params.get("return_5m", 0), 0.05, rel_tol=1e-6)
        assert math.isclose(params.get("return_15m", 0), 0.10, rel_tol=1e-6)
