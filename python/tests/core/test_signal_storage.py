"""Tests for SignalStorage — ClickHouse persistence for signal events."""

import json
from unittest.mock import MagicMock, patch

import pytest

from jerry_trader.services.signal.storage import SignalStorage


class TestSignalStorage:
    """Tests for SignalStorage."""

    def test_write_signal_event_success(self):
        """Successful write to ClickHouse."""
        mock_client = MagicMock()
        storage = SignalStorage.__new__(SignalStorage)
        # Bypass __init__ by setting attrs directly
        storage._shared_client = mock_client
        storage._ch_config = {}
        storage.session_id = "test-session"

        with patch.object(storage, "_get_thread_ch_client", return_value=mock_client):
            result = storage.write_signal_event(
                rule_id="test_rule_v1",
                rule_version=1,
                symbol="AAPL",
                timeframe="trade",
                trigger_time_ns=1700000000_000000000,
                factors={"trade_rate": 250.0, "ema_20": 150.25},
                trigger_price=None,
            )

        assert result is True
        mock_client.insert.assert_called_once()
        call_args = mock_client.insert.call_args
        assert call_args[1]["column_names"][0] == "id"
        assert call_args[1]["column_names"][3] == "ticker"

        # Verify factors is JSON-serialized
        row = call_args[1]["data"][0]
        factors_json = row[7]
        factors = json.loads(factors_json)
        assert factors["trade_rate"] == 250.0
        assert factors["ema_20"] == 150.25

    def test_write_signal_event_no_client(self):
        """Returns False when no ClickHouse client."""
        storage = SignalStorage.__new__(SignalStorage)
        storage._shared_client = None
        storage._ch_config = None
        storage.session_id = "test-session"

        with patch.object(storage, "_get_thread_ch_client", return_value=None):
            result = storage.write_signal_event(
                rule_id="test_rule",
                rule_version=1,
                symbol="AAPL",
                timeframe="trade",
                trigger_time_ns=1000,
                factors={},
            )
            assert result is False

    def test_write_signal_event_insert_fails(self):
        """Returns False when insert raises exception."""
        mock_client = MagicMock()
        mock_client.insert.side_effect = Exception("connection lost")
        storage = SignalStorage.__new__(SignalStorage)
        storage._shared_client = mock_client
        storage._ch_config = {}
        storage.session_id = "test-session"

        with patch.object(storage, "_get_thread_ch_client", return_value=mock_client):
            result = storage.write_signal_event(
                rule_id="test_rule",
                rule_version=1,
                symbol="AAPL",
                timeframe="trade",
                trigger_time_ns=1000,
                factors={"x": 1.0},
            )
            assert result is False
