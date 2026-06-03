"""Tests for services/data_prep/providers/fmp.py."""

import asyncio
from unittest.mock import MagicMock, patch

import polars as pl
import pytest


class TestFetchProfilesBatch:
    @pytest.mark.asyncio
    async def test_fetches_multiple_tickers(self):
        """Each ticker should produce one row in the output DataFrame."""

        def mock_json():
            return [
                {
                    "symbol": "AAPL",
                    "companyName": "Apple Inc.",
                    "sector": "Technology",
                    "country": "US",
                }
            ]

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = mock_json

        with patch("httpx.AsyncClient.get", return_value=mock_resp):
            from jerry_trader.services.data_prep.providers.fmp import (
                fetch_profiles_batch,
            )

            df = await fetch_profiles_batch(["AAPL", "TSLA"], concurrency=2)

        assert df.height == 2
        assert "symbol" in df.columns
        assert "sector" in df.columns
        assert "country" in df.columns

    @pytest.mark.asyncio
    async def test_handles_failed_ticker_gracefully(self):
        """A single failed ticker should be skipped, not crash the batch."""

        async def mock_get(url, params=None, **kwargs):
            resp = MagicMock()
            symbol = params.get("symbol", "") if params else ""
            if symbol == "FAIL":
                resp.raise_for_status.side_effect = Exception("API error")
            else:
                resp.raise_for_status = MagicMock()
                resp.json.return_value = [{"symbol": symbol, "sector": "Tech"}]
            return resp

        with patch("httpx.AsyncClient.get", side_effect=mock_get):
            from jerry_trader.services.data_prep.providers.fmp import (
                fetch_profiles_batch,
            )

            df = await fetch_profiles_batch(["AAPL", "FAIL"], concurrency=2)

        assert df.height == 1
        assert df["symbol"][0] == "AAPL"

    @pytest.mark.asyncio
    async def test_all_failures_returns_empty(self):
        """When every ticker fails, return an empty DataFrame."""
        with patch(
            "httpx.AsyncClient.get",
            side_effect=Exception("network down"),
        ):
            from jerry_trader.services.data_prep.providers.fmp import (
                fetch_profiles_batch,
            )

            df = await fetch_profiles_batch(["AAPL"], concurrency=1)

        assert df.height == 0

    @pytest.mark.asyncio
    async def test_empty_response_returns_none(self):
        """FMP returning empty list should be treated as no data."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = []

        with patch("httpx.AsyncClient.get", return_value=mock_resp):
            from jerry_trader.services.data_prep.providers.fmp import (
                fetch_profiles_batch,
            )

            df = await fetch_profiles_batch(["UNKNOWN"], concurrency=1)

        assert df.height == 0


class TestClientKwargs:
    def test_no_proxy_returns_basic_kwargs(self):
        from jerry_trader.services.data_prep.providers.fmp import _client_kwargs

        with patch.dict("os.environ", {}, clear=True):
            kwargs = _client_kwargs()
        assert "proxy" not in kwargs
        assert "timeout" in kwargs

    def test_http_proxy_added(self):
        from jerry_trader.services.data_prep.providers.fmp import _client_kwargs

        with patch.dict("os.environ", {"HTTP_PROXY": "http://proxy:8080"}, clear=True):
            kwargs = _client_kwargs()
        assert kwargs["proxy"] == "http://proxy:8080"
