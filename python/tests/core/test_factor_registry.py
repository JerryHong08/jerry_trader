"""Tests for services/factor/factor_registry.py.

Covers:
  - FactorDisplay construction and defaults
  - FactorSpec construction, to_dict(), defaults
  - FactorRegistry: init, load (mocked YAML), get_spec, get_all_specs,
    get_factors_by_type, get_factors_for_timeframe, create_indicator,
    create_indicators_for_type
  - Global singleton: get_factor_registry, reload_factor_registry
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from jerry_trader.services.factor.factor_registry import (
    FactorDisplay,
    FactorRegistry,
    FactorSpec,
    get_factor_registry,
    reload_factor_registry,
)

# ══════════════════════════════════════════════════════════════════════
# FactorDisplay
# ══════════════════════════════════════════════════════════════════════


class TestFactorDisplay:
    def test_defaults(self):
        d = FactorDisplay(name="Test")
        assert d.name == "Test"
        assert d.description == ""
        assert d.color == "#3b82f6"
        assert d.priceScale == "right"
        assert d.mode == "panel"

    def test_custom(self):
        d = FactorDisplay(
            name="EMA",
            description="EMA desc",
            color="#ff0000",
            priceScale="left",
            mode="overlay",
        )
        assert d.mode == "overlay"
        assert d.priceScale == "left"


# ══════════════════════════════════════════════════════════════════════
# FactorSpec
# ══════════════════════════════════════════════════════════════════════


class TestFactorSpec:
    def test_defaults(self):
        spec = FactorSpec(id="ema_20", class_name="EMA")
        assert spec.id == "ema_20"
        assert spec.class_name == "EMA"
        assert spec.rust_key == ""
        assert spec.type == ""
        assert spec.params == {}
        assert spec.timeframes == []

    def test_to_dict(self):
        spec = FactorSpec(
            id="ema_20",
            class_name="EMA",
            type="bar",
            timeframes=["1m", "5m"],
            display=FactorDisplay(name="EMA(20)", description="EMA desc"),
        )
        d = spec.to_dict()
        assert d["id"] == "ema_20"
        assert d["type"] == "bar"
        assert d["display"]["name"] == "EMA(20)"
        assert "1m" in d["timeframes"]

    def test_to_dict_default_display(self):
        spec = FactorSpec(id="test", class_name="Test")
        d = spec.to_dict()
        assert d["display"]["name"] == ""


# ══════════════════════════════════════════════════════════════════════
# FactorRegistry — construction and defaults
# ══════════════════════════════════════════════════════════════════════


class TestFactorRegistryConstruction:
    def test_default_config_path(self):
        registry = FactorRegistry()
        assert registry.config_path.name == "factors.yaml"

    def test_custom_config_path(self):
        registry = FactorRegistry(config_path="/custom/path/factors.yaml")
        assert str(registry.config_path) == "/custom/path/factors.yaml"

    def test_not_loaded_initially(self):
        registry = FactorRegistry()
        assert registry._loaded is False


# ══════════════════════════════════════════════════════════════════════
# FactorRegistry — load() with defaults (no config file)
# ══════════════════════════════════════════════════════════════════════


class TestFactorRegistryLoadDefaults:
    def test_loads_defaults_when_no_config(self):
        registry = FactorRegistry(config_path="/nonexistent/path.yaml")
        registry.load()
        assert registry._loaded is True
        assert "ema_20" in registry._specs
        assert "trade_rate" in registry._specs

    def test_load_is_idempotent(self):
        registry = FactorRegistry(config_path="/nonexistent/path.yaml")
        registry.load()
        specs_first = set(registry._specs.keys())
        registry.load()
        assert set(registry._specs.keys()) == specs_first


# ══════════════════════════════════════════════════════════════════════
# FactorRegistry — load() from YAML
# ══════════════════════════════════════════════════════════════════════


class TestFactorRegistryLoadFromYAML:
    def test_loads_from_yaml_file(self):
        yaml_content = """
factors:
  ema_20:
    class: EMA
    type: bar
    rust_key: ema
    params:
      period: 20
    display:
      name: "EMA(20)"
      description: "20-period EMA"
      mode: overlay
      priceScale: left
    timeframes: [1m, 5m]
  trade_rate:
    class: TradeRate
    type: trade
    rust_key: trade_rate
    params:
      window_ms: 20000
      min_trades: 5
    display:
      name: "TradeRate"
      mode: panel
    timeframes: [trade]
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            tmp_path = f.name

        try:
            registry = FactorRegistry(config_path=tmp_path)
            registry.load()
            assert registry._loaded is True
            assert "ema_20" in registry._specs
            assert "trade_rate" in registry._specs

            ema_spec = registry._specs["ema_20"]
            assert ema_spec.type == "bar"
            assert ema_spec.params == {"period": 20}
            assert ema_spec.display.name == "EMA(20)"
            assert ema_spec.display.mode == "overlay"
            assert ema_spec.timeframes == ["1m", "5m"]
        finally:
            Path(tmp_path).unlink()


# ══════════════════════════════════════════════════════════════════════
# FactorRegistry — query methods
# ══════════════════════════════════════════════════════════════════════


class TestFactorRegistryQuery:
    @pytest.fixture
    def registry(self):
        r = FactorRegistry(config_path="/nonexistent/path.yaml")
        r.load()
        return r

    def test_get_spec(self, registry):
        spec = registry.get_spec("ema_20")
        assert spec is not None
        assert spec.class_name == "EMA"

    def test_get_spec_unknown(self, registry):
        assert registry.get_spec("nonexistent") is None

    def test_get_all_specs_returns_copy(self, registry):
        specs = registry.get_all_specs()
        assert "ema_20" in specs
        assert "trade_rate" in specs
        # Modifying the returned dict doesn't affect internal state
        specs["new_factor"] = MagicMock()
        assert "new_factor" not in registry._specs

    def test_get_factors_by_type_bar(self, registry):
        bar_factors = registry.get_factors_by_type("bar")
        assert len(bar_factors) >= 1
        assert all(s.type == "bar" for s in bar_factors)

    def test_get_factors_by_type_trade(self, registry):
        trade_factors = registry.get_factors_by_type("trade")
        assert len(trade_factors) >= 1
        assert all(s.type == "trade" for s in trade_factors)

    def test_get_factors_by_type_unknown(self, registry):
        assert registry.get_factors_by_type("unknown") == []

    def test_get_factors_for_timeframe(self, registry):
        factors = registry.get_factors_for_timeframe("1m")
        assert len(factors) >= 1
        for s in factors:
            assert "1m" in s.timeframes

    def test_get_factors_for_timeframe_none(self, registry):
        assert registry.get_factors_for_timeframe("nonexistent") == []

    def test_get_specs_for_api(self, registry):
        api_specs = registry.get_specs_for_api()
        assert isinstance(api_specs, list)
        assert len(api_specs) >= 1
        assert "id" in api_specs[0]


# ══════════════════════════════════════════════════════════════════════
# FactorRegistry — create_indicator
# ══════════════════════════════════════════════════════════════════════


class TestFactorRegistryCreateIndicator:
    @pytest.fixture
    def registry(self):
        r = FactorRegistry(config_path="/nonexistent/path.yaml")
        r.load()
        return r

    def test_create_indicator_is_deprecated(self, registry):
        """create_indicator is deprecated — Python indicators removed, always returns None."""
        indicator = registry.create_indicator("ema_20")
        assert indicator is None

    def test_create_unknown_indicator_deprecated(self, registry):
        assert registry.create_indicator("nonexistent") is None

    def test_create_indicators_for_type_deprecated(self, registry):
        """create_indicators_for_type is deprecated — always returns []."""
        indicators = registry.create_indicators_for_type("bar")
        assert indicators == []


# ══════════════════════════════════════════════════════════════════════
# Global singleton
# ══════════════════════════════════════════════════════════════════════


class TestGlobalRegistry:
    def teardown_method(self):
        # Reset singleton after each test
        import jerry_trader.services.factor.factor_registry as mod

        mod._registry = None

    def test_get_creates_singleton(self):
        import jerry_trader.services.factor.factor_registry as mod

        mod._registry = None
        r1 = get_factor_registry()
        r2 = get_factor_registry()
        assert r1 is r2

    def test_reload_creates_new_instance(self):
        import jerry_trader.services.factor.factor_registry as mod

        mod._registry = None
        r1 = get_factor_registry()
        r2 = reload_factor_registry()
        assert r1 is not r2
        assert mod._registry is r2
