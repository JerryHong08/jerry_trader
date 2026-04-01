"""Factor Registry - Central configuration for all available factors.

Loads factor specifications from YAML config and provides:
1. Indicator instantiation for FactorEngine
2. Factor metadata for frontend (via API)
3. Validation of factor configurations

Usage:
    registry = FactorRegistry()
    registry.load()

    # Get indicator instance
    indicator = registry.create_indicator("ema_20")

    # Get all specs for frontend
    specs = registry.get_all_specs()

    # Get factors for a specific timeframe type
    bar_factors = registry.get_factors_by_type("bar")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Type

import yaml

from jerry_trader.services.factor.indicators import (
    TickIndicator,  # Note: TickIndicator handles trade data, renamed from base class
)
from jerry_trader.services.factor.indicators import (
    EMA,
    BarIndicator,
    QuoteIndicator,
    TradeRate,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class FactorDisplay:
    """Display metadata for a factor."""

    name: str
    description: str = ""
    color: str = "#3b82f6"
    priceScale: str = "right"  # 'left' or 'right'
    mode: str = "panel"  # 'overlay' (on price chart) or 'panel' (separate)


@dataclass
class FactorSpec:
    """Complete specification for a factor.

    Attributes:
        id: Unique identifier (e.g., "ema_20")
        class_name: Indicator class name (e.g., "EMA")
        type: "bar", "trade", or "quote"
        params: Constructor parameters for the indicator
        display: Display metadata for frontend
        timeframes: List of timeframes this factor applies to
    """

    id: str
    class_name: str
    type: str  # "bar", "trade", "quote"
    params: dict[str, Any] = field(default_factory=dict)
    display: FactorDisplay = field(default_factory=lambda: FactorDisplay(name=""))
    timeframes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dict for API response."""
        return {
            "id": self.id,
            "type": self.type,
            "display": {
                "name": self.display.name,
                "description": self.display.description,
                "color": self.display.color,
                "priceScale": self.display.priceScale,
                "mode": self.display.mode,
            },
            "timeframes": self.timeframes,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Indicator Class Registry
# ─────────────────────────────────────────────────────────────────────────────

# Map class names to actual classes
INDICATOR_CLASSES: dict[str, Type] = {
    "EMA": EMA,
    "TradeRate": TradeRate,
    # Add new indicator classes here as they are implemented
}


# ─────────────────────────────────────────────────────────────────────────────
# Factor Registry
# ─────────────────────────────────────────────────────────────────────────────


class FactorRegistry:
    """Central registry for all available factors.

    Loads configuration from YAML and provides methods to:
    - Create indicator instances
    - Get factor specifications for frontend
    - Query factors by type or timeframe
    """

    DEFAULT_CONFIG_PATH = (
        Path(__file__).parent.parent.parent.parent.parent.parent
        / "config"
        / "factors.yaml"
    )

    def __init__(self, config_path: Path | str | None = None):
        """Initialize the registry.

        Args:
            config_path: Path to factors.yaml. Defaults to config/factors.yaml
        """
        self.config_path = (
            Path(config_path) if config_path else self.DEFAULT_CONFIG_PATH
        )
        self._specs: dict[str, FactorSpec] = {}
        self._loaded = False

    def load(self) -> None:
        """Load factor specifications from YAML config."""
        if self._loaded:
            return

        if not self.config_path.exists():
            logger.warning(
                f"Factor config not found at {self.config_path}, using defaults"
            )
            self._load_defaults()
            self._loaded = True
            return

        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)

            factors = config.get("factors", {})
            for factor_id, factor_config in factors.items():
                spec = self._parse_spec(factor_id, factor_config)
                self._specs[factor_id] = spec
                logger.debug(f"Loaded factor spec: {factor_id}")

            logger.info(
                f"FactorRegistry loaded {len(self._specs)} factors from {self.config_path}"
            )
            self._loaded = True

        except Exception as e:
            logger.error(f"Failed to load factor config: {e}")
            self._load_defaults()
            self._loaded = True

    def _load_defaults(self) -> None:
        """Load default factors if config is unavailable."""
        self._specs = {
            "ema_20": FactorSpec(
                id="ema_20",
                class_name="EMA",
                type="bar",
                params={"period": 20},
                display=FactorDisplay(
                    name="EMA(20)",
                    description="Exponential Moving Average with 20-period",
                    color="#3b82f6",
                    priceScale="left",
                    mode="overlay",  # Overlay on price chart
                ),
                timeframes=[
                    "10s",
                    "1m",
                    "5m",
                    "15m",
                    "30m",
                    "1h",
                    "4h",
                    "1D",
                    "1W",
                    "1M",
                ],
            ),
            "trade_rate": FactorSpec(
                id="trade_rate",
                class_name="TradeRate",
                type="trade",
                params={"window_ms": 20000, "min_trades": 5},
                display=FactorDisplay(
                    name="TradeRate",
                    description="Trade rate (trades per second)",
                    color="#f97316",
                    priceScale="right",
                    mode="panel",  # Separate panel below price
                ),
                timeframes=["trade"],
            ),
        }
        logger.info("FactorRegistry loaded default factors")

    def _parse_spec(self, factor_id: str, config: dict) -> FactorSpec:
        """Parse a factor specification from config dict."""
        display_config = config.get("display", {})
        display = FactorDisplay(
            name=display_config.get("name", factor_id),
            description=display_config.get("description", ""),
            color=display_config.get("color", "#3b82f6"),
            priceScale=display_config.get("priceScale", "right"),
            mode=display_config.get("mode", "panel"),  # 'overlay' or 'panel'
        )

        return FactorSpec(
            id=factor_id,
            class_name=config.get("class", ""),
            type=config.get("type", "bar"),
            params=config.get("params", {}),
            display=display,
            timeframes=config.get("timeframes", []),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def get_spec(self, factor_id: str) -> FactorSpec | None:
        """Get a factor specification by ID."""
        if not self._loaded:
            self.load()
        return self._specs.get(factor_id)

    def get_all_specs(self) -> dict[str, FactorSpec]:
        """Get all factor specifications."""
        if not self._loaded:
            self.load()
        return self._specs.copy()

    def get_specs_for_api(self) -> list[dict]:
        """Get all factor specs formatted for API response."""
        if not self._loaded:
            self.load()
        return [spec.to_dict() for spec in self._specs.values()]

    def get_factors_by_type(self, factor_type: str) -> list[FactorSpec]:
        """Get all factors of a specific type (bar, tick, quote)."""
        if not self._loaded:
            self.load()
        return [spec for spec in self._specs.values() if spec.type == factor_type]

    def get_factors_for_timeframe(self, timeframe: str) -> list[FactorSpec]:
        """Get all factors applicable to a specific timeframe."""
        if not self._loaded:
            self.load()
        return [spec for spec in self._specs.values() if timeframe in spec.timeframes]

    def create_indicator(
        self, factor_id: str
    ) -> BarIndicator | TickIndicator | QuoteIndicator | None:
        """Create an indicator instance for a factor.

        Args:
            factor_id: Factor ID (e.g., "ema_20")

        Returns:
            Indicator instance or None if not found
        """
        if not self._loaded:
            self.load()

        spec = self._specs.get(factor_id)
        if not spec:
            logger.warning(f"Factor not found: {factor_id}")
            return None

        indicator_class = INDICATOR_CLASSES.get(spec.class_name)
        if not indicator_class:
            logger.warning(f"Indicator class not found: {spec.class_name}")
            return None

        try:
            instance = indicator_class(**spec.params)
            logger.debug(f"Created indicator: {factor_id} ({spec.class_name})")
            return instance
        except Exception as e:
            logger.error(f"Failed to create indicator {factor_id}: {e}")
            return None

    def create_indicators_for_type(
        self, factor_type: str
    ) -> list[BarIndicator | TickIndicator | QuoteIndicator]:
        """Create all indicators of a specific type.

        Args:
            factor_type: "bar", "tick", or "quote"

        Returns:
            List of indicator instances
        """
        indicators = []
        for spec in self.get_factors_by_type(factor_type):
            indicator = self.create_indicator(spec.id)
            if indicator:
                indicators.append(indicator)
        return indicators


# ─────────────────────────────────────────────────────────────────────────────
# Global Registry Instance
# ─────────────────────────────────────────────────────────────────────────────

# Singleton instance
_registry: FactorRegistry | None = None


def get_factor_registry() -> FactorRegistry:
    """Get the global FactorRegistry instance."""
    global _registry
    if _registry is None:
        _registry = FactorRegistry()
        _registry.load()
    return _registry


def reload_factor_registry(config_path: Path | str | None = None) -> FactorRegistry:
    """Reload the factor registry, optionally with a new config path."""
    global _registry
    _registry = FactorRegistry(config_path)
    _registry.load()
    return _registry
