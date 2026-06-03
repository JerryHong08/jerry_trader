"""Factor Registry - Central configuration for all available factors.

V4: Uses auto-registration via Factor._registry instead of manual INDICATOR_CLASSES dict.
Adding a new factor only requires writing the class + YAML config.

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

import inspect
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from jerry_trader.shared.utils.paths import PROJECT_ROOT

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
        rust_key: Canonical Rust registry key (e.g., "ema", "volume_acceleration")
    """

    id: str
    class_name: str
    type: str = ""  # "bar", "trade", "quote"
    params: dict[str, Any] = field(default_factory=dict)
    display: FactorDisplay = field(default_factory=lambda: FactorDisplay(name=""))
    timeframes: list[str] = field(default_factory=list)
    rust_key: str = (
        ""  # Canonical key for Rust FactorRegistry (e.g., "ema", "volume_acceleration")
    )

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
# Parameter Validation
# ─────────────────────────────────────────────────────────────────────────────

_ALLOWED_PARAM_TYPES = (int, float, bool, str)


def _validate_params(
    factor_id: str,
    class_name: str,
    params: dict[str, Any],
    factor_class: type | None,
) -> None:
    """Validate factor params at config load time.

    Catches common misconfigurations early (e.g., string instead of int).
    """
    for key, value in params.items():
        if not isinstance(value, _ALLOWED_PARAM_TYPES):
            logger.warning(
                f"Factor '{factor_id}': param '{key}' has unexpected type "
                f"{type(value).__name__} (value={value!r}). Expected int/float/bool/str."
            )
            continue

        # Warn on common mistake: numeric param passed as string
        if isinstance(value, str):
            # Check if it looks like it should be a number
            stripped = value.strip()
            if stripped and (
                stripped.isdigit()
                or (stripped.startswith("-") and stripped[1:].isdigit())
            ):
                logger.warning(
                    f"Factor '{factor_id}': param '{key}' is string '{value}' "
                    f"— did you mean int {int(value)}?"
                )
            else:
                try:
                    float(stripped)
                    logger.warning(
                        f"Factor '{factor_id}': param '{key}' is string '{value}' "
                        f"— did you mean float {float(value)}?"
                    )
                except ValueError:
                    pass  # legitimate string value

    # Cross-reference with class constructor signature if available
    if factor_class is not None:
        try:
            sig = inspect.signature(factor_class.__init__)
            known_params = {
                n
                for n, p in sig.parameters.items()
                if n != "self"
                and p.kind
                not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
            }
            unknown = set(params.keys()) - known_params
            if unknown:
                logger.warning(
                    f"Factor '{factor_id}' ({class_name}): unknown params {unknown}. "
                    f"Known params: {known_params or '(none)'}"
                )
        except (ValueError, TypeError) as e:
            logger.debug(f"Could not inspect {class_name}.__init__: {e}")


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

    DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "factors.yaml"

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
            self._validate_against_rust()
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
            self._validate_against_rust()

        except Exception as e:
            logger.error(f"Failed to load factor config: {e}")
            self._load_defaults()
            self._loaded = True

    def _validate_against_rust(self) -> None:
        """Cross-reference YAML rust_keys with the Rust FactorRegistry.

        Catches naming mismatches early — a YAML rust_key that doesn't exist
        in Rust means the factor will silently fail at runtime.
        """
        try:
            from jerry_trader._rust import PyFactorEngine

            engine = PyFactorEngine()
            rust_bar = set(engine.list_bar_factors())
            rust_trade = set(engine.list_trade_factors())
            rust_quote = set(engine.list_quote_factors())
            rust_all = rust_bar | rust_trade | rust_quote

            yaml_rust_keys: dict[str, str] = {}  # rust_key → spec.id
            for spec in self._specs.values():
                yaml_rust_keys[spec.rust_key] = spec.id

            # Check 1: every YAML rust_key exists in Rust
            for rust_key, spec_id in yaml_rust_keys.items():
                if rust_key not in rust_all:
                    valid_for_type = (
                        rust_bar
                        if spec_id
                        in {s.id for s in self._specs.values() if s.type == "bar"}
                        else (
                            rust_trade
                            if spec_id
                            in {s.id for s in self._specs.values() if s.type == "trade"}
                            else rust_quote
                        )
                    )
                    logger.error(
                        f"Factor '{spec_id}': rust_key='{rust_key}' not found in "
                        f"Rust registry. Known {spec_id}: "
                        f"{sorted(rust_bar | rust_trade | rust_quote)}"
                    )

            # Check 2: every Rust factor has at least one YAML config
            for rust_name in sorted(rust_all - set(yaml_rust_keys.keys())):
                logger.warning(
                    f"Rust factor '{rust_name}' has no config in factors.yaml "
                    f"— it will never be loaded"
                )

            logger.debug(
                f"Factor registry validation: {len(yaml_rust_keys)} YAML specs ↔ "
                f"{len(rust_all)} Rust factors OK"
            )

        except ImportError:
            logger.debug("Rust extension not available, skipping validation")
        except Exception as e:
            logger.warning(f"Factor registry validation failed: {e}")

    def _load_defaults(self) -> None:
        """Load default factors if config is unavailable.

        V4: Uses auto-discovered Factor._registry for class lookup.
        """
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
                    mode="overlay",
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
                rust_key="ema",
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
                    mode="panel",
                ),
                timeframes=["tick"],
                rust_key="trade_rate",
            ),
            # NOTE: New factors only need config/factors.yaml entries.
            # _load_defaults() is a minimal fallback for when the YAML file
            # is missing (never happens in production, only in certain tests).
        }
        logger.info("FactorRegistry loaded default factors")

    def _parse_spec(self, factor_id: str, config: dict[str, Any]) -> FactorSpec:
        """Parse a factor specification from config dict.

        V4: Also looks up factor_class from auto-discovered registry.
        """
        display_config = config.get("display", {})
        display = FactorDisplay(
            name=display_config.get("name", factor_id),
            description=display_config.get("description", ""),
            color=display_config.get("color", "#3b82f6"),
            priceScale=display_config.get("priceScale", "right"),
            mode=display_config.get("mode", "panel"),  # 'overlay' or 'panel'
        )

        class_name = config.get("class", "")
        params = config.get("params", {})

        # Read rust_key from YAML config (single source of truth)
        rust_key = config.get("rust_key", class_name.lower())

        # Validate params (basic type checks, no class cross-reference needed)
        _validate_params(factor_id, class_name, params, None)

        return FactorSpec(
            id=factor_id,
            class_name=class_name,
            type=config.get("type", "bar"),
            params=params,
            display=display,
            timeframes=config.get("timeframes", []),
            rust_key=rust_key,
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

    def create_indicator(self, factor_id: str) -> None:
        """DEPRECATED: Python indicator classes have been removed.

        All factor computation now goes through Rust PyFactorEngine.
        This method exists only for backward compatibility and always returns None.
        """
        logger.warning(
            f"create_indicator({factor_id}): Python indicators removed — "
            f"use Rust PyFactorEngine instead"
        )
        return None

    def create_indicators_for_type(self, factor_type: str) -> list:
        """DEPRECATED: Python indicator classes have been removed.

        All factor computation now goes through Rust PyFactorEngine.
        This method exists only for backward compatibility and always returns [].
        """
        logger.warning(
            f"create_indicators_for_type({factor_type}): Python indicators removed"
        )
        return []


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
