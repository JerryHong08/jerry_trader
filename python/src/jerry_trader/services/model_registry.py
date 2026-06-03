"""Model Registry - Central registry for ML models.

Provides lazy loading, caching, and version management for ML models
used in event evaluation.

Usage:
    registry = ModelRegistry()
    registry.register("return_predictor_v1", "models/return_predictor_v1.json")

    # Get model (lazy load on first access)
    predictor = registry.get("return_predictor_v1")
    expected_return = predictor.predict(factors)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from jerry_trader.services.backtest.ml_model import ReturnPredictor
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.paths import PROJECT_ROOT

logger = setup_logger("model_registry", log_to_file=True)

# Default models directory
DEFAULT_MODELS_DIR = PROJECT_ROOT / "models"


@dataclass
class ModelEntry:
    """Entry in the model registry."""

    name: str
    path: str | Path
    predictor: ReturnPredictor | None = None  # Lazy loaded
    loaded: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


class ModelRegistry:
    """Central registry for ML models.

    Features:
    - Lazy loading: models loaded on first access
    - Caching: loaded models stay in memory
    - Version management: multiple model versions supported

    Example:
        registry = ModelRegistry()

        # Register models
        registry.register("return_predictor_v1", "models/v1/predictor.json")
        registry.register("return_predictor_v2", "models/v2/predictor.json")

        # Get model (lazy load)
        predictor = registry.get("return_predictor_v1")

        # Predict
        expected_return = predictor.predict({"relative_volume": 2.5})
    """

    def __init__(self, models_dir: Path | str | None = None):
        """Initialize registry.

        Args:
            models_dir: Directory containing model files
        """
        self.models_dir = Path(models_dir) if models_dir else DEFAULT_MODELS_DIR
        self._registry: dict[str, ModelEntry] = {}

        # Auto-discover models in directory
        self._auto_discover()

    def _auto_discover(self) -> None:
        """Auto-discover models in models_dir."""
        if not self.models_dir.exists():
            logger.info(
                f"Models directory {self.models_dir} does not exist, skipping auto-discover"
            )
            return

        # Find all .json model config files
        for config_file in self.models_dir.glob("**/*.json"):
            # Skip if corresponding .joblib doesn't exist
            joblib_file = config_file.with_suffix(".joblib")
            if not joblib_file.exists():
                logger.warning(f"Skipping {config_file}: no corresponding .joblib file")
                continue

            # Extract model name from path
            # e.g., models/v1/return_predictor.json -> return_predictor_v1
            # or models/return_predictor.json -> return_predictor
            relative_path = config_file.relative_to(self.models_dir)
            name_parts = list(relative_path.parts[:-1])  # Directory parts
            base_name = config_file.stem  # File name without extension

            if name_parts:
                model_name = f"{base_name}_{name_parts[-1]}"
            else:
                model_name = base_name

            self.register(model_name, str(config_file))
            logger.info(f"Auto-discovered model: {model_name}")

    def register(
        self,
        name: str,
        path: str | Path,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a model in the registry.

        Args:
            name: Model identifier
            path: Path to model config file (.json)
            metadata: Optional metadata (version, description, etc.)
        """
        self._registry[name] = ModelEntry(
            name=name,
            path=Path(path),
            metadata=metadata or {},
        )
        logger.debug(f"Registered model: {name} -> {path}")

    def get(self, name: str) -> ReturnPredictor:
        """Get model by name (lazy load on first access).

        Args:
            name: Model identifier

        Returns:
            ReturnPredictor instance

        Raises:
            KeyError: Model not registered
            FileNotFoundError: Model file not found
        """
        if name not in self._registry:
            raise KeyError(
                f"Model '{name}' not registered. Available: {list(self._registry.keys())}"
            )

        entry = self._registry[name]

        # Lazy load
        if not entry.loaded:
            logger.info(f"Loading model: {name}")
            predictor = ReturnPredictor()
            predictor.load(entry.path)
            entry.predictor = predictor
            entry.loaded = True

        return entry.predictor

    def is_registered(self, name: str) -> bool:
        """Check if model is registered."""
        return name in self._registry

    def is_loaded(self, name: str) -> bool:
        """Check if model is loaded."""
        if name not in self._registry:
            return False
        return self._registry[name].loaded

    def list_models(self) -> list[str]:
        """List all registered models."""
        return list(self._registry.keys())

    def list_loaded_models(self) -> list[str]:
        """List all loaded models."""
        return [name for name, entry in self._registry.items() if entry.loaded]

    def unload(self, name: str) -> None:
        """Unload a model from memory."""
        if name not in self._registry:
            return

        entry = self._registry[name]
        entry.predictor = None
        entry.loaded = False
        logger.info(f"Unloaded model: {name}")

    def unload_all(self) -> None:
        """Unload all models from memory."""
        for name in self._registry:
            self.unload(name)

    def preload_all(self) -> None:
        """Preload all registered models."""
        for name in self._registry:
            self.get(name)

    def get_metadata(self, name: str) -> dict[str, Any]:
        """Get model metadata."""
        if name not in self._registry:
            raise KeyError(f"Model '{name}' not registered")
        return self._registry[name].metadata

    def reload(self, name: str) -> ReturnPredictor:
        """Force reload a model from disk.

        Useful for hot-reloading after model retraining.

        Args:
            name: Model identifier

        Returns:
            Freshly loaded ReturnPredictor instance
        """
        if name not in self._registry:
            raise KeyError(
                f"Model '{name}' not registered. Available: {list(self._registry.keys())}"
            )

        entry = self._registry[name]
        logger.info(f"Reloading model: {name}")
        predictor = ReturnPredictor()
        predictor.load(entry.path)
        entry.predictor = predictor
        entry.loaded = True
        return predictor


# Global singleton registry
_global_registry: ModelRegistry | None = None


def get_model_registry() -> ModelRegistry:
    """Get global model registry singleton."""
    global _global_registry
    if _global_registry is None:
        _global_registry = ModelRegistry()
    return _global_registry


__all__ = ["ModelRegistry", "ModelEntry", "get_model_registry"]
