"""Training Data Collection Pipeline.

Collects factor-return pairs from signal events for ML model training.

Components:
- TrainingDataCollector: Collects and stores training samples
- TrainingDataExporter: Exports data for model training
- TrainingDataStats: Statistics and quality checks
"""

from jerry_trader.services.training_data.collector import (
    TrainingDataCollector,
    TrainingSample,
)
from jerry_trader.services.training_data.exporter import TrainingDataExporter

__all__ = [
    "TrainingDataCollector",
    "TrainingSample",
    "TrainingDataExporter",
]
