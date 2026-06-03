"""Training Data Exporter - Exports training data for ML model training.

Supports multiple export formats:
- Parquet (recommended for large datasets)
- CSV (for compatibility)
- JSON (for debugging)

Also provides data splitting for train/test/validation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from jerry_trader.services.training_data.collector import TrainingSample
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("training_data_exporter", log_to_file=True)


class TrainingDataExporter:
    """Exports training data for ML model training.

    Features:
    - Multiple export formats
    - Train/test split by date
    - Cross-validation fold generation
    - Feature normalization

    Example:
        exporter = TrainingDataExporter()

        # Export single file
        exporter.export(samples, "data/training/v1.parquet")

        # Export with train/test split
        exporter.export_with_split(
            samples,
            output_dir="data/training/v1",
            test_dates=["2026-03-11", "2026-03-12", "2026-03-13"],
        )
    """

    def __init__(self):
        pass

    def export(
        self,
        samples: list[TrainingSample],
        output_path: str | Path,
        format: str = "parquet",
    ) -> bool:
        """Export samples to file.

        Args:
            samples: List of TrainingSample
            output_path: Output file path
            format: Output format (parquet, csv, json)

        Returns:
            True if successful
        """
        if not samples:
            logger.warning("No samples to export")
            return False

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to DataFrame
        df = pd.DataFrame([s.to_dict() for s in samples])

        try:
            if format == "parquet":
                df.to_parquet(output_path, index=False)
            elif format == "csv":
                df.to_csv(output_path, index=False)
            elif format == "json":
                df.to_json(output_path, orient="records", indent=2)
            else:
                logger.error(f"Unknown format: {format}")
                return False

            logger.info(f"Exported {len(samples)} samples to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export: {e}")
            return False

    def export_with_split(
        self,
        samples: list[TrainingSample],
        output_dir: str | Path,
        test_dates: Optional[list[str]] = None,
        test_ratio: float = 0.3,
        random_seed: int = 42,
    ) -> dict[str, int]:
        """Export samples with train/test split.

        Args:
            samples: List of TrainingSample
            output_dir: Output directory
            test_dates: Dates to use for test set (overrides test_ratio)
            test_ratio: Ratio of test set if test_dates not specified
            random_seed: Random seed for reproducibility

        Returns:
            Dict with train/test counts
        """
        if not samples:
            return {"train": 0, "test": 0}

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame([s.to_dict() for s in samples])

        # Convert trigger_time_ns to date string
        df["date"] = pd.to_datetime(df["trigger_time_ns"], unit="ns").dt.strftime(
            "%Y-%m-%d"
        )

        if test_dates:
            # Split by date
            train_df = df[~df["date"].isin(test_dates)]
            test_df = df[df["date"].isin(test_dates)]
        else:
            # Random split
            np.random.seed(random_seed)
            mask = np.random.rand(len(df)) < test_ratio
            test_df = df[mask]
            train_df = df[~mask]

        # Export
        train_path = output_dir / "train.parquet"
        test_path = output_dir / "test.parquet"

        train_df.drop(columns=["date"]).to_parquet(train_path, index=False)
        test_df.drop(columns=["date"]).to_parquet(test_path, index=False)

        logger.info(f"Exported train: {len(train_df)}, test: {len(test_df)}")

        return {
            "train": len(train_df),
            "test": len(test_df),
        }

    def export_cv_folds(
        self,
        samples: list[TrainingSample],
        output_dir: str | Path,
        n_folds: int = 5,
    ) -> dict:
        """Export samples split into cross-validation folds.

        Folds are split by date to avoid data leakage.

        Args:
            samples: List of TrainingSample
            output_dir: Output directory
            n_folds: Number of folds

        Returns:
            Dict with fold info
        """
        if not samples:
            return {"folds": 0}

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame([s.to_dict() for s in samples])

        # Get unique dates
        df["date"] = pd.to_datetime(df["trigger_time_ns"], unit="ns").dt.strftime(
            "%Y-%m-%d"
        )
        unique_dates = sorted(df["date"].unique())

        # Assign dates to folds
        dates_per_fold = len(unique_dates) // n_folds
        fold_info = {}

        for fold_idx in range(n_folds):
            start_idx = fold_idx * dates_per_fold
            end_idx = (
                start_idx + dates_per_fold
                if fold_idx < n_folds - 1
                else len(unique_dates)
            )

            test_dates = unique_dates[start_idx:end_idx]
            train_dates = unique_dates[:start_idx] + unique_dates[end_idx:]

            test_df = df[df["date"].isin(test_dates)]
            train_df = df[df["date"].isin(train_dates)]

            # Export
            fold_dir = output_dir / f"fold_{fold_idx}"
            fold_dir.mkdir(parents=True, exist_ok=True)

            train_df.drop(columns=["date"]).to_parquet(
                fold_dir / "train.parquet", index=False
            )
            test_df.drop(columns=["date"]).to_parquet(
                fold_dir / "test.parquet", index=False
            )

            fold_info[fold_idx] = {
                "train": len(train_df),
                "test": len(test_df),
                "test_dates": test_dates,
            }

        logger.info(f"Exported {n_folds} CV folds")
        return fold_info


__all__ = ["TrainingDataExporter"]
