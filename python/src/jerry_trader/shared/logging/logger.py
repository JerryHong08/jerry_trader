import logging
import os
from datetime import datetime
from pathlib import Path

from jerry_trader.shared.utils.paths import PROJECT_ROOT

LOG_DIR = PROJECT_ROOT / "logs" / "jerry_trader" / datetime.now().strftime("%Y%m%d")


def setup_logger(
    name: str, log_dir: str = None, level=logging.DEBUG, log_to_file: bool = True
) -> logging.Logger:
    """
    Setup a logger with file and console handlers

    Args:
        name: Logger name (usually __name__)
        log_dir: Directory to store log files (default: project logs/YYYYMMDD/)
        level: Logging level
        log_to_file: Whether to create file handler

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Console handler (always add)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_to_file:
        if log_dir is None:
            log_dir = LOG_DIR

        os.makedirs(log_dir, exist_ok=True)

        # 鉁?Use module name for log file
        module_name = name.split(".")[-1]
        log_filename = f"{module_name}_{datetime.now().strftime('%Y%m%d')}.log"
        log_filepath = os.path.join(log_dir, log_filename)

        file_handler = logging.FileHandler(log_filepath, encoding="utf-8")
        file_handler.setLevel(level)
        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

        logger.info(f"Logging to file: {log_filepath}")

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a simple logger without file handler (for data models)

    Args:
        name: Logger name

    Returns:
        Logger instance that inherits parent configuration
    """
    return logging.getLogger(name)
