import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from jerry_trader.shared.utils.paths import PROJECT_ROOT

# Standard LogRecord attributes to exclude from extra fields
_LOG_RECORD_STANDARD_ATTRS = frozenset(logging.LogRecord.__dict__.keys()) | {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
    "asctime",
    "message",
    "getMessage",
}


class JsonFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs log records as JSON objects with standard fields:
    - timestamp: ISO format timestamp
    - level: Log level name
    - logger: Logger name
    - message: Log message
    - extra: Additional fields from extra={} dict
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields if present (via logging.info(..., extra={...}))
        extra_data = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _LOG_RECORD_STANDARD_ATTRS and k not in log_data
        }
        if extra_data:
            log_data["extra"] = extra_data

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


LOG_DIR = PROJECT_ROOT / "logs" / "jerry_trader" / datetime.now().strftime("%Y%m%d")


def setup_logger(
    name: str,
    log_dir: Optional[str] = None,
    level: int = logging.DEBUG,
    log_to_file: bool = True,
    json_format: bool = False,
) -> logging.Logger:
    """
    Setup a logger with file and console handlers

    Args:
        name: Logger name (usually __name__)
        log_dir: Directory to store log files (default: project logs/YYYYMMDD/)
        level: Logging level
        log_to_file: Whether to create file handler
        json_format: Whether to output JSON formatted logs (structured logging)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Choose formatter based on json_format
    if json_format:
        console_format: logging.Formatter = JsonFormatter()
        file_format: logging.Formatter = JsonFormatter()
    else:
        console_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Console handler (always add)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_to_file:
        effective_log_dir = log_dir if log_dir is not None else str(LOG_DIR)
        os.makedirs(effective_log_dir, exist_ok=True)

        # Use module name for log file
        module_name = name.split(".")[-1]
        log_filename = f"{module_name}_{datetime.now().strftime('%Y%m%d')}.log"
        log_filepath = os.path.join(effective_log_dir, log_filename)

        file_handler = logging.FileHandler(log_filepath, encoding="utf-8")
        file_handler.setLevel(level)
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
