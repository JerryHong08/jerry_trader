"""
Config builder utilities for JerryTrader Backend Starter.

Handles:
- YAML config loading
- Runtime config assembly (defaults → machine → CLI overrides)
- Database reference resolution (``redis-b@OLDMAN`` → resolved host/port)
- CLI override parsing (dot-notation ``--defaults.replay_date 20260115``)
- Date-field normalisation (YAML parses ``20260115`` as int → str)
"""

from __future__ import annotations

import copy
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from jerry_trader.shared.utils.paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"


# ── YAML loading ────────────────────────────────────────────────────


def load_yaml_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    """Load YAML configuration file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ── Dict helpers ────────────────────────────────────────────────────


def deep_merge(base: dict, override: dict) -> dict:
    """Deep merge *override* into *base* dict.  Override values take precedence."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def set_nested_value(d: dict, key_path: str, value: Any) -> None:
    """Set a nested value in *d* using dot notation.

    Example::

        set_nested_value(d, "llm.models.deepseek.thinking_mode", True)

    String values are auto-coerced to ``bool``, ``None``, ``int``, or
    ``float`` when possible.
    """
    keys = key_path.split(".")
    current = d
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]

    # Type coercion for common cases
    final_key = keys[-1]
    if isinstance(value, str):
        if value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        elif value.lower() in ("null", "none"):
            value = None
        else:
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass  # Keep as string

    current[final_key] = value


# ── CLI override parsing ────────────────────────────────────────────


def parse_override_args(unknown_args: list) -> Dict[str, Any]:
    """Parse unknown CLI args as config overrides.

    Supports ``--key.subkey.subsubkey value`` and ``--key.subkey=value``.
    """
    overrides: Dict[str, Any] = {}
    i = 0
    while i < len(unknown_args):
        arg = unknown_args[i]
        if arg.startswith("--"):
            key = arg[2:]  # Remove --

            if "=" in key:
                key, value = key.split("=", 1)
                overrides[key] = value
            elif i + 1 < len(unknown_args) and not unknown_args[i + 1].startswith("--"):
                overrides[key] = unknown_args[i + 1]
                i += 1
            else:
                overrides[key] = True
        i += 1
    return overrides


# ── Runtime config assembly ─────────────────────────────────────────


def build_runtime_config(
    yaml_cfg: dict,
    machine: str,
    overrides: Dict[str, Any],
) -> dict:
    """Build runtime configuration by merging defaults → machine → CLI overrides.

    Overrides use dot notation:
    - ``"replay_date"`` or ``"defaults.replay_date"`` → sets replay_date
    - ``"llm.active_model"`` → sets llm.active_model
    - ``"roles.JerryTraderBFF.port"`` → sets role-specific param

    Database references use ``"db-name@MACHINE"`` format:
    - ``"redis-b@OLDMAN"`` → resolves host from ``OLDMAN_IP`` env var
    - ``"redis-b"`` → defaults to LOCAL (127.0.0.1)
    """
    if machine not in yaml_cfg.get("machines", {}):
        available = list(yaml_cfg.get("machines", {}).keys())
        raise ValueError(f"Unknown machine '{machine}'. Available: {available}")

    machine_cfg = yaml_cfg["machines"][machine]

    # Start with defaults (these become top-level in runtime config)
    runtime = copy.deepcopy(yaml_cfg.get("defaults", {}))

    # Add machine-level limit (market_open | market_close | null)
    runtime["limit"] = machine_cfg.get("limit", None)

    # Add LLM config
    runtime["llm"] = copy.deepcopy(yaml_cfg.get("llm", {}))

    # Add enabled roles with their config, resolving database references
    runtime["roles"] = {}
    for role_name, role_cfg in machine_cfg.get("roles", {}).items():
        if role_cfg.get("enabled", True):
            resolved_cfg = copy.deepcopy(role_cfg)

            # Resolve database references.
            # `clock_redis` uses the same reference format as `redis`
            # (e.g. "redis-a@WSL2") but is stored under a distinct role key.
            for db_type in ["redis", "postgres", "influxdb", "clickhouse"]:
                db_ref = role_cfg.get(db_type)
                if db_ref and isinstance(db_ref, str):
                    resolved_cfg[db_type] = _resolve_db_reference(
                        yaml_cfg, db_type, db_ref, role_name
                    )

            clock_redis_ref = role_cfg.get("clock_redis")
            if clock_redis_ref and isinstance(clock_redis_ref, str):
                resolved_cfg["clock_redis"] = _resolve_db_reference(
                    yaml_cfg, "redis", clock_redis_ref, role_name
                )

            heartbeat_redis_ref = role_cfg.get("heartbeat_redis")
            if heartbeat_redis_ref and isinstance(heartbeat_redis_ref, str):
                resolved_cfg["heartbeat_redis"] = _resolve_db_reference(
                    yaml_cfg, "redis", heartbeat_redis_ref, role_name
                )

            runtime["roles"][role_name] = resolved_cfg

    # Apply CLI overrides using dot notation
    # Support both "replay_date" and "defaults.replay_date" for convenience
    for key_path, value in overrides.items():
        if key_path.startswith("defaults."):
            key_path = key_path[9:]  # Remove "defaults."
        set_nested_value(runtime, key_path, value)

    # Normalize date fields to strings (YAML parses YYYYMMDD as int)
    _normalize_date_fields(runtime)

    return runtime


# ── Internal helpers ────────────────────────────────────────────────


def _resolve_db_reference(
    yaml_cfg: dict, db_type: str, db_ref: str, role_name: str
) -> Optional[dict]:
    """Resolve database reference string to actual config.

    Format: ``"db-name@MACHINE"`` or ``"db-name"`` (defaults to LOCAL).

    Examples::

        "redis-b@OLDMAN"   → {host: env(OLDMAN_IP), port: 6379, db: 0}
        "redis-b"          → {host: "127.0.0.1",    port: 6379, db: 0}
        "postgres-a@WSL2"  → {host: …, port: 5432, database: …, url: …}
    """
    # Parse "db-name@MACHINE" format
    if "@" in db_ref:
        db_name, machine_name = db_ref.rsplit("@", 1)
    else:
        db_name = db_ref
        machine_name = "LOCAL"

    # Get database template from databases section
    databases = yaml_cfg.get("databases", {})
    db_configs = databases.get(db_type, {})

    if db_name not in db_configs:
        logger.warning(
            f"{db_type.capitalize()} config '{db_name}' not found for role {role_name}. "
            f"Available: {list(db_configs.keys())}"
        )
        return None

    # Copy database template (handle None/empty entries in YAML)
    template = db_configs[db_name]
    resolved = copy.deepcopy(template) if template is not None else {}

    # Resolve host from network section
    network = yaml_cfg.get("network", {})
    if machine_name not in network:
        logger.warning(
            f"Machine '{machine_name}' not found in network section for {role_name}.{db_type}. "
            f"Available: {list(network.keys())}"
        )
        return None

    host_env_var = network[machine_name]

    if host_env_var is None:
        resolved["host"] = "127.0.0.1"
    else:
        host_value = os.getenv(host_env_var)
        if host_value:
            resolved["host"] = host_value
        else:
            logger.warning(
                f"Environment variable '{host_env_var}' not set for {role_name}.{db_type}. "
                f"Using env var name as placeholder."
            )
            resolved["host"] = f"${{{host_env_var}}}"

    # For influxdb, resolve the URL env var based on machine
    if db_type == "influxdb":
        resolved["influx_url_env"] = f"{machine_name}_INFLUXDB_URL"

    # For postgres, resolve the URL env var based on machine
    if db_type == "postgres":
        database_url_env = f"{machine_name}_DATABASE_URL"
        resolved["database_url_env"] = database_url_env
        resolved["url"] = os.getenv(database_url_env)

    return resolved


def _normalize_date_fields(config: dict) -> None:
    """Convert date fields from int to string format.

    YAML interprets values like ``20260115`` as integers, but we need
    them as strings.
    """
    date_fields = [
        "replay_date",
        "replay_time",
        "load_history",
        "start_from",
        "rollback_to",
    ]

    for field in date_fields:
        if field in config and config[field] is not None:
            config[field] = str(config[field])

    # Also check in roles for role-specific date fields
    if "roles" in config:
        for _role_name, role_cfg in config["roles"].items():
            if isinstance(role_cfg, dict):
                for field in date_fields:
                    if field in role_cfg and role_cfg[field] is not None:
                        role_cfg[field] = str(role_cfg[field])
