"""Canonical project-root discovery for jerry_trader.

Walks up from this file until it finds pyproject.toml, which marks the
repository root.  All other modules should import PROJECT_ROOT from here
instead of computing it with fragile `parents[N]` offsets.
"""

from pathlib import Path


def _find_project_root(marker: str = "pyproject.toml") -> Path:
    """Walk up from this file's directory until *marker* is found."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / marker).exists():
            return current
        current = current.parent
    raise FileNotFoundError(
        f"Could not locate project root (looked for {marker} above {Path(__file__).resolve()})"
    )


PROJECT_ROOT = _find_project_root()
