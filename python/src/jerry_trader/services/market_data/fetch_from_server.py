"""
Fetch data from remote server to local machine.

This script syncs metadata/reference data from the server machine to the client.
Used when UPDATE_MODE=client to get splits, tickers, indices, and float shares data.

Usage:
    python src/data/fetcher/fetch_from_server.py [--dry-run] [--paths PATH1,PATH2]
"""

import argparse
import os
import subprocess
import sys

import yaml

from jerry_trader.shared.utils.paths import PROJECT_ROOT


def load_config() -> dict:
    """Load update configuration from YAML file."""
    config_path = PROJECT_ROOT / "basic_config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            "Please create basic_config.yaml in the project root (copy from basic_config.yaml.example)."
        )

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_update_mode(config: dict) -> str:
    """Get update mode from environment variable or config file."""
    return os.environ.get("UPDATE_MODE", config["update"]["mode"])


def get_latest_files_info(directory: str) -> list[str]:
    """
    Get info about the latest versioned files in a directory.
    Looks for files with date suffixes like *_YYYYMMDD.parquet

    Returns:
        List of file descriptions with version dates
    """
    if not os.path.exists(directory):
        return []

    files = []
    for f in os.listdir(directory):
        if f.endswith(".parquet"):
            files.append(f)

    if not files:
        return []

    # Group by prefix (e.g., "all_splits", "all_stocks")
    from collections import defaultdict

    grouped = defaultdict(list)
    for f in files:
        # Extract prefix before the date (e.g., "all_splits_20260125.parquet" -> "all_splits")
        parts = f.rsplit("_", 1)
        if len(parts) == 2 and parts[1].replace(".parquet", "").isdigit():
            prefix = parts[0]
            grouped[prefix].append(f)
        else:
            grouped[f].append(f)

    # Get latest file for each prefix
    result = []
    for prefix, file_list in sorted(grouped.items()):
        latest = max(file_list)
        # Extract date from filename
        date_part = latest.rsplit("_", 1)[-1].replace(".parquet", "")
        if date_part.isdigit() and len(date_part) == 8:
            formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]}"
            result.append(f"  • {latest} (version: {formatted_date})")
        else:
            result.append(f"  • {latest}")

    return result


def sync_path(
    ssh_alias: str,
    remote_base: str,
    local_base: str,
    relative_path: str,
    rsync_options: str,
    dry_run: bool = False,
) -> bool:
    """
    Sync a single path from remote server to local machine using rsync.

    Args:
        ssh_alias: SSH alias or hostname for the remote server
        remote_base: Base data directory on remote server
        local_base: Base data directory on local machine
        relative_path: Relative path to sync (from base directory)
        rsync_options: Rsync command options
        dry_run: If True, only show what would be transferred

    Returns:
        bool: True if sync succeeded, False otherwise
    """
    remote_path = f"{ssh_alias}:{remote_base}/{relative_path}/"
    local_path = f"{local_base}/{relative_path}/"

    # Ensure local directory exists
    os.makedirs(local_path, exist_ok=True)

    # Build rsync command
    cmd = ["rsync"]
    cmd.extend(rsync_options.split())

    if dry_run:
        cmd.append("--dry-run")

    cmd.extend([remote_path, local_path])

    print(f"\n{'=' * 60}")
    print(f"Syncing: {relative_path}")
    print(f"  From: {remote_path}")
    print(f"  To:   {local_path}")
    if dry_run:
        print("  Mode: DRY RUN (no actual changes)")
    print(f"{'=' * 60}")

    try:
        result = subprocess.run(cmd, check=True, text=True)
        print(f"✓ Successfully synced: {relative_path}")

        # Display latest file versions
        files_info = get_latest_files_info(local_path)
        if files_info:
            print("  Latest files:")
            for info in files_info:
                print(info)

        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to sync {relative_path}: {e}")
        return False
    except FileNotFoundError:
        print("✗ Error: rsync command not found. Please install rsync.")
        return False


def fetch_from_server(
    dry_run: bool = False,
    specific_paths: list[str] | None = None,
) -> bool:
    """
    Fetch all configured paths from the remote server.

    Args:
        dry_run: If True, only show what would be transferred
        specific_paths: If provided, only sync these paths (relative paths)

    Returns:
        bool: True if all syncs succeeded, False otherwise
    """
    config = load_config()
    mode = get_update_mode(config)

    if mode != "client":
        print(f"Warning: Update mode is '{mode}', not 'client'.")
        print("This script is intended for client machines to sync from server.")
        print("Set UPDATE_MODE=client or update basic_config.yaml to proceed.")
        return False

    ssh_alias = config["multi_machine"]["server"]["ssh_alias"]
    remote_base = config["multi_machine"]["server"]["data_dir"]
    local_base = config["data"]["data_dir"]
    sync_paths = config["sync_paths"]
    rsync_options = config["rsync"]["options"]

    # Override dry_run from config if not explicitly set
    if not dry_run:
        dry_run = config["rsync"].get("dry_run", False)

    # Filter paths if specific ones are requested
    if specific_paths:
        sync_paths = [p for p in sync_paths if p in specific_paths]
        if not sync_paths:
            print(f"Warning: No matching paths found for: {specific_paths}")
            print(f"Available paths: {config['sync_paths']}")
            return False

    print(f"\n{'#' * 60}")
    print(f"# Fetching data from server: {ssh_alias}")
    print(f"# Remote base: {remote_base}")
    print(f"# Local base:  {local_base}")
    print(f"# Paths to sync: {len(sync_paths)}")
    print(f"{'#' * 60}")

    # Test SSH connection first
    print("\nTesting SSH connection...")
    try:
        result = subprocess.run(
            ["ssh", ssh_alias, "echo", "Connection successful"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            print(f"✗ SSH connection failed: {result.stderr}")
            return False
        print(f"✓ {result.stdout.strip()}")
    except subprocess.TimeoutExpired:
        print("✗ SSH connection timed out")
        return False
    except Exception as e:
        print(f"✗ SSH connection error: {e}")
        return False

    # Sync each path
    success_count = 0
    fail_count = 0

    for path in sync_paths:
        if sync_path(
            ssh_alias=ssh_alias,
            remote_base=remote_base,
            local_base=local_base,
            relative_path=path,
            rsync_options=rsync_options,
            dry_run=dry_run,
        ):
            success_count += 1
        else:
            fail_count += 1

    # Summary
    print(f"\n{'#' * 60}")
    print(f"# Sync Summary")
    print(f"#   Successful: {success_count}")
    print(f"#   Failed:     {fail_count}")
    print(f"{'#' * 60}")

    return fail_count == 0


def main():
    parser = argparse.ArgumentParser(
        description="Fetch data from remote server to local machine."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be transferred without actually syncing",
    )
    parser.add_argument(
        "--paths",
        type=str,
        help="Comma-separated list of specific paths to sync (relative paths)",
    )

    args = parser.parse_args()

    specific_paths = None
    if args.paths:
        specific_paths = [p.strip() for p in args.paths.split(",")]

    success = fetch_from_server(
        dry_run=args.dry_run,
        specific_paths=specific_paths,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
