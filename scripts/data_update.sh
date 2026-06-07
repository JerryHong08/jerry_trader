#!/bin/bash
# ===========================
# Incremental Data Update Script
# ===========================
# Supports three modes:
#   - standalone: Single machine — fetches APIs + downloads Polygon data + processes
#   - server:     Multi-machine — fetches metadata from external APIs (Polygon, FMP)
#   - client:     Multi-machine — downloads Polygon data, syncs metadata from server
#
# Usage:
#   ./scripts/data_update.sh                         # standalone (default)
#   UPDATE_MODE=server ./scripts/data_update.sh      # multi-machine: server
#   UPDATE_MODE=client ./scripts/data_update.sh      # multi-machine: client

set -e

# ===========================
# Configuration
# ===========================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="$PROJECT_ROOT/basic_config.yaml"

LOGFILE="$PROJECT_ROOT/logs/data_update_logs/data_update_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$PROJECT_ROOT/logs/data_update_logs"

TOTAL_DAYS=7

# ===========================
# Helper Functions
# ===========================
get_update_mode() {
    # Priority: 1. Environment variable, 2. Config file, 3. Default
    if [ -n "$UPDATE_MODE" ]; then
        echo "$UPDATE_MODE"
        return
    fi

    if [ -f "$CONFIG_FILE" ]; then
        local mode=$(grep -A1 "^update:" "$CONFIG_FILE" | grep "mode:" | sed 's/.*mode:[[:space:]]*"\([^"]*\)".*/\1/')
        if [ -n "$mode" ]; then
            echo "$mode"
            return
        fi
    fi

    # Default to standalone
    echo "standalone"
}

UPDATE_MODE=$(get_update_mode)
current_task=0

# Set total tasks based on mode
if [ "$UPDATE_MODE" = "server" ]; then
    TOTAL_TASKS=4
elif [ "$UPDATE_MODE" = "client" ]; then
    TOTAL_TASKS=6
else
    # standalone: API fetch (4) + download (2) + convert (2) = 8
    TOTAL_TASKS=8
fi

run_task() {
    local task_name="$1"
    shift
    local command=("$@")

    current_task=$((current_task + 1))
    echo "" | tee -a "$LOGFILE"
    echo "[$current_task/$TOTAL_TASKS] $task_name" | tee -a "$LOGFILE"
    echo "Command: ${command[*]}" | tee -a "$LOGFILE"
    echo "---" | tee -a "$LOGFILE"
    "${command[@]}" 2>&1 | tee -a "$LOGFILE"
}

MKT_DATA="python/src/jerry_trader/services/market_data"
STATIC_DIR="$MKT_DATA/static"
BOOTSTRAP_DIR="$MKT_DATA/bootstrap"

# ===========================
# Main Execution
# ===========================
cd "$PROJECT_ROOT"

echo "=============================================" | tee -a "$LOGFILE"
echo "Data Update - $(date)" | tee -a "$LOGFILE"
echo "Update Mode: $UPDATE_MODE" | tee -a "$LOGFILE"
echo "Config File: $CONFIG_FILE" | tee -a "$LOGFILE"
echo "=============================================" | tee -a "$LOGFILE"

if [ "$UPDATE_MODE" = "standalone" ]; then
    # ===========================
    # STANDALONE MODE (single machine)
    # ===========================
    # Does everything: API fetch + Polygon download + convert + process

    echo "" | tee -a "$LOGFILE"
    echo "=== Running in STANDALONE mode ===" | tee -a "$LOGFILE"
    echo "Fetching APIs + downloading data + processing on this machine..." | tee -a "$LOGFILE"

    # Step 1: Fetch metadata from APIs
    run_task "Fetching splits data..." \
        python $STATIC_DIR/splits_fetch.py

    run_task "Fetching tickers list (stocks, otc, indices)..." \
        python $STATIC_DIR/all_tickers_fetch.py

    run_task "Fetching indices data (IRX, SPX, etc.)..." \
        python $STATIC_DIR/indices_fetch.py

    run_task "Fetching float shares data..." \
        python $STATIC_DIR/fmp_fundamental_fetch.py

    # Step 2: Download stock data from Polygon
    run_task "Downloading minute_aggs_v1 data..." \
        python $BOOTSTRAP_DIR/polygon_downloader.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days $TOTAL_DAYS

    run_task "Downloading day_aggs_v1 data..." \
        python $BOOTSTRAP_DIR/polygon_downloader.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days $TOTAL_DAYS

    # Step 3: Convert to Parquet format
    run_task "Transforming minute_aggs_v1 to Parquet..." \
        python $BOOTSTRAP_DIR/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days $TOTAL_DAYS

    run_task "Transforming day_aggs_v1 to Parquet..." \
        python $BOOTSTRAP_DIR/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days $TOTAL_DAYS

    echo "" | tee -a "$LOGFILE"
    echo "=== Standalone tasks completed ===" | tee -a "$LOGFILE"

elif [ "$UPDATE_MODE" = "server" ]; then
    # ===========================
    # SERVER MODE (multi-machine)
    # ===========================
    # Fetches metadata from external APIs (Polygon, FMP)
    # This data will be synced to clients via fetch_from_server.py

    echo "" | tee -a "$LOGFILE"
    echo "=== Running as SERVER ===" | tee -a "$LOGFILE"
    echo "Fetching metadata from external APIs..." | tee -a "$LOGFILE"

    run_task "Fetching splits data..." \
        python $STATIC_DIR/splits_fetch.py

    run_task "Fetching tickers list (stocks, otc, indices)..." \
        python $STATIC_DIR/all_tickers_fetch.py

    run_task "Fetching indices data (IRX, SPX, etc.)..." \
        python $STATIC_DIR/indices_fetch.py

    run_task "Fetching float shares data..." \
        python $STATIC_DIR/fmp_fundamental_fetch.py

    echo "" | tee -a "$LOGFILE"
    echo "=== Server tasks completed ===" | tee -a "$LOGFILE"
    echo "Clients can now sync this data using fetch_from_server.py" | tee -a "$LOGFILE"

elif [ "$UPDATE_MODE" = "client" ]; then
    # ===========================
    # CLIENT MODE (multi-machine)
    # ===========================
    # 1. Downloads stock data from Polygon
    # 2. Converts to Parquet format
    # 3. Syncs metadata from server

    echo "" | tee -a "$LOGFILE"
    echo "=== Running as CLIENT ===" | tee -a "$LOGFILE"
    echo "Downloading stock data and syncing metadata from server..." | tee -a "$LOGFILE"

    # Step 1: Download stock data from Polygon
    run_task "Downloading minute_aggs_v1 data..." \
        python $BOOTSTRAP_DIR/polygon_downloader.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days $TOTAL_DAYS

    run_task "Downloading day_aggs_v1 data..." \
        python $BOOTSTRAP_DIR/polygon_downloader.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days $TOTAL_DAYS

    # Step 2: Convert to Parquet format
    run_task "Transforming minute_aggs_v1 to Parquet..." \
        python $BOOTSTRAP_DIR/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days $TOTAL_DAYS

    run_task "Transforming day_aggs_v1 to Parquet..." \
        python $BOOTSTRAP_DIR/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days $TOTAL_DAYS

    # Step 3: Sync metadata from server (splits, tickers, indices, float_shares)
    run_task "Syncing metadata from server..." \
        python $MKT_DATA/fetch_from_server.py

    echo "" | tee -a "$LOGFILE"
    echo "=== Client tasks completed ===" | tee -a "$LOGFILE"

else
    echo "ERROR: Unknown UPDATE_MODE: $UPDATE_MODE" | tee -a "$LOGFILE"
    echo "Valid modes: standalone, server, client" | tee -a "$LOGFILE"
    exit 1
fi

echo "" | tee -a "$LOGFILE"
echo "=============================================" | tee -a "$LOGFILE"
echo "Data Update Completed - $(date)" | tee -a "$LOGFILE"
echo "Log file: $LOGFILE" | tee -a "$LOGFILE"
echo "=============================================" | tee -a "$LOGFILE"

exit 0

# Usage:
# chmod +x scripts/data_update.sh
# ./scripts/data_update.sh                         # standalone (default)
# UPDATE_MODE=server ./scripts/data_update.sh      # multi-machine: server
# UPDATE_MODE=client ./scripts/data_update.sh      # multi-machine: client
