#!/usr/bin/env bash
# ====================================================================
# sliper_tools_run_mover_files_cron.sh
# Wrapper to run sliper_tools_mover_data_files.sh from CRON safely
# ====================================================================

# Set strict mode for reliability
set -euo pipefail
IFS=$'\n\t'

# Environment setup (cron has a minimal env)
export PATH="/usr/local/bin:/usr/bin:/bin"
export TZ="Europe/Rome"

# Define paths
ROOT="/home/admin/soilslips-system/manager_system"
SCRIPT="$ROOT/sliper_tools_mover_data_files.sh"
RULES_FILE="$ROOT/sliper_tools_mover_data_files.txt"
CATCH_FILE="$ROOT/sliper_tools_mover_data_catchments.txt"
LOG_DIR="$ROOT/logs"
LOG_FILE="/home/admin/log/manager_system/mover_files_$(date +'%Y%m%d_%H%M%S').log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Helper function to log both to console and file
log() {
    echo "$@" | tee -a "$LOG_FILE"
}

log "=============================================================="
log "[$(date '+%Y-%m-%d %H:%M:%S')] Starting sliper_tools_mover_data_files run"
log "--------------------------------------------------------------"
log "Command:"
log "$SCRIPT --rules $RULES_FILE --catch-file $CATCH_FILE"
log "--------------------------------------------------------------"

# Run the command and capture its output
if "$SCRIPT" --rules "$RULES_FILE" --catch-file "$CATCH_FILE" 2>&1 | tee -a "$LOG_FILE"; then
    log "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ SUCCESS"
else
    EXIT_CODE=$?
    log "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ ERROR (exit code $EXIT_CODE)"
fi

log "--------------------------------------------------------------"
log "Completed at $(date '+%Y-%m-%d %H:%M:%S')"
log "=============================================================="

