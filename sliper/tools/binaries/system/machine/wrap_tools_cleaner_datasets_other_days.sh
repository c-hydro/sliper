#!/usr/bin/env bash
# ====================================================================
# sliper_tools_run_cleaner_cron.sh
# Wrapper to run sliper_tools_cleaner_datasets_other_days.sh from CRON safely
# ====================================================================

# Set strict mode for reliability
set -euo pipefail
IFS=$'\n\t'

# Environment setup (cron has a minimal env)
export PATH="/usr/local/bin:/usr/bin:/bin"
export TZ="Europe/Rome"

# Define paths
ROOT="/home/admin/soilslips-system/manager_system"
SCRIPT="$ROOT/sliper_tools_cleaner_datasets_other_days.sh"
CATCH_FILE="$ROOT/sliper_tools_cleaner_datasets_catchments.txt"
LOG_DIR="/home/admin/log/manager_system"
LOG_FILE="$LOG_DIR/cleaner_datasets_other_days_$(date +'%Y%m%d_%H%M').log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Helper function to log to both console and file
log() {
    echo "$@" | tee -a "$LOG_FILE"
}

log "=============================================================="
log "[$(date '+%Y-%m-%d %H:%M:%S')] Starting sliper_tools_cleaner_datasets_other_days run"
log "--------------------------------------------------------------"
log "Command:"
log "$SCRIPT --catch-file $CATCH_FILE --when today-1 --n-days 7 --log-file $LOG_FILE"
log "--------------------------------------------------------------"

# Run command and capture output
if "$SCRIPT" \
    --catch-file "$CATCH_FILE" \
    --when today-1 \
    --n-days 10 \
    --log-file "$LOG_FILE" 2>&1 | tee -a "$LOG_FILE"; then
    log "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ SUCCESS"
else
    EXIT_CODE=$?
    log "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ ERROR (exit code $EXIT_CODE)"
fi

log "--------------------------------------------------------------"
log "Completed at $(date '+%Y-%m-%d %H:%M:%S')"
log "=============================================================="

