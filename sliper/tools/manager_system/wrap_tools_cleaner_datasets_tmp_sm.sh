#!/usr/bin/env bash
# ====================================================================
# sliper_tools_run_cleaner_tmp_sm_cron.sh
# Wrapper to run sliper_tools_cleaner_datasets_tmp.sh from CRON safely
# ====================================================================

set -euo pipefail
IFS=$'\n\t'

export PATH="/usr/local/bin:/usr/bin:/bin"
export TZ="Europe/Rome"

ROOT="/home/admin/soilslips-system/manager_system"
SCRIPT="$ROOT/sliper_tools_cleaner_datasets_tmp.sh"
DATA_ROOT="/home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/{catchment_name}/{Y}/{m}/{d}"
LOG_DIR="$ROOT/logs"
LOG_FILE="home/admin/log/manager_system/cleaner_tmp_sm_$(date +'%Y%m%d_%H%M%S').log"

mkdir -p "$LOG_DIR"

log(){ echo "$@" | tee -a "$LOG_FILE"; }

log "=============================================================="
log "[$(date '+%Y-%m-%d %H:%M:%S')] Starting sliper_tools_cleaner_datasets_tmp_sm run"
log "--------------------------------------------------------------"
log "Command:"
log "$SCRIPT --mode realtime --n-days 3 --root $DATA_ROOT --dry-run --verbose"
log "--------------------------------------------------------------"

if [[ ! -x "$SCRIPT" ]]; then
  log "[ERROR] Script not found or not executable: $SCRIPT"
  exit 2
fi

if "$SCRIPT" \
    --mode realtime \
    --n-days 3 \
    --root "$DATA_ROOT" \
    --dry-run \
    --verbose 2>&1 | tee -a "$LOG_FILE"; then
  log "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ SUCCESS"
else
  ec=$?
  log "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ ERROR (exit code $ec)"
  exit "$ec"
fi

log "--------------------------------------------------------------"
log "Completed at $(date '+%Y-%m-%d %H:%M:%S')"
log "=============================================================="

