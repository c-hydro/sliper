#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="/home/admin/soilslips-system/manager_data"

CONFIG_FOLDER="$ROOT"
CONFIG_FILENAME="sliper_tools_sync_data_sm_obs.json"

LOCK_FOLDER="/home/admin/lock/manager_data"
LOCK_FILENAME="sliper_tools_sync_data_sm_obs.lock"

LOG_FOLDER="/home/admin/log/manager_data"
LOG_FILENAME="sync_data_sm_obs.log"

mkdir -p "$LOCK_FOLDER" "$LOG_FOLDER"

LOGFILE="$LOG_FOLDER/$LOG_FILENAME"
LOCKFILE="$LOCK_FOLDER/$LOCK_FILENAME"

# Remove old log explicitly
[ -f "$LOGFILE" ] && rm -f "$LOGFILE"

# Build the command
CMD="$ROOT/sliper_tools_sync_data.sh \
  --mode now \
  --config $CONFIG_FOLDER/$CONFIG_FILENAME \
  --verbose \
  --n-days 1 \
  --debug-gate \
  --start-hour 00:00 \
  --end-at-date-obs \
  --filename-date-regex 'hmc\.output-grid\.(?P<ts>[0-9]{12})\.nc\.gz' \
  --lockfile $LOCKFILE"

# Print the command to screen
echo "About to run:"
echo "$CMD"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting script ..."
  eval "$CMD"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ... DONE!"
  echo
} >> "$LOGFILE" 2>&1








