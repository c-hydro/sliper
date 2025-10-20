#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="/home/admin/soilslips-system/manager_data"

CONFIG_FOLDER="$ROOT"
CONFIG_FILENAME="sliper_tools_sync_data_rain_frc.json"

LOCK_FOLDER="/home/admin/lock/manager_data"
LOCK_FILENAME="sliper_tools_sync_data_rain_frc.lock"

LOG_FOLDER="/home/admin/log/manager_data"
LOG_FILENAME="sync_data_rain_frc.log"

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












