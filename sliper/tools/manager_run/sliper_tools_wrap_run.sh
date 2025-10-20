#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="/home/admin/soilslips-system/manager_run"

CONFIG_FOLDER="$ROOT"
CONFIG_FILENAME="sliper_tools_select_run.json"

LOCK_FOLDER="/home/admin/lock/manager_run"
LOCK_FILENAME="sliper_tools_select_run.lock"

LOG_FOLDER="/home/admin/log/manager_run"
LOG_FILENAME="select_run.log"

mkdir -p "$LOCK_FOLDER" "$LOG_FOLDER"

LOGFILE="$LOG_FOLDER/$LOG_FILENAME"
LOCKFILE="$LOCK_FOLDER/$LOCK_FILENAME"

# Remove old log explicitly
[ -f "$LOGFILE" ] && rm -f "$LOGFILE"

# Build the command
CMD="$ROOT/sliper_tools_select_run.sh \
  --mode now \
  --config $CONFIG_FOLDER/$CONFIG_FILENAME"

# Print the command to screen
echo "About to run:"
echo "$CMD"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting script ..."
  eval "$CMD"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ... DONE!"
  echo
} >> "$LOGFILE" 2>&1












