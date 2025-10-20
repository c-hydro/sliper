#!/usr/bin/env bash
# run_time_ref.sh
# Usage:
#   ./sliper_app_time_reference.sh                     # realtime (today)
#   ./sliper_app_time_reference.sh --mode history --date 2025-10-01
#   ./sliper_app_time_reference.sh --mode now --folder-template "/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"
#
# Tip: if you want TIME_REF in your current shell, source it:
#   . ./sliper_app_time_reference.sh --mode history --date 2025-10-01

set -euo pipefail

# ---------- defaults ----------
MODE="now"   # now | history
DATE=""      # required when MODE=history
FOLDER_TEMPLATE="/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"

# ---------- parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"; shift 2;;
    --date)
      DATE="${2:-}"; shift 2;;
    --folder-template)
      FOLDER_TEMPLATE="${2:-}"; shift 2;;
    -h|--help)
      echo "Usage: $0 [--mode now|history] [--date YYYY-MM-DD] [--folder-template TEMPLATE]"
      exit 0;;
    *)
      echo "Unknown arg: $1" >&2; exit 1;;
  esac
done

# ---------- resolve date ----------
if [[ "$MODE" == "now" ]]; then
  DATE="$(date +%F)"   # YYYY-MM-DD from system time
elif [[ "$MODE" == "history" ]]; then
  if [[ -z "${DATE}" ]]; then
    echo "Error: --date YYYY-MM-DD is required in history mode." >&2
    exit 2
  fi
else
  echo "Error: --mode must be 'now' or 'history'." >&2
  exit 2
fi

# ---------- build actual folder path (for info/logging/export) ----------
# NOTE: this uses GNU date's strftime expansion on the given DATE.
FOLDER="$(date -d "${DATE}" +"${FOLDER_TEMPLATE}")"

# ---------- compute TIME_REF via Python helper ----------
# - prints either 'export TIME_REF="YYYY-MM-DD HH:MM"' (with --export) or the plain value
# - we eval the export line so TIME_REF is set in this shell
if ! EXPORT_LINE="$(python3 sliper_app_time_reference.py --date "${DATE}" --folder-template "${FOLDER_TEMPLATE}" --export)"; then
  echo "Error: failed to compute TIME_REF for ${DATE} in template ${FOLDER_TEMPLATE}" >&2
  exit 3
fi
eval "${EXPORT_LINE}"

# also export the resolved folder path for convenience
export FOLDER

# ---------- print results ----------
echo "FOLDER=${FOLDER}"
echo "TIME_REF=${TIME_REF}"


