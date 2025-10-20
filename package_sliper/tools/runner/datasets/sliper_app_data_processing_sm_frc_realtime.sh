#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------------------
# Script information
SCRIPT_NAME='SLIPER - APP DATA PROCESSING SM - REALTIME'
SCRIPT_VERSION="1.1.0"
SCRIPT_DATE='2025/10/03'
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Defaults (overridden by CLI)
SCRIPT_MODE="now"      # now|history
TIME_RUN=""            # user input; in history mode: 'YYYY-MM-DD' OR 'YYYY-MM-DD HH:MM'; empty in now mode
TIME_ALG=""            # normalized datetime for algorithm: 'YYYY-MM-DD HH:MM'
TIME_ANC=""            # normalized date for ancillary: 'YYYY-MM-DD'
SKIP_ALGORITHM=false   # test mode: skip algorithm step
SKIP_ANCILLARY=false   # optionally skip ancillary step as well
TZ_LOCAL=""            # timezone (e.g., Europe/Rome) for 'now' mode time calc, and validations
ROUND_HOUR=true        # optionally round TIME_ALG minutes to 0

# User-configurable env/paths
CONDA_HOME="/home/admin/library/env_conda_sliper_data"
CONDA_ENV="sliper_data_libraries"

ANC_FOLDER="/home/admin/library/package_sliper/time"
ANC_SCRIPT="/home/admin/library/package_sliper/time/sliper_app_time_reference.py"
ANC_SETTINGS_FOLDER_TEMPLATE="/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"

ALG_FOLDER="/home/admin/library/package_sliper/data/sm"
ALG_SCRIPT="/home/admin/library/package_sliper/data/sm/sliper_app_data_processing_sm_main.py"
ALG_SETTINGS="/home/admin/soilslips-exec/data/sliper_app_data_processing_sm_frc_realtime.json"

LOG_FOLDER="/home/admin/lock/runner_data"
LOCKFILE="${LOCKFILE:-/tmp/process_data_sm_frc.lock}"
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# script usage
usage() {
  cat <<EOF
Usage: $(basename "$0") [--mode now|history] [--date 'YYYY-MM-DD'|'YYYY-MM-DD HH:MM'] [--skip-algorithm] [--skip-ancillary] [--tz ZONE]

Options:
  --mode            Processing mode (default: now). Use 'history' with --date.
  --date            Date/time to process. Accepts 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM'.
                    Required if --mode history. For --mode now, current time is used.
  --skip-algorithm  Test mode: run ancillary, print algorithm command, but do NOT execute it.
  --skip-ancillary  Skip ancillary step entirely (useful with --skip-algorithm or quick runs).
  --round-hour      Truncate TIME_ALG minutes to HH:00 (always down).
  --tz ZONE         Timezone for 'now' mode and input validation (e.g., Europe/Rome).
  -h, --help        Show this help.

Behavior:
  - In 'now' mode, TIME_ALG defaults to current 'YYYY-MM-DD HH:MM' (in --tz if given),
    and TIME_ANC to current 'YYYY-MM-DD'.
  - In 'history' mode, --date is normalized:
      * 'YYYY-MM-DD'        -> TIME_ALG='YYYY-MM-DD 00:00', TIME_ANC='YYYY-MM-DD'
      * 'YYYY-MM-DD HH:MM'  -> TIME_ALG as given,        TIME_ANC='YYYY-MM-DD'
  - Ancillary is always called with --date TIME_ANC (YYYY-MM-DD).
  - Algorithm is always called with -time TIME_ALG (YYYY-MM-DD HH:MM).

Examples:
  $(basename "$0") --mode now --tz Europe/Rome
  $(basename "$0") --mode history --date 2025-09-20
  $(basename "$0") --mode history --date '2025-09-20 13:45'
  $(basename "$0") --mode history --date '2025-09-20 13:45' --round-hour
  $(basename "$0") --mode now --skip-algorithm
EOF
}
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Parse CLI
while [[ ${1:-} ]]; do
  case "$1" in
    --mode)            SCRIPT_MODE="${2:-}"; shift 2 ;;
    --date)            TIME_RUN="${2:-}";    shift 2 ;;
    --skip-algorithm)  SKIP_ALGORITHM=true;  shift ;;
    --skip-ancillary)  SKIP_ANCILLARY=true;  shift ;;
    --round-hour)      ROUND_HOUR=true;      shift ;;
    --tz)              TZ_LOCAL="${2:-}";    shift 2 ;;
    -h|--help)         usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

# validate script mode
if [[ "$SCRIPT_MODE" != "now" && "$SCRIPT_MODE" != "history" ]]; then
  echo " ===> ERROR: --mode must be 'now' or 'history'." >&2
  exit 1
fi
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Logging setup
mkdir -p "$LOG_FOLDER"
LOG_TIMESTAMP="$(date +'%Y%m%d_%H')"
LOG_FILE="$LOG_FOLDER/process_data_sm_frc_${LOG_TIMESTAMP}.txt"
exec > >(tee -a "$LOG_FILE") 2>&1
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# algorithm headers - start
echo " ==================================================================================="
echo " ==> $SCRIPT_NAME (Version: $SCRIPT_VERSION  Release_Date: $SCRIPT_DATE)"
echo " ==> START ..."
echo " "

# Resolve TIME_REF (YYYY-MM-DD HH:MM) and DATE_REF (YYYY-MM-DD)
if [[ "$SCRIPT_MODE" == "now" ]]; then
  if [[ -n "${TIME_RUN:-}" ]]; then
    # --date provided in now mode: use it (same parsing rules as history)
    if [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      TIME_ANC="$TIME_RUN"
      TIME_ALG="$TIME_RUN 00:00"
    elif [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]][0-9]{2}:[0-9]{2}$ ]]; then
      TIME_ALG="$TIME_RUN"
      TIME_ANC="${TIME_RUN:0:10}"
    else
      echo " ===> ERROR: --date must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' format." >&2
      exit 1
    fi

    # Validate using date(1) in the chosen TZ
    if [[ -n "$TZ_LOCAL" ]]; then
      TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:%M' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2; exit 1; }
      TZ="$TZ_LOCAL" date -d "$TIME_ANC" '+%F'       >/dev/null 2>&1 || { echo " ===> ERROR: invalid date: $TIME_ANC" >&2; exit 1; }
    else
      date -d "$TIME_ALG" '+%F %H:%M' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2; exit 1; }
      date -d "$TIME_ANC" '+%F'       >/dev/null 2>&1 || { echo " ===> ERROR: invalid date: $TIME_ANC" >&2; exit 1; }
    fi
  else
    # no --date: fall back to current time (in TZ if provided)
    if [[ -n "$TZ_LOCAL" ]]; then
      TIME_ALG="$(TZ="$TZ_LOCAL" date '+%F %H:%M')"
      TIME_ANC="$(TZ="$TZ_LOCAL" date '+%F')"
    else
      TIME_ALG="$(date '+%F %H:%M')"
      TIME_ANC="$(date '+%F')"
    fi
  fi
else
  # history mode: require TIME_RUN
  if [[ -z "${TIME_RUN:-}" ]]; then
    echo " ===> ERROR: --date 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' is required in history mode." >&2
    exit 1
  fi

  if [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    TIME_ANC="$TIME_RUN"
    TIME_ALG="$TIME_RUN 00:00"
  elif [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]][0-9]{2}:[0-9]{2}$ ]]; then
    TIME_ALG="$TIME_RUN"
    TIME_ANC="${TIME_RUN:0:10}"
  else
    echo " ===> ERROR: --date must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' format." >&2
    exit 1
  fi

  # Validate using date(1) in the chosen TZ
  if [[ -n "$TZ_LOCAL" ]]; then
    TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:%M' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2; exit 1; }
    TZ="$TZ_LOCAL" date -d "$TIME_ANC" '+%F'       >/dev/null 2>&1 || { echo " ===> ERROR: invalid date: $TIME_ANC" >&2; exit 1; }
  else
    date -d "$TIME_ALG" '+%F %H:%M' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2; exit 1; }
    date -d "$TIME_ANC" '+%F'       >/dev/null 2>&1 || { echo " ===> ERROR: invalid date: $TIME_ANC" >&2; exit 1; }
  fi
fi

# truncate TIME_ALG to HH:00 (always down if selected))
if $ROUND_HOUR; then
  if [[ -n "$TZ_LOCAL" ]]; then
    TIME_ALG="$(TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:00')"
  else
    TIME_ALG="$(date -d "$TIME_ALG" '+%F %H:00')"
  fi
fi

# print info
echo " ==> MODE: $SCRIPT_MODE | TIME_RUN: ${TIME_RUN:-<now>} | TIME_ALG: $TIME_ALG | TIME_ANC: $TIME_ANC | SKIP_ALGORITHM=$SKIP_ALGORITHM | SKIP_ANCILLARY=$SKIP_ANCILLARY | TZ=${TZ_LOCAL:-system}"
echo " ==> LOGGING TO: $LOG_FILE"
echo " "
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Optional: prevent overlapping runs via flock
exec {LOCKFD}>"$LOCKFILE"
if ! flock -n "$LOCKFD"; then
  echo " ===> Another instance is running (lock: $LOCKFILE)"; exit 9
fi

# Tidy exit logs
trap 'rc=$?; echo " ==> EXIT (rc=$rc)"; exit $rc' EXIT
trap 'echo " ==> Caught SIGINT"; exit 130' INT
trap 'echo " ==> Caught SIGTERM"; exit 143' TERM
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# CONDA STEP
# conda check
if [[ ! -d "$CONDA_HOME" ]]; then
  echo " ===> ERROR: CONDA_HOME not found: $CONDA_HOME" >&2
  exit 2
fi
CONDA_SH="$CONDA_HOME/etc/profile.d/conda.sh"
if [[ ! -f "$CONDA_SH" ]]; then
  echo " ===> ERROR: conda.sh not found in $CONDA_HOME/etc/profile.d/" >&2
  exit 2
fi

# conda activation
# shellcheck source=/dev/null
source "$CONDA_SH"
if ! command -v conda >/dev/null 2>&1; then
  echo " ===> ERROR: 'conda' command not available after sourcing $CONDA_SH" >&2
  exit 2
fi
if ! conda activate "$CONDA_ENV" >/dev/null 2>&1; then
  echo " ===> ERROR: could not activate env '$CONDA_ENV'" >&2
  echo " ===> Hint: run '$CONDA_HOME/bin/conda env list' to see available envs" >&2
  exit 2
fi
echo " ===> CONDA ENVIRONMENT '$CONDA_ENV' ACTIVATED"

# Ensure python is present
if ! command -v python >/dev/null 2>&1; then
  echo " ===> ERROR: 'python' not found in env '$CONDA_ENV'." >&2
  exit 2
fi

# PYTHONPATH STEP
export PYTHONPATH="${PYTHONPATH:-}:$ANC_FOLDER:$ALG_FOLDER"
echo " ===> PYTHONPATH FOLDERS:"
echo "$PYTHONPATH" \
  | tr ':' '\n' \
  | grep -v '^$' \
  | awk '{print " :::: [" NR "] " $0}'

# Ensure required Python entrypoints exist
for p in "$ANC_SCRIPT" "$ALG_SCRIPT"; do
  if [[ ! -f "$p" ]]; then
    echo " ===> ERROR: required script not found: $p" >&2
    exit 2
  fi
done
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# ANCILLARY STEP
echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ..."

if [[ "${SKIP_ANCILLARY:-false}" != "true" ]]; then
  
  
  # Build settings folder with DATE_REF (respect TZ for template expansion as well)
  if [[ -n "$TZ_LOCAL" ]]; then
    ANC_SETTINGS_FOLDER="$(TZ="$TZ_LOCAL" date -d "$TIME_ANC" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"
  else
    ANC_SETTINGS_FOLDER="$(date -d "$TIME_ANC" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"
  fi
  
  CMD_ANC_DEF=( python "$ANC_SCRIPT" --date "$TIME_ANC" --folder-template "$ANC_SETTINGS_FOLDER" --export )
  
  # Run ancillary, capture stdout even if exit code != 0
  set +e
  CMD_ANC_EXPORT="$("${CMD_ANC_DEF[@]}")"
  ANC_RC=$?
  set -e

  if [[ -z "$CMD_ANC_EXPORT" ]]; then
    echo " ===> ERROR: ancillary tool produced no output." >&2
    exit 3
  fi

  # Optionally, validate the export format (relaxed)
  # [[ "$CMD_ANC_EXPORT" =~ ^(export[[:space:]]+[A-Z_]+=\"[^\"]*\"[[:space:]]*)+$ ]] || { echo " ===> ERROR: unsafe ancillary export"; exit 3; }

  echo " :::: ANC EXPORT ${CMD_ANC_EXPORT}"
  eval "$CMD_ANC_EXPORT"
  echo " :::: SUMMARY_INFO_FOLDER_REF: ${FOLDER_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_FILE_REF:   ${FILE_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_REF:   ${TIME_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_ANC:   ${TIME_ANC:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_RUN:   ${TIME_RUN:-<unset>}"
  
  # Skip algorithm if NOT_FOUND
  if [[ "$TIME_REF" == "NOT_FOUND" ]]; then
	  echo " ===> WARNING: Ancillary returned NOT_FOUND for $TIME_ANC (folder: $FOLDER_REF); skipping algorithm."
	  SKIP_ALGORITHM=true
  fi

  echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... DONE"
else
  echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... SKIPPED (BY FLAG)"
fi
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# ALGORITHM STEP
echo " ===> RUN ALGORITHM ..."

CMD_ALG_DEF=( python "$ALG_SCRIPT" -settings_file "$ALG_SETTINGS" -time "$TIME_REF" )
echo " :::: ALG CMD  ${CMD_ALG_DEF[*]}"


# Check TIME_REF validity
if [[ -z "${TIME_REF:-}" || "$TIME_REF" == "NOT_FOUND" ]]; then
  echo " ===> RUN ALGORITHM ... SKIPPED (invalid TIME_REF: ${TIME_REF:-<unset>})"
else

  if $SKIP_ALGORITHM; then
    echo " ===> RUN ALGORITHM ... SKIPPED (user flag --skip-algorithm)"
  else
    "${CMD_ALG_DEF[@]}"
    echo " ===> RUN ALGORITHM ... DONE"
  fi
fi
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# algorithm headers - end
echo " ==> $SCRIPT_NAME (Version: $SCRIPT_VERSION  Release_Date: $SCRIPT_DATE)"
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# -----------------------------------------------------------------------------------------

