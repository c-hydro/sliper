#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------------------
# Cron-safe environment
export HOME=/home/admin
export SHELL=/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
umask 022
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Script information
SCRIPT_NAME='SLIPER - APP SCENARIOS - OBS DATABASE'
SCRIPT_VERSION="1.3.0"
SCRIPT_DATE='2026/06/29'
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Defaults
SCRIPT_MODE="now"      # now|history|dataset

TIME_RUN=""
TIME_ALG=""
TIME_ANC=""

TIME_START=""
TIME_END=""
START_SEC=""
END_SEC=""
N_DAYS=""

SKIP_ALGORITHM=false
SKIP_ANCILLARY=false
TZ_LOCAL=""
ROUND_HOUR=true
FORCE_RUN=false

# User-configurable env/paths
CONDA_HOME="/home/admin/library/env_conda_sliper_data"
CONDA_ENV="sliper_data_libraries"

ANC_FOLDER="/home/admin/library/package_sliper/time"
ANC_SCRIPT="/home/admin/library/package_sliper/time/sliper_app_time_reference.py"
ANC_SETTINGS_FOLDER_TEMPLATE="/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"

ALG_FOLDER="/home/admin/library/package_sliper/apps/scenarios"
ALG_SCRIPT="/home/admin/library/package_sliper/apps/scenarios/sliper_app_scenarios_main.py"
ALG_SETTINGS="/home/admin/soilslips-exec/scenarios/sliper_app_scenarios_obs_db.json"

# log setup
LOG_FOLDER="${LOG_FOLDER:-/home/admin/log/runner_scenarios}"
LOG_BASENAME="${LOG_BASENAME:-bash_scenarios_obs_db}"
LOG_TIMESTAMP="${LOG_TIMESTAMP:-$(date +'%Y%m%d_%H')}"
LOG_FILE="${LOG_FILE:-$LOG_FOLDER/${LOG_BASENAME}_${LOG_TIMESTAMP}.log}"

# lock setup
LOCK_FOLDER="${LOCK_FOLDER:-/home/admin/lock/runner_scenarios}"
LOCK_BASENAME="${LOCK_BASENAME:-bash_scenarios_obs_db}"
LOCK_FILE="${LOCK_FILE:-$LOCK_FOLDER/${LOCK_BASENAME}.lock}"

ts() { date '+%F %T %Z'; }
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# usage
usage() {
cat <<EOF
Usage:
  $(basename "$0") --mode now [--date 'YYYY-MM-DD HH:MM']
  $(basename "$0") --mode history --date 'YYYY-MM-DD HH:MM'
  $(basename "$0") --mode dataset --time-start 'YYYY-MM-DD HH:MM' --time-end 'YYYY-MM-DD HH:MM'

Options:
  --mode now|history|dataset

  --date 'YYYY-MM-DD'|'YYYY-MM-DD HH:MM'
      Used by now/history modes.

  --time-start 'YYYY-MM-DD HH:MM'
      Dataset start time. Required in dataset mode.

  --time-end 'YYYY-MM-DD HH:MM'
      Dataset end time. Required in dataset mode.
      This is the only time passed to the Python algorithm.

  --skip-algorithm
  --skip-ancillary
  --round-hour
  --tz ZONE
  --force
  -h, --help

Examples:
  $(basename "$0") --mode now

  $(basename "$0") --mode history \\
      --date "2026-06-29 04:00"

  $(basename "$0") --mode dataset \\
      --time-start "2024-01-01 00:00" \\
      --time-end   "2025-05-01 00:00"
EOF
}
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Parse CLI
while [[ ${1:-} ]]; do
  case "$1" in
    --mode)            SCRIPT_MODE="${2:-}"; shift 2 ;;
    --date)            TIME_RUN="${2:-}"; shift 2 ;;
    --time-start)      TIME_START="${2:-}"; shift 2 ;;
    --time-end)        TIME_END="${2:-}"; shift 2 ;;
    --skip-algorithm)  SKIP_ALGORITHM=true; shift ;;
    --skip-ancillary)  SKIP_ANCILLARY=true; shift ;;
    --round-hour)      ROUND_HOUR=true; shift ;;
    --force)           FORCE_RUN=true; shift ;;
    --tz)              TZ_LOCAL="${2:-}"; shift 2 ;;
    -h|--help)         usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ "$SCRIPT_MODE" != "now" && "$SCRIPT_MODE" != "history" && "$SCRIPT_MODE" != "dataset" ]]; then
  echo " ===> ERROR: --mode must be 'now', 'history' or 'dataset'." >&2
  exit 1
fi
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Logging setup
mkdir -p "$LOG_FOLDER"

if command -v tee >/dev/null 2>&1; then
  exec > >(tee -a "$LOG_FILE") 2>&1
else
  exec >>"$LOG_FILE" 2>&1
fi
# -----------------------------------------------------------------------------------------

echo " ==================================================================================="
echo " ==> $SCRIPT_NAME (Version: $SCRIPT_VERSION  Release_Date: $SCRIPT_DATE)"
echo " ==> START ..."
echo " "

# -----------------------------------------------------------------------------------------
# Resolve time
if [[ "$SCRIPT_MODE" == "dataset" ]]; then

  if [[ -z "$TIME_START" || -z "$TIME_END" ]]; then
    echo " ===> ERROR: --time-start and --time-end are required in dataset mode." >&2
    exit 1
  fi

  if [[ -n "$TZ_LOCAL" ]]; then
    TIME_START="$(TZ="$TZ_LOCAL" date -d "$TIME_START" '+%F %H:%M')" || {
      echo " ===> ERROR: invalid --time-start" >&2
      exit 1
    }

    TIME_END="$(TZ="$TZ_LOCAL" date -d "$TIME_END" '+%F %H:%M')" || {
      echo " ===> ERROR: invalid --time-end" >&2
      exit 1
    }

    START_SEC="$(TZ="$TZ_LOCAL" date -d "$TIME_START" '+%s')"
    END_SEC="$(TZ="$TZ_LOCAL" date -d "$TIME_END" '+%s')"
  else
    TIME_START="$(date -d "$TIME_START" '+%F %H:%M')" || {
      echo " ===> ERROR: invalid --time-start" >&2
      exit 1
    }

    TIME_END="$(date -d "$TIME_END" '+%F %H:%M')" || {
      echo " ===> ERROR: invalid --time-end" >&2
      exit 1
    }

    START_SEC="$(date -d "$TIME_START" '+%s')"
    END_SEC="$(date -d "$TIME_END" '+%s')"
  fi

  if (( END_SEC <= START_SEC )); then
    echo " ===> ERROR: --time-end must be after --time-start." >&2
    exit 1
  fi

  N_DAYS=$(( (END_SEC - START_SEC) / 86400 ))

else

  if [[ "$SCRIPT_MODE" == "now" ]]; then

    if [[ -n "${TIME_RUN:-}" ]]; then

      if [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        TIME_ANC="$TIME_RUN"
        TIME_ALG="$TIME_RUN 00:00"

      elif [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]][0-9]{2}:[0-9]{2}$ ]]; then
        TIME_ALG="$TIME_RUN"
        TIME_ANC="${TIME_RUN:0:10}"

      else
        echo " ===> ERROR: --date must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM'." >&2
        exit 1
      fi

    else

      if [[ -n "$TZ_LOCAL" ]]; then
        TIME_ALG="$(TZ="$TZ_LOCAL" date '+%F %H:%M')"
        TIME_ANC="$(TZ="$TZ_LOCAL" date '+%F')"
        TIME_RUN="$TIME_ALG"
      else
        TIME_ALG="$(date '+%F %H:%M')"
        TIME_ANC="$(date '+%F')"
        TIME_RUN="$TIME_ALG"
      fi

    fi

  elif [[ "$SCRIPT_MODE" == "history" ]]; then

    if [[ -z "${TIME_RUN:-}" ]]; then
      echo " ===> ERROR: --date is required in history mode." >&2
      exit 1
    fi

    if [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      TIME_ANC="$TIME_RUN"
      TIME_ALG="$TIME_RUN 00:00"

    elif [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]][0-9]{2}:[0-9]{2}$ ]]; then
      TIME_ALG="$TIME_RUN"
      TIME_ANC="${TIME_RUN:0:10}"

    else
      echo " ===> ERROR: --date must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM'." >&2
      exit 1
    fi

  fi

  if [[ -n "$TZ_LOCAL" ]]; then
    TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:%M' >/dev/null || {
      echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2
      exit 1
    }

    TZ="$TZ_LOCAL" date -d "$TIME_ANC" '+%F' >/dev/null || {
      echo " ===> ERROR: invalid date: $TIME_ANC" >&2
      exit 1
    }
  else
    date -d "$TIME_ALG" '+%F %H:%M' >/dev/null || {
      echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2
      exit 1
    }

    date -d "$TIME_ANC" '+%F' >/dev/null || {
      echo " ===> ERROR: invalid date: $TIME_ANC" >&2
      exit 1
    }
  fi

  if $ROUND_HOUR; then
    if [[ -n "$TZ_LOCAL" ]]; then
      TIME_ALG="$(TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:00')"
    else
      TIME_ALG="$(date -d "$TIME_ALG" '+%F %H:00')"
    fi
  fi

fi

echo " ==> MODE: $SCRIPT_MODE | TIME_SYSTEM: $(ts)"
echo " ==> TIME_RUN: ${TIME_RUN:-<unset>}"
echo " ==> TIME_ALG: ${TIME_ALG:-<unset>}"
echo " ==> TIME_ANC: ${TIME_ANC:-<unset>}"
echo " ==> TIME_START: ${TIME_START:-<unset>}"
echo " ==> TIME_END: ${TIME_END:-<unset>}"
echo " ==> N_DAYS: ${N_DAYS:-<unset>}"
echo " ==> SKIP_ALGORITHM=$SKIP_ALGORITHM | SKIP_ANCILLARY=$SKIP_ANCILLARY | TZ=${TZ_LOCAL:-system}"
echo " ==> LOGGING TO: $LOG_FILE"
echo " "

# -----------------------------------------------------------------------------------------
# Lock
mkdir -p "$LOCK_FOLDER"

if [[ "$FORCE_RUN" == "true" && -e "$LOCK_FILE" ]]; then
  echo " ===> [FORCE] Removing existing lock: $LOCK_FILE"
  rm -f "$LOCK_FILE" || true
fi

LOCK_DIR="$(dirname "$LOCK_FILE")"
mkdir -p "$LOCK_DIR" || {
  echo " ===> ERROR: cannot create lock dir $LOCK_DIR"
  exit 9
}

exec 200>"$LOCK_FILE" || {
  echo " ===> ERROR: cannot open lockfile $LOCK_FILE"
  exit 9
}

if ! /usr/bin/flock -n 200; then
  echo " ===> Another instance is running (lock: $LOCK_FILE)"
  if [[ "$FORCE_RUN" == "true" ]]; then
    echo " ===> [FORCE] Continuing anyway"
  else
    exit 9
  fi
fi

echo " ===> Lock acquired: $LOCK_FILE"

# -----------------------------------------------------------------------------------------
# Conda / Python runner
echo " ===> [CONDA] 1/8 Locate conda"

if [[ -x "$CONDA_HOME/bin/conda" ]]; then
  CONDA_BIN="$CONDA_HOME/bin/conda"
else
  CONDA_BIN="$(command -v conda || true)"
fi

if [[ -z "${CONDA_BIN:-}" ]]; then
  echo " ===> [CONDA][WARN] conda not found on system PATHs"
fi

echo " ===> [CONDA] 2/8 Candidate direct interpreters"

CANDIDATES=(
  "$CONDA_HOME/envs/$CONDA_ENV/bin/python"
  "$CONDA_HOME/bin/python"
  "$(command -v python || true)"
)

PY=""

for p in "${CANDIDATES[@]}"; do
  if [[ -n "$p" && -x "$p" ]]; then
    PY="$p"
    break
  fi
done

echo "       candidates: ${CANDIDATES[*]}"

if [[ -n "$PY" ]]; then
  echo " ===> [CONDA] Testing direct python: $PY"

  if "$PY" -V >/dev/null 2>&1; then
    echo " ===> [CONDA] Using direct interpreter: $PY"
    PYRUN=( "$PY" )
    "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'
  else
    echo " ===> [CONDA][WARN] direct python exists but not runnable"
    PY=""
  fi
fi

if [[ -z "$PY" && -n "${CONDA_BIN:-}" ]]; then
  echo " ===> [CONDA] 3/8 Try conda run"

  if "$CONDA_BIN" run -n "$CONDA_ENV" --no-capture-output python -V >/dev/null 2>&1; then
    echo " ===> [CONDA] Using: conda run -n $CONDA_ENV python"
    PYRUN=( "$CONDA_BIN" run -n "$CONDA_ENV" --no-capture-output python )
    "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'

  else
    echo " ===> [CONDA][WARN] conda run failed; trying activate with timeout"

    if timeout 8s env CONDA_HOME="$CONDA_HOME" CONDA_ENV="$CONDA_ENV" bash -lc \
      'eval "$("$CONDA_HOME/bin/conda" shell.bash hook)"; conda activate "$CONDA_ENV"; python -V' >/dev/null 2>&1; then

      echo " ===> [CONDA] Activate current shell"
      eval "$("$CONDA_HOME/bin/conda" shell.bash hook)"
      conda activate "$CONDA_ENV" >/dev/null 2>&1

      echo " ===> [CONDA] Env activated: $CONDA_ENV"
      PYRUN=( python )
      "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'

    else
      echo " ===> [CONDA][ERROR] Activation hangs or fails under cron"
      exit 2
    fi
  fi
fi

if [[ -z "${PYRUN[*]:-}" ]]; then
  echo " ===> [CONDA][ERROR] No working python runner determined"
  exit 2
fi

echo " ===> [CONDA] Python runner OK: ${PYRUN[*]}"

"${PYRUN[@]}" - <<'PYCHK' 2>&1 | sed 's/^/       /'
import sys, os, site
print("python:", sys.version.replace("\n", " "))
print("executable:", sys.executable)
print("PATH(head):", os.environ.get("PATH", "")[:200] + "...")
try:
    print("site-packages:", site.getsitepackages())
except Exception as e:
    print("site-packages: n/a", e)
PYCHK

# -----------------------------------------------------------------------------------------
# PYTHONPATH
export PYTHONPATH="${PYTHONPATH:-}:$ANC_FOLDER:$ALG_FOLDER"

echo " ===> PYTHONPATH FOLDERS:"
echo "$PYTHONPATH" \
  | tr ':' '\n' \
  | grep -v '^$' \
  | awk '{print " :::: [" NR "] " $0}'

if [[ "$SCRIPT_MODE" != "dataset" && ! -f "$ANC_SCRIPT" ]]; then
  echo " ===> ERROR: required ancillary script not found: $ANC_SCRIPT" >&2
  exit 2
fi

if [[ ! -f "$ALG_SCRIPT" ]]; then
  echo " ===> ERROR: required algorithm script not found: $ALG_SCRIPT" >&2
  exit 2
fi

# -----------------------------------------------------------------------------------------
# Ancillary step
echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ..."

if [[ "$SCRIPT_MODE" == "dataset" ]]; then

  echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... SKIPPED (dataset mode)"

elif [[ "${SKIP_ANCILLARY:-false}" != "true" ]]; then

  if [[ -n "$TZ_LOCAL" ]]; then
    ANC_SETTINGS_FOLDER="$(TZ="$TZ_LOCAL" date -d "$TIME_ANC" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"
  else
    ANC_SETTINGS_FOLDER="$(date -d "$TIME_ANC" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"
  fi

  CMD_ANC_DEF=(
    "${PYRUN[@]}"
    "$ANC_SCRIPT"
    --date "$TIME_ANC"
    --folder-template "$ANC_SETTINGS_FOLDER"
    --export
  )

  echo " :::: ANC CMD  ${CMD_ANC_DEF[*]}"

  set +e
  CMD_ANC_EXPORT="$("${CMD_ANC_DEF[@]}")"
  ANC_RC=$?
  set -e

  if [[ -z "$CMD_ANC_EXPORT" ]]; then
    echo " ===> ERROR: ancillary tool produced no output." >&2
    exit 3
  fi

  echo " :::: ANC EXPORT ${CMD_ANC_EXPORT}"
  eval "$CMD_ANC_EXPORT"

  echo " :::: SUMMARY_INFO_FOLDER_REF: ${FOLDER_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_FILE_REF:   ${FILE_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_REF:   ${TIME_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_ANC:   ${TIME_ANC:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_RUN:   ${TIME_RUN:-<unset>}"

  if [[ "${TIME_REF:-}" == "NOT_FOUND" ]]; then
    echo " ===> WARNING: Ancillary returned NOT_FOUND for $TIME_ANC; skipping algorithm."
    SKIP_ALGORITHM=true
  fi

  echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... DONE (rc=${ANC_RC})"

else

  echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... SKIPPED (BY FLAG)"

fi

# -----------------------------------------------------------------------------------------
# Algorithm step
echo " ===> RUN ALGORITHM ..."

if [[ "$SCRIPT_MODE" == "dataset" ]]; then

  if $SKIP_ALGORITHM; then
    echo " ===> RUN ALGORITHM ... SKIPPED (user flag --skip-algorithm)"
  else

    CMD_ALG_DEF=(
      "${PYRUN[@]}"
      "$ALG_SCRIPT"
      -settings_file "$ALG_SETTINGS"
      -time "$TIME_END"
    )

    echo " ===> DATASET MODE:"
    echo "      TIME_START        : $TIME_START"
    echo "      TIME_END          : $TIME_END"
    echo "      N_DAYS            : $N_DAYS"
    echo "      TIME PASSED TO APP: $TIME_END"
    echo " :::: ALG CMD  ${CMD_ALG_DEF[*]}"

    "${CMD_ALG_DEF[@]}"

    echo " ===> RUN ALGORITHM ... DONE"

  fi

else

  CMD_ALG_DEF=(
    "${PYRUN[@]}"
    "$ALG_SCRIPT"
    -settings_file "$ALG_SETTINGS"
    -time "${TIME_REF:-}"
  )

  echo " :::: ALG CMD  ${CMD_ALG_DEF[*]}"

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

fi

echo " ==> $SCRIPT_NAME (Version: $SCRIPT_VERSION  Release_Date: $SCRIPT_DATE)"
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
