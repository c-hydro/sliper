#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------------------
# Cron-safe environment (helps when run via crontab)
export HOME=/home/admin
export SHELL=/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
umask 022
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# Script information
SCRIPT_NAME='SLIPER - APP SCENARIOS - FRC REALTIME'
SCRIPT_VERSION="1.3.0"
SCRIPT_DATE='2026/10/23'
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
FORCE_RUN=false

# User-configurable env/paths
CONDA_HOME="/home/admin/library/env_conda_sliper_data"
CONDA_ENV="sliper_data_libraries"

ANC_FOLDER="/home/admin/library/package_sliper/time"
ANC_SCRIPT="/home/admin/library/package_sliper/time/sliper_app_time_reference.py"
ANC_SETTINGS_FOLDER_TEMPLATE="/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"

ANC_SYNC_FOLDER="/home/admin/library/package_sliper/time"
ANC_SYNC_SCRIPT="/home/admin/library/package_sliper/time/sliper_app_sync_run_and_data.py"
ANC_SYNC_RUN_FOLDER_TEMPLATE="/home/admin/soilslips-ws/data_dynamic/source/run/%Y/%m/%d"
ANC_SYNC_DATA_FOLDER_TEMPLATE_RAIN="/home/admin/soilslips-ws/data_dynamic/destination/indicators/rain/frc/%Y/%m/%d/%H00"
ANC_SYNC_DATA_FOLDER_TEMPLATE_SM="/home/admin/soilslips-ws/data_dynamic/destination/indicators/soil_moisture/frc/%Y/%m/%d/%H00"

ALG_FOLDER="/home/admin/library/package_sliper/apps/scenarios"
ALG_SCRIPT="/home/admin/library/package_sliper/apps/scenarios/sliper_app_scenarios_main.py"
ALG_SETTINGS="/home/admin/soilslips-exec/scenarios/sliper_app_scenarios_frc_realtime.json"

# log setup
LOG_FOLDER="${LOG_FOLDER:-/home/admin/log/runner_scenarios}"
LOG_BASENAME="${LOG_BASENAME:-bash_scenarios_frc_realtime}"

# lock setup (override-able variables; no timestamp)
LOCK_FOLDER="${LOCK_FOLDER:-/home/admin/lock/runner_scenarios}"
LOCK_BASENAME="${LOCK_BASENAME:-bash_scenarios_frc_realtime}"

# get timestamp from system
ts() { date '+%F %T %Z'; }
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# script usage
usage() {
  cat <<EOF
Usage: $(basename "$0") [--mode now|history] [--date 'YYYY-MM-DD'|'YYYY-MM-DD HH:MM'] [--skip-algorithm] [--skip-ancillary] [--round-hour] [--tz ZONE]

Options:
  --mode            Processing mode (default: now). Use 'history' with --date.
  --date            Date/time to process. Accepts 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM'.
                    Required if --mode history. For --mode now, current time is used.
  --skip-algorithm  Test mode: run ancillary, print algorithm command, but do NOT execute it.
  --skip-ancillary  Skip ancillary step entirely (useful with --skip-algorithm or quick runs).
  --round-hour      Truncate TIME_ALG minutes to HH:00 (always down).
  --tz ZONE         Timezone for 'now' mode and input validation (e.g., Europe/Rome).
  --force 			Request forced execution; never bypass an active lock.
  -h, --help        Show this help.
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
    --force)           FORCE_RUN=true;       shift ;;
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
# Resolve selected execution time before creating log and lock filenames.
if [[ "$SCRIPT_MODE" == "history" && -z "$TIME_RUN" ]]; then
  echo " ===> ERROR: --date is required in history mode." >&2
  exit 1
fi

if [[ -n "$TIME_RUN" ]]; then
  if [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    TIME_ANC="$TIME_RUN"
    TIME_ALG="$TIME_RUN 00:00"
  elif [[ "$TIME_RUN" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]][0-9]{2}:[0-9]{2}$ ]]; then
    TIME_ALG="$TIME_RUN"
    TIME_ANC="${TIME_RUN:0:10}"
  else
    echo " ===> ERROR: invalid --date format: $TIME_RUN" >&2
    exit 1
  fi
else
  if [[ -n "$TZ_LOCAL" ]]; then
    TIME_ALG="$(TZ="$TZ_LOCAL" date '+%F %H:%M')"
  else
    TIME_ALG="$(date '+%F %H:%M')"
  fi
  TIME_ANC="${TIME_ALG:0:10}"
fi

if [[ -n "$TZ_LOCAL" ]]; then
  TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:%M' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2; exit 1; }
  TZ="$TZ_LOCAL" date -d "$TIME_ANC" '+%F' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date: $TIME_ANC" >&2; exit 1; }
else
  date -d "$TIME_ALG" '+%F %H:%M' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date/time: $TIME_ALG" >&2; exit 1; }
  date -d "$TIME_ANC" '+%F' >/dev/null 2>&1 || { echo " ===> ERROR: invalid date: $TIME_ANC" >&2; exit 1; }
fi

if $ROUND_HOUR; then
  if [[ -n "$TZ_LOCAL" ]]; then
    TIME_ALG="$(TZ="$TZ_LOCAL" date -d "$TIME_ALG" '+%F %H:00')"
  else
    TIME_ALG="$(date -d "$TIME_ALG" '+%F %H:00')"
  fi
fi

# Keep the selected time independent of ancillary TIME_REF and exported TIME_RUN.
TIME_RUN="${TIME_RUN:-$TIME_ALG}"
SELECTED_TIME_RUN="$TIME_RUN"
if [[ -n "$TZ_LOCAL" ]]; then
  TIME_RUN_TAG="$(TZ="$TZ_LOCAL" date -d "$TIME_RUN" '+%Y%m%d_%H%M')"
else
  TIME_RUN_TAG="$(date -d "$TIME_RUN" '+%Y%m%d_%H%M')"
fi
TIME_MACHINE_TAG="$(date +'%Y%m%d_%H%M%S')"
LOG_FILE="${LOG_FILE:-$LOG_FOLDER/${LOG_BASENAME}_${TIME_RUN_TAG}_${TIME_MACHINE_TAG}.log}"
LOCK_FILE="${LOCK_FILE:-$LOCK_FOLDER/${LOCK_BASENAME}_${TIME_RUN_TAG}.lock}"

mkdir -p "$(dirname "$LOG_FILE")"
if command -v tee >/dev/null 2>&1; then
  exec > >(tee -a "$LOG_FILE") 2>&1
else
  exec >>"$LOG_FILE" 2>&1
fi

echo " ==================================================================================="
echo " ==> $SCRIPT_NAME (Version: $SCRIPT_VERSION  Release_Date: $SCRIPT_DATE)"
echo " ==> START ..."
echo " ==> MODE: $SCRIPT_MODE | TIME_SYSTEM: $(ts) | TIME_RUN: $TIME_RUN | TIME_ALG: $TIME_ALG | TIME_ANC: $TIME_ANC | SKIP_ALGORITHM=$SKIP_ALGORITHM | SKIP_ANCILLARY=$SKIP_ANCILLARY | TZ=${TZ_LOCAL:-system}"
echo " ==> LOGGING TO: $LOG_FILE"
echo " ==> LOCK FILE: $LOCK_FILE"

# Never remove a flock lock file: --force cannot bypass an active run.
mkdir -p "$(dirname "$LOCK_FILE")"
exec 200>"$LOCK_FILE" || { echo " ===> ERROR: cannot open lockfile $LOCK_FILE"; exit 9; }
if ! /usr/bin/flock -n 200; then
  echo " ===> ERROR: another execution is active for TIME_RUN=$TIME_RUN (lock: $LOCK_FILE)"
  echo " ===> --force does not bypass an active lock"
  exit 9
fi
echo " ===> Lock acquired: $LOCK_FILE"
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------
# CONDA STEP — robust: verbose, timeout, and fallbacks (no hanging in cron)
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
  [[ -n "$p" && -x "$p" ]] && PY="$p" && break
done
echo "       candidates: ${CANDIDATES[*]}"
if [[ -n "$PY" ]]; then
  echo " ===> [CONDA] Testing direct python: $PY"
  if "$PY" -V >/dev/null 2>&1; then
    echo " ===> [CONDA] Using direct interpreter (no activate): $PY"
    PYRUN=( "$PY" )
    "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'
  else
    echo " ===> [CONDA][WARN] direct python exists but not runnable"
    PY=""
  fi
fi

if [[ -z "$PY" && -n "${CONDA_BIN:-}" ]]; then
  echo " ===> [CONDA] 3/8 Try 'conda run' (no shell activation)"
  if "$CONDA_BIN" run -n "$CONDA_ENV" --no-capture-output python -V >/dev/null 2>&1; then
    echo " ===> [CONDA] Using: conda run -n $CONDA_ENV python"
    PYRUN=( "$CONDA_BIN" run -n "$CONDA_ENV" --no-capture-output python )
    "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'
  else
    echo " ===> [CONDA][WARN] conda run failed; trying activate with timeout"
    echo " ===> [CONDA] 4/8 Probe activation in a sandboxed shell (8s timeout)"
    if timeout 8s env CONDA_HOME="$CONDA_HOME" CONDA_ENV="$CONDA_ENV" bash -lc \
      'eval "$("$CONDA_HOME/bin/conda" shell.bash hook)"; conda activate "$CONDA_ENV"; python -V' >/dev/null 2>&1; then
      echo " ===> [CONDA] 5/8 Activate here (current shell)"
      # Now actually activate in THIS shell
      eval "$("$CONDA_HOME/bin/conda" shell.bash hook)"
      conda activate "$CONDA_ENV" >/dev/null 2>&1
      echo " ===> [CONDA] Env activated: $CONDA_ENV"
      PYRUN=( python )
      "${PYRUN[@]}" -V 2>&1 | sed 's/^/       /'
    else
      echo " ===> [CONDA][ERROR] Activation hangs or fails under cron"
      echo "       Tried: direct python, conda run, and activation probe"
      exit 2
    fi
  fi
fi

# Final guard
if [[ -z "${PYRUN[*]:-}" ]]; then
  echo " ===> [CONDA][ERROR] No working python runner determined"
  exit 2
fi

echo " ===> [CONDA] 6/8 Python runner OK: ${PYRUN[*]}"
echo " ===> [CONDA] 7/8 sys.executable / paths sanity"
"${PYRUN[@]}" - <<'PYCHK' 2>&1 | sed 's/^/       /'
import sys, os, site
print("python:", sys.version.replace("\n"," "))
print("executable:", sys.executable)
print("PATH(head):", os.environ.get("PATH","")[:200]+"...")
try:
    print("site-packages:", site.getsitepackages())
except Exception as e:
    print("site-packages: n/a", e)
PYCHK
echo " ===> [CONDA] 8/8 Done"
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
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
# ANCILLARY STEP (1)
echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ..."

if [[ "${SKIP_ANCILLARY:-false}" != "true" ]]; then
  # Build settings folder with DATE_REF (respect TZ for template expansion as well)
  if [[ -n "$TZ_LOCAL" ]]; then
    ANC_SETTINGS_FOLDER="$(TZ="$TZ_LOCAL" date -d "$TIME_ANC" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"
  else
    ANC_SETTINGS_FOLDER="$(date -d "$TIME_ANC" +"$ANC_SETTINGS_FOLDER_TEMPLATE")"
  fi

  CMD_ANC_DEF=( "${PYRUN[@]}" "$ANC_SCRIPT" --date "$TIME_ANC" --folder-template "$ANC_SETTINGS_FOLDER" --export )
  
  echo " :::: ANC CMD  ${CMD_ANC_DEF[*]}"

  # Run ancillary, capture stdout even if exit code != 0
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
  TIME_RUN="$SELECTED_TIME_RUN"
  echo " :::: SUMMARY_INFO_FOLDER_REF: ${FOLDER_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_FILE_REF:   ${FILE_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_REF:   ${TIME_REF:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_ANC:   ${TIME_ANC:-<unset>}"
  echo " :::: SUMMARY_INFO_TIME_RUN:   ${TIME_RUN:-<unset>}"
  
  TIME_ANC_1=$TIME_REF
  
  # Skip algorithm if NOT_FOUND
  if [[ "${TIME_REF:-}" == "NOT_FOUND" ]]; then
    echo " ===> WARNING: Ancillary returned NOT_FOUND for $TIME_ANC (folder: ${FOLDER_REF:-<unset>}); skipping algorithm."
    SKIP_ALGORITHM=true
  fi
  
  echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... DONE (rc=${ANC_RC})"
else
  echo " ===> RUN ANCILLARY SCRIPT [TIME_REF] ... SKIPPED (BY FLAG)"
fi
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# ANCILLARY STEP (2 - RAIN)
echo " ===> RUN ANCILLARY SCRIPT [RAIN - SYNC RUN AND DATA] ..."

if [[ "${SKIP_ANCILLARY:-false}" != "true" ]]; then

  # Run Python and capture output
  CMD_ANC_SYNC_DEF=( "${PYRUN[@]}" "$ANC_SYNC_SCRIPT" --time-run "$TIME_REF" --run-path-pattern "$ANC_SYNC_RUN_FOLDER_TEMPLATE" --data-path-pattern "$ANC_SYNC_DATA_FOLDER_TEMPLATE_RAIN" --tolerance-hours 10 --export)
	  
  echo " :::: ANC SYNC CMD  ${CMD_ANC_SYNC_DEF[*]}"

  # Run ancillary, capture stdout even if exit code != 0
  set +e
  CMD_ANC_SYNC_EXPORT="$("${CMD_ANC_SYNC_DEF[@]}")"
  ANC_RC=$?
  set -e

  if [[ -z "$CMD_ANC_SYNC_EXPORT" ]]; then
    echo " ===> ERROR: ancillary tool produced no output." >&2
	exit 3 
  fi

  echo " :::: ANC EXPORT ${CMD_ANC_SYNC_EXPORT}"

  # Evaluate and load env vars (safe: values are single-quoted)
  eval "$CMD_ANC_SYNC_EXPORT"
  TIME_RUN="$SELECTED_TIME_RUN"

  echo " :::: SYNC_FOLDER_DATA: ${FOLDER_REF:-<unset>}"
  echo " :::: SYNC_TIME_REF:   ${TIME_REF:-<unset>}"
  
  TIME_ANC_2_RAIN=$TIME_REF
  
  if [[ "$TIME_ANC_1" != "$TIME_ANC_2_RAIN" ]]; then
    echo " :::: WARNING: TIME_REF WAS CHANGED FROM ($TIME_ANC_1) TO ($TIME_ANC_2_RAIN)"
  else
    echo " :::: TIME REF WAS UNCHANGED"
  fi
  
  echo " ===> RUN ANCILLARY SCRIPT [RAIN - SYNC RUN AND DATA] ... DONE"

else
  echo " ===> RUN ANCILLARY SCRIPT [RAIN - SYNC RUN AND DATA] ... SKIPPED (BY FLAG)"
fi

# ANCILLARY STEP (2 - SM)
echo " ===> RUN ANCILLARY SCRIPT [SM - SYNC RUN AND DATA] ..."

if [[ "${SKIP_ANCILLARY:-false}" != "true" ]]; then

  # Run Python and capture output
  CMD_ANC_SYNC_DEF=( "${PYRUN[@]}" "$ANC_SYNC_SCRIPT" --time-run "$TIME_REF" --run-path-pattern "$ANC_SYNC_RUN_FOLDER_TEMPLATE" --data-path-pattern "$ANC_SYNC_DATA_FOLDER_TEMPLATE_SM" --tolerance-hours 10 --export)
	  
  echo " :::: ANC SYNC CMD  ${CMD_ANC_SYNC_DEF[*]}"

  # Run ancillary, capture stdout even if exit code != 0
  set +e
  CMD_ANC_SYNC_EXPORT="$("${CMD_ANC_SYNC_DEF[@]}")"
  ANC_RC=$?
  set -e

  if [[ -z "$CMD_ANC_SYNC_EXPORT" ]]; then
    echo " ===> ERROR: ancillary tool produced no output." >&2
	exit 3 
  fi

  echo " :::: ANC EXPORT ${CMD_ANC_SYNC_EXPORT}"

  # Evaluate and load env vars (safe: values are single-quoted)
  eval "$CMD_ANC_SYNC_EXPORT"
  TIME_RUN="$SELECTED_TIME_RUN"

  echo " :::: SYNC_FOLDER_DATA: ${FOLDER_REF:-<unset>}"
  echo " :::: SYNC_TIME_REF:   ${TIME_REF:-<unset>}"
  
  TIME_ANC_2_SM=$TIME_REF
  
  if [[ "$TIME_ANC_1" != "$TIME_ANC_2_SM" ]]; then
    echo " :::: WARNING: TIME_REF WAS CHANGED FROM ($TIME_ANC_1) TO ($TIME_ANC_2_SM)"
  else
    echo " :::: TIME REF WAS UNCHANGED"
  fi
  
  echo " ===> RUN ANCILLARY SCRIPT [SM - SYNC RUN AND DATA] ... DONE"

else
  echo " ===> RUN ANCILLARY SCRIPT [SM - SYNC RUN AND DATA] ... SKIPPED (BY FLAG)"
fi


# HARMONIZE ANCILLARY TIMES (RAIN vs SM)
#
# Priority:
#
#   1. Try the requested algorithm hour (TIME_ALG)
#   2. If both RAIN and SM are available at TIME_ALG, use it
#   3. Otherwise harmonize using the ancillary times returned by RAIN and SM
#
echo " ===> HARMONIZE ANCILLARY TIMES (RAIN vs SM) ..."

if [[ "${SKIP_ANCILLARY:-false}" != "true" ]]; then

    # -------------------------------------------------------------------------
    # Sanity checks
    if [[ -z "${TIME_ANC_2_RAIN:-}" || -z "${TIME_ANC_2_SM:-}" ]]; then
        echo " ===> ERROR: TIME_ANC_2_RAIN or TIME_ANC_2_SM is unset." >&2
        exit 3
    fi
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Helper: convert time to epoch
    to_epoch() {

        date -u -d "$1" +%s 2>/dev/null || \
        date -u -jf "%Y-%m-%dT%H:%M:%S%z" "$1" +%s 2>/dev/null
    }
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Helper: check if a dataset is available exactly for a given time
    probe_anc() {

        # probe_anc <label> <data_template> <time>

        local label="$1"
        local data_tpl="$2"
        local time_ref="$3"

        local out
        local rc

        local cmd=(
            "${PYRUN[@]}"
            "$ANC_SYNC_SCRIPT"
            --time-run "$time_ref"
            --run-path-pattern "$ANC_SYNC_RUN_FOLDER_TEMPLATE"
            --data-path-pattern "$data_tpl"
            --tolerance-hours 0
            --export
        )

        echo " :::: PROBE[$label] ${cmd[*]}"

        set +e
        out="$("${cmd[@]}")"
        rc=$?
        set -e

        if [[ "$rc" -ne 0 || -z "$out" ]]; then
            echo " :::: PROBE[$label] -> NOT AVAILABLE (rc=$rc)"
            return 1
        fi

        # Check expected exporter format
        if ! grep -qE "^[A-Za-z_][A-Za-z0-9_]*='.*'$" <<< "$out"; then
            echo " :::: PROBE[$label] -> UNEXPECTED EXPORT FORMAT"
            return 1
        fi

        # Save current values because eval changes them
        local folder_ref_saved="${FOLDER_REF:-}"
        local time_ref_saved="${TIME_REF:-}"

        eval "$out"
        TIME_RUN="$SELECTED_TIME_RUN"

        local folder_found="${FOLDER_REF:-}"
        local time_found="${TIME_REF:-}"

        # Restore globals
        FOLDER_REF="$folder_ref_saved"
        TIME_REF="$time_ref_saved"

        # Exporter must return a valid reference
        if [[ -z "$time_found" || "$time_found" == "NOT_FOUND" ]]; then
            echo " :::: PROBE[$label] -> TIME NOT FOUND"
            return 1
        fi

        # Check folder/file when available
        if [[ -n "$folder_found" ]]; then

            if [[ ! -d "$folder_found" && ! -f "$folder_found" ]]; then
                echo " :::: PROBE[$label] -> MISSING TARGET: $folder_found"
                return 1
            fi

        fi

        # Make sure the returned time belongs to the requested hour
        local requested_hour
        local found_hour

        requested_hour=$(date -d "$time_ref" '+%Y-%m-%d %H:00' 2>/dev/null)
        found_hour=$(date -d "$time_found" '+%Y-%m-%d %H:00' 2>/dev/null)

        if [[ -z "$requested_hour" || -z "$found_hour" ]]; then
            echo " :::: PROBE[$label] -> UNABLE TO NORMALIZE TIME"
            return 1
        fi

        if [[ "$requested_hour" != "$found_hour" ]]; then
            echo " :::: PROBE[$label] -> DIFFERENT HOUR"
            echo "      requested : $requested_hour"
            echo "      found     : $found_hour"
            return 1
        fi

        echo " :::: PROBE[$label] -> AVAILABLE AT $found_hour"

        return 0
    }

    # FIRST TRY: requested run hour
    #
    # TIME_ALG is already normalized by the script to:
    #
    #   YYYY-MM-DD HH:00
    #
    TIME_REQUESTED="$TIME_ALG"

    echo " :::: FIRST TRY REQUESTED HOUR: $TIME_REQUESTED"

    requested_rain_ok=0
    requested_sm_ok=0

    probe_anc \
        "RAIN@REQUESTED" \
        "$ANC_SYNC_DATA_FOLDER_TEMPLATE_RAIN" \
        "$TIME_REQUESTED" && requested_rain_ok=1

    probe_anc \
        "SM@REQUESTED" \
        "$ANC_SYNC_DATA_FOLDER_TEMPLATE_SM" \
        "$TIME_REQUESTED" && requested_sm_ok=1

    # Requested hour is available for both
    if (( requested_rain_ok && requested_sm_ok )); then

        COMMON_TIME_REF="$TIME_REQUESTED"

        echo " :::: REQUESTED HOUR AVAILABLE FOR BOTH DATASETS"
        echo " :::: COMMON_TIME_REF=$COMMON_TIME_REF"

    # Requested hour is not available for both -> fallback to harmonization
    else

        echo " :::: REQUESTED HOUR NOT AVAILABLE FOR BOTH DATASETS"
        echo "      RAIN_OK=$requested_rain_ok"
        echo "      SM_OK=$requested_sm_ok"
        echo " :::: FALLBACK TO ANCILLARY TIME HARMONIZATION"

        # Ancillary times already match
        if [[ "$TIME_ANC_2_RAIN" == "$TIME_ANC_2_SM" ]]; then

            COMMON_TIME_REF="$TIME_ANC_2_RAIN"

            echo " :::: ANCILLARY TIMES ALREADY MATCH"
            echo " :::: COMMON_TIME_REF=$COMMON_TIME_REF"

        # Ancillary times are different
        else

            # Convert ancillary times to epoch
            tr=$(to_epoch "$TIME_ANC_2_RAIN") || {
                echo " ===> ERROR: cannot parse TIME_ANC_2_RAIN: $TIME_ANC_2_RAIN" >&2
                exit 3
            }

            ts=$(to_epoch "$TIME_ANC_2_SM") || {
                echo " ===> ERROR: cannot parse TIME_ANC_2_SM: $TIME_ANC_2_SM" >&2
                exit 3
            }

            # Select oldest available time as harmonization candidate
            if (( tr <= ts )); then

                OLDEST="$TIME_ANC_2_RAIN"
                OLDEST_LABEL="RAIN"

            else

                OLDEST="$TIME_ANC_2_SM"
                OLDEST_LABEL="SM"

            fi

            echo " :::: ANCILLARY TIMES DO NOT MATCH"
            echo "      RAIN : $TIME_ANC_2_RAIN"
            echo "      SM   : $TIME_ANC_2_SM"
            echo " :::: CHOOSING OLDEST=$OLDEST FROM $OLDEST_LABEL"
            echo " :::: CHECKING BOTH DATASETS AT OLDEST TIME"

            oldest_rain_ok=0
            oldest_sm_ok=0

            probe_anc \
                "RAIN@OLDEST" \
                "$ANC_SYNC_DATA_FOLDER_TEMPLATE_RAIN" \
                "$OLDEST" && oldest_rain_ok=1

            probe_anc \
                "SM@OLDEST" \
                "$ANC_SYNC_DATA_FOLDER_TEMPLATE_SM" \
                "$OLDEST" && oldest_sm_ok=1

            if (( oldest_rain_ok && oldest_sm_ok )); then

                COMMON_TIME_REF="$OLDEST"

                echo " :::: COMMON_TIME_REF CONFIRMED"
                echo " :::: COMMON_TIME_REF=$COMMON_TIME_REF"

            else

                echo " ===> ERROR: Unable to select BOTH RAIN and SM data at $OLDEST" >&2
                echo "      RAIN_OK=$oldest_rain_ok" >&2
                echo "      SM_OK=$oldest_sm_ok" >&2
                exit 4

            fi

        fi

    fi

    # Override TIME_REF using the agreed common time
    TIME_REF="$COMMON_TIME_REF"
    export TIME_REF

    echo " :::: FINAL TIME_REF = $TIME_REF"
    echo " ===> HARMONIZE ANCILLARY TIMES (RAIN vs SM) ... DONE"

else

    echo " ===> HARMONIZE ANCILLARY TIMES (RAIN vs SM) ... SKIPPED (BY FLAG)"

fi
# -----------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------
# ALGORITHM STEP
echo " ===> RUN ALGORITHM ..."

CMD_ALG_DEF=( "${PYRUN[@]}" "$ALG_SCRIPT" -settings_file "$ALG_SETTINGS" -time "${TIME_REF:-}" )
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
