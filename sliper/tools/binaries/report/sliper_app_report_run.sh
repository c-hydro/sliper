
#!/usr/bin/env bash
set -euo pipefail

# -------------------------------------------------------------------------------------
# SLIPER REPORT RUN - INSTALLATION AND EXECUTION
#
# Description:
#   Install the Python virtual environment and execute the monitoring service.
#
# Execution modes:
#   realtime : Monitor the current day continuously or once
#   history  : Monitor a selected historical day once
#
# Examples:
#   ./sliper_report_run.sh --install-only
#   ./sliper_report_run.sh --mode realtime
#   ./sliper_report_run.sh --mode realtime --once
#   ./sliper_report_run.sh --mode history --date 2026-10-20
#
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Section 1 - Define application paths

# Define the directory containing this Bash script
BASE_DIR="/home/admin/soilslips-report/"

# Define Python environment configuration
PYTHON_SYSTEM="python3"
VENV_DIR="/home/admin/soilslips-report/.venv"
PYTHON_EXEC="${VENV_DIR}/bin/python"

# Define application executable
APP_DIR="/home/admin/library/package_sliper/report/"
APP_NAME="sliper_app_report_run_main.py"
APP_EXEC="${APP_DIR}/${APP_NAME}"

# Define application configuration
CONFIG_DIR="/home/admin/soilslips-report/"
CONFIG_NAME="sliper_app_report_run_realtime.json"
CONFIG_FILE="${CONFIG_DIR}/${CONFIG_NAME}"

# Define application logging
LOG_DIR="/home/admin/log/report/"
LOG_NAME="sliper_app_report_run_$(date '+%Y%m%d%H00').log"
LOG_FILE="${LOG_DIR}/${LOG_NAME}"
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 2 - Define execution settings

# Execution mode: realtime or history
MODE="realtime"
# Reference date (YYYY-MM-DD), used in history mode
DATE=""
# Execute a single monitoring cycle
ONCE=0
# Install environment without executing the application
INSTALL_ONLY=0
# Enable Bash execution logging
ENABLE_LOG=0
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 3 - Define logging functions
log_info()    { echo "$(date '+%Y-%m-%d %H:%M:%S') | INFO    | $*"; }
log_warning() { echo "$(date '+%Y-%m-%d %H:%M:%S') | WARNING | $*" >&2; }
log_error()   { echo "$(date '+%Y-%m-%d %H:%M:%S') | ERROR   | $*" >&2; }
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Section 4 - Define usage information

usage() {

    cat <<'EOF'

FRC MONITOR - INSTALLATION AND EXECUTION

Usage:
    ./install_and_run.sh [OPTIONS]

Options:
    --mode MODE       Execution mode: realtime or history
    --date DATE       Reference date (YYYY-MM-DD)
    --install-only    Install Python environment only
    --config PATH     Override the JSON configuration file
    --log             Enable execution logging
    -h, --help        Display this help message

Examples:

    # Install environment
    ./sliper_monitor_run.sh --install-only

    # Realtime continuous monitoring
    ./sliper_monitor_run.sh --mode realtime

    # Historical monitoring
    ./sliper_monitor_run.sh --mode history --date 2026-10-20

    # Custom configuration
    ./sliper_monitor_run.sh --mode realtime --config /path/config.json

EOF
}
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 5 - Parse command-line arguments

while (($#)); do

    case "$1" in

        --mode)
            MODE="${2:?Missing execution mode}"
            shift 2
            ;;

        --date)
            DATE="${2:?Missing reference date}"
            shift 2
            ;;

        --config)
            CONFIG_FILE="${2:?Missing configuration file}"
            shift 2
            ;;

        --install-only)
            INSTALL_ONLY=1
            shift
            ;;

        --log)
            ENABLE_LOG=1
            shift
            ;;

        -h|--help)
            usage
            exit 0
            ;;

        *)
            log_error "Unknown argument: $1"
            usage >&2
            exit 2
            ;;

    esac

done
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 6 - Initialize logging

if ((ENABLE_LOG)); then
    mkdir -p "${LOG_DIR}"
    # Redirect stdout and stderr to terminal and log file
    exec > >(tee -a "${LOG_FILE}") 2>&1
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 7 - Check Python installation

log_info "Checking Python installation ..."

if ! command -v "${PYTHON_SYSTEM}" >/dev/null 2>&1; then
    log_error "Python executable '${PYTHON_SYSTEM}' not found."
    exit 1
fi

log_info "System Python: $(command -v "${PYTHON_SYSTEM}")"
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 8 - Initialize Python virtual environment

log_info "Checking Python virtual environment ..."
if [[ ! -x "${PYTHON_EXEC}" ]]; then
    log_info "Creating Python environment: ${VENV_DIR}"
    "${PYTHON_SYSTEM}" -m venv "${VENV_DIR}"
else
    log_info "Python environment already exists."
fi

# Verify virtual environment executable
if [[ ! -x "${PYTHON_EXEC}" ]]; then
    log_error "Python virtual environment initialization failed."
    exit 1
fi

log_info "Python executable: ${PYTHON_EXEC}"
"${PYTHON_EXEC}" --version
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 9 - Check installation-only mode

if ((INSTALL_ONLY)); then
    log_info "Python environment installation completed."
    exit 0
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 10 - Validate application files

log_info "Checking application configuration ..."

if [[ ! -f "${APP_EXEC}" ]]; then
    log_error "Application executable not found: ${APP_EXEC}"
    exit 1
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
    log_error "Configuration file not found: ${CONFIG_FILE}"
    exit 1
fi

# Validate JSON syntax
if ! "${PYTHON_EXEC}" -m json.tool "${CONFIG_FILE}" >/dev/null; then
    log_error "Invalid JSON configuration: ${CONFIG_FILE}"
    exit 1
fi

log_info "Application executable: ${APP_EXEC}"
log_info "Configuration file: ${CONFIG_FILE}"
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Section 11 - Validate execution mode

if [[ "${MODE}" != "realtime" && "${MODE}" != "history" ]]; then
    log_error "Invalid execution mode: ${MODE}"
    exit 2
fi

if [[ "${MODE}" == "history" ]]; then

    if [[ -z "${DATE}" ]]; then
        log_error "History mode requires --date YYYY-MM-DD"
        exit 2
    fi

    # Validate date format and calendar date
    if ! "${PYTHON_EXEC}" -c \
        'import datetime,sys; datetime.date.fromisoformat(sys.argv[1])' \
        "${DATE}"; then
        log_error "Invalid historical date: ${DATE}"
        exit 2

    fi

    # Historical mode always executes once
    ONCE=1

else

    if [[ -n "${DATE}" ]]; then
        log_error "--date is supported only in history mode"
        exit 2
    fi

fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 12 - Build application arguments

ARGS=(--config "${CONFIG_FILE}" --mode "${MODE}")

# Add historical reference date
if [[ -n "${DATE}" ]]; then
    ARGS+=(--date "${DATE}")
fi
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 13 - Print execution summary

log_info "--------------------------------------------------"
log_info "REPORT - SUMMARY"
log_info " "
log_info "Base directory   : ${BASE_DIR}"
log_info "Python executable: ${PYTHON_EXEC}"
log_info "Application      : ${APP_EXEC}"
log_info "Configuration    : ${CONFIG_FILE}"
log_info "Execution mode   : ${MODE}"
log_info "Reference date   : ${DATE:-current day}"
log_info "Logging enabled  : ${ENABLE_LOG}"
log_info " "
log_info "--------------------------------------------------"
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Section 14 - Execute application
log_info "Execute report ..."

"${PYTHON_EXEC}" "${APP_EXEC}" "${ARGS[@]}"

log_info "Execute report ... COMPLETED"
# -------------------------------------------------------------------------------------

