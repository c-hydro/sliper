# ----------------------------------------------------------------------------------------
# Libraries
import os
import logging
import json
import argparse
from datetime import datetime, timedelta
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Argument Parsing
parser = argparse.ArgumentParser(description="SLIPER Tools - File Checker")
parser.add_argument(
    "-settings_file",
    type=str,
    help="Path to the configuration JSON file (default: config.json)"
)
args = parser.parse_args()
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Load Configuration
default_config_path = "config.json"
config_path = args.settings_file if args.settings_file else default_config_path

if args.settings_file and not os.path.isfile(args.settings_file):
    print(f"⚠️ WARNING: Settings file '{args.settings_file}' not found. Falling back to default: {default_config_path}")
    config_path = default_config_path

if not os.path.isfile(config_path):
    print(f"❌ ERROR: Default config file '{default_config_path}' not found. Cannot proceed.")
    exit(1)

with open(config_path) as f:
    config = json.load(f)
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Extract Config Values
SCRIPT_NAME = config["script_name"]
SCRIPT_VERSION = config["script_version"]
SCRIPT_DATE = config["script_date"]
MODE = config["mode"]

RUN_DATE_FORMAT = config.get("path_date_format", "%Y/%m/%d/%H00")
STEP_DATE_FORMAT = config.get("file_date_format", "%Y%m%d%H00")
FILE_PATH_TEMPLATE = config["file_path_template"]

N_DAYS_PAST = config.get("n_days_past", 0)
N_DAYS_FUTURE = config.get("n_days_future", 0)
MAX_CYCLES = config.get("max_cycles", 48)

NORMALIZE_PAST = config.get("normalize_search_window_past", True)
NORMALIZE_FUTURE = config.get("normalize_search_window_future", True)

DOMAIN_LIST = config["domain"]

ENV_VAR_NAME = config.get("env_variable_name", "RUN_DATE")
ENV_DATE_FORMAT = config.get("env_date_format", "%Y%m%d%H")
PATH_ENV_TEMPLATE = config.get("path_env_template", "run_date_{env_date}.env")
MISSING_PATH_TEMPLATE = config.get("missing_path_template", "missing_files_{missing_date}.json")
MISSING_DATE_FORMAT = config.get("missing_date_format", "%Y%m%d%H%M")
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Logging Setup
log_config = config.get("logging", {})
log_level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)
log_to_console = log_config.get("log_to_console", True)
log_to_file = log_config.get("log_to_file", True)

log_date_format = log_config.get("log_date_format", "%Y%m%d%H%M%S")
log_date = datetime.now().strftime(log_date_format)
log_file_path = log_config.get("log_file_path", f"/tmp/hmc_grid_copy_{log_date}.log").replace("{log_date}", log_date)

# Ensure log folder exists
log_folder = os.path.dirname(log_file_path)
if log_to_file and log_folder and not os.path.exists(log_folder):
    os.makedirs(log_folder, exist_ok=True)

logging.basicConfig(level=log_level, format='[%(asctime)s] %(message)s')

if log_to_file:
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s'))
    logging.getLogger().addHandler(file_handler)

if log_to_console:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s'))
    logging.getLogger().addHandler(console_handler)

def log(message):
    logging.log(log_level, message)
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Utilities
def generate_time_steps(run_date):
    start = run_date - timedelta(days=N_DAYS_PAST)
    end = run_date + timedelta(days=N_DAYS_FUTURE)

    if NORMALIZE_PAST:
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    if NORMALIZE_FUTURE:
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)

    log(f"🔍 Searching files from {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')} (normalized: past={NORMALIZE_PAST}, future={NORMALIZE_FUTURE})")

    steps = []
    current = start
    while current <= end:
        steps.append(current)
        current += timedelta(hours=1)
    return steps

def resolve_path(domain, run_date, step_date):
    run_date_str = run_date.strftime(RUN_DATE_FORMAT)
    step_date_str = step_date.strftime(STEP_DATE_FORMAT)
    path = FILE_PATH_TEMPLATE.format(domain=domain, run_date=run_date_str, step_date=step_date_str)
    folder = os.path.dirname(path)
    if not os.path.isdir(folder):
        log(f"⚠️ Folder not found for domain '{domain}' at: {folder}")
        return None
    return path

def check_run_date_status(run_date):
    time_steps = generate_time_steps(run_date)
    missing_files = {}
    folder_missing = False

    for domain in DOMAIN_LIST:
        for step_date in time_steps:
            full_path = resolve_path(domain, run_date, step_date)
            if full_path is None:
                folder_missing = True
                break
            if not os.path.isfile(full_path):
                missing_files[domain] = full_path
                break
        if folder_missing:
            break

    if folder_missing:
        return "RUN_DATE_NOT_EXISTS"
    return "OK" if not missing_files else missing_files

def find_first_valid_run_date_and_report():
    now = datetime.now().replace(minute=0, second=0)
    report = {}
    first_valid_run_date = None

    for i in range(MAX_CYCLES):
        candidate_run_date = now - timedelta(hours=i)
        run_key = candidate_run_date.strftime(ENV_DATE_FORMAT)
        status = check_run_date_status(candidate_run_date)
        report[run_key] = status

        if status == "OK" and not first_valid_run_date:
            first_valid_run_date = candidate_run_date
            log(f"✅ First valid run_date found: {run_key}")
        elif status == "RUN_DATE_NOT_EXISTS":
            log(f"⚠️ Folder missing for run_date: {run_key}")
        else:
            log(f"❌ Missing files for run_date: {run_key}")

    return first_valid_run_date, report
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Main Execution
def main():
    log("===================================================================================")
    log(f"==> {SCRIPT_NAME} (Version: {SCRIPT_VERSION} Release_Date: {SCRIPT_DATE})")
    log(f"==> MODE: {MODE.upper()} | STARTING FILE CHECK ...")
    log(f"==> Using config file: {config_path}")

    first_valid_run_date, report = find_first_valid_run_date_and_report()

    # Save missing file report
    missing_timestamp = datetime.now().strftime(MISSING_DATE_FORMAT)
    missing_file_path = MISSING_PATH_TEMPLATE.replace("{missing_date}", missing_timestamp)
    os.makedirs(os.path.dirname(missing_file_path), exist_ok=True)

    with open(missing_file_path, "w") as f:
        json.dump(report, f, indent=4)
    log(f"📝 Saved missing file report to: {missing_file_path}")

    # Save RUN_DATE to .env
    env_date_str = datetime.now().strftime(ENV_DATE_FORMAT)
    env_file_path = PATH_ENV_TEMPLATE.replace("{env_date}", env_date_str)
    os.makedirs(os.path.dirname(env_file_path), exist_ok=True)

    with open(env_file_path, "w") as env_file:
        if first_valid_run_date:
            run_date_str = first_valid_run_date.strftime("%Y-%m-%d %H:%M")
            os.environ[ENV_VAR_NAME] = run_date_str
            env_file.write(f'{ENV_VAR_NAME}="{run_date_str}"\n')
            log(f"📁 Saved {ENV_VAR_NAME} to {env_file_path}")
            log(f"📌 Environment variable {ENV_VAR_NAME} set to: {run_date_str}")
        else:
            env_file.write(f'{ENV_VAR_NAME}="NONE"\n')
            log(f"⚠️ No valid run_date found. Wrote 'NONE' to {ENV_VAR_NAME} in {env_file_path}")

    log("===================================================================================")
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Entry Point
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------------------

