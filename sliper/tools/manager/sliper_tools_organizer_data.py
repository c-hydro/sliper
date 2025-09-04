# ----------------------------------------------------------------------------------------
# Libraries
import os
import shutil
import logging
import json
import argparse
from datetime import datetime, timedelta
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Argument Parsing
parser = argparse.ArgumentParser(description="SLIPER Tools - File Organizer")
parser.add_argument(
    "-settings_file",
    type=str,
    help="Path to the configuration JSON file (default: config.json)"
)
args = parser.parse_args()
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Load Configuration with Fallback
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

RUN_DATE_RAW = config.get("run_date", "now")
RUN_DATE_FORMAT_SRC = config.get("run_date_format_src", "%Y/%m/%d")
RUN_DATE_FORMAT_DEST = config.get("run_date_format_dest", "%Y/%m/%d/%H")

N_DAYS_PAST = config.get("n_days_past", 0)
N_DAYS_FUTURE = config.get("n_days_future", 0)

START_DATE = config.get("start_date", "")
END_DATE = config.get("end_date", "")
DOMAIN_STRING = config["domain_string"]

SRC_PATH_TEMPLATE = config["src_path_template"]
DEST_PATH_TEMPLATE = config["dest_path_template"]
TIME_FORMAT_FILE = config["time_format_file"]
FILE_NAME_PATTERN = config["file_name_pattern"]

NORMALIZE_PAST = config.get("normalize_search_window_past", False)
NORMALIZE_FUTURE = config.get("normalize_search_window_future", False)
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
# Time Utilities
def get_run_date():
    if RUN_DATE_RAW == "now":
        return datetime.now().replace(minute=0, second=0, microsecond=0)
    else:
        return datetime.strptime(RUN_DATE_RAW, "%Y-%m-%d")

def generate_time_list(run_date):
    if MODE == "realtime":
        start = run_date - timedelta(days=N_DAYS_PAST)
        end = run_date + timedelta(days=N_DAYS_FUTURE)

        if NORMALIZE_PAST:
            start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        if NORMALIZE_FUTURE:
            end = end.replace(hour=0, minute=0, second=0, microsecond=0)

    elif MODE == "history":
        start = datetime.strptime(START_DATE, "%Y-%m-%d")
        end = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(hours=23)

        if NORMALIZE_PAST:
            start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        if NORMALIZE_FUTURE:
            end = end.replace(hour=0, minute=0, second=0, microsecond=0)

    else:
        raise ValueError(f"Invalid mode: {MODE}")

    log(f"🔍 Time window for file copy: {start.strftime('%Y-%m-%d %H:%M')} to {end.strftime('%Y-%m-%d %H:%M')} "
        f"(normalized: past={NORMALIZE_PAST}, future={NORMALIZE_FUTURE})")

    time_list = []
    current = start
    while current <= end:
        time_list.append(current)
        current += timedelta(hours=1)
    return time_list
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Path Resolver
def resolve_paths(domain, step_time, run_date):
    step_date_str = step_time.strftime(TIME_FORMAT_FILE)
    run_date_src_str = run_date.strftime(RUN_DATE_FORMAT_SRC)
    run_date_dest_str = run_date.strftime(RUN_DATE_FORMAT_DEST)

    path_vars_src = {
        "domain": domain,
        "run_date_src": run_date_src_str
    }

    path_vars_dest = {
        "domain": domain,
        "run_date_dest": run_date_dest_str
    }

    src_folder = SRC_PATH_TEMPLATE.format(**path_vars_src)
    dest_folder = DEST_PATH_TEMPLATE.format(**path_vars_dest)
    file_name = FILE_NAME_PATTERN.replace("{step_date}", step_date_str)

    src_file_path = os.path.join(src_folder, file_name)
    dest_file_path = os.path.join(dest_folder, file_name)

    return src_file_path, dest_folder, file_name
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Main Execution
def main():
    log("===================================================================================")
    log(f"==> {SCRIPT_NAME} (Version: {SCRIPT_VERSION} Release_Date: {SCRIPT_DATE})")
    log(f"==> MODE: {MODE.upper()} | STARTING ...")
    log(f"==> Using config file: {config_path}")

    if not DOMAIN_STRING:
        log("ERROR: DOMAIN_STRING is empty. Aborting.")
        return

    run_date = get_run_date()
    time_list = generate_time_list(run_date)

    domains = DOMAIN_STRING if isinstance(DOMAIN_STRING, list) else DOMAIN_STRING.split(",")

    for domain in domains:
        for step_time in time_list:
            src_file_path, dest_folder, file_name = resolve_paths(domain.strip(), step_time, run_date)
            log(f"====> Processing: {src_file_path}")

            os.makedirs(dest_folder, exist_ok=True)

            if os.path.isfile(src_file_path):
                try:
                    shutil.copy(src_file_path, os.path.join(dest_folder, file_name))
                    log(f"====> SUCCESS: Copied to {dest_folder}")
                except Exception as e:
                    log(f"====> ERROR: Failed to copy {src_file_path} - {e}")
            else:
                log(f"====> WARNING: File not found: {src_file_path}")

    log("===> Finished.")
    log(f"==> {SCRIPT_NAME} (Version: {SCRIPT_VERSION} Release_Date: {SCRIPT_DATE})")
    log("==> ... END")
    log("==> Bye, Bye")
    log("===================================================================================")
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Entry Point
if __name__ == "__main__":
    main()
# ----------------------------------------------------------------------------------------

