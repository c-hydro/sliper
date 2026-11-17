"""
Library Features:

Name:          lib_utils_io
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261021'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd
from pathlib import Path

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to solve path (using config structure)
def resolve_file_path(file_config: dict, time_run: pd.Timestamp) -> Path:

    # Convert to Timestamp if needed
    time_run = pd.Timestamp(time_run)

    # Get datetime formats
    folder_date_format = file_config.get("folder_date_format", "%Y/%m/%d")
    file_date_format = file_config.get("file_date_format", "%Y%m%d%H%M")

    # Format datetime
    folder_date = time_run.strftime(folder_date_format)
    file_date = time_run.strftime(file_date_format)

    # Resolve folder and filename
    folder_name = file_config["folder_name"].format(folder_date=folder_date)
    file_name = file_config["file_name"].format(file_date=file_date)

    return Path(folder_name) / file_name
# ----------------------------------------------------------------------------------------------------------------------
