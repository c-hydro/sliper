"""
Library Features:

Name:          lib_data_io_json
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261021'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import json
import os
from pathlib import Path
from typing import Union
from datetime import datetime

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read file report
def read_file_report(file_name: Union[str, Path]) -> Path:

    # Ensure file_name is a Path object
    file_name = Path(file_name)
    # Check if file exists
    if not file_name.is_file():
        log_stream.warning('Report file not found: %s', file_name)
        return None

    # Read report
    try:
        file_data = json.loads(file_name.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        log_stream.warning('Could not read report %s: %s', file_name, exc)
        return None

    return file_data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Method to save file report
def save_file_report(report: dict, file_path: Union[str, Path]) -> Path:

    # Ensure file_path is a Path object
    file_path = Path(file_path)
    # Create destination folder
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Define temporary file
    tmp = file_path.with_name(file_path.name + f'.{os.getpid()}.tmp')
    try:
        # Write report to temporary file
        tmp.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        # Atomically replace destination file
        os.replace(tmp, file_path)
    finally:
        # Remove temporary file if still present
        tmp.unlink(missing_ok=True)

    return file_path
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read file settings
def read_file_settings(file_name: Union[str, Path], tag: str = 'run') -> dict:

    # Ensure file_name is a Path object
    file_name = Path(file_name)

    # Check if file exists
    if not file_name.is_file():
        raise FileNotFoundError(f'Settings file not found: {file_name}')

    # Read JSON settings
    with file_name.open('r', encoding='utf-8') as file_handle:
        json_dict = json.load(file_handle)

    # Get parameters
    cfg_parameters = json_dict['parameters']

    # Validate completion delay
    if cfg_parameters.get('completion_delay_minutes', 0) < 0:
        raise ValueError('completion_delay_minutes must be nonnegative')

    # Validate expected times
    for slot in cfg_parameters['expected_times']:
        try:
            datetime.strptime(slot, '%H:%M')
        except (ValueError, TypeError) as exc:
            raise ValueError(f'Invalid expected time "{slot}". Expected HH:MM') from exc

    # Validate workflow source
    if not isinstance(json_dict[tag]['source'], dict):
        raise ValueError('workflow.source must be an object')

    return json_dict

# ----------------------------------------------------------------------------------------------------------------------
