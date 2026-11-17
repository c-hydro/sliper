"""
Library Features:

Name:          lib_analysis
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261021'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from typing import Optional
from datetime import datetime

from lib_utils_io import resolve_file_path
from lib_info_args import logger_name, time_format_algorithm

# Logging
log_stream = logging.getLogger(logger_name)

# general settings
FIELDS = ('name_run', 'time_start', 'time_end', 'time_run', 'exit_code')
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to monitor the data
def monitor(cfg_data: dict, cfg_parameters: dict,
            time_day: pd.Timestamp, time_now: Optional[pd.Timestamp] = None,
            previous: Optional[dict] = None, tag: str = 'workflow') -> dict:

    # info monitoring start
    log_stream.info(f" ----> Monitoring results ... ")

    # Set time references
    time_now = pd.Timestamp.now() if time_now is None else pd.Timestamp(time_now)
    time_day = pd.Timestamp(time_day).normalize()

    # Get monitoring parameters
    time_delay = pd.Timedelta(minutes=cfg_parameters.get('completion_delay_minutes', 0))
    time_expected = cfg_parameters.get('expected_times', ['04:00'])

    # Normalize expected times
    if isinstance(time_expected, str):
        time_expected = [time_expected]

    # Get previous results
    previous_results = (previous or {}).get(tag, {})
    # Initialize current results
    current_result = {
        'date': time_day.strftime('%Y-%m-%d'),
        'checked_at': time_now.isoformat(timespec='seconds'),
        'expected_times': time_expected,
        tag: {}, 'summary': {'success': 0, 'failed': 0, 'missing': 0, 'pending': 0}
    }

    # Iterate over data definitions
    for path_keys, definition in _flatten_sections(cfg_data):

        # info data start
        log_stream.info(f" -----> Workflow: {' | '.join(path_keys)} ... ")

        # Iterate over expected execution times
        runs = {}
        for time_slot in time_expected:

            # Define reference time
            time_reference = time_day + pd.Timedelta(
                hours=int(time_slot.split(':')[0]), minutes=int(time_slot.split(':')[1]))

            # info time start
            log_stream.info(f" ------> Time {time_reference} ... ")

            # Resolve source file path
            file_path_src = resolve_file_path(file_config=definition, time_run=time_reference)

            # Initialize file item
            file_item = {'time_run': time_reference.isoformat(timespec='seconds'), 'file': str(file_path_src)}
            # Check file existence
            if file_path_src.is_file():

                try:

                    # Parse information file
                    common_info = _parse_info(file_path_src)

                    # Check reference time
                    time_info = pd.Timestamp(common_info['time_run'])
                    # check time info
                    if time_info != time_reference:
                        raise ValueError('time_run differs from expected reference time')

                    # Update file information
                    file_item.update(common_info)

                    # Define execution status
                    file_item['status'] = ('success' if common_info['exit_code'] == 0 else 'failed')

                except (ValueError, OSError, KeyError, TypeError) as exc:

                    file_item.update(status='failed', exit_code=None, error=str(exc))

            else:

                # define completion deadline
                time_deadline = time_reference + time_delay
                # set status missing or pending
                file_item['status'] = ('pending' if time_now < time_deadline else 'missing')
                # set deadline
                file_item['deadline'] = time_deadline.isoformat(timespec='seconds')

            # Get previous results for current workflow
            prior = previous_results
            for key in path_keys:
                prior = prior.get(key, {}) if isinstance(prior, dict) else {}

            prior_run = (prior.get(time_slot, {}) if isinstance(prior, dict) else {})

            # Define current check timestamp
            timestamp = time_now.isoformat(timespec='seconds')
            file_item['status_checked_at'] = timestamp

            # Preserve status change timestamp if unchanged
            if prior_run.get('status') == file_item['status']:
                file_item['status_changed_at'] = (prior_run.get('status_changed_at') or timestamp)
            else:
                file_item['status_changed_at'] = timestamp

            # Preserve first success detection timestamp
            if file_item['status'] == 'success':
                if prior_run.get('status') == 'success':
                    file_item['success_checked_at'] = (prior_run.get('success_checked_at') or timestamp)
                else:
                    file_item['success_checked_at'] = timestamp
            else:
                file_item['success_checked_at'] = None

            # Restore previous status history
            history = list(prior_run.get('status_history', []))

            # Initialize history from previous status if needed
            if not history and prior_run.get('status'):

                history.append({
                    'status': prior_run['status'],
                    'status_changed_at': prior_run.get('status_changed_at',prior_run.get('status_checked_at')),
                    'status_checked_at': prior_run.get('status_checked_at'),
                    'exit_code': prior_run.get('exit_code'), 'file': prior_run.get('file'),
                    'time_start': prior_run.get('time_start'),'time_end': prior_run.get('time_end'),
                    'error': prior_run.get('error')
                })

            # Append history only when status changes
            if not history or history[-1].get('status') != file_item['status']:

                history.append({
                    'status': file_item['status'],
                    'status_changed_at': timestamp,'status_checked_at': timestamp,
                    'exit_code': file_item.get('exit_code'), 'file': file_item.get('file'),
                    'time_start': file_item.get('time_start'), 'time_end': file_item.get('time_end'),
                    'error': file_item.get('error')
                })

            # Assign status history
            file_item['status_history'] = history

            # Store run results
            runs[time_slot] = file_item

            # Update summary
            current_result['summary'][file_item['status']] += 1

            # info time end
            log_stream.info(f" ------> Time {time_reference} ... DONE")

        # Store results in nested workflow structure
        target = current_result[tag]
        for key in path_keys[:-1]:
            target = target.setdefault(key, {})
        target[path_keys[-1]] = runs

        # info data end
        log_stream.info(f" -----> Workflow: {' | '.join(path_keys)} ... DONE")

    # info monitoring end
    log_stream.info(f" ----> Monitoring results ... DONE")

    return current_result

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper to parse info
def _parse_info(path):
    values = {}
    for line in path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            raise ValueError(f'Invalid info line: {line!r}')
        key, value = line.split('=', 1)
        values[key.strip()] = value.strip()
    missing = [key for key in FIELDS if key not in values]
    if missing:
        raise ValueError(f'Missing fields: {missing}')
    for key in ('time_start', 'time_end', 'time_run'):
        datetime.strptime(values[key], '%Y-%m-%d %H:%M:%S')
    values['exit_code'] = int(values['exit_code'])
    return values

# helper to flat data sections
def _flatten_sections(node, prefix=()):
    """Yield (hierarchy, leaf definition), supporting nested algorithm sections."""
    if node in (None, ''):
        return
    if not isinstance(node, dict):
        raise ValueError(f'Invalid section {".".join(prefix)}: expected object or empty string')
    if 'file_name' in node or 'folder_name' in node:
        if not isinstance(node.get('file_name'), str) or not isinstance(node.get('folder_name'), str):
            raise ValueError(f'{".".join(prefix)} requires string file_name and folder_name')
        yield prefix, node
    else:
        for key, value in node.items():
            yield from _flatten_sections(value, prefix + (key,))
# ----------------------------------------------------------------------------------------------------------------------
