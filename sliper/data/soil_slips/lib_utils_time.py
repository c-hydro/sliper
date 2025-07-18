"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries
import logging
import numpy as np
import re
import pandas as pd

from copy import deepcopy
from datetime import datetime, date

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to set time information
def set_time(time_run_args=None, time_run_file=None, time_format='%Y-%m-%d %H:%M',
             time_run_file_start=None, time_run_file_end=None,
             time_period=1, time_frequency='H', time_rounding='H', time_reverse=True):

    # info start
    log_stream.info(' ----> Set time period ... ')

    # Choose a default current time in correct format
    time_now = pd.Timestamp.now()
    default_time_str = time_now.strftime(time_format)

    # Determine base time string
    if time_run_args:
        time_tmp = deepcopy(time_run_args)
        log_stream.info(f' -----> Time {time_tmp} set by argument')
    elif time_run_file:
        time_tmp = deepcopy(time_run_file)
        log_stream.info(f' -----> Time {time_tmp} set by user input file')
    else:
        time_tmp = default_time_str
        log_stream.info(f' -----> Time {time_tmp} set by system')

    try:
        time_run = pd.Timestamp(time_tmp).floor(time_rounding.lower())
    except Exception as e:
        log_stream.error(f' ===> Failed to parse time "{time_tmp}" with format "{time_format}"')
        raise ValueError(f'Invalid time format: {e}')

    # CASE A: Use time_run + period
    if time_run_file_start is None and time_run_file_end is None:
        log_stream.info(' -----> Time info defined by "time_run" argument ... ')
        if time_period > 0:
            time_range = pd.date_range(end=time_run, periods=time_period, freq=time_frequency.lower())
        else:
            log_stream.warning(' ===> TimePeriod must be greater than 0. Reset to 1')
            time_range = pd.date_range(end=time_run, periods=1, freq=time_frequency.lower())
        log_stream.info(' -----> Time info defined by "time_run" argument ... DONE')

    # CASE B: Use explicit start and end times
    elif time_run_file_start is not None and time_run_file_end is not None:
        log_stream.info(' -----> Time info defined by "time_start" and "time_end" arguments ... ')

        try:
            time_start = pd.Timestamp(time_run_file_start).floor(time_rounding)
            time_end = pd.Timestamp(time_run_file_end).floor(time_rounding)
        except Exception as e:
            log_stream.error(f' ===> Failed to parse start or end time: {e}')
            raise ValueError(f'Invalid start or end time format: {e}')

        if time_start > time_end:
            log_stream.error(' ===> "time_start" is greater than "time_end". Check your settings.')
            raise RuntimeError('Time_Range is not correctly defined.')

        time_range = pd.date_range(start=time_start, end=time_end, freq=time_frequency)
        log_stream.info(' -----> Time info defined by "time_start" and "time_end" arguments ... DONE')

    # CASE C: Invalid combo
    else:
        log_stream.error(' ===> Either both "time_start" and "time_end" must be set, or neither.')
        raise IOError('Time type or format is wrong')

    # Reverse time range if needed
    if time_reverse:
        time_range = time_range[::-1]

    # info end
    log_stream.info(' ----> Set time period ... DONE')

    return [time_run, time_range]

# ----------------------------------------------------------------------------------------------------------------------
