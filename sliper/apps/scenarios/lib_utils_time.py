"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250618'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
import pandas as pd

from copy import deepcopy
from datetime import date, timedelta
from typing import Optional, Union, List

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to set time run
def set_time(
    time_run_args: Optional[str] = None,
    time_run_file: Optional[str] = None,
    time_format: str = '%Y-%m-%d %H:%M',
    time_run_file_start: Optional[str] = None,
    time_run_file_end: Optional[str] = None,
    time_period: int = 24,
    time_frequency: str = 'H',
    time_rounding: str = 'H',
    time_reverse: bool = True
) -> List[Union[pd.Timestamp, pd.DatetimeIndex]]:
    """
    Set the time run and compute a time range.

    Parameters:
    - time_run_args (str, optional): Explicit time string (from argument).
    - time_run_file (str, optional): Time string from file metadata.
    - time_format (str): String format for parsing default/system time.
    - time_run_file_start (str, optional): Start of time range (if provided).
    - time_run_file_end (str, optional): End of time range (if provided).
    - time_period_observed (int): Number of observed periods in the time range.
    - time_period_forecast (int): Number of forecast periods in the time range.
    - time_frequency (str): Frequency string for the time range (e.g., 'H', 'D').
    - time_rounding (str): Rounding frequency (e.g., 'H' for hour).
    - time_reverse (bool): Whether to reverse the time range.

    Returns:
    - List[Union[pd.Timestamp, pd.DatetimeIndex]]: The reference time and time range.
    """

    log_stream.info(' ----> Set time period ... ')

    if time_run_file_start is None and time_run_file_end is None:
        log_stream.info(' -----> Time info defined by "time_run" argument ... ')

        if time_run_args is not None:
            time_tmp = time_run_args
            log_stream.info(f' ------> Time {time_tmp} set by argument')
        elif time_run_file is not None:
            time_tmp = time_run_file
            log_stream.info(f' ------> Time {time_tmp} set by user')
        else:
            time_now = date.today()
            time_tmp = time_now.strftime(time_format)
            log_stream.info(f' ------> Time {time_tmp} set by system')

        # ensure time format is valid
        time_run = pd.Timestamp(time_tmp).floor(time_rounding.lower())

        # generate observed and forecast time ranges
        time_part_obs = deepcopy(time_run)
        time_range = get_time_range(time_part_obs, time_period, time_frequency, 'Default')

        log_stream.info(' -----> Time info defined by "time_run" argument ... DONE')

    elif time_run_file_start is not None and time_run_file_end is not None:

        log_stream.info(' -----> Time info defined by "time_start" and "time_end" arguments ... ')

        time_start = pd.Timestamp(time_run_file_start).floor(time_rounding)
        time_end = pd.Timestamp(time_run_file_end).floor(time_rounding)

        if time_start > time_end:
            log_stream.error(' ===> Variable "time_start" is greater than "time_end". Check your settings file.')
            raise RuntimeError('Time_Range is not correctly defined.')

        if time_run_args is not None:
            time_tmp = deepcopy(time_run_args)
            log_stream.info(f' ------> Time {time_tmp} set by argument')
        elif time_run_file is not None:
            time_tmp = deepcopy(time_run_file)
            log_stream.info(f' ------> Time {time_tmp} set by user')
        else:
            time_now = date.today()
            time_tmp = time_now.strftime(time_format)
            log_stream.info(f' ------> Time {time_tmp} set by system')

        time_run = pd.Timestamp(time_tmp).floor(time_rounding)
        time_range = pd.date_range(start=time_start, end=time_end, freq=time_frequency)

        log_stream.info(' -----> Time info defined by "time_start" and "time_end" arguments ... DONE')

    else:
        log_stream.info(' ----> Set time period ... FAILED')
        log_stream.error(' ===> Arguments "time_start" and/or "time_end" is/are not correctly set')
        raise IOError('Time type or format is wrong')

    if time_reverse:
        time_range = time_range[::-1]

    log_stream.info(' ----> Set time period ... DONE')

    return [time_run, time_range]
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get time range
def get_time_range(time_run: pd.Timestamp, time_period: int = 24,
                   time_frequency: str = 'h', label: str = 'Observed') -> (pd.DatetimeIndex, None):

    if time_period is None or time_period <= 0:
        if label == 'Observed':
            log_stream.warning(f' ===> {label}: TimePeriod must be defined and greater than 0. Automatically set to 1.')
            time_period = 1
        elif label == 'Forecast':
            log_stream.warning(f' ===> {label}: TimePeriod must be defined and greater than 0. Automatically set to 1.')
            return None
        elif label == 'Default':
            log_stream.warning(f' ===> {label}: TimePeriod must be defined and greater than 0. Automatically set to 1.')
            return None

    try:
        if label == 'Observed':
            return pd.date_range(end=time_run, periods=time_period, freq=time_frequency.lower())
        elif label == 'Forecast':
            return pd.date_range(start=time_run, periods=time_period, freq=time_frequency.lower())
        if label == 'Default':
            return pd.date_range(end=time_run, periods=time_period, freq=time_frequency.lower())
        else:
            log_stream.error(f' ===> {label}: Invalid label for time range generation.')
            raise ValueError(f'Invalid label: {label}. Use "Observed" or "Forecast".')
    except Exception as e:
        log_stream.error(f' ===> {label}: Failed to generate time range: {e}')
        return pd.DatetimeIndex([time_run])
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to divide time range into observed and forecast periods
def divide_time_range(index: pd.DatetimeIndex, ref_time: pd.Timestamp,
                      observed_hours: str = '24h', forecast_hours: str = '48h',
                      observed_partition: str = 'multiple', forecast_partition: str = 'multiple',
                      ref_frequency: str = 'h',
                      observed_label: str = "observed", forecast_label: str = "forecast") -> pd.DataFrame:
    """
    Divide a DatetimeIndex into variable-length blocks before and after a reference time.
    Automatically labels periods using customizable names (e.g., 'observed', 'forecast').

    Parameters:
        index: DatetimeIndex to segment
        ref_time: Reference timestamp dividing observed and forecast
        ref_frequency: Frequency of the reference time (default: 'h' for hours)
        observed_hours: Block size (in hours) before ref_time
        forecast_hours: Block size (in hours) after ref_time
        observed_label: Base name for periods before ref_time (default: 'observed')
        forecast_label: Base name for periods after ref_time (default: 'forecast')

    Returns:
        DataFrame with columns: ['period_name', 'time_start', 'time_end']
    """

    index = index.sort_values()
    ref_frequency = ref_frequency.lower()

    obs_period, obs_frequency = split_time_window(observed_hours)
    fc_period, fc_frequency = split_time_window(forecast_hours)

    # re-evaluate index to use the unique observed partition if needed
    if observed_partition == "unique":
        # filter the DatetimeIndex to start from the cut point
        index_fc = index[index > ref_time]
        index_obs = pd.date_range(end=ref_time, periods=int(obs_period), freq=obs_frequency.lower())
        # merge
        index_merged = index_obs.append(index_fc)
        # sort and drop duplicates
        index_unique = index_merged.sort_values().unique()
        # convert back to DatetimeIndex
        index = pd.DatetimeIndex(index_unique)

    # re-evaluate index to use the unique observed partition if needed
    if forecast_partition == "unique":
        # filter the DatetimeIndex to end at the cut point
        index_obs = index[index <= ref_time]
        index_fc = pd.date_range(start=ref_time, periods=int(fc_period), freq=fc_frequency.lower())
        # merge
        index_merged = index_obs.append(index_fc)
        # sort and drop duplicates
        index_unique = index_merged.sort_values().unique()
        # convert back to DatetimeIndex
        index = pd.DatetimeIndex(index_unique)

    periods = []
    i_obs, i_fc = 1, 1
    start_time = index[0]
    end_time = index[-1]

    ref_freq = parse_time_window(ref_frequency)
    obs_freq = parse_time_window(observed_hours)
    fc_freq = parse_time_window(forecast_hours)

    # Build observed periods (before ref_time)
    current_end = ref_time
    while current_end >= start_time:
        current_start = current_end - obs_freq + ref_freq
        clipped_start = max(current_start, start_time)
        clipped_end = min(current_end, end_time)
        if clipped_start <= clipped_end:
            periods.append({
                "period_type": observed_label,
                "period_tag": f"{observed_label}_{str(i_obs).zfill(4)}_Period",
                "period_id": i_obs,
                "time_start": clipped_start,
                "time_end": clipped_end,
                "time_key": clipped_start.strftime('%Y-%m-%d'),
                "time_range": pd.date_range(start=clipped_start, end=clipped_end, freq=ref_frequency)
            })
        current_end = current_start - ref_freq
        i_obs += 1

    # Build forecast periods (at or after ref_time)
    current_start = ref_time + ref_freq
    while current_start <= end_time:
        current_end = current_start + fc_freq - ref_freq
        clipped_start = max(current_start, start_time)
        clipped_end = min(current_end, end_time)
        if clipped_start <= clipped_end:
            periods.append({
                "period_type": forecast_label,
                "period_tag": f"{forecast_label}_{str(i_fc).zfill(4)}_Period",
                "period_id": i_fc,
                "time_start": clipped_start,
                "time_end": clipped_end,
                "time_key": clipped_start.strftime('%Y-%m-%d'),
                "time_range": pd.date_range(start=clipped_start, end=clipped_end, freq=ref_frequency)
            })
        current_start = current_end + ref_freq
        i_fc += 1

    # define time dataframe
    df_time = pd.DataFrame(periods).sort_values(["time_key", "period_tag"]).reset_index(drop=True)

    # Sort by time
    return df_time
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to split time window
def split_time_window(s):
    match = re.match(r'^(\d+)(.*)', s)
    if match:
        digits, rest = match.groups()
        return digits, rest
    return 1, s  # No leading digits
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to parse time window
def parse_time_window(code: str) -> timedelta:
    """
    Parse a string starting with 'h' or 'd' followed by an optional integer.
    Returns a timedelta object.
    Examples:
        'h'   -> timedelta(hours=1)
        'h2'  -> timedelta(hours=2)
        'd'   -> timedelta(days=1)
        'd3'  -> timedelta(days=3)
    """

    code = code.strip().lower()  # Normalize input to lowercase and strip whitespace

    if not code:
        raise ValueError("Code cannot be empty.")

    amount_str, unit = split_time_window(code)

    if isinstance(amount_str, str):
        if not amount_str.isdigit():
            raise ValueError("Expected a digit after 'h' or 'd'.")

    amount = int(amount_str)

    if unit == 'h':
        return timedelta(hours=amount)
    elif unit == 'd':
        return timedelta(days=amount)
    else:
        raise ValueError("Unsupported time unit. Use 'h' for hours or 'd' for days.")
# ----------------------------------------------------------------------------------------------------------------------
