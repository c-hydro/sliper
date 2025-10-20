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
from datetime import timedelta, datetime

from copy import deepcopy
from datetime import date, timedelta
from typing import Optional, Union, List, Tuple

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# method to set time run
def set_time(
    # choose the reference time
    time_run_args: str | pd.Timestamp | datetime | None = None,
    time_run_file: str | pd.Timestamp | datetime | None = None,
    time_format: str = "%Y-%m-%d %H:%M",

    # explicit window (overrides periods if both provided)
    time_run_file_start: str | pd.Timestamp | datetime | None = None,
    time_run_file_end: str | pd.Timestamp | datetime | None = None,

    # periods in units of freq (e.g., 'h' -> hours)
    time_period_obs: int = 1,   # steps ending at time_run (inclusive)
    time_period_frc: int = 0,   # steps strictly after time_run

    # grid frequency
    freq: str = "h",
    # time round
    rounding : str = "h",

    # snap boundaries to midnight if requested: "none" | "prev" | "next"
    align_start_to_midnight: str = "prev",
    align_end_to_midnight: str = "next",

    # flip sides and reverse the returned range
    time_reverse: bool = False,

    # other
    tz: str | None = None,
):
    """
    Returns:
        time_run   (pd.Timestamp)     -> reference time aligned to 'freq'
        time_range (pd.DatetimeIndex) -> range at 'freq' across [start, end]

    Rules:
      • OBS steps include time_run; FRC steps start right after time_run.
      • If start/end provided, they define the window; you can still snap to midnight.
    """
    # ---- helpers ----
    def _to_ts(x):
        if isinstance(x, pd.Timestamp):
            ts = x
        elif isinstance(x, datetime):
            ts = pd.Timestamp(x)
        elif isinstance(x, str):
            try:
                ts = pd.to_datetime(x, format=time_format)
            except Exception:
                ts = pd.to_datetime(x)
        elif x is None:
            return None
        else:
            raise TypeError(f"Unsupported time type: {type(x)}")
        if tz is not None:
            ts = ts.tz_localize(tz) if ts.tzinfo is None else ts.tz_convert(tz)
        return ts

    def _coerce_nonneg(v, default):
        try:
            iv = int(v)
            return max(iv, 0)
        except Exception:
            return default

    def _midnight(ts): return ts.floor("D")
    def _snap(ts, mode):
        if mode == "none": return ts
        if mode == "prev": return _midnight(ts)
        if mode == "next": return _midnight(ts) + pd.Timedelta(days=1)
        raise ValueError('align_*_to_midnight must be "none", "prev", or "next"')

    try:
        step = pd.tseries.frequencies.to_offset(freq)
    except Exception:
        step = pd.Timedelta(hours=1)  # robust fallback

    # sanitize periods
    time_period_obs = _coerce_nonneg(time_period_obs, 1)
    time_period_frc = _coerce_nonneg(time_period_frc, 0)

    # ---- Branch A: explicit start/end ----
    if (time_run_file_start is not None) and (time_run_file_end is not None):
        ts_start = _to_ts(time_run_file_start)
        ts_end   = _to_ts(time_run_file_end)
        if ts_start is None or ts_end is None:
            raise IOError('Time type or format is wrong for start/end')
        if ts_end < ts_start:
            raise ValueError("time_run_file_end must be >= time_run_file_start")

        # align to grid
        start = ts_start if ts_start == ts_start.floor(freq) else ts_start.ceil(freq)
        end   = ts_end.floor(freq)

        # optional snapping
        start = _snap(start, align_start_to_midnight)
        end   = _snap(end, align_end_to_midnight)

        if end < start:
            end = start

        # choose representative time_run as the last grid step
        time_run = end.floor(freq)
        time_ref = time_run

        time_range = pd.date_range(start=start, end=end, freq=freq, tz=start.tz)
        return time_ref, time_run, time_range

    # ---- Branch B: relative to time_run ----
    # pick time_run (args > file > now)
    if time_run_args is not None:
        time_run = _to_ts(time_run_args)
    elif time_run_file is not None:
        time_run = _to_ts(time_run_file)
    else:
        time_run = pd.Timestamp.now(tz) if tz else pd.Timestamp.now()

    # set time run
    time_run = time_run.floor(rounding)
    # set time reference (align to data)
    time_ref = time_run.floor(freq)

    if time_period_obs == 0 and time_period_frc == 0:
        start = end = time_ref
    else:
        # OBS block includes time_run; if obs>0, first step is time_run - (obs-1)*step
        start = time_ref - (time_period_obs - 1) * step if time_period_obs > 0 else time_ref + (step if time_period_frc > 0 else pd.Timedelta(0))
        # FRC block end is time_run + frc*step (time_run excluded)
        end = time_ref + time_period_frc * step if time_period_frc > 0 else time_ref

    # optional snapping to midnight
    start = _snap(start, align_start_to_midnight)
    end   = _snap(end, align_end_to_midnight)

    if end < start:
        # if snapping inverted the window, collapse to boundary
        end = start

    time_range = pd.date_range(start=start, end=end, freq=freq, tz=start.tz)

    # For relative mode, if flipped we present most-recent-first to match "forward OBS" feel
    if time_reverse:
        time_range = time_range[::-1]

    return time_run, time_range

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to set time run
def set_time_OLD(
    time_run_args: Optional[str] = None,
    time_run_file: Optional[str] = None,
    time_format: str = '%Y-%m-%d %H:%M',
    time_run_file_start: Optional[str] = None,
    time_run_file_end: Optional[str] = None,
    time_period_observed: int = 24, time_period_forecast: int = 48,
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
        time_range_observed = get_time_range(time_part_obs, time_period_observed, time_frequency, 'Observed')
        time_part_forecast = time_run + parse_time_window(time_frequency)
        time_range_forecast = get_time_range(time_part_forecast, time_period_forecast, time_frequency, 'Forecast')

        # combine both ranges into a common unified time range
        if time_range_forecast is not None:
            time_range = time_range_observed.union(time_range_forecast)
        else:
            time_range = time_range_observed

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

    try:
        if label == 'Observed':
            return pd.date_range(end=time_run, periods=time_period, freq=time_frequency.lower())
        elif label == 'Forecast':
            return pd.date_range(start=time_run, periods=time_period, freq=time_frequency.lower())
        else:
            log_stream.error(f' ===> {label}: Invalid label for time range generation.')
            raise ValueError(f'Invalid label: {label}. Use "Observed" or "Forecast".')
    except Exception as e:
        log_stream.error(f' ===> {label}: Failed to generate time range: {e}')
        return pd.DatetimeIndex([time_run])
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to extract time information
def extract_time_info(time_range: pd.DatetimeIndex, time_frequency_def: str = 'h') -> (pd.Timestamp, pd.Timestamp, str):

    # sort the time range
    time_range = time_range.sort_values(ascending=True)

    # check time range length
    if len(time_range) > 1:

        # extract the start, end, and frequency from the time range
        time_start = time_range[0]
        time_end = time_range[-1]
        time_frequency = pd.infer_freq(time_range)

    elif len(time_range) == 1:

        # extract the start, end, and frequency from the time range
        time_start = time_range[0]
        time_end = time_range[0]
        time_frequency = time_frequency_def

    else:

        log_stream.error(' ===> Time range length is equal to zero. Check your settings for having a right time info')
        raise ValueError('Time range must be defined by one or more values')

    return time_start, time_end, time_frequency
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