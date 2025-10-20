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
from datetime import datetime
from datetime import date, timedelta

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

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

        time_range = pd.date_range(start=start, end=end, freq=freq, tz=start.tz)
        return time_run, time_range

    # ---- Branch B: relative to time_run ----
    # pick time_run (args > file > now)
    if time_run_args is not None:
        time_run = _to_ts(time_run_args)
    elif time_run_file is not None:
        time_run = _to_ts(time_run_file)
    else:
        time_run = pd.Timestamp.now(tz) if tz else pd.Timestamp.now()

    # align to freq grid
    time_run = time_run.floor(freq)

    if time_period_obs == 0 and time_period_frc == 0:
        start = end = time_run
    else:
        # OBS block includes time_run; if obs>0, first step is time_run - (obs-1)*step
        start = time_run - (time_period_obs - 1) * step if time_period_obs > 0 else time_run + (step if time_period_frc > 0 else pd.Timedelta(0))
        # FRC block end is time_run + frc*step (time_run excluded)
        end = time_run + time_period_frc * step if time_period_frc > 0 else time_run

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
# method to divide time range into observed and forecast periods
def divide_time_range(index: pd.DatetimeIndex, ref_time: pd.Timestamp,
                      observed_hours: str = '24h', forecast_hours: str = '48h',
                      observed_partition: str = 'multiple', forecast_partition: str = 'multiple',
                      ref_frequency: str = 'h', ref_rounding='d',
                      observed_label: str = "observed", forecast_label: str = "forecast",
                      mixed_label='observed_forecast') -> pd.DataFrame:
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

    pivot_time = deepcopy(ref_time)
    ref_time = ref_time.floor(ref_rounding)

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

    # Assuming `periods` is your list of dictionaries
    for p in periods:
        if p["time_start"] < pivot_time < p["time_end"]:
            p["period_type"] = mixed_label

    # define time dataframe
    df_time = pd.DataFrame(periods).sort_values(["time_key", "period_tag"]).reset_index(drop=True)
    df_time['time_pivot'] = pivot_time

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
