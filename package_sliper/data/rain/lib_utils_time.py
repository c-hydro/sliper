"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251003'
Version:       '1.5.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from datetime import datetime

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
    return time_run, time_range

# ----------------------------------------------------------------------------------------------------------------------
