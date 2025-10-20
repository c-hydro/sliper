"""
Library Features:

Name:          lib_utils_data_predictors
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250730'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from collections import Counter, defaultdict
from typing import Optional

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# setting for destination data
fmr_default = {
    # --- Mandatory fields ---
    "time": "timestamp",
    "day_of_the_year": "numeric:integer",

    # --- Rain ---
    "rain_peak_3H": "numeric:float",
    "rain_accumulated_12H": "numeric:float",
    "rain_time_run": "timestamp",
    "rain_time_start": "timestamp",
    "rain_time_end": "timestamp",
    "rain_time_pivot": "timestamp",
    "rain_period_type": "string",
    "rain_latest_from": None,

    # --- Soil moisture ---
    "sm_value_first": "numeric:float",
    "sm_value_last": "numeric:float",
    "sm_time_run": "timestamp",
    "sm_time_start": "timestamp",
    "sm_time_end": "timestamp",
    "sm_time_pivot": "timestamp",
    "sm_period_type": "string",
    "sm_latest_from": None,

    # --- Slips ---
    "slips_domain": "string",
    "slips_obs_n": "numeric:integer",
    "slips_obs_id": "numeric:integer",
    #"slips_obs_alert_level": "string",
    "slips_obs_alert_color": "string",
    "slips_obs_alert_rgb": "string",
    "slips_pred_n": "numeric:integer",
    "slips_pred_id": "numeric:integer",
    #"slips_pred_alert_level": "string",
    "slips_pred_alert_color": "string",
    "slips_pred_alert_rgb": "string",
}
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to fill data
import pandas as pd

def fill_data(df: pd.DataFrame,
              fmr_schema: dict | None = None,
              time_start: pd.Timestamp | None = None,
              time_end : pd.Timestamp | None = None,
              generic_no_data: str = 'NA',
              freq: str | None = None) -> pd.DataFrame:
    """
    Keep only schema columns whose type is not None (plus 'time'), drop others.
    Fill NaNs in input per type. Expand on 'time' to [time_start, time_end],
    filling new rows per type with no-data codes.
    """
    if fmr_schema is None:
        fmr_schema = fmr_default

    df = df.copy()

    # 0) Ensure there's a 'time' column even if it's currently the index
    if 'time' not in df.columns:
        if df.index.name == 'time':
            df = df.reset_index()
        else:
            # If index is datetime-like but unnamed, treat it as time
            if pd.api.types.is_datetime64_any_dtype(df.index):
                df = df.reset_index().rename(columns={'index': 'time'})
            else:
                raise ValueError("No 'time' column found and index is not datetime-like.")

    # 1) Keep only schema columns with non-None type (plus 'time')
    schema_keep = {col: kind for col, kind in fmr_schema.items() if kind is not None}
    keep_cols = ['time'] + [c for c in schema_keep.keys() if c != 'time']
    existing_keep = [c for c in keep_cols if c in df.columns]
    df = df[existing_keep]  # drop columns not in schema or schema==None

    # 2) Ensure 'time' is datetime
    df['time'] = pd.to_datetime(df['time'], errors='coerce')

    # 3) Ensure all kept schema columns exist; add missing as NA (will be typed+filled)
    for col in keep_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # 4) Cast + prep fill values by type
    fill_values = {}
    for col, kind in schema_keep.items():

        k = (kind or "").lower()
        if "timestamp" in k:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            fill_values[col] = pd.NaT
        elif "numeric:integer" in k:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            fill_values[col] = -9999
        elif "numeric:float" in k:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
            fill_values[col] = -9999.0
        else:  # treat as string
            df[col] = df[col].astype('object')
            fill_values[col] = "NA"

    # 5) Fill existing NaN/NA in the *input* rows according to type
    for col, val in fill_values.items():
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # already NaT where missing
            continue
        df[col] = df[col].fillna(val)

    # 6) Determine frequency
    if freq is None:
        t_sorted = df['time'].dropna().sort_values().drop_duplicates()
        if len(t_sorted) >= 3:
            try:
                freq = pd.infer_freq(t_sorted)
            except Exception:
                freq = None
        if freq is None:
            freq = 'D'  # fallback

    # 7) Derive time_start/time_end if not provided
    if time_start is None:
        time_start = df['time'].min()
    if time_end is None:
        time_end = df['time'].max()
    if pd.isna(time_start) or pd.isna(time_end):
        raise ValueError("Cannot determine time range (all 'time' values are NaT).")

    # 8) Build full time index and reindex
    time_index = pd.date_range(pd.to_datetime(time_start),
                               pd.to_datetime(time_end),
                               freq=freq)
    df = (df
          .set_index('time')
          .sort_index()
          .reindex(time_index))

    # 9) Fill new rows (introduced by reindex) with the same no-data codes
    for col, val in fill_values.items():
        if col != 'time':
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # keep NaT for missing datetimes
                df[col] = df[col].astype('datetime64[ns]')
            else:
                df[col] = df[col].fillna(val)

    # 10) Restore 'time' and final column order (time first, then schema order kept)
    df = df.rename_axis('time').reset_index()
    schema_order = [c for c in keep_cols if c != 'time']
    df = df[['time'] + schema_order]

    df = df.fillna(generic_no_data)

    return df
# ----------------------------------------------------------------------------------------------------------------------

def remap_data(df, mapping, *, copy=True) -> pd.DataFrame:
    """
    Rename df columns using `mapping` where value is not None.
    - Warn if a key in mapping is missing from df.columns (except 'time' if it's the index).
    - Warn if multiple source columns map to the same target.
    - Leave the index alone (assumes time is already the index).
    """
    if copy:
        df = df.copy()

    # Build the effective rename map (skip None values)
    rename_map = {src: dst for src, dst in mapping.items() if dst is not None}

    # Handle 'time' specially if it's the index (not a column)
    if "time" in mapping:
        if "time" not in df.columns:
            # If index is named 'time', consider it satisfied; do nothing.
            if df.index.name != "time":
                log.warning("Key 'time' not found as a column and index name != 'time'. Leaving as-is.")
            # Drop 'time' from rename map so we don't try to rename a non-existent column
            rename_map.pop("time", None)

    # Warn about missing source columns
    missing = [src for src in rename_map.keys() if src not in df.columns]
    for src in missing:
        log.warning(f"Column '{src}' not found in DataFrame; skipping.")
        rename_map.pop(src, None)

    # Check for target collisions
    targets = list(rename_map.values())
    collisions = {t for t in targets if targets.count(t) > 1}
    if collisions:
        log.warning(f"Multiple columns map to the same target name(s): {sorted(collisions)}. "
                    "Later mappings will overwrite earlier ones in pandas.rename().")

    # Finally, rename
    df = df.rename(columns=rename_map)
    return df
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to rename keys in a dictionary
def remap_dat_olda(data: dict, rename_map: dict) -> pd.DataFrame:
    """
    Rename keys in a dictionary based on rename_map.
    - If mapping value is None -> remove the key
    - Warn if old_key not found in data
    - If new_key duplicates occur, append index to make them unique
    """

    result = {}

    # 1. Warn for missing keys
    for old_key in rename_map:
        if old_key not in data:
            log_stream.warning(f" ===> Warning: Key '{old_key}' not found in data.")

    # 2. Detect duplicates in new_key values (ignoring None)
    new_keys = [v for v in rename_map.values() if v is not None]
    counts = Counter(new_keys)

    # Prepare a tracker to rename duplicates
    duplicate_count = defaultdict(int)

    def unique_new_key(base_key):
        """Generate a unique key by appending index if needed."""
        if counts[base_key] <= 1:
            return base_key
        # If multiple duplicates
        duplicate_count[base_key] += 1
        if duplicate_count[base_key] == 1:
            # First occurrence uses base_key
            return base_key
        else:
            # Next occurrences get suffix
            return f"{base_key}_{duplicate_count[base_key]-1}"

    # 3. Build the new dictionary
    for key, value in data.items():
        if key in rename_map:
            new_key = rename_map[key]
            if new_key is not None:
                final_key = unique_new_key(new_key)
                # Warn if there were duplicates
                if counts[new_key] > 1:
                    log_stream.warning(f" ===> Warning: Duplicate target name '{new_key}', "
                          f"renamed to '{final_key}'")
                result[final_key] = value
            # else skip (removal)
        else:
            result[key] = value

    # Convert to DataFrame
    data = pd.DataFrame(result)

    return data
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to define analysis period
def define_analysis_period(
    time_start: Optional[pd.Timestamp] = None,
    time_end: Optional[pd.Timestamp] = None,
    time_period: Optional[int] = None,
    time_frequency: str = 'D',
    time_rounding: str = 'D',
    time_reference: Optional[pd.Timestamp] = None, **kwargs
) -> (pd.DatetimeIndex, None):
    """
    Define a period for analysis as a DatetimeIndex.

    Priority:
    1. If time_period and time_reference are provided:
       - Generate range ending at time_reference.
    2. Else if time_start and time_end are provided:
       - Generate range between start and end.
    3. If only one of time_start or time_end is given, raise an error.
    4. If no valid configuration, raise ValueError.

    Parameters
    ----------
    time_start : pd.Timestamp, optional
        Start of period.
    time_end : pd.Timestamp, optional
        End of period.
    time_period : int, optional
        Number of periods to generate.
    time_frequency : str, default 'D'
        Frequency string (pandas offset alias).
    time_rounding : str, default 'D'
        Rounding unit for time_reference.
    time_reference : pd.Timestamp, optional
        Reference time (used with time_period).

    Returns
    -------
    pd.DatetimeIndex
        Generated DatetimeIndex.

    Raises
    ------
    ValueError
        If no valid combination of inputs is provided.
    """

    if time_start == 'ref_data' and time_end == 'ref_data':
        return time_start, time_end

    # Round reference time if provided
    if time_reference is not None:
        time_reference = time_reference.round(time_rounding)

    # 1. Use time_reference and time_period
    if time_period is not None and time_reference is not None:
        return pd.date_range(end=time_reference, periods=time_period, freq=time_frequency)

    # 2. Use time_start and time_end (both required)
    if time_start is not None and time_end is not None:
        return pd.date_range(start=time_start, end=time_end, freq=time_frequency)

    # 3. Partial ranges with period
    if time_start is not None and time_period is not None:
        return pd.date_range(start=time_start, periods=time_period, freq=time_frequency)

    if time_end is not None and time_period is not None:
        return pd.date_range(end=time_end, periods=time_period, freq=time_frequency)

    if time_reference is not None and time_start is None and time_end is None:
        return None

# ----------------------------------------------------------------------------------------------------------------------