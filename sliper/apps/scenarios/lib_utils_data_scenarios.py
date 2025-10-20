"""
Library Features:

Name:          lib_utils_data_scenarios
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import os
import re

import pandas as pd
import numpy as np

from datetime import timedelta, time

from typing import Optional
from copy import deepcopy

from lib_data_io_csv import read_file_csv

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to analyze time summary
def analyze_time_summary(dict_data, mode='newest', tag='folder_name_tag'):

    """
    Extract the newest or oldest 'time_tag' timestamp from a nested dictionary.
    Always returns a pandas.Timestamp.

    Parameters
    ----------
    dict_data : dict
        Dictionary of the form:
        {key_date: {'time_ref': Timestamp, 'time_tag': Timestamp}, ...}
    mode : str, optional
        Either 'newest' or 'oldest' (default: 'newest')

    Returns
    -------
    pd.Timestamp or None
        The newest or oldest 'time_tag' found, or None if empty.
    """
    if not dict_data:
        return None

    # Collect time_tag values and ensure they are Timestamps
    time_tags = []
    for v in dict_data.values():
        if tag in v and v[tag] is not None:
            time_tags.append(pd.Timestamp(v[tag]))

    if not time_tags:
        return None

    # Choose mode
    if mode == 'newest':
        return pd.Timestamp(max(time_tags))
    elif mode == 'oldest':
        return pd.Timestamp(min(time_tags))
    else:
        raise ValueError("mode must be either 'newest' or 'oldest'")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure that time as index
def ensure_time_index(dframe: pd.DataFrame, time_col: str ='time') -> pd.DataFrame:

    # Ensure 'time' is a proper index
    if time_col not in dframe.columns:
        raise KeyError(f"Column {time_col} not found in analysis_collections.")

    if dframe.index.name != 'time':
        # Convert to datetime if not already
        dframe[time_col] = pd.to_datetime(dframe['time'], errors='coerce')
        if dframe[time_col].isnull().any():
            raise ValueError(f"Column {time} contains invalid datetime values.")
        # Set as index
        dframe = dframe.set_index('time')

    return dframe
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to add missing days to df
def add_missing_days_with_nodata(
    df,
    start_date=None,
    end_date=None,
    date_col="time",
    no_data_numeric=-9999,
    no_data_string="NoData",
    time_start=pd.Timestamp("01:00").time(),
    time_end=pd.Timestamp("00:00").time(),  # can be None
    # --- new options ---
    run_time_cols=None,    # e.g. ["rain_time_run"]
    start_time_cols=None,  # e.g. ["rain_time_start", "something_time_start"]
    end_time_cols=None,    # e.g. ["rain_time_end", "something_time_end"]
    detect_time_cols=True, # auto-detect *_time_run/start/end if lists above are None
    suffixes=("time_run", "time_start", "time_end"),  # detection targets
):
    """
    Add missing daily steps between start_date and end_date with user-defined NoData values.
    Automatically fills any *_time_run, *_time_start, *_time_end (or exact matches)
    unless explicit column lists are provided.

    Logic:
    - If both start_date and end_date are None → return the original df unchanged.
    - If start_date is None → use the first date in df for the start.
    - If end_date is None → use the last date in df for the end.
    - If time_end is None → infer from existing *_time_end columns.

    - *_time_run   -> YYYY-MM-DD
    - *_time_start -> YYYY-MM-DD HH:MM   (time_start)
    - *_time_end   -> YYYY-MM-DD HH:MM   (next day with inferred or provided time_end)
    """

    # --- early exit if both bounds missing ---
    if start_date is None and end_date is None:
        return df.copy()

    df2 = df.copy()

    # normalize date column to date
    df2[date_col] = pd.to_datetime(df2[date_col]).dt.date

    # infer missing bounds from existing data
    if start_date is None:
        start_date = df2[date_col].min()
    if end_date is None:
        end_date = df2[date_col].max()

    # Create complete daily range
    full_range = pd.date_range(start=start_date, end=end_date, freq="D").date

    # Column typing
    numeric_cols = df2.select_dtypes(include=[np.number]).columns.tolist()
    other_cols = [c for c in df2.columns if c not in numeric_cols and c != date_col]

    # ---- discover time columns (unless explicitly provided) ----
    def _endswith_or_equal(col, suffix):
        return col == suffix or col.endswith("_" + suffix)

    all_cols = set(df2.columns)

    if run_time_cols is None and detect_time_cols:
        run_time_cols = [c for c in all_cols if _endswith_or_equal(c, suffixes[0])]
    if start_time_cols is None and detect_time_cols:
        start_time_cols = [c for c in all_cols if _endswith_or_equal(c, suffixes[1])]
    if end_time_cols is None and detect_time_cols:
        end_time_cols = [c for c in all_cols if _endswith_or_equal(c, suffixes[2])]

    run_time_cols = list(run_time_cols or [])
    start_time_cols = list(start_time_cols or [])
    end_time_cols = list(end_time_cols or [])

    # ---- helpers ----
    hhmm_re = re.compile(r"(\d{2}):(\d{2})")

    def _to_time_obj(x):
        if pd.isna(x):
            return None
        if isinstance(x, time):
            return x
        try:
            ts = pd.to_datetime(x, errors="coerce")
            if ts is not pd.NaT:
                return ts.time()
        except Exception:
            pass
        if isinstance(x, str):
            m = hhmm_re.search(x)
            if m:
                try:
                    return time(int(m.group(1)), int(m.group(2)))
                except ValueError:
                    return None
        return None

    # ---- infer per-column time_end if needed ----
    inferred_end_times = {}
    if time_end is None:
        for c in end_time_cols:
            if c in df2.columns:
                candidates = df2[c][(~df2[c].isna()) & (df2[c].astype(str) != str(no_data_string))]
                t_obj = None
                for val in candidates:
                    t_obj = _to_time_obj(val)
                    if t_obj is not None:
                        break
                inferred_end_times[c] = t_obj or time(0, 0)
        if not inferred_end_times:
            inferred_end_times["_default_"] = time(0, 0)
    else:
        if not isinstance(time_end, time):
            te = _to_time_obj(time_end)
            time_end = te if te is not None else time(0, 0)

    # ---- row factory for missing days ----
    def nodata_row(d):
        d_str = d.strftime("%Y-%m-%d")
        time_start_str = f"{d_str} {time_start.strftime('%H:%M')}"

        row = {date_col: d}

        # numeric: fill with numeric nodata
        for c in numeric_cols:
            row[c] = no_data_numeric

        # others: default to string nodata
        for c in other_cols:
            row[c] = no_data_string

        for c in run_time_cols:
            if c in all_cols:
                row[c] = d_str
        for c in start_time_cols:
            if c in all_cols:
                row[c] = time_start_str
        for c in end_time_cols:
            if c in all_cols:
                if time_end is None:
                    t_for_c = inferred_end_times.get(c, inferred_end_times.get("_default_", time(0, 0)))
                else:
                    t_for_c = time_end
                next_day = d + timedelta(days=1)
                row[c] = f"{next_day.strftime('%Y-%m-%d')} {t_for_c.strftime('%H:%M')}"
        return row

    # build missing rows
    existing_dates = set(df2[date_col])
    new_rows = [nodata_row(d) for d in full_range if d not in existing_dates]

    if new_rows:
        df2 = pd.concat([df2, pd.DataFrame(new_rows)], ignore_index=True)

    df2 = df2.sort_values(by=date_col).reset_index(drop=True)
    df2[date_col] = df2[date_col].astype(str)
    return df2

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to analyze data alignment
def analyze_data_alignment(
        time_run, time_range,
        df1, df2, df3,
        dn1='rain', dn2='sm', dn3='slips',
        time_col='time', freq=None,
        use1=True, use2=True, use3=True):
    """
    Analyzes common time periods, missing timestamps, and data completeness stats for three DataFrames.

    Returns:
        dict with:
            - 'time_reference': str
            - 'time_period_ref': (start, end)
            - 'time_period_data': (start, end)
            - 'frequency': frequency string
            - 'missing': dict of missing timestamps
            - 'stats': dict with expected/found/completeness per dataset
    """

    # Step 1: Ensure datetime and sort
    for df in [df1, df2, df3]:
        if df is not None and not df.empty:
            df[time_col] = pd.to_datetime(df[time_col])
            df.sort_values(by=time_col, inplace=True)

    # Helper to find missing & stats against an expected_range
    def find_missing_and_stats(df, start, end, expected_range):
        if df is None or df.empty:
            found = 0
            expected = len(expected_range)
            missing = expected_range  # everything missing
            percent = 0.0 if expected > 0 else 0.0
            return missing, expected, found, percent

        trimmed = df[(df[time_col] >= start) & (df[time_col] <= end)]
        found = trimmed[time_col].nunique()
        expected = len(expected_range)
        missing = expected_range.difference(trimmed[time_col])
        percent = round((found / expected) * 100, 2) if expected > 0 else 0.0
        return missing, expected, found, percent

    # Step 2: Select which DataFrames to include in time alignment (only non-empty + selected)
    periods = []
    if use1 and df1 is not None and not df1.empty:
        periods.append((df1[time_col].min(), df1[time_col].max()))
    if use2 and df2 is not None and not df2.empty:
        periods.append((df2[time_col].min(), df2[time_col].max()))
    if use3 and df3 is not None and not df3.empty:
        periods.append((df3[time_col].min(), df3[time_col].max()))

    # Step 3: Find common start and end OR handle empty-case via time_range
    if periods:
        start = max(s for s, _ in periods)
        end = min(e for _, e in periods)

        if start == end:
            expected_range = pd.date_range(start=start, end=end, freq=None)
        elif start > end:
            raise ValueError("No overlapping time range between the selected datasets.")
        else:
            # Step 4: Determine frequency (try infer from first non-empty DF, else fallback)
            if not freq:
                # Try infer from whichever DF contributed to periods first
                for df in [df1, df2, df3]:
                    if df is not None and not df.empty:
                        freq = pd.infer_freq(df[time_col].sort_values())
                        if freq:
                            break
                if freq is None:
                    freq = 'D'  # fallback default
            expected_range = pd.date_range(start=start, end=end, freq=freq)
    else:
        # ---------- EMPTY CASE (your requested else) ----------
        # No non-empty datasets among the selected ones: fall back to the provided time_range
        if time_range is None or len(time_range) == 0:
            # If even time_range is unavailable, return a minimal, empty scaffold
            start = end = None
            if not freq:
                freq = 'D'
            expected_range = pd.DatetimeIndex([])
        else:
            # Use the passed time_range as reference
            # time_range can be a DatetimeIndex/Series/list-like of datetimes
            tr = pd.to_datetime(pd.Index(time_range))
            start = tr.min()
            end = tr.max()
            if not freq:
                # Try to infer from time_range itself
                freq = pd.infer_freq(tr)
                if freq is None:
                    freq = 'D'
            expected_range = pd.date_range(start=start, end=end, freq=freq)

    # Step 5: Find missing timestamps and compute stats for all three (even if not used)
    missing1, exp1, found1, perc1 = find_missing_and_stats(df1, start, end, expected_range)
    missing2, exp2, found2, perc2 = find_missing_and_stats(df2, start, end, expected_range)
    missing3, exp3, found3, perc3 = find_missing_and_stats(df3, start, end, expected_range)

    return {
        'time_reference': time_run,
        'time_period_ref': (time_range[0], time_range[-1]) if time_range is not None and len(time_range) > 0 else (None, None),
        'time_period_data': (start, end),
        'frequency': freq,
        'missing': {
            dn1: missing1,
            dn2: missing2,
            dn3: missing3,
        },
        'stats': {
            dn1: {'expected': exp1, 'found': found1, 'percent': perc1},
            dn2: {'expected': exp2, 'found': found2, 'percent': perc2},
            dn3: {'expected': exp3, 'found': found3, 'percent': perc3},
        }
    }

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to memorize data to archive
def memorize_data(file_archive: dict, file_path: str) -> dict:

    if file_path in list(file_archive.keys()):
        file_active = False
        tmp_active = file_archive[file_path]
        if not tmp_active:
            log_stream.warning(' ===> File "' + file_path + '" is not available.')
    else:
        if os.path.exists(file_path):
            file_active = True
        else:
            file_active = False
        file_archive[file_path] = file_active
    return file_archive, file_active

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to fill missing data in DataFrame
def fill_data(df: pd.DataFrame, time_key: Optional[str] = 'time', time_index: bool = False) -> pd.DataFrame:
    """
    Fills missing values in a DataFrame based on column types and names.

    Rules:
    - Columns containing the `time_key` substring (case-insensitive) are:
        - Converted to datetime
        - Filled with pd.NaT for missing values
    - Numeric columns:
        - Fill NaNs with -9999
    - String/object columns:
        - Fill NaNs with 'NA'
    - Sets the first matching time-related column as index (if found)

    Parameters:
    - df (pd.DataFrame): Input DataFrame with potential missing values
    - time_key (str, optional): Substring to identify time-related columns (default: 'time')

    Returns:
    - pd.DataFrame: A new DataFrame with missing values filled and datetime conversion applied
    """
    df_filled = df.copy()
    time_cols = [col for col in df_filled.columns if time_key.lower() in col.lower()]

    for col in df_filled.columns:
        if col in time_cols:
            df_filled[col] = pd.to_datetime(df_filled[col], errors='coerce')
            df_filled[col] = df_filled[col].fillna(pd.NaT)
        elif pd.api.types.is_numeric_dtype(df_filled[col]):
            df_filled[col] = df_filled[col].fillna(-9999)
        elif pd.api.types.is_string_dtype(df_filled[col]):
            df_filled[col] = df_filled[col].fillna('NA')
        else:
            df_filled[col] = df_filled[col].fillna('NA')  # Fallback

    # Set the first matching time-related column as index if any exist
    if time_index:
        if time_cols:
            index_col = time_cols[0]
            df_filled.set_index(index_col, inplace=True, drop=False)
            df_filled.index.name = index_col

    if df_filled.index.name in df_filled.columns:
        df_filled = df_filled.drop(columns=[df_filled.index.name])

    return df_filled
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to merge data by time
def merge_data_by_time(
        df_common: Optional[pd.DataFrame],
        df_step: Optional[pd.DataFrame],
        tag_data_db: str = 'db',
        tag_data_now: str = 'latest',
        prefix_keys: Optional[str] = None,
        key_cols: Optional[list] = ['{:}_start', '{:}_end'],
        time_col: str = 'time',
        delimiter: str = '_') -> pd.DataFrame:

    """
    Merge two DataFrames on time and optional dynamic key columns.

    - Dynamically replaces '{:}' in key_cols with prefix_keys + delimiter.
    - Merges by time and resolved keys, giving priority to newer df_step rows.
    - Avoids duplicate entries and adds provenance via 'latest_from'.
    - Skips re-prefixing if prefix already exists.

    Parameters:
    - df_common (pd.DataFrame): Existing data.
    - df_step (pd.DataFrame): New incoming step data.
    - tag_data_db (str): Tag for existing data rows.
    - tag_data_upd (str): Tag for updated rows from df_step.
    - prefix_keys (str): Optional prefix for dynamic columns.
    - key_cols (list): Keys to match, supports '{:}' placeholder.
    - time_col (str): Time column used for merging.
    - delimiter (str): Delimiter between prefix and column name.

    Returns:
    - pd.DataFrame: Combined DataFrame with updates and tags.
    """

    # Resolve key columns with prefix if placeholder present
    if key_cols is None:
        resolved_key_cols = [time_col]
    else:
        resolved_key_cols = []
        for col in key_cols:
            if '{:}_' in col and prefix_keys is not None:
                col = col.replace('{:}', f"{prefix_keys}")
            elif '{:}' in col and prefix_keys is not None:
                col = col.replace('{:}', f"{prefix_keys}{delimiter}")
            elif '{:}' in col and prefix_keys is None:
                raise ValueError("prefix_keys must be provided if '{:}' is used in key_cols")

            resolved_key_cols.append(col)

        resolved_key_cols += [time_col]

    # Return early if df_step is None or empty
    if df_step is None or df_step.empty:
        return df_common.reset_index(drop=True) if df_common is not None else df_common

    # Validate df_step has required structure
    for col in resolved_key_cols:
        if col not in df_step.columns:
            raise ValueError(f"df_step must contain columns: {resolved_key_cols} but columns {col} is not available")

    if not all(col in df_step.columns for col in resolved_key_cols):
        raise ValueError(f"df_step must contain columns: {resolved_key_cols}")

    # Prepare df_step
    df_step = df_step.copy()
    df_step[time_col] = pd.to_datetime(df_step[time_col], errors='coerce')

    if prefix_keys is not None:
        extra_var_latest_from = '{:}_latest_from'.format(prefix_keys)
    else:
        extra_var_latest_from = 'latest_from'

    # Initialize if df_common is invalid or uninitialized
    if df_common is None or df_common.empty or not all(col in df_common.columns for col in resolved_key_cols):
        df_step[extra_var_latest_from] = tag_data_now
        df_result = df_step.reset_index(drop=True)
    else:
        # Base copies + dtype alignment
        df_common_tmp = df_common.copy()
        df_common_tmp[time_col] = pd.to_datetime(df_common_tmp[time_col], errors='coerce')

        df_step_tmp = df_step.copy()
        df_step_tmp[time_col] = pd.to_datetime(df_step_tmp[time_col], errors='coerce')

        # Keep ONLY rows from df_step that are not already in df_common_tmp
        df_step_filtered = df_step_tmp[~df_step_tmp[time_col].isin(df_common_tmp[time_col])].copy()

        # Provenance
        df_common_tagged = df_common_tmp.copy()
        df_common_tagged[extra_var_latest_from] = tag_data_now
        df_step_filtered[extra_var_latest_from] = tag_data_db

        # Combine (all df_common_tmp + only non-overlapping df_step)
        df_combined = pd.concat([df_common_tagged, df_step_filtered], ignore_index=True)
        df_result = df_combined.sort_values(by=time_col).reset_index(drop=True)

    # Prefix data columns if needed
    if prefix_keys:
        time_related = set(resolved_key_cols + [extra_var_latest_from])
        data_cols = [col for col in df_result.columns if col not in time_related]

        rename_map = {
            col: f"{prefix_keys}{delimiter}{col}"
            for col in data_cols
            if not col.startswith(f"{prefix_keys}{delimiter}")
        }

        df_result = df_result.rename(columns=rename_map)

    return df_result

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to merge data by multiple variables
def merge_data_by_vars(
        time_start, time_end,
        rain_df, sm_df, slips_df,
        rain_var: str = 'rain', sm_var: str = 'soil_moisture', slips_var: str = 'slips',
        domain_var: str = 'domain', domain_label: str = 'domain',
        time_frequency: str = 'D', time_label: str = 'time') -> pd.DataFrame:
    """
    Merge rainfall, soil moisture, and landslide datasets based on a common time range.
    Adds prefixes only when column name conflicts arise across datasets (excluding the priority one).

    Parameters:
    - time_start: Start datetime (str or pd.Timestamp)
    - time_end: End datetime (str or pd.Timestamp)
    - rain_df: Rainfall DataFrame
    - sm_df: Soil moisture DataFrame
    - slips_df: Landslide DataFrame
    - time_frequency: Frequency for date range (default: 'D')
    - time_label: Time column name used for alignment

    Returns:
    - Merged DataFrame with harmonized columns
    """

    # --- Validation ---
    if rain_df is None or rain_df.empty:
        raise ValueError("Rainfall data (rain_df) is missing or empty.")
    if sm_df is None or sm_df.empty:
        raise ValueError("Soil moisture data (sm_df) is missing or empty.")
    if slips_df is None or slips_df.empty:
        warnings.warn("Landslide data (slips_df) is missing or empty. Proceeding without it.")
        slips_df = pd.DataFrame(columns=[time_label])

    # --- Ensure datetime type and warn on duplicates ---
    for name, df in [('rain_df', rain_df), ('sm_df', sm_df), ('slips_df', slips_df)]:
        try:
            df[time_label] = pd.to_datetime(df[time_label])
        except Exception as e:
            raise ValueError(f"Error parsing {time_label} in {name}: {e}")
        if df[time_label].duplicated().any():
            warnings.warn(f"Duplicate {time_label} values found in {name}")

    # --- Prepare mapping ---
    df_map = {
        rain_var: rain_df.copy(), sm_var: sm_df.copy(), slips_var: slips_df.copy()
    }
    all_columns = {time_label}

    # --- Rename columns if needed ---
    for key, df in df_map.items():
        new_cols = {}
        for col in df.columns:
            if col == time_label:
                continue
            if col in all_columns:
                new_cols[col] = f"{key}_{col}"  # prefix if name already seen
            all_columns.add(new_cols.get(col, col))  # update used names

        df.rename(columns=new_cols, inplace=True)

    # --- Ensure consistent time column ---
    for df in df_map.values():
        for prefix in [rain_var, sm_var, slips_var]:
            if f"{prefix}{time_label}" in df.columns:
                df.rename(columns={f"{prefix}{time_label}": time_label}, inplace=True)

    # --- Build time index and merge ---
    expected_time_range = pd.DataFrame({
        time_label: pd.date_range(start=time_start, end=time_end, freq=time_frequency)
    })

    merged = expected_time_range.merge(df_map[rain_var], on=time_label, how='left')
    merged = merged.merge(df_map[sm_var], on=time_label, how='left')
    merged = merged.merge(df_map[slips_var], on=time_label, how='left')

    merged[domain_label] = domain_var

    return merged

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read data (from csv file)
def read_data(file_data, var_data: str ='rain',
              type_data: str ='vector', format_data: str ='csv',
              fields_data: dict = {}, delimiter_data: str = ';', fields_extra: dict = {},
              prefix_key: (str, None) = 'var', prefix_delimiter: (str, None) = '_'):

    # info get data start
    log_stream.info(f' ---------> Get source data {var_data} ... ')

    # check the type of source data (point or grid)
    if type_data == 'vector' or type_data == 'point':

        # check the source data format (csv or xlsx)
        if format_data == 'csv':

            # check if the source file exists
            if os.path.exists(file_data):

                # read datasets
                file_dframe = read_file_csv(
                    file_data, fields=fields_data, key_column=None,
                    extra_fields=fields_extra,
                    prefix_key=prefix_key, prefix_delimiter=prefix_delimiter,
                    delimiter=delimiter_data, result_format='dataframe')

                # check the reader output
                if file_dframe is not None:
                    # info get data end
                    log_stream.info(f' ---------> Get source data {var_data} ... DONE')
                else:
                    # info get data end
                    log_stream.warning(' ===> File "' + file_data + '" exists but is empty')
                    log_stream.info(f' ---------> Get source data {var_data} ... FAILED. Datasets is defined by NoneType')

            else:
                # if the source file does not exist, set file_dframe to None and log a warning
                file_dframe = None
                log_stream.warning(' ===> File "' + file_data + '" is not available.')
                # info get data end
                log_stream.info(f' ---------> Get source data {var_data} ... FAILED')

        else:
            # info get data end
            log_stream.error(' ===> Source data format is not supported. Check your source datasets')
            log_stream.info(f' ---------> Get source data {var_data} ... FAILED')
            raise NotImplementedError('Only "csv" formats are available.')

    else:
        # if the source data type is not supported, raise an error
        log_stream.error(' ===> Source data type is not supported. Check your source datasets')
        log_stream.info(f' ---------> Get source data {var_data} ... FAILED')
        raise NotImplementedError('Only "vector" types are available.')

    return file_dframe
# ----------------------------------------------------------------------------------------------------------------------
