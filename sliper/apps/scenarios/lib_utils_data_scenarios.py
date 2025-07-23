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
import pandas as pd
import numpy as np

from typing import Optional
from copy import deepcopy

from lib_data_io_csv import read_file_csv

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


def keep_unique_time_columns(df, time_col='time'):
    time_cols = [col for col in df.columns if time_col in col.lower()]
    unique_time_cols = []
    seen = []

    for col in time_cols:
        is_duplicate = False
        for seen_col in seen:
            if df[col].equals(df[seen_col]):
                is_duplicate = True
                break
        if not is_duplicate:
            unique_time_cols.append(col)
            seen.append(col)

    # Keep only non-duplicate time columns + all non-time columns
    non_time_cols = [col for col in df.columns if col not in time_cols]
    cleaned_df = df[non_time_cols + unique_time_cols]

    return cleaned_df


# ----------------------------------------------------------------------------------------------------------------------
# method to format data
def format_data(df, fields=None):
    # Step 1: Drop 'time' column if redundant with index
    if df.index.name in df.columns and df[df.index.name].equals(df.index.to_series()):
        df = df.drop(columns=[df.index.name])

    # Step 2: Standardize column names
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Step 3: Replace sentinel values
    sentinel_values = ['NA', 'na', -9999, '-9999', '', None]
    df = df.replace(sentinel_values, np.nan)

    # Step 4: Attempt to parse datetime columns (non-numeric objects only)
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Step 5: Format datetime columns to string (ISO-like)
    for col in df.select_dtypes(include='datetime'):
        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Step 6: Apply custom field name mappings
    if fields:
        for original, new_name in fields.items():
            if original not in df.columns:
                warnings.warn(f"Column '{original}' not found in DataFrame.", UserWarning)
            elif new_name is None:
                df = df.drop(columns=[original])
            else:
                df = df.rename(columns={original: new_name})

    # Step 7: Reset index if named and ensure it's included
    if df.index.name is not None:
        df = df.reset_index()

    return df

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
def fill_data(df: pd.DataFrame, time_key: Optional[str] = 'time') -> pd.DataFrame:
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
        tag_data_upd: str = 'latest',
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
        df_step[extra_var_latest_from] = tag_data_upd
        df_result = df_step.reset_index(drop=True)
    else:
        df_common_tmp = df_common.copy()
        df_common_tmp[time_col] = pd.to_datetime(df_common_tmp[time_col], errors='coerce')

        # Drop overlapping time entries from df_common
        df_common_filtered = deepcopy(df_common_tmp[~df_common_tmp[time_col].isin(df_step[time_col])])
        df_common_filtered[extra_var_latest_from] = tag_data_db

        # Combine
        df_combined = pd.concat([df_common_filtered, df_step], ignore_index=True)
        df_combined[extra_var_latest_from] = df_combined[extra_var_latest_from].fillna(tag_data_upd)
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

    return merged

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read data (from csv file)
def read_data(file_data, var_data: str ='rain',
              type_data: str ='vector', format_data: str ='csv',
              fields_data: dict = {}, delimiter_data: str = ';',
              prefix_key: (str, None) = 'var', prefix_delimiter: (str, None) = '_'):

    # info get data start
    log_stream.info(f' -----> Get source data {var_data} ... ')

    # check the type of source data (point or grid)
    if type_data == 'vector' or type_data == 'point':

        # check the source data format (csv or xlsx)
        if format_data == 'csv':

            # check if the source file exists
            if os.path.exists(file_data):

                # read datasets
                file_dframe = read_file_csv(
                    file_data, fields=fields_data, key_column=None,
                    prefix_key=prefix_key, prefix_delimiter=prefix_delimiter,
                    delimiter=delimiter_data, result_format='dataframe')

                # info get data end
                log_stream.info(f' -----> Get source data {var_data} ... DONE')

            else:
                # if the source file does not exist, set file_dframe to None and log a warning
                file_dframe = None
                log_stream.warning(' ===> File "' + file_data + '" is not available.')

        else:
            # info get data end
            log_stream.error(' ===> Source data format is not supported. Check your source datasets')
            log_stream.info(' -----> Get source data ... FAILED')
            raise NotImplementedError('Only "csv" formats are available.')

    else:
        # if the source data type is not supported, raise an error
        log_stream.error(' ===> Source data type is not supported. Check your source datasets')
        log_stream.info(' -----> Get source data ... FAILED')
        raise NotImplementedError('Only "vector" types are available.')

    return file_dframe
# ----------------------------------------------------------------------------------------------------------------------
