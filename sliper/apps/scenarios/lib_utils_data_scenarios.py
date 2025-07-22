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

from copy import deepcopy

from lib_data_io_csv import read_file_csv

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
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
def fill_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing values in a DataFrame based on the following rules:
    - Numeric columns: fill NaNs with -9999
    - Columns with 'time' in the name: attempt to convert to datetime, then fill NaT with pd.NaT
    - Object (string) columns: fill NaNs with 'NA'
    """
    df_filled = df.copy()

    for col in df_filled.columns:
        if 'time' in col.lower():
            # Convert to datetime where applicable
            df_filled[col] = pd.to_datetime(df_filled[col], errors='coerce')
            df_filled[col] = df_filled[col].fillna(pd.NaT)
        elif pd.api.types.is_numeric_dtype(df_filled[col]):
            df_filled[col] = df_filled[col].fillna(-9999)
        else:
            df_filled[col] = df_filled[col].fillna('NA')

    return df_filled
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to merge data by time
def merge_data_by_time(df_common: (pd.DataFrame, None), df_step: (pd.DataFrame, None),
               tag_data_db: str = 'db', tag_data_upd: str = 'latest',
               key_cols: (list, None) = ['time_start', 'time_end'], time_col: str = 'time') -> pd.DataFrame:
    """
    Merge two DataFrames on ['time_start', 'time_end'].
    - Updates rows in df_common if df_step has a newer time.
    - Appends rows from df_step not found in df_common.
    - Ensures structure is valid even if df_common is an uninitialized empty DataFrame.
    - Tracks which source had the latest update.

    Returns:
    - pd.DataFrame: Merged DataFrame with updated rows and 'latest_from' column.
    """

    # organize key columns and time column
    if key_cols is None:
        required_cols = [time_col]
    else:
        required_cols = key_cols + [time_col]

    # if df_step is None or empty, return df_common as is
    if df_step is None or df_step.empty:
        # If df_step is None or empty, return df_common as is
        return df_common.reset_index(drop=True)

    # validate df_step has required structure
    if not all(col in df_step.columns for col in required_cols):
        raise ValueError("df_step must contain columns: 'time', 'time_start', 'time_end'.")

    # convert 'time' in df_step to datetime safely
    df_step = df_step.copy()
    df_step[time_col] = pd.to_datetime(df_step[time_col], errors='coerce')

    # If df_common is empty or uninitialized, initialize from df_step
    if df_common.empty or not all(col in df_common.columns for col in required_cols):
        # Initialize df_common from df_step structure
        df_step['latest_from'] = tag_data_db
        return df_step.reset_index(drop=True)

    # clean and align df_common time column
    df_common_tmp = df_common.copy()
    df_common_tmp[time_col] = pd.to_datetime(df_common_tmp[time_col], errors='coerce')

    # remove from df_common all rows where 'time' is also in df_step
    df_common_filtered = deepcopy(df_common_tmp[~df_common_tmp['time'].isin(df_step['time'])])
    df_common_filtered['latest_from'] = tag_data_db

    # append all rows from df_step (they will override matching 'time' entries)
    df_common_updated = pd.concat([df_common_filtered, df_step], ignore_index=True)
    df_common_updated['latest_from'] = df_common_updated['latest_from'].fillna(tag_data_upd)

    # sort the DataFrame by the 'time' column
    df_common_sorted = df_common_updated.sort_values(by='time')

    # reset the index and drop the old one
    df_common = df_common_sorted.reset_index(drop=True)

    return df_common

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to merge data by multiple variables
def merge_data_by_vars(time_start, time_end, rain_df, sm_df, slips_df,
                       time_frequency: str = 'D', time_label: str = 'time',
                       priority: str = 'rain') -> pd.DataFrame:
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
    - priority: Which dataset keeps original column names ('rain', 'sm', or 'slips')

    Returns:
    - Merged DataFrame with harmonized columns
    """

    # --- Validation ---
    if priority not in ['rain', 'sm', 'slips']:
        raise ValueError("priority must be one of: 'rain', 'sm', 'slips'")

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
    df_map = {'rain': rain_df.copy(), 'sm': sm_df.copy(), 'slips': slips_df.copy()}
    all_columns = {time_label}
    priority_df = df_map[priority]

    # --- Rename columns if needed (only non-priority) ---
    for key, df in df_map.items():
        if key == priority:
            all_columns.update(set(df.columns) - {time_label})
            continue

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
        for prefix in ['rain_', 'sm_', 'slips_']:
            if f"{prefix}{time_label}" in df.columns:
                df.rename(columns={f"{prefix}{time_label}": time_label}, inplace=True)

    # --- Build time index and merge ---
    expected_time_range = pd.DataFrame({
        time_label: pd.date_range(start=time_start, end=time_end, freq=time_frequency)
    })

    merged = expected_time_range.merge(df_map['rain'], on=time_label, how='left')
    merged = merged.merge(df_map['sm'], on=time_label, how='left')
    merged = merged.merge(df_map['slips'], on=time_label, how='left')

    return merged

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read data (from csv file)
def read_data(file_data, var_data: str ='rain',
              type_data: str ='vector', format_data: str ='csv',
              fields_data: dict = {}, delimiter_data: str = ';'):

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
