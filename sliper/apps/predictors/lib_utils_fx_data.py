"""
Library Features:

Name:          lib_utils_fx_data
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251010'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd
import numpy as np

from lib_info_args import logger_name

# set options
pd.options.mode.chained_assignment = None

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure data attributes
def ensure_data_attrs(df: pd.DataFrame, attrs: dict = None) -> pd.DataFrame:

    attrs_cleaned = {k: v for k, v in attrs.items() if k not in ("__comment__", "_comment_", "_comment", "comment_")}

    if attrs:
        df.attrs = attrs_cleaned
    return df
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure data valid
def ensure_data_valid(df: pd.DataFrame,
                      no_data_numeric: float = -9999, no_data_string: str = "NoData",
                      tags: list[str] = ["rain", "sm"]) -> pd.DataFrame:

    # Replace both string and numeric no-data markers with NaN
    df = df.replace(no_data_string, np.nan)
    df = df.replace(no_data_numeric, np.nan)

    # Select only columns that start with the given tags
    tag_cols = [c for c in df.columns if any(c.startswith(tag) for tag in tags)]

    # Drop rows where any of those tagged columns are NaN
    df_cleaned = df.dropna(subset=tag_cols, how='any')

    return df_cleaned
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure 'day_of the year' is the index of the dataframe
def ensure_time_doy(df, time_col_or_idx='time', time_col_doy='day_of_the_year'):
    """
    Ensure that the DataFrame has a 'day_of_the_year' column based on the 'time' column or index.
    If 'time' is not present, raises an error.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame.
    time_col_or_idx : str
        Name of the column or index to be treated as time.
    time_col_doy : str
        Name of the column to store the day-of-year.

    Returns
    -------
    pd.DataFrame
        The DataFrame with a 'day_of_the_year' column added.
    """
    # Determine and normalize time column
    if time_col_or_idx in df.columns:
        time_series = pd.to_datetime(df[time_col_or_idx])
    elif df.index.name == time_col_or_idx:
        # Convert index to a Series (so .dt works)
        time_series = pd.Series(pd.to_datetime(df.index), index=df.index)
        # Optionally keep a 'time' column
        df['time'] = time_series
    else:
        raise ValueError(f'Column or index "{time_col_or_idx}" not found in the DataFrame')

    # Add day-of-the-year column
    df[time_col_doy] = time_series.dt.dayofyear

    return df

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure 'time' is the index of the dataframe
def ensure_time_index(df, time_col='time'):
    """
    If 'time' is a column, set it as the index and drop the column.
    If 'time' is already the index, leave unchanged.
    """
    if time_col in df.columns:
        df = df.set_index(time_col)
    elif df.index.name == time_col:
        # already set as index, do nothing
        pass
    else:
        raise ValueError('Column "' + str(time_col) + '" not found as a column or index')
    return df
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to ensure unique step observed_forecast
def ensure_unique_obsfrc_data(
        df,
        rain_col="rain_period_type",
        sm_col="sm_period_type",
        time_col="time",
        search_value="Observed_Forecast",
        keep="last",                      # "last" (default) or "first"
        log_times=True,                   # log/print the times that match search_value
        no_data_numeric=-9999,
        no_data_string="NoData",
        no_data_time=pd.NaT):

    if keep not in {"last", "first"}:
        raise ValueError("Parameter 'keep' must be either 'last' or 'first'.")

    # Identify matching rows (logical OR)
    mask = (df[rain_col] == search_value) | (df[sm_col] == search_value)
    idx_match = df.index[mask].tolist()

    # Log all matching times (before any mutation)
    if log_times:
        if len(idx_match) > 0:
            times = [t for t in df.index[mask]]
            log_stream.warning(f" ===> Rows with {search_value!r}: {len(times)} -> times = {times}")

    # If more than one, keep only first/last and nullify the others
    if len(idx_match) > 1:
        keep_idx = idx_match[-1] if keep == "last" else idx_match[0]
        to_null = [i for i in idx_match if i != keep_idx]

        log_stream.warning(
            f"===> Source DataFrame contains {len(idx_match)} values of {search_value!r}. "
            f"Keep the {keep} (index={keep_idx}); nullifying {len(to_null)} others."
        )

        # Identify column types
        num_cols = df.select_dtypes(include=["number"]).columns
        dt_cols = df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns
        # Strings = all non-numeric and non-datetime columns except the time_col (if it's not datetime)
        str_cols = [c for c in df.columns if c not in num_cols and c not in dt_cols and c != time_col]

        # Nullify corresponding columns for the rows we are dropping
        df.loc[to_null, num_cols] = no_data_numeric
        df.loc[to_null, str_cols] = no_data_string
        df.loc[to_null, dt_cols] = no_data_time

    return df
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert dframe to arrays
def convert_df2array(fx_dframe):
    fx_arrays = fx_dframe.to_numpy()
    return fx_arrays
# ----------------------------------------------------------------------------------------------------------------------
