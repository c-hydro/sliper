"""
Library Features:

Name:          lib_utils_fx_data
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

# -------------------------------------------------------------------------------------
# Library
import logging
import pandas as pd

from copy import deepcopy

from lib_info_args import logger_name

# Option(s)
pd.options.mode.chained_assignment = None

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------

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
# method to convert dframe to arrays
def convert_df2array(fx_dframe):
    fx_arrays = fx_dframe.to_numpy()
    return fx_arrays
# ----------------------------------------------------------------------------------------------------------------------
