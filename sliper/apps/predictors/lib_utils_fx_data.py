"""
Library Features:

Name:          lib_data_io_fx_kernel
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

# -------------------------------------------------------------------------------------
# Method to convert dframe to arrays
def convert_dframe2array(fx_dframe):
    fx_arrays = fx_dframe.to_numpy()
    return fx_arrays
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to order dataframe columns
def order_dataframe_columns(file_dframe, file_cols_order=None, type_cols_order=None):

    if file_cols_order is None:
        file_cols_order = ['event_domain']
    if not isinstance(file_cols_order, list):
        file_cols_order = [file_cols_order]

    if type_cols_order is None:
        type_cols_order = ['ascending']

    flag_cols_order = []
    for type_order in type_cols_order:
        if type_order == 'descending':
            flag_cols_order.append(False)
        elif type_order == 'ascending':
            flag_cols_order.append(True)
        else:
            log_stream.error(' ===> Type order "' + type_order + '" value is not accepted')
            raise IOError('Accepted values are "ascending" and "descending" ')

    file_dframe = file_dframe.sort_values(file_cols_order, ascending=flag_cols_order)

    return file_dframe
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to filter dataframe columns
def filter_dataframe_columns(file_dframe, file_cols_filter=None):

    if file_cols_filter is not None:
        if not isinstance(file_cols_filter, list):
            file_cols_filter = list(file_cols_filter)
        for file_col in file_cols_filter:
            if file_col not in list(file_dframe.columns):
                log_stream.error(' ===> DataFrame column "' + file_col + '" is expected but not found in the datasets')
                raise IOError('Column not found. Check your source file')
        file_dframe_filtered = file_dframe[file_cols_filter]
    else:
        file_dframe_filtered = deepcopy(file_dframe)
    return file_dframe_filtered

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to fill dataframe nans
def fill_dataframe_nan(file_dframe, column_list=None, fill_value_list=None):

    if column_list is None:
        column_list = ['event_threshold']
    if fill_value_list is None:
        fill_value_list = ['NA']

    for column_step, fill_value_step in zip(column_list, fill_value_list):
        file_series = file_dframe[column_step]
        file_series = file_series.fillna(value=fill_value_step)
        file_dframe[column_step] = file_series
    return file_dframe

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to add dataframe columns
def add_dataframe_columns(file_dframe,
                          tag_day_of_the_year='day_of_the_year', field_day_of_the_year=True, obj_day_of_the_year=None,
                          tag_n_domain='n_domain', field_n_domain=True, obj_n_domain=None):

    if field_day_of_the_year:
        file_dframe[tag_day_of_the_year] = pd.DatetimeIndex(file_dframe.index).dayofyear

    if field_n_domain:
        if obj_n_domain is None:
            obj_n_domain = {'event_domain': {'alert_area_a': 1}}
        if tag_n_domain not in list(file_dframe.columns):
            file_dframe[tag_n_domain] = None

        for obj_key in obj_n_domain.keys():
            for obj_value_in, obj_value_out in obj_n_domain[obj_key].items():
                file_dframe[tag_n_domain][file_dframe[obj_key].astype(str).str.contains(obj_value_in)] = obj_value_out

    return file_dframe

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to drop dataframe columns
def drop_dataframe_columns(file_dframe, file_cols_drop=None):

    if file_cols_drop is None:
        file_cols_drop = ['event_domain']
    if not isinstance(file_cols_drop, list):
        file_cols_drop = [file_cols_drop]

    file_dframe = file_dframe.drop(file_cols_drop, axis=1)

    return file_dframe

# -------------------------------------------------------------------------------------
