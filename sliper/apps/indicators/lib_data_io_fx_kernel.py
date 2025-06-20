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

from lib_info_args import logger_name_predictors as logger_name

# Option(s)
pd.options.mode.chained_assignment = None

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------


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

    if ('time' in list(file_cols_filter)) and ('time' == file_dframe.index.name):
        file_cols_filter.remove('time')
    else:
        log_stream.error(' ===> DataFrame column "time" is expected as an index.')
        raise RuntimeError('Dataframe index returned a wrong index definition')

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
