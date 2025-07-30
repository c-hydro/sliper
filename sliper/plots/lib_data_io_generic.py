"""
Library Features:

Name:          lib_data_io_generic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import pandas as pd

from copy import deepcopy
from lib_info_args import logger_name

# logging
warnings.filterwarnings('ignore')
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to remap dataframe object
def remap_dframe(obj_df_in, obj_map_in=None):

    index_name_in = obj_df_in.index.name

    if index_name_in in list(obj_map_in):
        index_name_out = obj_map_in[index_name_in]
        obj_df_in.index.name = index_name_out
    else:
        log_stream.warning(' ===> Index "' + index_name_in + '" not in map object. Fields keep the standard name')

    obj_map_out = {}
    for field_name_in, field_name_out in obj_map_in.items():
        if field_name_in != index_name_in:
            if field_name_in in list(obj_df_in.columns):
                obj_map_out[field_name_in] = field_name_out
            else:
                log_stream.warning(' ===> Column "' + field_name_in + '" not in the dataframe object.')

    obj_df_out = obj_df_in.rename(columns=obj_map_out)

    return obj_df_out
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to remap dict object
def remap_dict(obj_dict_in, obj_map=None):

    obj_dict_out = {}
    for field_name_in, field_name_out in obj_map.items():
        if field_name_in in list(obj_dict_in.keys()):
            obj_dict_out[field_name_out] = obj_dict_in[field_name_in]
        else:
            obj_dict_out[field_name_in] = obj_dict_in[field_name_in]

    return obj_dict_out

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to convert data to variable(s)
def convert_data_to_vars(obj_data, obj_row_key='alert_area_a', obj_column_key='soil_slips_domain_name',
                         obj_fields=None, time_start=None, time_end=None, time_range=None, time_freq='D'):

    if obj_fields is not None:
        dict_data = {}
        for data_name, data_value in obj_data.items():
            if data_name in list(obj_fields.values()):
                field_idx = list(obj_fields.values()).index(data_name)
                field_name = list(obj_fields.keys())[field_idx]
                dict_data[field_name] = data_value
            else:
                dict_data[data_name] = data_value
    else:
        dict_data = deepcopy(obj_data)

    if obj_data.index.name == 'time':
        obj_time = obj_data.index.values
        if 'time' in list(dict_data.keys()):
            dict_data.pop('time')
        dframe_data = pd.DataFrame(data=dict_data, index=obj_time)
        dframe_data.index.name = 'time'
    else:
        dframe_data = pd.DataFrame(data=dict_data)

    if obj_row_key is not None:
        dframe_data_selected = dframe_data.loc[dframe_data[obj_column_key] == obj_row_key]
    else:
        dframe_data_selected = deepcopy(dframe_data)

    dframe_data_selected = dframe_data_selected.sort_index(ascending=True)

    if time_start is not None and time_end is not None:
        time_range = pd.date_range(start=time_start, end=time_end, freq=time_freq)
        dframe_data_expected = pd.DataFrame(index=time_range)
        dframe_data_expected = dframe_data_expected.join(dframe_data_selected)
    else:
        dframe_data_expected = deepcopy(dframe_data_selected)

    dframe_data_expected.index.name = 'time'
    return dframe_data_expected
# ----------------------------------------------------------------------------------------------------------------------
