"""
Library Features:

Name:          lib_utils_data_table
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import os
from pathlib import Path
from copy import deepcopy

from lib_data_io_json import read_file_json
from lib_utils_system import get_dict_nested_value

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to read table data (file or dictionary)
def read_table_obj(table_obj):

    if isinstance(table_obj, dict):
        table_data = deepcopy(table_obj)
    elif isinstance(table_obj, str):

        folder_name_table, file_name_table = os.path.split(table_obj)
        if folder_name_table == '':
            folder_name_default = Path(__file__).parent.absolute()
            table_obj = os.path.join(folder_name_default, file_name_table)

        if os.path.exists(table_obj):
            table_data = read_file_json(table_obj)
        else:
            log_stream.error(' ===> Table data file "' + table_obj + '" is not available')
            raise IOError('File not found!')
    else:
        log_stream.error(' ===> Table data object is not supported')
        raise NotImplementedError('Case not implemented yet')

    return table_data

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to select table data
def select_table_obj(table_data, table_tags=None):
    if (table_tags is not None) and (isinstance(table_tags, list)):
        return get_dict_nested_value(table_data, table_tags)
    else:
        return table_data
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get table value
def get_table_value(table_data, table_key, table_value_default=None):
    table_value = deepcopy(table_value_default)
    if (table_data is not None) and (table_key is not None):
        if table_key in list(table_data.keys()):
            table_value = table_data[table_key]
    return table_value
# -------------------------------------------------------------------------------------
