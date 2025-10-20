"""
Library Features:

Name:          lib_data_io_xlsx
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd
from copy import deepcopy

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# NOTE:
# if install optional dependency "xlrd" >= 1.0.0 for Excel support using pip or conda
# "conda install -y -c conda-forge xlrd" or "pip install xlrd"
# if install optional dependency "xlrd" >= 2.0.0 for Excel support install "openpyxl" instead with pip or conda
# "conda install -y -c conda-forge openpyxl" or "pip install openpyxl"
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read file xlsx
def read_file_xlsx(file_name, file_time=None, file_header=None, file_format=None,
                   file_sheet_name=0,
                   file_skiprows=1, file_time_format='%Y%m%d%H%M',
                   scale_factor_longitude=10, scale_factor_latitude=10, scale_factor_data=1,
                   file_dframe_active=None):

    log_stream.info(' -----> Get dataframe from file "' + file_name + '" ... ')

    if file_dframe_active is None:

        if file_header is None:
            file_header = ['code', 'name', 'longitude', 'latitude', 'time', 'data']
        if file_format is None:
            file_format = {'code': str, 'name': str, 'longitude': float, 'latitude': float, 'data': float}

        file_dframe = pd.read_excel(file_name, sheet_name=file_sheet_name, names=file_header, skiprows=file_skiprows)

        file_dframe = file_dframe.replace(to_replace=',', value='.', regex=True)
        file_dframe = file_dframe.dropna(axis='columns', how='all')

        file_dframe = file_dframe.reset_index()
        file_dframe = file_dframe.set_index('time')

        file_dframe = file_dframe.astype(file_format)

        file_dframe.index = pd.to_datetime(file_dframe.index, format=file_time_format)
        file_dframe['longitude'] = file_dframe['longitude'] / scale_factor_longitude
        file_dframe['latitude'] = file_dframe['latitude'] / scale_factor_latitude
        file_dframe['data'] = file_dframe['data'] / scale_factor_data

        file_dframe_active = deepcopy(file_dframe)

        log_stream.info(' -----> Get dataframe from file  "' + file_name + '" ... DONE')

    else:

        file_dframe = deepcopy(file_dframe_active)

        log_stream.info(' -----> Get dataframe from file  "' + file_name + '" ... USED A PREVIOUS LOADED DATAFRAME')

    log_stream.info(' -----> Select dataframe by reference time ... ')

    if file_time is not None:
        if file_time in file_dframe.index:
            file_dframe_select = file_dframe.loc[file_time]
            log_stream.info(' -----> Select dataframe by reference time ... DONE')
        else:
            file_dframe_select = None
            log_stream.warning(' ===> Time "' + str(file_time) + '" is not available in file: "' + file_name + '"')
            log_stream.info(' -----> Select dataframe by reference time ... FAILED')
    else:
        file_dframe_select = file_dframe
        log_stream.info(' -----> Select dataframe by reference time ... SKIPPED')

    return file_dframe_select, file_dframe_active

# ----------------------------------------------------------------------------------------------------------------------
