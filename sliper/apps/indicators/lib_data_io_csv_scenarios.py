"""
Library Features:

Name:          lib_data_io_csv_scenarios
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

# -------------------------------------------------------------------------------------
# Libraries
import logging
import pandas as pd

from copy import deepcopy

from lib_info_args import logger_name_predictors as logger_name

# Logging
logging.getLogger('rasterio').setLevel(logging.WARNING)
log_stream = logging.getLogger(logger_name)

# Debug
# import matplotlib.pylab as plt

# Default settings
proj_default_wkt = \
    'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],' \
    'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],' \
    'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to write file csv
def write_file_csv(file_name, file_data, file_sep=',', file_header=True, file_index=True, file_format='%.3f'):
    if isinstance(file_data, pd.DataFrame):
        file_data.to_csv(file_name, sep=file_sep, header=file_header, index=file_index, float_format=file_format)
    else:
        log_stream.error(' ===> Variable type for writing csv file not supported')
        raise NotImplementedError('Case not implemented yet')
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read csv and transform to a dataframe
def convert_file_csv2df(file_name):
    file_df = pd.read_csv(file_name)

    if 'Unnamed' in list(file_df.columns)[0]:
        file_df.rename(columns={'Unnamed: 0': 'time'}, inplace=True)
        file_df['time'] = pd.to_datetime(file_df['time'], format="%Y-%m-%d")
        file_df.set_index('time', inplace=True)

    return file_df
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read file csv
def read_file_csv(file_name, file_time=None, file_header=None, file_format=None,
                  file_sep=',', file_skiprows=1, file_time_format='%Y%m%d%H%M',
                  scale_factor_longitude=10, scale_factor_latitude=10, scale_factor_data=1):

    if file_header is None:
        file_header = ['code', 'name', 'longitude', 'latitude', 'time', 'data']
    if file_format is None:
        file_format = {'code': str, 'name': str, 'longitude': float, 'latitude': float, 'data': float}

    file_dframe = pd.read_table(file_name, sep=file_sep, names=file_header, skiprows=file_skiprows)

    file_dframe = file_dframe.replace(to_replace=',', value='.', regex=True)
    file_dframe = file_dframe.replace(to_replace=':', value=file_sep, regex=True)

    file_dframe = file_dframe.dropna(axis='columns', how='all')

    if (file_dframe.columns.__len__() == 1) and (file_dframe.columns.__len__() != file_header.__len__()):

        log_stream.warning(' ===> The format of csv file "' + file_name +
                           '" is not in the expected format. Try to correct due to wrong file delimiter')

        file_cols_name = list(file_dframe.columns)[0]
        file_n_expected = file_header.__len__()

        file_dframe_tmp = file_dframe[file_cols_name].str.split(file_sep, file_n_expected, expand=True)
        file_dframe_tmp.columns = file_header

        file_dframe = deepcopy(file_dframe_tmp)

    elif file_dframe.columns.__len__() == file_header.__len__():
        pass
    else:
        log_stream.error(' ===> Parser of csv file "' + file_name + '" failed')
        raise IOError('Check the format of csv file')

    # values = [float(i) for i in file_dframe['data'].values]

    file_dframe = file_dframe.reset_index()
    file_dframe = file_dframe.set_index('time')

    file_dframe = file_dframe.astype(file_format)

    file_dframe.index = pd.to_datetime(file_dframe.index, format=file_time_format)
    file_dframe['longitude'] = file_dframe['longitude'] / scale_factor_longitude
    file_dframe['latitude'] = file_dframe['latitude'] / scale_factor_latitude
    file_dframe['data'] = file_dframe['data'] / scale_factor_data

    if file_time is not None:
        if file_time in file_dframe.index:
            file_dframe_select = file_dframe.loc[file_time]
        else:
            file_dframe_select = None
            log_stream.warning(' ===> Time "' + str(file_time) + '" is not available in file: "' + file_name + '"')
    else:
        file_dframe_select = file_dframe

    return file_dframe_select

# -------------------------------------------------------------------------------------
