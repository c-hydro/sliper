"""
Library Features:

Name:          lib_data_io_csv_weather_stations
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

# -------------------------------------------------------------------------------------
# Library
import logging
import pandas as pd

from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read file point
def read_file_point(file_name, file_header=None, file_subset_columns=None, file_subset_format=None,
                    file_pivot_column='code',
                    # file_points_column='point',
                    file_geo_x_column='longitude', file_geo_y_column='latitude',
                    file_sep=',', file_skiprows=1,
                    scale_factor_longitude=10, scale_factor_latitude=10):

    if file_header is None:
        file_header = ['code', 'name', 'longitude', 'latitude', 'time', 'data']

    if file_subset_columns is None:
        file_subset_columns = ['code', 'name', 'longitude', 'latitude']
    if file_subset_format is None:
        file_subset_format = {'code': str, 'name': str, 'longitude': float, 'latitude': float}

    file_dframe = pd.read_table(file_name, sep=file_sep, names=file_header, skiprows=file_skiprows)

    file_dframe = file_dframe.replace(to_replace=',', value='.', regex=True)
    file_dframe = file_dframe.replace(to_replace=':', value=file_sep, regex=True)

    file_dframe = file_dframe.dropna(axis='columns', how='all')
    file_dframe = file_dframe[file_subset_columns]
    file_dframe = file_dframe.drop_duplicates(subset=[file_pivot_column], keep='first')
    file_dframe = file_dframe.astype(file_subset_format)

    file_dframe[file_geo_x_column] = file_dframe[file_geo_x_column] / scale_factor_longitude
    file_dframe[file_geo_y_column] = file_dframe[file_geo_y_column] / scale_factor_latitude

    # file_dframe[file_points_column] = file_dframe[[file_geo_y_column, file_geo_x_column]].apply(tuple, axis=1)

    return file_dframe

# -------------------------------------------------------------------------------------
