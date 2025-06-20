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
import re
import pandas as pd
import numpy as np
import geopandas as gpd

from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read file point
def read_file_point(file_name, file_delimiter: str = ';', file_encoding: str = 'latin',
                    file_geo_x: str = 'lon', file_geo_y: str = 'lat'):

    # read CSV with a custom delimiter (e.g., semicolon)
    csv_dframe = pd.read_csv(file_name, delimiter=file_delimiter,  encoding=file_encoding)
    # convert DataFrame to GeoDataFrame (if coordinates exist)
    csv_obj = gpd.GeoDataFrame(
        csv_dframe, geometry=gpd.points_from_xy(csv_dframe[file_geo_x], csv_dframe[file_geo_y]))

    # get dates (to check format)
    csv_dates = csv_obj['data']
    # Define a regex pattern to match YYYY/MM/DD format
    pattern_dates = r'^\d{2}/\d{2}/\d{4}$'
    # Check if all entries match the pattern
    if csv_dates.str.match(pattern_dates).all():
        # convert to datetime and reformat
        tmp_dates = pd.to_datetime(csv_dates.values, format='%d/%m/%Y').strftime('%Y-%m-%d')
        csv_obj['data'] = pd.Series(tmp_dates)
        log_stream.warning(' ===> Soil slips DB dates converted to from "%d/%m/%Y" format to "%Y-%m-%d" format')

    # sort by 'data' in ascending order
    csv_obj = csv_obj.sort_values(by='data')

    # get geometry
    csv_geoms = ((feature['geometry'], 1) for feature in csv_obj.iterfeatures())
    # extract collections
    csv_collections = list(csv_obj.values)

    return csv_dframe, csv_collections, csv_geoms

# -------------------------------------------------------------------------------------
