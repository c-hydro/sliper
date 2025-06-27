"""
Library Features:

Name:          lib_utils_data_point_soil_slips
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import os
import pandas as pd
import numpy as np

from copy import deepcopy

from shapely.geometry import Point

from pandas.tseries import offsets

from lib_data_io_shp import read_file_shp as read_file_point_shp
from lib_data_io_csv_soil_slips import read_file_point as read_file_point_csv

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# map dictionary
alert_area_map = {
    'alert_area_a': 'A', 'alert_area_b': 'B',
    'alert_area_c': 'C', 'alert_area_d': 'D', 'alert_area_e': 'E'}
#######################################################################################


# -------------------------------------------------------------------------------------
# Read point soil slips
def read_point_file(file_name, file_type: str = 'shapefile', file_delimiter: str = ';'):

    if os.path.exists(file_name):
        if file_type == 'shapefile':
            point_dframe, point_collections, point_geoms = read_file_point_shp(file_name)
        elif file_type == 'csv':
            point_dframe, point_collections, point_geoms = read_file_point_csv(file_name, file_delimiter=file_delimiter)
        else:
            log_stream.error(' ===> Soil slips database file "' + file_name + '" format is not supported')
            raise NotImplemented('Case not implemented yet')
    else:
        log_stream.error(' ===> Soil slips database file "' + file_name + '" is not available')
        raise IOError('File not found!')

    return point_dframe

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to join point and its grid
def join_point_and_grid(point_dframe_in, polygons_collections,
                        max_distance=0.01,
                        point_code_tag='code', point_area_tag='alert_area',
                        point_longitude_tag='longitude', point_latitude_tag='latitude',
                        point_delimiter_tag=':'):

    log_stream.info(' ------> Assign "' + point_area_tag + '" for soil slips with undefined value ... ')

    point_dframe_out = deepcopy(point_dframe_in)
    if (point_area_tag.lower() in point_dframe_in.columns) or (point_area_tag in point_dframe_in.columns):

        if point_area_tag in point_dframe_in.columns:
            select_dframe = point_dframe_in.loc[point_dframe_in[point_area_tag].isnull()]
        elif point_area_tag.lower() in point_dframe_in.columns:
            log_stream.warning(' ===> Tag "' + point_area_tag +
                               '" is available but in lower case format "' + point_area_tag.lower() + '"')
            select_dframe = point_dframe_in.loc[point_dframe_in[point_area_tag.lower()].isnull()]
        else:
            log_stream.error(' ===> Tag "' + point_area_tag +
                             '" is mandatory and unexpected error occurred in the script')
            raise RuntimeError('Check your soil slips shapefile for the requested column')

        if not select_dframe.empty:

            point_code = select_dframe[point_code_tag].values
            point_x = select_dframe[point_longitude_tag].values
            point_y = select_dframe[point_latitude_tag].values

            point_domain_array = np.array([None] * select_dframe.shape[0], dtype=object)
            for polygon_key, polygon_fields in polygons_collections.items():

                polygon_geometry = list(polygon_fields['geometry'].values)[0]

                for i, (code, x, y) in enumerate(zip(point_code, point_x, point_y)):
                    var_point = Point(x, y)
                    if polygon_geometry.contains(var_point):
                        polygon_alert_area = polygon_key.split(point_delimiter_tag)[2]
                        if polygon_alert_area in list(alert_area_map.keys()):
                            log_stream.info(' -------> Point Code "' + str(code) +
                                            '" joins to alert area ...  ')

                            value_alert_area = alert_area_map[polygon_alert_area]
                            point_domain_array[i] = value_alert_area

                            point_dframe_out.loc[point_dframe_out[point_code_tag] == code, point_area_tag.lower()] = value_alert_area

                            log_stream.info(' -------> Point Code "' + str(code) +
                                            '" joins to alert area ... "' + value_alert_area + '"')

            check_dframe = point_dframe_out.loc[point_dframe_out[point_area_tag.lower()].isnull()]
            if check_dframe.empty:
                log_stream.info(' ------> Assign "' + point_area_tag +
                                '" for soil slips with undefined value ... DONE. '
                                'All the soil slips have the "' + point_area_tag +
                                '" defined by automatic detection or user.')
            else:
                log_stream.info(' ------> Assign "' + point_area_tag +
                                '" for soil slips with undefined value ... PARTIALLY DONE. '
                                'Not all the soil slips have the "' + point_area_tag +
                                '" defined by automatic detection or user.')

        else:
            log_stream.info(' ------> Assign "' + point_area_tag +
                            '" for soil slips with undefined value ... SKIPPED. '
                            'All the soil slips have the "' + point_area_tag + '" defined by user.')

    else:
        log_stream.info(' ------> Assign "' + point_area_tag +
                        '" for soil slips with undefined value ... FAILED.')
        log_stream.error(' ===> Tag "' + point_area_tag +
                         '" is mandatory and not included in the soil slips shapefile')
        raise RuntimeError('Check your soil slips shapefile for the requested column')

    return point_dframe_out
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to join point and grid
def collect_point_data(point_dframe_src, point_group_obj, point_time_format='%Y-%m-%d',
                       point_dframe_alert_area_tag='ZONA_ALLER', point_dframe_time_tag='DATA',
                       point_group_name_tag_src='name', point_group_threshold_tag_src='warning_threshold',
                       point_group_index_tag_src='warning_index', point_group_feature_tag_src=None,
                       point_group_n_tag_dst='event_n', point_group_name_tag_dst=None,
                       point_group_threshold_tag_dst='event_threshold', point_group_index_tag_dst='event_index',
                       point_group_feature_tag_dst='event_feature'):

    points_collections = {}
    for group_key, group_data in point_group_obj.items():

        group_selection = group_data[point_group_name_tag_src]
        group_threshold = group_data[point_group_threshold_tag_src]
        group_index = group_data[point_group_index_tag_src]

        # get points datasets filtered by alert area name
        if point_dframe_alert_area_tag in list(point_dframe_src.columns):
            point_selection = point_dframe_src.loc[point_dframe_src[point_dframe_alert_area_tag] == group_selection]
        elif point_dframe_alert_area_tag.lower() in list(point_dframe_src.columns):
            log_stream.warning(' ===> Tag "' + point_dframe_alert_area_tag +
                               '" is available but in lower case format "' + point_dframe_alert_area_tag.lower() + '"')
            point_selection = point_dframe_src.loc[point_dframe_src[point_dframe_alert_area_tag.lower()] == group_selection]
            # update the tag in lower case
            point_dframe_alert_area_tag = point_dframe_alert_area_tag.lower()
        else:
            log_stream.error(' ===> Tag "' + point_dframe_alert_area_tag +
                             '" is mandatory and not included in the soil slips shapefile')
            raise RuntimeError('Check your soil slips shapefile for the requested column')

        # geo_point_selection = geo_point_selection.reset_index()
        # geo_point_selection = geo_point_selection.set_index(self.column_db_tag_time)

        # get points time sorted
        if point_dframe_time_tag in list(point_dframe_src.columns):
            time_point_selection = pd.DatetimeIndex(
                point_selection[point_dframe_time_tag].values).unique().sort_values()
        elif point_dframe_time_tag.lower() in list(point_dframe_src.columns):
            log_stream.warning(' ===> Tag "' + point_dframe_time_tag +
                               '" is available but in lower case format "' + point_dframe_time_tag.lower() + '"')
            time_point_selection = pd.DatetimeIndex(
                point_selection[point_dframe_time_tag.lower()].values).unique().sort_values()
            # update the tag in lower case
            point_dframe_time_tag = point_dframe_time_tag.lower()
        else:
            log_stream.error(' ===> Tag "' + point_dframe_time_tag +
                             '" is mandatory and not included in the soil slips shapefile')
            raise RuntimeError('Check your soil slips shapefile for the requested column')

        point_list_n, point_list_feature, point_list_threshold, point_list_index, point_list_time = [], [], [], [], []
        for time_point_id, time_point_step in enumerate(time_point_selection):

            if not pd.isnull(time_point_step):
                time_str_step = time_point_step.strftime(point_time_format)

                point_step = point_selection.loc[point_selection[point_dframe_time_tag] == time_str_step]
                point_threshold = find_point_category(point_step.shape[0], group_threshold)
                point_index = find_point_value(point_threshold, group_index)

                point_list_n.append(point_step.shape[0])
                point_list_feature.append(point_step)
                point_list_threshold.append(point_threshold)
                point_list_index.append(point_index)

                point_list_time.append(time_point_step)

            else:
                log_stream.warning(
                    ' ===> Time id  "' + str(time_point_id) + '" for the group "' + group_key +
                    '" is defined by NatType; the soil-slips will be not insert in the selected dataset.'
                    'Check your database to delete or complete this record!')

        point_data = {point_group_n_tag_dst: point_list_n, point_group_threshold_tag_dst: point_list_threshold,
                      point_group_index_tag_dst: point_list_index,
                      point_group_feature_tag_dst: point_list_feature}

        point_time = pd.DatetimeIndex(point_list_time)

        point_dframe_dst = pd.DataFrame(point_data, index=point_time)

        points_collections[group_key] = {}
        points_collections[group_key] = point_dframe_dst

    return points_collections
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to select point data
def select_point_by_time(time_step, point_dframe=None, time_format='%Y-%m-%d 00:00:00'):

    if point_dframe is not None:

        point_dframe_time_from = point_dframe.index[0]
        point_dframe_time_to = point_dframe.index[-1]

        if isinstance(time_step, pd.Timestamp):
            time_tmp = time_step.strftime('%Y-%m-%d')
            time_step = pd.Timestamp(time_tmp)
        elif isinstance(time_step, pd.Timestamp):
            time_tmp = pd.Timestamp(time_step)
            time_tmp = time_tmp.strftime('%Y-%m-%d')
            time_step = pd.Timestamp(time_tmp)
        else:
            log_stream.error(' ===> Time step format to select point data is not supported')
            raise NotImplemented('Case not implemented yet')

        if time_step in list(point_dframe.index):
            point_dframe_by_time = point_dframe.loc[time_step.strftime(time_format)]
        else:
            point_dframe_by_time = None
    else:
        log_stream.warning(' ===> Soil slips dataframe is defined by NoneType')
        point_dframe_by_time, point_dframe_time_from, point_dframe_time_to = None, None, None

    return point_dframe_by_time, point_dframe_time_from, point_dframe_time_to
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to set time range
def set_point_time_range(point_dframe, time_columns='time', time_frequency='D', time_format='%Y-%m-%d'):

    time_db = pd.DatetimeIndex(point_dframe[time_columns].values).unique().sort_values()

    time_start = time_db[0] - offsets.YearBegin()
    time_end = time_db[-1] + offsets.YearEnd()

    time_range = pd.date_range(start=time_start, end=time_end, freq=time_frequency)
    time_range = pd.DatetimeIndex(time_range.format(formatter=lambda x: x.strftime(time_format)))

    return time_range
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to find point value
def find_point_value(category, value):
    if category in list(value.keys()):
        val = value[category]
    else:
        val = np.nan
    return val
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to find point category
def find_point_category(value, category):
    for cat_key, cat_limits in category.items():
        cat_min = cat_limits[0]
        cat_max = cat_limits[1]
        if (cat_min is not None) and (cat_max is not None):
            if (value >= cat_min) and (value <= cat_max):
                break
        elif cat_min and cat_max is None:
            break
    return cat_key
# -------------------------------------------------------------------------------------
