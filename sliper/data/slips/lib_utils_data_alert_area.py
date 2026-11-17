"""
Library Features:

Name:          lib_utils_data_alert_area
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
import pandas as pd
import geopandas as gpd

from copy import deepcopy
from shapely.geometry import Point

import numpy as np

from lib_utils_data_geo import normalize_crs
from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to filter points object
def filter_points_object(df, fields: dict):

    # clean the field mapping
    df_columns = list(df.columns)
    valid_fields = {}
    for key, val in fields.items():
        if key in df_columns:
            if val is not None:
                # If the value is not None, use it as the new column name
                valid_fields[key] = val

    # subset the dataframe to only include valid columns
    filtered_df = df[list(valid_fields.keys())]

    # check if the DataFrame has a time index or column
    filtered_time, index_time = None, False
    if filtered_df.index.name == 'time':
        filtered_time = filtered_df.index
        index_time = True
    else:
        # If 'time' is a column, extract it
        if 'time' in filtered_df.columns:
            filtered_df = filtered_df.reset_index(drop=True)
            filtered_time = filtered_df['time']
        else:
            log_stream.warning(' ===> Time info not found in the DataFrame (columns or index). Skipping time processing.')

    # rename the columns based on the valid fields mapping
    for key, val in valid_fields.items():
        filtered_df = filtered_df.rename(columns={key: val})

    if index_time:
        if 'time' in list(fields.keys()):
            filtered_df.index.name = fields['time']

    return filtered_df, filtered_time
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to translate soil slips object
def translate_points_object(point_dframe,
                            time_column: (str, None) = 'time',
                            time_format_in: (str, None) = "%d/%m/%Y", time_format_out: (str, None) = "%Y-%m-%d",
                            geo_x_column: str = 'longitude', geo_y_column: str = 'latitude'):

    # check and reformat time column if it exists
    if time_column in point_dframe.columns:
        point_dframe[time_column] = pd.to_datetime(point_dframe[time_column],
                                         format=time_format_in, errors='coerce').dt.strftime(time_format_out)

    # convert DataFrame to GeoDataFrame (if coordinates exist)
    geo_dframe = gpd.GeoDataFrame(
        point_dframe, geometry=gpd.points_from_xy(point_dframe[geo_x_column], point_dframe[geo_y_column]))

    # sort by 'time' in ascending order
    geo_dframe = geo_dframe.sort_values(by=time_column)

    # get geometry
    geo_geoms = ((feature['geometry'], 1) for feature in geo_dframe.iterfeatures())
    # extract collections
    geo_collections = list(geo_geoms)

    return geo_dframe, geo_collections, geo_geoms

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to join points and grids
def join_points_and_grid(point_dframe_in, areas_dframe,
                         point_area_tag='ALLERTA',
                         area_code_tag='alert_area_code',
                         point_longitude_tag='longitude',
                         point_latitude_tag='latitude',
                         points_epsg_code=4326,
                         flag_print_na=False) -> gpd.GeoDataFrame:

    log_stream.info(' ------> Assign soil slips to alert areas ... ')

    points_dframe_tmp = deepcopy(point_dframe_in)

    if area_code_tag not in areas_dframe.columns:
        log_stream.error(
            f' ===> Tag "{area_code_tag}" is mandatory and not included '
            f'in the alert areas dataframe'
        )
        raise RuntimeError('Check your alert areas dataframe for the requested column')

    if point_area_tag not in points_dframe_tmp.columns:
        log_stream.warning(
            f' ===> Tag "{point_area_tag}" is not available in the points dataframe. '
            f'Initialize it with "NA".'
        )
        points_dframe_tmp[point_area_tag] = "NA"

    points_epsg_code = normalize_crs(points_epsg_code)

    points_gdf_tmp = gpd.GeoDataFrame(
        points_dframe_tmp,
        geometry=[
            Point(xy) for xy in zip(
                points_dframe_tmp[point_longitude_tag],
                points_dframe_tmp[point_latitude_tag]
            )
        ],
        crs=points_epsg_code
    )

    if areas_dframe.crs != points_gdf_tmp.crs:
        areas_dframe = areas_dframe.to_crs(points_gdf_tmp.crs)

    point_gdf_joined = gpd.sjoin(
        points_gdf_tmp,
        areas_dframe[[area_code_tag, "geometry"]],
        how="left",
        predicate="within"
    )

    area_code_right = f"{area_code_tag}_right"

    if area_code_right in point_gdf_joined.columns:
        point_gdf_joined[point_area_tag] = point_gdf_joined[area_code_right].fillna("NA")
    elif area_code_tag in point_gdf_joined.columns:
        point_gdf_joined[point_area_tag] = point_gdf_joined[area_code_tag].fillna("NA")
    else:
        point_gdf_joined[point_area_tag] = "NA"

    point_gdf_joined = point_gdf_joined.drop(
        columns=[
            "index_right",
            f"{area_code_tag}_left",
            f"{area_code_tag}_right"
        ],
        errors="ignore"
    )

    # check missing alert areas
    mask_na = (
            point_gdf_joined[point_area_tag].isna() |
            (point_gdf_joined[point_area_tag] == "NA") |
            (point_gdf_joined[point_area_tag].astype(str).str.strip() == "")
    )

    n_na = mask_na.sum()

    if n_na > 0:

        log_stream.warning(
            f' ===> Found {n_na} point(s) not associated with any alert area.'
        )

        # optional detailed logging
        if flag_print_na:

            cols_print = [
                c for c in [
                    "id",
                    point_longitude_tag,
                    point_latitude_tag,
                    point_area_tag
                ]
                if c in point_gdf_joined.columns
            ]

            for _, row in point_gdf_joined.loc[mask_na, cols_print].iterrows():
                log_stream.warning(
                    f' ===> Point ID={row.get("id", "NA")} '
                    f'Lon={row.get(point_longitude_tag, "NA")} '
                    f'Lat={row.get(point_latitude_tag, "NA")} '
                    f'Area={row.get(point_area_tag, "NA")}'
                )

    log_stream.info(' ------> Assign soil slips to alert areas ... DONE')

    return point_gdf_joined
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to count points and thresholds
def count_points_and_thresholds(point_dframe,
                                alert_mapping,
                                alert_thresholds, alert_ids=None, alert_rgb=None,
                                alert_area_code_column='alert_area_code',
                                alert_area_label_column='alert_area_label',
                                alert_area_range_name_column='alert_area_range',
                                alert_area_range_color_column='alert_area_color',
                                alert_area_range_id_column='alert_area_id',
                                alert_area_range_rgb_column='alert_area_rgb',
                                data_column='soil_slips',
                                time_column='time'):
    """
    Adds 'alert_area_label' and 'alert_color' columns to the input DataFrame
    using the provided alert_mapping and alert_thresholds.

    Parameters:
        point_dframe (pd.DataFrame): DataFrame with columns ['data', 'ALLERTA', 'count']
        alert_mapping (dict): e.g. {'alert_area_a': 'A', 'alert_area_b': 'B', ...}
        alert_thresholds (dict): e.g. {'alert_area_a': {'white_range': [0, 0], ...}, ...}
        alert_area_color_column (str): column name of alert area color
        alert_area_code_column (str): column name of alert area code
        alert_area_label_column (str): column name of alert area label
        data_column (str): column name of data to count
        time_column (str): column name of time

    Returns:
        pd.DataFrame: enriched DataFrame
    """

    group_keys_order = [time_column, alert_area_code_column]

    missing = [col for col in group_keys_order if col not in point_dframe.columns]
    if missing:
        log_stream.error(f" ===> Missing required column(s): {', '.join(missing)}")
        raise KeyError(f"Missing required column(s): {', '.join(missing)}")

    # check if time_column exists in the grouped DataFrame
    if time_column not in list(point_dframe.columns):
        log_stream.error(f" ===> Time column '{time_column}' not found in the point DataFrame.")
        raise KeyError(f"Time column '{time_column}' not found in the point DataFrame.")

    # Group by the specified keys and count occurrences
    count_dframe = point_dframe.groupby(group_keys_order).size().reset_index(name=data_column)

    # Reverse the alert mapping to map codes to area labels
    reverse_mapping = {v: k for k, v in alert_mapping.items()}
    count_dframe[alert_area_label_column] = count_dframe[alert_area_code_column].map(reverse_mapping)

    # range classification
    count_dframe[alert_area_range_name_column] = count_dframe.apply(
        lambda row: get_alert_range(row[alert_area_label_column], row[data_column], alert_thresholds), axis=1
    )

    # get range color
    count_dframe[alert_area_range_color_column] = count_dframe.apply(
        lambda row: get_alert_color(row), axis=1
    )

    # get range id
    count_dframe[alert_area_range_id_column] = count_dframe.apply(
        lambda row: get_alert_id(row, lookup=alert_ids), axis=1
    )

    # get range rgb
    count_dframe[alert_area_range_rgb_column] = count_dframe.apply(
        lambda row: get_alert_rgb(row, lookup=alert_rgb), axis=1
    )

    # sort by data
    count_dframe[time_column] = pd.to_datetime(count_dframe[time_column], format='%Y-%m-%d', errors='coerce')
    count_dframe['year'] = count_dframe[time_column].dt.year
    count_dframe['month'] = count_dframe[time_column].dt.month
    count_dframe['day'] = count_dframe[time_column].dt.day

    count_dframe = count_dframe.sort_values(by=['year', 'month', 'day'])

    # split into groups by alert_area_code_column (ALLERTA) and sort each by data
    count_sorted = {
        key: group.sort_values(by=['year', 'month', 'day'])
        for key, group in count_dframe.groupby(alert_area_label_column)
    }



    # re-index each group to ensure consistent ordering
    obj_collections = {}
    for key, group in count_sorted.items():

        # check if time_column exists in the grouped DataFrame
        if time_column not in list(group.columns):
            log_stream.error(f"Time column '{time_column}' not found in the grouped DataFrame.")
            raise KeyError(f"Time column '{time_column}' not found in the grouped DataFrame.")

        # get time column
        time = group[time_column]

        # convert 'time' column to datetime if it's not already
        group[time_column] = pd.to_datetime(time, errors='coerce')  # Handles invalid formats as NaT
        # drop rows where 'time' couldn't be parsed (optional but recommended)
        group = group.dropna(subset=[time_column])
        # reindex the DataFrame using 'time'
        group = group.set_index(time_column)
        # (Optional) Sort by time index
        group = group.sort_index()

        # store the group in the dictionary
        obj_collections[key] = group

    return obj_collections

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get alert rgb from row
def get_alert_rgb(row, tag_data='alert_area_color', tag_geo='alert_area_label', lookup=None):
    color, area = row[tag_data], row[tag_geo]
    data = lookup.get(area, None)  # e.g. "green", "alert_area_a"
    key = f"{color}_rgb"  # e.g. "green_id"

    obj_dict = data.get(key, None)

    color_data = ''
    if obj_dict is not None:

        rgb_data = list(obj_dict.get('rgb', (0, 0, 0)))
        opacity_data = obj_dict.get('opacity', 1.0)

        obj_list = []
        obj_list.extend(rgb_data)
        obj_list.append(opacity_data)

        color_data = ",".join(map(str, obj_list))

    return color_data

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get alert id from row
def get_alert_id(row, tag_data='alert_area_color', tag_geo='alert_area_label', lookup=None):

    color, area = row[tag_data], row[tag_geo]
    ids = lookup.get(area, None)# e.g. "green", "alert_area_a"
    key = f"{color}_id" # e.g. "green_id"

    id = ids.get(key, None)
    return id
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get alert color from row
def get_alert_color(row, tag='alert_area_range'):
    color = row[tag].replace("_range", "")        # e.g. "green"
    return color
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# helper function to classify based on thresholds
def get_alert_range(area_label, count, alert_thresholds):
    thresholds = alert_thresholds.get(area_label, {})
    for color, (min_val, max_val) in thresholds.items():
        if max_val is None and count >= min_val:
            return color
        elif max_val is not None and min_val <= count <= max_val:
            return color
    return 'unknown'
# ----------------------------------------------------------------------------------------------------------------------
