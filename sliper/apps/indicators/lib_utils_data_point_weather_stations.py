"""
Library Features:

Name:          lib_utils_data_point_weather_stations
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

from scipy.spatial import cKDTree
from shapely.geometry import Point

from lib_data_io_csv_weather_stations import read_file_point

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Read point weather stations
def read_point_file(file_name, file_sep=';', index_name='index'):

    # Read points original file
    if os.path.exists(file_name):
        point_dframe = read_file_point(file_name, file_sep=file_sep)
    else:
        log_stream.error(' ===> Weather stations database file "' + file_name + '" is not available')
        raise IOError('File not found!')

    # Adjust points dataframe
    point_dframe = point_dframe.reset_index()
    point_dframe = point_dframe.drop(columns=[index_name])
    point_dframe.index.name = index_name

    return point_dframe
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to save points to csv
def save_point_dframe2csv(file_path, point_dframe):
    point_dframe.to_csv(file_path)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read point from csv
def read_point_csv2dframe(file_name):
    # Read points modified file
    if os.path.exists(file_name):
        point_dframe = pd.read_csv(file_name)
    else:
        log_stream.error(' ===> Weather stations points file "' + file_name + '" is not available')
        raise IOError('File not found!')
    return point_dframe
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to join point and its grid
def join_point_and_grid(point_dframe, polygons_collections,
                        max_distance=0.01,
                        point_code_tag='code', point_area_tag='alert_area',
                        point_longitude_tag='longitude', point_latitude_tag='latitude'):

    point_x = point_dframe[point_longitude_tag].values
    point_y = point_dframe[point_latitude_tag].values

    point_domain_array = np.array([None] * point_dframe.shape[0], dtype=object)
    point_polygon = {}
    for polygon_key, polygon_fields in polygons_collections.items():

        polygon_geometry = list(polygon_fields['geometry'].values)[0]

        for i, (x, y) in enumerate(zip(point_x, point_y)):
            var_point = Point(x, y)
            if polygon_geometry.contains(var_point):
                point_domain_array[i] = polygon_key

        if polygon_key not in list(point_polygon.keys()):
            point_polygon[polygon_key] = polygon_geometry

    point_domain_list = point_domain_array.tolist()
    for point_id, point_aa in enumerate(point_domain_list):
        if point_aa is None:

            code = point_dframe[point_code_tag].values[point_id]
            x = point_dframe[point_longitude_tag].values[point_id]
            y = point_dframe[point_latitude_tag].values[point_id]

            log_stream.warning(' ===> Reference area for point "' + code +
                               '" is not defined. Try using a polygon build around the point')
            var_polygon = Point(x, y).buffer(max_distance)

            for var_name, file_polygon in point_polygon.items():
                if file_polygon.intersects(var_polygon):
                    point_domain_array[point_id] = var_name

            if point_domain_array[point_id] is None:
                log_stream.warning(' ===> Reference area for point "' + code + '" is undefined. Use default assignment')
            else:
                log_stream.warning(' ===> Reference area for point "' + code + '" is correctly defined')

    point_domain_list = point_domain_array.tolist()
    point_dframe[point_area_tag] = point_domain_list

    # DEFAULT STATIC CONDITION TO FIX POINTS OUTSIDE THE DOMAINS (IF NEEDED AFTER POINT AND POLYGONS APPROACHES)
    if point_dframe.loc[point_dframe[point_code_tag] == 'ALTOM', point_area_tag].values[0] is None:
        point_dframe.loc[point_dframe[point_code_tag] == 'ALTOM', point_area_tag] = "alert_area_a"
    if point_dframe.loc[point_dframe[point_code_tag] == 'CASON', point_area_tag].values[0] is None:
        point_dframe.loc[point_dframe[point_code_tag] == 'CASON', point_area_tag] = "alert_area_c"

    return point_dframe
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to join point and its neighbours
def join_point_and_neighbours(point_dframe,
                              max_distance=0.1, inf_distance=float('Inf'),
                              point_code_tag='code', point_name_tag='name', point_area_tag='alert_area',
                              point_longitude_tag='longitude', point_latitude_tag='latitude'):

    code_points = point_dframe[point_code_tag].values
    name_points = point_dframe[point_name_tag].values
    lats_points = point_dframe[point_latitude_tag].values
    lons_points = point_dframe[point_longitude_tag].values
    aa_points = point_dframe[point_area_tag].values

    coord_points = np.dstack([lats_points.ravel(), lons_points.ravel()])[0]
    coord_tree = cKDTree(coord_points)

    points_collections = {}
    for code_point, aa_point, coord_point in zip(code_points, aa_points, coord_points):

        distances, indices = coord_tree.query(
            coord_point, len(coord_points), p=2, distance_upper_bound=max_distance)

        code_points_neighbors = []
        name_points_neighbors = []
        coord_points_neighbors = []
        lats_points_neighbors = []
        lons_points_neighbors = []
        aa_points_neighbors = []
        for index, distance in zip(indices, distances):
            if distance == inf_distance:
                break
            coord_points_neighbors.append(coord_points[index])
            code_points_neighbors.append(code_points[index])
            name_points_neighbors.append(name_points[index])
            lons_points_neighbors.append(lons_points[index])
            lats_points_neighbors.append(lats_points[index])
            aa_points_neighbors.append(aa_points[index])

        coord_dict = {
            point_code_tag: code_points_neighbors, point_name_tag: name_points_neighbors,
            point_latitude_tag: lats_points_neighbors, point_longitude_tag: lons_points_neighbors,
            point_area_tag: aa_points_neighbors
        }
        coord_dframe = pd.DataFrame(data=coord_dict)

        if aa_point not in list(points_collections.keys()):
            points_collections[aa_point] = {}
        points_collections[aa_point][code_point] = coord_dframe

    return points_collections
# -------------------------------------------------------------------------------------
