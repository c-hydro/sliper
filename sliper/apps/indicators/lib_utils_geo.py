"""
Library Features:

Name:          lib_utils_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220420'
Version:       '1.5.0'
"""
#######################################################################################
# Libraries
import logging
import numpy as np
import pyproj
import os

import rasterio

from rasterio.crs import CRS

from lib_data_io_tiff import write_file_tiff

from lib_info_args import proj_wkt as proj_default_wkt

from scipy.interpolate import griddata
from copy import deepcopy
from functools import partial
from shapely.ops import transform
from shapely.geometry import Point
from shapely.vectorized import contains

from lib_utils_io_obj import create_darray_2d
from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to save data values in geotiff format
def save_data_tiff(file_name, file_data, file_geo_x, file_geo_y, file_metadata=None, file_epsg_code=None):

    if file_metadata is None:
        file_metadata = {'description': 'data'}

    file_data_height, file_data_width = file_data.shape

    file_geo_x_west = np.min(file_geo_x)
    file_geo_x_east = np.max(file_geo_x)
    file_geo_y_south = np.min(file_geo_y)
    file_geo_y_north = np.max(file_geo_y)

    file_data_transform = rasterio.transform.from_bounds(
        file_geo_x_west, file_geo_y_south, file_geo_x_east, file_geo_y_north,
        file_data_width, file_data_height)

    if not isinstance(file_data, list):
        file_data = [file_data]

    file_wkt = deepcopy(proj_default_wkt)
    try:
        if isinstance(file_epsg_code, str):
            file_crs = CRS.from_string(file_epsg_code)
            file_wkt = file_crs.to_wkt()
        elif (file_epsg_code is None) or (not isinstance(file_epsg_code, str)):
            log_stream.warning(' ===> Geographical projection is not defined in string format. '
                               ' Will be used the Default projection EPSG:4326')
            file_crs = CRS.from_string('EPSG:4326')
            file_wkt = file_crs.to_wkt()
    except BaseException as b_exp:
        log_stream.warning(' ===> Issue in defining geographical projection. Particularly ' + str(b_exp) +
                           ' error was fuond. A default wkt definition will be used')

    write_file_tiff(
        file_name, file_data, file_data_width, file_data_height, file_data_transform, file_wkt,
        file_metadata=file_metadata)

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read tiff file
def get_data_tiff(file_name, file_mandatory=True):

    if os.path.exists(file_name):
        if file_name.endswith('tif') or file_name.endswith('.tiff'):

            dset = rasterio.open(file_name)
            bounds = dset.bounds
            res = dset.res
            transform = dset.transform
            data = dset.read()
            values = data[0, :, :]
            if dset.crs is None:
                proj = proj_default_wkt
            else:
                proj = dset.crs.wkt
            geotrans = dset.transform

            decimal_round = 7

            dims = values.shape
            high = dims[0]
            wide = dims[1]

            center_right = bounds.right - (res[0] / 2)
            center_left = bounds.left + (res[0] / 2)
            center_top = bounds.top - (res[1] / 2)
            center_bottom = bounds.bottom + (res[1] / 2)

            if center_bottom > center_top:
                center_bottom_tmp = center_top
                center_top_tmp = center_bottom
                center_bottom = center_bottom_tmp
                center_top = center_top_tmp

                values = np.flipud(values)

            lon = np.arange(center_left, center_right + np.abs(res[0] / 2), np.abs(res[0]), float)
            lat = np.arange(center_bottom, center_top + np.abs(res[0] / 2), np.abs(res[1]), float)
            lons, lats = np.meshgrid(lon, lat)

            min_lon_round = round(np.min(lons), decimal_round)
            max_lon_round = round(np.max(lons), decimal_round)
            min_lat_round = round(np.min(lats), decimal_round)
            max_lat_round = round(np.max(lats), decimal_round)

            center_right_round = round(center_right, decimal_round)
            center_left_round = round(center_left, decimal_round)
            center_bottom_round = round(center_bottom, decimal_round)
            center_top_round = round(center_top, decimal_round)

            assert min_lon_round == center_left_round
            assert max_lon_round == center_right_round
            assert min_lat_round == center_bottom_round
            assert max_lat_round == center_top_round

            lats = np.flipud(lats)

            da_frame = create_darray_2d(values, lons, lats, coord_name_x='west_east', coord_name_y='south_north',
                                        dim_name_x='west_east', dim_name_y='south_north')

        else:
            log_stream.error(' ===> File ' + file_name + ' format unknown')
            raise NotImplementedError('File type reader not implemented yet')
    else:
        if file_mandatory:
            log_stream.error(' ===> File ' + file_name + ' not found')
            raise IOError('File location or name is wrong')
        else:
            log_stream.warning(' ===> File ' + file_name + ' not found')
            da_frame, proj, geotrans = None, None, None

    return da_frame, proj, geotrans
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create grid information
def create_grid(
        xll_corner=None, yll_corner=None, rows=None, cols=None, cell_size=None,
        tag_geo_values='data', grid_format='data_array',
        tag_geo_x='geo_x', tag_geo_y='geo_y',
        tag_nodata='nodata_value', value_no_data=-9999.0, value_default_data=1):

    geo_x_start = xll_corner + cell_size / 2
    geo_x_end = xll_corner + cell_size / 2 + cell_size * (cols - 1)
    geo_x_values = np.linspace(geo_x_start, geo_x_end, cols)

    geo_y_start = yll_corner + cell_size / 2
    geo_y_end = yll_corner + cell_size / 2 + cell_size * (rows - 1)
    geo_y_values = np.linspace(geo_y_start, geo_y_end, rows)

    geo_x_values_2d, geo_y_values_2d = np.meshgrid(geo_x_values, geo_y_values)

    geo_y_right = geo_x_values_2d[0, 0]
    geo_y_left = geo_x_values_2d[0, -1]
    geo_y_upper = geo_y_values_2d[0, 0]
    geo_y_lower = geo_y_values_2d[-1, 0]
    if geo_y_lower > geo_y_upper:
        geo_y_values_2d = np.flipud(geo_y_values_2d)

    geo_data_values = np.zeros([geo_y_values.shape[0], geo_x_values.shape[0]])
    geo_data_values[:, :] = value_default_data

    if grid_format == 'dictionary':

        data_grid = {tag_geo_values: geo_data_values, tag_geo_x: geo_x_values_2d[0, :],
                     tag_geo_y: geo_y_values_2d[:, 0]}

        if tag_nodata not in list(data_grid.keys()):
            data_grid[tag_nodata] = value_no_data

    elif grid_format == 'data_array':

        data_attrs = {
            'xll_corner': xll_corner, 'yll_corner': yll_corner,
            'rows': rows, 'cols': cols,
            'cell_size': cell_size, tag_nodata: value_no_data}

        data_grid = create_darray_2d(
            geo_data_values, geo_x_values_2d[0, :], geo_y_values_2d[:, 0],
            coord_name_x='west_east', coord_name_y='south_north',
            dim_name_x='west_east', dim_name_y='south_north')

        data_grid.attrs = data_attrs
    else:
        log_stream.error(' ===> Grid format "' + grid_format + '" is not expected')
        raise NotImplementedError('Only "dictionary" and "data_array" formats are available.')

    return data_grid
# -------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------
# Method to convert curve number to s (vmax)
def convert_cn2s(data_cn, data_terrain):

    data_s = (1000.0 / data_cn - 10) * 25.4
    data_s[data_cn <= 0] = np.nan
    data_s[data_cn > 100] = np.nan

    data_s[(data_terrain >= 0) & (data_s < 1.0)] = 1.0

    data_s[data_s < 0] = 0.0

    data_s[data_terrain < 0] = np.nan

    data_s[0, :] = np.nan
    data_s[-1, :] = np.nan
    data_s[:, 0] = np.nan
    data_s[:, -1] = np.nan

    # Debug
    # plt.figure()
    # plt.imshow(data_s)
    # plt.colorbar()
    # plt.show()

    return data_s
# ------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to join circle and its grid
def find_points_with_buffer(points_values, points_x_values_2d, points_y_values_2d,
                            mask_values, mask_x_values_2d, mask_y_values_2d,
                            point_buffer=10, tag_point_name='point_{:}'):
    points_collections = {}
    points_summary = np.zeros(shape=(mask_values.shape[0], mask_values.shape[1]))
    points_summary[:, :] = np.nan
    for point_id, (point_mask, point_x, point_y) in enumerate(
            zip(points_values.ravel(), points_x_values_2d.ravel(), points_y_values_2d.ravel())):

        points_map = np.ones(shape=(mask_values.shape[0], mask_values.shape[1]))
        if point_mask == 1:
            mask_polygon, mask_coords = compute_geodesic_point_buffer(point_x, point_y, point_buffer)
            points_mask = contains(mask_polygon, mask_x_values_2d, mask_y_values_2d)
            points_masked = np.ma.masked_array(points_map, points_mask)

            points_data = points_masked.data
            points_data[~points_mask] = np.nan
            points_data[mask_values == 0] = np.nan

            points_idxs = np.nonzero(points_data == 1)

            points_collections[tag_point_name.format(point_id)] = points_idxs
            points_summary[points_idxs] = point_id

    # plt.figure()
    # plt.imshow(mask_values)
    # plt.colorbar()
    # plt.figure()
    # plt.imshow(points_summary)
    # plt.colorbar()
    # plt.show()

    return points_collections

# -------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------
# Method to compute grid from bounds
def compute_grid_from_bounds(geo_mask_2d, geo_x_2d, geo_y_2d, km=10):

    # Get geo boundary
    geo_x_min, geo_x_max = np.min(geo_x_2d), np.max(geo_x_2d)
    geo_y_min, geo_y_max = np.min(geo_y_2d), np.max(geo_y_2d)

    # Set up transformers, EPSG:3857 is metric, same as EPSG:900913
    to_proxy_transformer = pyproj.Transformer.from_crs('epsg:4326', 'epsg:3857')
    to_original_transformer = pyproj.Transformer.from_crs('epsg:3857', 'epsg:4326')

    # Create corners of rectangle to be transformed to a grid
    geo_sw = Point((geo_x_min, geo_y_min))
    geo_ne = Point((geo_x_max, geo_y_max))
    # Define step size
    step_size = km * 1000  # km grid step size

    # Project corners to target projection
    transformed_sw = to_proxy_transformer.transform(geo_sw.x, geo_sw.y)  # Transform NW point to 3857
    transformed_ne = to_proxy_transformer.transform(geo_ne.x, geo_ne.y)  # .. same for SE

    # Iterate over 2D area
    collection_point_obj, collection_points_x, collection_points_y = [], [], []
    x = transformed_sw[0]
    while x < transformed_ne[0]:
        y = transformed_sw[1]
        while y < transformed_ne[1]:
            p = Point(to_original_transformer.transform(x, y))
            collection_point_obj.append(p)
            collection_points_x.append(p.x)
            collection_points_y.append(p.y)
            y += step_size
        x += step_size

    point_x_1d = np.unique(np.array(collection_points_x).astype(float))
    point_y_1d = np.unique(np.array(collection_points_y).astype(float))

    point_x_2d, point_y_2d = np.meshgrid(point_x_1d, point_y_1d)
    point_y_upper, point_y_lower = point_y_2d[0, 0], point_y_2d[-1, 0]
    if point_y_lower > point_y_upper:
        point_y_2d = np.flipud(point_y_2d)

    point_mask_2d = griddata((geo_x_2d.ravel(), geo_y_2d.ravel()), geo_mask_2d.ravel(),
                             (point_x_2d, point_y_2d), method='nearest', fill_value=-9999.0)

    return point_mask_2d, point_x_2d, point_y_2d

# ------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------
# Method to define a buffer around a geographical point
def compute_geodesic_point_buffer(lon, lat, km=10):

    # Azimuthal equidistant projection
    local_azimuthal_projection = '+proj=aeqd +R=6371000 +units=m +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'

    wgs84_to_aeqd = partial(
        pyproj.transform,
        pyproj.Proj('+proj=longlat +datum=WGS84 +no_defs'),
        pyproj.Proj(local_azimuthal_projection.format(lat=lat, lon=lon)),
    )

    aeqd_to_wgs84 = partial(
        pyproj.transform,
        pyproj.Proj(local_azimuthal_projection.format(lat=lat, lon=lon)),
        pyproj.Proj('+proj=longlat +datum=WGS84 +no_defs'),
    )

    point_obj = Point(lon, lat)

    point_aeqd = transform(wgs84_to_aeqd, point_obj)
    buffer = point_aeqd.buffer(km * 1000)  # distance in metres
    points_wgs84 = transform(aeqd_to_wgs84, buffer)

    point_polygon = deepcopy(points_wgs84)
    points_coords = points_wgs84.exterior.coords[:]

    return point_polygon, points_coords
# ------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------
# Method to convert decimal degrees to km (2)
def degree_2_km(deg):
    earth_radius = 6378.1370
    km = deg * (np.pi * earth_radius) / 180
    return km
# ------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------
# Method to convert km to decimal degrees
def km_2_degree(km):
    earth_radius = 6378.1370
    deg = 180 * km / (np.pi * earth_radius)
    return deg
# ------------------------------------------------------------------------------------
