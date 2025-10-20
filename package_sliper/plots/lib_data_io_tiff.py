"""
Library Features:

Name:          lib_data_io_tiff
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import rasterio
import numpy as np
import xarray as xr

from copy import deepcopy
from rasterio.crs import CRS
from rasterio.transform import Affine
from osgeo import gdal, gdalconst

from lib_utils_generic import create_darray
from lib_info_args import logger_name
from lib_info_args import proj_epsg as proj_default_epsg, proj_wkt as proj_default_wkt

# logging
logging.getLogger('rasterio').setLevel(logging.WARNING)
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to read tiff file
def read_file_tiff(file_name, output_format='data_array', output_dtype='float32',
                   var_limit_min=None, var_limit_max=None, var_proj='EPSG:4326'):

    try:
        dset = rasterio.open(file_name)
        bounds = dset.bounds
        res = dset.res
        transform = dset.transform
        data = dset.read()

        # manage crs import
        crs = None
        try:
            if dset.crs is None:
                crs = CRS.from_string(var_proj)
            else:
                crs = dset.crs
        except BaseException as base_exp:
            log_stream.warning(' ===> CRS not set due to unknown reason "' + str(base_exp) + '"')
            log_stream.warning(' ===> GDAL_DATA folder must be defined from the reference conda environment')

        epsg_code = crs.to_epsg()

        if epsg_code is not None:
            epsg_string = f"EPSG:{epsg_code}"
        else:
            epsg_string = var_proj

        if output_dtype == 'float32':
            values = np.float32(data[0, :, :])
        else:
            log_stream.error(' ===> Data type is not allowed.')
            raise NotImplementedError('Case not implemented yet')

        if var_limit_min is not None:
            var_limit_min = np.float32(var_limit_min)
            values[values < var_limit_min] = np.nan
        if var_limit_max is not None:
            var_limit_max = np.float32(var_limit_max)
            values[values > var_limit_max] = np.nan

        decimal_round_geo = 7

        center_right = bounds.right - (res[0] / 2)
        center_left = bounds.left + (res[0] / 2)
        center_top = bounds.top - (res[1] / 2)
        center_bottom = bounds.bottom + (res[1] / 2)

        if center_bottom > center_top:
            log_stream.warning(' ===> Coords "center_bottom": ' + str(center_bottom) + ' is greater than "center_top": '
                               + str(center_top) + '. Try to inverse the bottom and top coords. ')
            center_tmp = center_top
            center_top = center_bottom
            center_bottom = center_tmp

            values = np.flipud(values)

        # rows (height) and cols (width) of the data
        height, width = values.shape
        rows, cols = values.shape

        lon = np.linspace(center_left, center_right, width)
        lat = np.flip(np.linspace(center_bottom, center_top, height))
        lons, lats = np.meshgrid(lon, lat)

        lat_upper = lats[0, 0]
        lat_lower = lats[-1, 0]
        if lat_lower > lat_upper:
            lats = np.flipud(lats)
            values = np.flipud(values)

        min_lon_round = round(np.min(lons), decimal_round_geo)
        max_lon_round = round(np.max(lons), decimal_round_geo)
        min_lat_round = round(np.min(lats), decimal_round_geo)
        max_lat_round = round(np.max(lats), decimal_round_geo)

        center_right_round = round(center_right, decimal_round_geo)
        center_left_round = round(center_left, decimal_round_geo)
        center_bottom_round = round(center_bottom, decimal_round_geo)
        center_top_round = round(center_top, decimal_round_geo)

        assert min_lon_round == center_left_round
        assert max_lon_round == center_right_round
        assert min_lat_round == center_bottom_round
        assert max_lat_round == center_top_round

        data_attrs = {'transform': transform, 'crs': crs, 'epsg': epsg_string,
                      'height': height, 'width': width,
                      'bbox': [bounds.left, bounds.bottom, bounds.right, bounds.top],
                      'bb_left': bounds.left, 'bb_right': bounds.right,
                      'bb_top': bounds.top, 'bb_bottom': bounds.bottom,
                      'res_lon': res[0], 'res_lat': res[1]}

        """ debugging plots
        plt.figure()
        plt.imshow(values)
        plt.colorbar()

        plt.figure()
        plt.imshow(lons)
        plt.colorbar()

        plt.figure()
        plt.imshow(lats)
        plt.colorbar()

        plt.show(block=False)
        """

        if output_format == 'data_array':

            data_obj = create_darray(values, lons[0, :], lats[:, 0],
                                     coord_name_x='longitude', coord_name_y='latitude',
                                     dim_name_x='longitude', dim_name_y='latitude')

            data_obj.attrs = data_attrs

        elif output_format == 'dictionary':

            data_var = {'values': values, 'longitude': lons[0, :], 'latitude': lats[:, 0]}
            data_obj = {**data_var, **data_attrs}

        else:
            log_stream.error(' ===> File static "' + file_name + '" output format not allowed')
            raise NotImplementedError('Case not implemented yet')

    except IOError as io_error:

        data_obj = None
        log_stream.warning(' ===> File static in ascii grid was not correctly open with error "' + str(io_error) + '"')
        log_stream.warning(' ===> Filename "' + os.path.split(file_name)[1] + '"')

    return data_obj
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to define file tiff metadata
def organize_file_tiff_OLD(file_data, file_geo_x, file_geo_y, file_geo_transform=None, file_geo_epsg=None):

    file_height, file_width = file_data.shape

    file_geo_x_west = np.min(np.min(file_geo_x))
    file_geo_x_east = np.max(np.max(file_geo_x))

    file_geo_y_south = np.min(np.min(file_geo_y))
    file_geo_y_north = np.max(np.max(file_geo_y))

    if file_geo_transform is None:
        # TO DO: fix the 1/2 pixel of resolution in x and y ... using resolution/2
        file_geo_transform = rasterio.transform.from_bounds(
            file_geo_x_west, file_geo_y_south, file_geo_x_east, file_geo_y_north,
            file_width, file_height)

    if file_geo_epsg is None:
        file_geo_epsg = deepcopy(proj_default_epsg)

    if not isinstance(file_geo_epsg, str):
        file_geo_epsg = file_geo_epsg.to_string()

    return file_height, file_width, file_geo_transform, file_geo_epsg
# ----------------------------------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to save grid data in geotiff format
def save_file_tiff_OLD(file_name, file_data, file_geo_x, file_geo_y, file_metadata=None, file_epsg_code=None):

    if file_metadata is None:
        file_metadata = {'description': 'data'}

    # Debug
    # plt.figure()
    # plt.imshow(file_data)
    # plt.colorbar()
    # plt.show()

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


# ----------------------------------------------------------------------------------------------------------------------
# method to write file tiff
def write_file_tiff_OLD(file_name, file_data, file_wide, file_high, file_geotrans, file_proj,
                    file_metadata=None, file_format=gdalconst.GDT_Float32):

    if not isinstance(file_data, list):
        file_data = [file_data]

    if file_metadata is None:
        file_metadata = {'description_field': 'data'}
    if not isinstance(file_metadata, list):
        file_metadata = [file_metadata] * file_data.__len__()

    if isinstance(file_geotrans, Affine):
        file_geotrans = file_geotrans.to_gdal()

    file_n = file_data.__len__()
    dset_handle = gdal.GetDriverByName('GTiff').Create(file_name, file_wide, file_high, file_n, file_format,
                                                       options=['COMPRESS=DEFLATE'])
    dset_handle.SetGeoTransform(file_geotrans)
    dset_handle.SetProjection(file_proj)

    for file_id, (file_obj_step, file_metadata_step) in enumerate(zip(file_data, file_metadata)):

        if isinstance(file_obj_step, xr.DataArray):
            file_data_step = file_obj_step.values
        elif isinstance(file_obj_step, np.ndarray):
            file_data_step = deepcopy(file_obj_step)
        else:
            log_stream.error(' ===> Data type is not allowed. Only xr.DataArray and np.ndarray are supported.')
            raise NotImplementedError('Case not implemented yet')

        dset_handle.GetRasterBand(file_id + 1).WriteArray(file_data_step)
        dset_handle.GetRasterBand(file_id + 1).SetMetadata(file_metadata_step)
    del dset_handle
# ----------------------------------------------------------------------------------------------------------------------
