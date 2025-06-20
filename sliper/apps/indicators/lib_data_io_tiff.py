"""
Library Features:

Name:          lib_data_io_tiff
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import rasterio
import gdal
import osr
import numpy as np

from copy import deepcopy
from rasterio.transform import Affine
from osgeo import ogr, gdal, gdalconst

from lib_info_args import proj_wkt as proj_default_wkt
from lib_info_args import logger_name_predictors as logger_name
from lib_info_args import proj_epsg as proj_default_epsg

# Logging
logging.getLogger('rasterio').setLevel(logging.WARNING)
log_stream = logging.getLogger(logger_name)

# Debug
# import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# method to define file tiff metadata
def organize_file_tiff(file_data, file_geo_x, file_geo_y, file_geo_transform=None, file_geo_epsg=None):

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
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to write file tiff
def write_file_tiff(file_name, file_data, file_wide, file_high, file_geotrans, file_proj,
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

    for file_id, (file_data_step, file_metadata_step) in enumerate(zip(file_data, file_metadata)):
        dset_handle.GetRasterBand(file_id + 1).WriteArray(file_data_step)
        dset_handle.GetRasterBand(file_id + 1).SetMetadata(file_metadata_step)
    del dset_handle
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read file tiff
def read_file_tiff(file_name):

    file_handle = rasterio.open(file_name)
    file_proj = file_handle.crs.wkt
    file_geotrans = file_handle.transform

    file_tags = file_handle.tags()
    file_bands = file_handle.count
    file_metadata = file_handle.profile

    if file_bands == 1:
        file_data = file_handle.read(1)
    elif file_bands > 1:
        file_data = []
        for band_id in range(0, file_bands):
            file_data_tmp = file_handle.read(band_id + 1)
            file_data.append(file_data_tmp)
    else:
        log_stream.error(' ===> File multi-band are not supported')
        raise NotImplementedError('File multi-band not implemented yet')

    return file_data, file_proj, file_geotrans
# -------------------------------------------------------------------------------------
