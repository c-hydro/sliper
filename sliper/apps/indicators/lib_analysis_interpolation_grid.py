"""
Library Features:

Name:          lib_analysis_interpolation_grid
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20200515'
Version:       '1.0.0'
"""

#######################################################################################
# Library
import logging
import numpy as np

from pyresample.geometry import GridDefinition
from pyresample.kd_tree import resample_nearest

from scipy.interpolate import griddata

from lib_data_io_tiff_OLD import organize_file_tiff, write_file_tiff
from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to interpolate grid index to a reference dataset (change interpolation method in the grid2map function)
def interp_grid2index(lons_in, lats_in, lons_out, lats_out, nodata=-9999, interp_method='nearest'):

    if lons_in.shape.__len__() == 1 and lats_in.shape.__len__() == 1:
        shape_in = lons_in.shape[0] * lats_in.shape[0]
        lons_in_2d, lats_in_2d = np.meshgrid(lons_in, lats_in)
    elif lons_in.shape.__len__() == 2 and lats_in.shape.__len__() == 2:
        shape_in = lons_in.shape[0] * lats_in.shape[1]
        lons_in_2d = lons_in
        lats_in_2d = lats_in
    else:
        log_stream.error(' ===> Geographical datasets input dimensions in bad format')
        raise IOError('Geographical data format not allowed')

    if lons_out.shape.__len__() == 1 and lats_out.shape.__len__() == 1:
        lons_out_2d, lats_out_2d = np.meshgrid(lons_out, lats_out)
    elif lons_out.shape.__len__() == 2 and lats_out.shape.__len__() == 2:
        lons_out_2d = lons_out
        lats_out_2d = lats_out
    else:
        log_stream.error(' ===> Geographical datasets output dimensions in bad format')
        raise IOError('Geographical data format not allowed')

    index_in = np.arange(0, shape_in)
    index_out = griddata((lons_in_2d.ravel(), lats_in_2d.ravel()), index_in,
                         (lons_out_2d.ravel(), lats_out_2d.ravel()), method=interp_method, fill_value=nodata)
    ''' debug
    index_tmp = np.reshape(index_out, [lons_out_2d.shape[0], lats_out_2d.shape[1]])
    plt.figure()
    plt.imshow(index_tmp)
    plt.colorbar()
    plt.show()
    '''

    return index_out

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to interpolate grid data to a reference dataset (version with pyresample)
def interp_grid2map(lons_in, lats_in, values_in, lons_out, lats_out, interp_method='nearest',
                    interpolating_max_distance=10000, interpolating_fill_value=0):

    geo_grid_in = GridDefinition(lons=lons_in, lats=lats_in)
    geo_grid_ref = GridDefinition(lons=lons_out, lats=lats_out)

    if interp_method == 'nearest':
        values_out = resample_nearest(
            geo_grid_in, values_in, geo_grid_ref,
            radius_of_influence=interpolating_max_distance,
            fill_value=interpolating_fill_value)
    else:
        log_stream.error(' ===> Interpolation method is not allowed')
        raise NotImplementedError('Method not implemented yet')

    ''' start debug
    file_height_in, file_width_in, file_geo_transform_in, file_geo_epsg_in = organize_file_tiff(values_in, lons_in, lats_in)
    write_file_tiff('file_data_in.tif', values_in, file_width_in, file_height_in, file_geo_transform_in, file_geo_epsg_in)

    file_height_out, file_width_out, file_geo_transform_out, file_geo_epsg_out = organize_file_tiff(values_out, lons_out, lats_out)
    write_file_tiff('file_data_out.tif', values_out, file_width_out, file_height_out, file_geo_transform_out, file_geo_epsg_out)

    plt.figure()
    plt.imshow(values_in)
    plt.colorbar()
    plt.clim(0, 1)
    plt.figure()
    plt.imshow(values_out)
    plt.colorbar()
    plt.clim(0, 1)
    plt.show()
    '''

    return values_out
# -------------------------------------------------------------------------------------
