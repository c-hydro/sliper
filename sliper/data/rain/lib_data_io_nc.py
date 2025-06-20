"""
Library Features:

Name:          lib_data_io_nc
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import xarray as xr

from lib_utils_generic import create_dset, write_dset

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to read mat obj
def read_file_nc(file_name, var_name='SM', geo_x_name='Longitude', geo_y_name='Latitude'):

    dset_file = xr.open_dataset(file_name)

    var_data = dset_file[var_name].values
    geo_x = dset_file[geo_x_name].values
    geo_y = dset_file[geo_y_name].values

    geo_y_upper, geo_y_lower = geo_y[0, 0], geo_y[-1, 0]
    if geo_y_lower > geo_y_upper:
        geo_y = np.flipud(geo_y)
        var_data = np.flipud(var_data)

    return var_data, geo_x, geo_y
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to save file netcdf
def save_file_nc(file_name, file_data, file_time, ref_data, ref_geo_x_2d, ref_geo_y_2d,
                 var_name='rain'):

    # crete dataset
    file_dset = create_dset(
        file_data,
        ref_data, ref_geo_x_2d, ref_geo_y_2d,
        var_data_time=file_time,
        var_data_name=var_name,
        var_geo_name='mask', var_data_attrs=None, var_geo_attrs=None,
        coord_name_x='west_east', coord_name_y='south_north', coord_name_time='time',
        dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
        dims_order_2d=None, dims_order_3d=None)

    # write dataset to file
    write_dset(
        file_name,
        file_dset, dset_mode='w', dset_engine='netcdf4', dset_compression=0,
        dset_format='NETCDF4', dim_key_time='time', no_data=-9999.0)
# -------------------------------------------------------------------------------------

