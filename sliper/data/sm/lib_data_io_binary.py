"""
Library Features:

Name:          lib_data_io_binary
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20210408'
Version:       '1.0.0'
"""
#######################################################################################
# Library
import logging
import os
import struct

from copy import deepcopy

import numpy as np
import pandas as pd
import xarray as xr

from lib_info_args import logger_name
from lib_info_args import proj_epsg, proj_wkt

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
#######################################################################################


# --------------------------------------------------------------------------------
# Method to read 2d variable in binary format (saved as 1d integer array)
def read_data_binary(file_name, var_geo_x, var_geo_y, var_geo_data=None, var_geo_attrs=None,
                     var_format='i', var_scale_factor=10000,
                     var_name=None, var_time=None, var_geo_1d=True, var_time_freq='H', var_time_steps_expected=1,
                     coord_name_geo_x='west_east', coord_name_geo_y='south_north', coord_name_time='time',
                     dim_name_geo_x='west_east', dim_name_geo_y='south_north', dim_name_time='time',
                     dims_order=None, debug: bool = False):

    if dims_order is None:
        dims_order = [dim_name_geo_y, dim_name_geo_x, dim_name_time]

    # Initialize attributes
    file_attrs, geo_attrs = {}, {}

    if os.path.exists(file_name):

        # Normalize variable name
        if var_name is None:
            var_name = "data"
        elif isinstance(var_name, list):
            if len(var_name) == 1:
                var_name = var_name[0]
            else:
                log_stream.error('Argument "var_name" must be a string or a list of length 1.')
                raise NotImplementedError("Multiple variable names are not supported.")
        elif not isinstance(var_name, str):
            raise TypeError('Argument "var_name" must be a string, list or None.')

        # Normalize scale factor
        if isinstance(var_scale_factor, list):
            if len(var_scale_factor) == 1:
                var_scale_factor = var_scale_factor[0]
            else:
                log_stream.error('Argument "var_scale_factor" must be a scalar or a list of length 1.')
                raise NotImplementedError("Multiple scale factors are not supported.")

        # Shape values 1d
        rows = var_geo_y.shape[0]
        cols = var_geo_x.shape[0]
        geo_n = rows * cols

        # Open and read binary file
        open_func = gzip.open if file_name.endswith('.gz') else open
        with open_func(file_name, 'rb') as file_handle:
            data_stream = file_handle.read()

        # Decode binary values
        dtype_map = {
            'i': '<i4',
            'f': '<f4',
            'd': '<f8'
        }

        var_data_1d = np.frombuffer(
            data_stream,
            dtype=np.dtype(dtype_map[var_format])
        ).astype(np.float32).copy()

        # Apply scale factor
        var_data_1d /= var_scale_factor

        # Check binary dimensions
        var_n = var_data_1d.size
        if var_n == 0 or var_n % geo_n != 0:
            raise ValueError(
                f'Invalid binary dimensions for {file_name}: '
                f'values={var_n}, grid={geo_n}'
            )

        var_time_steps_cmp = int(var_n / geo_n)
        var_data_3d = np.reshape(var_data_1d, (rows, cols, var_time_steps_cmp), order='F')

        if var_geo_1d:
            var_geo_x_2d, var_geo_y_2d = np.meshgrid(var_geo_x, var_geo_y)
        else:
            var_geo_x_2d = var_geo_x
            var_geo_y_2d = var_geo_y

        geo_y_upper = var_geo_y_2d[0, 0]
        geo_y_lower = var_geo_y_2d[-1, 0]
        if geo_y_lower > geo_y_upper:
            var_geo_y_2d = np.flipud(var_geo_y_2d)

        var_dims = var_data_3d.shape
        var_high = var_dims[0]
        var_wide = var_dims[1]

        if var_geo_attrs is not None:

            nodata_value = var_geo_attrs.get('nodata_value', -9999)
            xll_corner = var_geo_attrs.get('xllcorner', None)
            yll_corner = var_geo_attrs.get('yllcorner', None)
            proj = var_geo_attrs.get('proj', proj_epsg)
            transform = var_geo_attrs.get('transform', proj_wkt)
            cellsize = var_geo_attrs.get('cellsize', None)

            geo_attrs = {'nrows': var_geo_y_2d.shape[0], 'ncols': var_geo_x_2d.shape[1],
                         'nodata_value': nodata_value,
                         'xllcorner': xll_corner,
                         'yllcorner': yll_corner, 'cellsize': abs(cellsize),
                         'proj': proj, 'transform': transform}
        else:
            geo_attrs = {}

        if var_time_steps_cmp == var_time_steps_expected:

            var_data = np.zeros(shape=[var_geo_x_2d.shape[0], var_geo_y_2d.shape[1], var_time_steps_cmp])
            var_data[:, :, :] = np.nan
            for step in np.arange(0, var_time_steps_cmp, 1):
                var_data_step = var_data_3d[:, :, step]

                if var_geo_data is not None:
                    var_data_step[var_geo_data < 0] = np.nan

                if debug:
                    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
                    for ax, (title, data) in zip(
                            axes.flat, [('Volume Total', var_data_step), ('Terrain', var_geo_data),]):
                        im = ax.imshow(data)
                        ax.set_title(title)
                        plt.colorbar(im, ax=ax)

                    plt.tight_layout()
                    plt.show()

                var_data[:, :, step] = var_data_step

        elif (var_time_steps_cmp == 1) and (var_time_steps_cmp < var_time_steps_expected):

            log_stream.warning(' ===> File ' + file_name +
                               ' steps expected [' + str(var_time_steps_expected) +
                               '] and found [' + str(var_time_steps_cmp) + '] are different!')

            var_data = np.zeros(shape=[var_geo_x_2d.shape[0], var_geo_y_2d.shape[1], var_time_steps_expected])
            var_data[:, :, :] = np.nan
            for step in np.arange(0, var_time_steps_expected, 1):
                var_data_step = var_data_3d[:, :, 0]

                if var_geo_data is not None:
                    var_data_step[var_geo_data < 0] = np.nan

                var_data[:, :, step] = var_data_step

            var_time_steps_cmp = deepcopy(var_time_steps_expected)

        else:
            log_stream.error(' ===> File ' + file_name + ' format are not expected!')
            raise NotImplemented('Case not implemented yet')

    else:
        log_stream.warning(' ===> File ' + file_name + ' not available in loaded datasets!')
        var_data = None

    if var_data is not None:

        if var_time is not None:

            if isinstance(var_time, pd.Timestamp):
                var_data_time = pd.DatetimeIndex([var_time])
            elif isinstance(var_time, pd.DatetimeIndex):
                var_data_time = deepcopy(var_time)
            else:
                log_stream.error(' ===> Time format is not allowed. Expected Timestamp or Datetimeindex')
                raise NotImplemented('Case not implemented yet')

            var_dset = xr.Dataset(coords={coord_name_time: ([dim_name_time], var_data_time)})
            var_dset.coords[coord_name_time] = var_dset.coords[coord_name_time].astype('datetime64[ns]')

            var_da = xr.DataArray(var_data, name=var_name, dims=dims_order,
                                  coords={coord_name_time: ([dim_name_time], var_data_time),
                                          coord_name_geo_x: ([dim_name_geo_x], var_geo_x_2d[0, :]),
                                          coord_name_geo_y: ([dim_name_geo_y], var_geo_y_2d[:, 0])})

            # Merge attributes safely
            obj_attrs = {**(file_attrs or {}),**(geo_attrs or {})}
            if obj_attrs:
                var_dset.attrs = obj_attrs

            var_dset[var_name] = var_da

        elif var_time is None:

            var_dset = xr.Dataset()
            var_da = xr.DataArray(var_data, name=var_name, dims=dims_order,
                                  coords={coord_name_geo_x: ([dim_name_geo_x], var_geo_x_2d[0, :]),
                                          coord_name_geo_y: ([dim_name_geo_y], var_geo_y_2d[:, 0])})

            # Merge attributes safely
            obj_attrs = {**(file_attrs or {}),**(geo_attrs or {})}
            if obj_attrs:
                var_dset.attrs = obj_attrs

            var_dset[var_name] = var_da

        else:
            log_stream.error(' ===> Error in creating time information for dataset object')
            raise RuntimeError('Unknown error in creating dataset. Check the procedure.')

    else:
        log_stream.warning(' ===> All filenames in the selected period are not available')
        var_dset = None

    return var_dset
# -------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------
# Method to create 2D/3D variable dataset from provided values
def adjust_data_binary(
        var_values, var_geo_x, var_geo_y, var_geo_attrs=None,
        var_scale_factor=1,
        var_name=None, var_time=None, var_geo_1d=True,
        var_time_freq='H', var_time_steps_expected=1,
        coord_name_geo_x='west_east', coord_name_geo_y='south_north',
        coord_name_time='time',
        dim_name_geo_x='west_east', dim_name_geo_y='south_north',
        dim_name_time='time',
        dims_order=None):

    # Define dimensions order
    if dims_order is None:
        dims_order = [dim_name_geo_y, dim_name_geo_x, dim_name_time]

    # Normalize variable name
    if var_name is None:
        var_name = 'data'
    elif isinstance(var_name, list):
        if len(var_name) == 1:
            var_name = var_name[0]
        else:
            raise ValueError('var_name must contain exactly one element.')
    elif not isinstance(var_name, str):
        raise TypeError('var_name must be a string, list or None.')

    # Normalize scale factor
    if isinstance(var_scale_factor, list):
        if len(var_scale_factor) == 1:
            var_scale_factor = var_scale_factor[0]
        else:
            raise ValueError('var_scale_factor must contain exactly one element.')

    if var_scale_factor == 0:
        raise ValueError('var_scale_factor cannot be zero.')

    # Check input values
    if var_values is None:
        log_stream.warning(' ===> Input values are not defined.')
        return None

    if isinstance(var_values, xr.DataArray):
        var_values = var_values.values

    var_values = np.asarray(var_values, dtype=np.float32)

    # Define geographical shape
    if var_geo_1d:
        var_geo_x_2d, var_geo_y_2d = np.meshgrid(var_geo_x, var_geo_y)
    else:
        var_geo_x_2d = np.asarray(var_geo_x)
        var_geo_y_2d = np.asarray(var_geo_y)

    rows, cols = var_geo_x_2d.shape

    if var_geo_y_2d.shape != (rows, cols):
        raise ValueError('Geographical X and Y shapes are different.')

    # Normalize input values to 3D
    if var_values.ndim == 1:

        geo_n = rows * cols

        if var_values.size % geo_n != 0:
            raise ValueError('Input values size is not compatible with geographical shape.')

        time_steps = var_values.size // geo_n
        var_data = np.reshape(var_values,(rows, cols, time_steps),order='F')

    elif var_values.ndim == 2:
        if var_values.shape != (rows, cols):
            raise ValueError('Input values shape does not match geographical shape.')
        var_data = var_values[:, :, np.newaxis]
    elif var_values.ndim == 3:
        if var_values.shape[:2] != (rows, cols):
            raise ValueError('Input values shape does not match geographical shape.')
        var_data = var_values
    else:
        raise ValueError('Input values must be a 1D, 2D or 3D array.')

    # Apply scale factor
    var_data = np.asarray(var_data / var_scale_factor, dtype=np.float32)

    # Check expected time steps
    time_steps = var_data.shape[2]
    if time_steps == var_time_steps_expected:
        pass
    elif time_steps == 1 and var_time_steps_expected > 1:
        log_stream.warning(
            f' ===> Time steps expected [{var_time_steps_expected}] '
            f'and found [{time_steps}] are different. Replicating values.'
        )
        var_data = np.repeat(var_data, var_time_steps_expected,axis=2)
    else:
        raise ValueError(f'Unexpected time steps: expected {var_time_steps_expected}, found {time_steps}.')

    # Normalize geographical orientation
    geo_y_upper = var_geo_y_2d[0, 0]
    geo_y_lower = var_geo_y_2d[-1, 0]

    if geo_y_lower > geo_y_upper:
        var_geo_y_2d = np.flipud(var_geo_y_2d)

    # Define geographical attributes
    geo_attrs = {}
    if var_geo_attrs is not None:

        geo_attrs = {
            'nrows': rows,
            'ncols': cols,
            'nodata_value': var_geo_attrs.get('nodata_value', -9999),
            'xllcorner': var_geo_attrs.get('xllcorner'),
            'yllcorner': var_geo_attrs.get('yllcorner'),
            'cellsize': (
                abs(var_geo_attrs['cellsize'])
                if var_geo_attrs.get('cellsize') is not None
                else None
            ),
            'proj': var_geo_attrs.get('proj'),
            'transform': var_geo_attrs.get('transform')
        }

        geo_attrs = {key: value for key, value in geo_attrs.items() if value is not None}

    # Define coordinates
    coords = {
        coord_name_geo_x: ([dim_name_geo_x],var_geo_x_2d[0, :]),
        coord_name_geo_y: ([dim_name_geo_y],var_geo_y_2d[:, 0])
    }

    # Define time coordinates
    if var_time is not None:

        if isinstance(var_time, pd.Timestamp):
            var_data_time = pd.DatetimeIndex([var_time])
        elif isinstance(var_time, pd.DatetimeIndex):
            var_data_time = var_time.copy()
        else:
            raise TypeError('var_time must be a pandas Timestamp or DatetimeIndex.')

        # Generate time steps if only one timestamp is provided
        if len(var_data_time) == 1 and var_data.shape[2] > 1:
            var_data_time = pd.date_range(start=var_data_time[0], periods=var_data.shape[2], freq=var_time_freq)

        if len(var_data_time) != var_data.shape[2]:
            raise ValueError('Time coordinates length does not match data time dimension.')

        coords[coord_name_time] = ([dim_name_time], var_data_time)

    # Create DataArray
    var_da = xr.DataArray(data=var_data, name=var_name,dims=dims_order,coords=coords)
    # Create Dataset
    var_dset = xr.Dataset(data_vars={var_name: var_da},attrs=geo_attrs)

    return var_dset
# --------------------------------------------------------------------------------