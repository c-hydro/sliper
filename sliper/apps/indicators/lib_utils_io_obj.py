"""
Library Features:

Name:          lib_utils_io_obj
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20200515'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import json

import numpy as np
import pandas as pd
import xarray as xr

from copy import deepcopy

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
# import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to filter datasets obj
def filter_obj_datasets(variable_collections, variable_names='alert_area:vector'):
    if not (isinstance(variable_names, list)):
        variable_names = [variable_names]

    variable_collections_selected = {}
    for variable_step in variable_names:
        if variable_step in list(variable_collections.keys()):
            variable_obj = variable_collections[variable_step]
            variable_collections_selected[variable_step] = variable_obj
        else:
            log_stream.warning(' ===> Variable "' + variable_step +
                               '" is not available in the variable collections')

    return variable_collections_selected


# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Method to filter variables
def filter_obj_variables(variable_list_all, variable_pivot='alert_area:vector'):

    if '{:}' in variable_pivot:
        variable_pivot = variable_pivot.replace('{:}', '')

    variable_list_selected = []
    for variable_step in variable_list_all:
        if variable_pivot in variable_step:
            variable_list_selected.append(variable_step)
    return variable_list_selected
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create a data array
def create_darray_2d(data, geo_x, geo_y, geo_1d=True, time=None,
                     coord_name_x='west_east', coord_name_y='south_north', coord_name_time='time',
                     dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                     dims_order=None):

    if dims_order is None:
        dims_order = [dim_name_y, dim_name_x]
    if time is not None:
        dims_order = [dim_name_y, dim_name_x, dim_name_time]

    if geo_1d:
        if geo_x.shape.__len__() == 2:
            geo_x = geo_x[0, :]
        if geo_y.shape.__len__() == 2:
            geo_y = geo_y[:, 0]

        if isinstance(geo_x, xr.DataArray):
            geo_x = geo_x.values
        elif isinstance(geo_x, np.ndarray):
            pass
        else:
            log_stream.error(' ===> Geographical object x format is not supported')
            raise NotImplemented('Case not implemented yet')
        if isinstance(geo_y, xr.DataArray):
            geo_y = geo_y.values
        elif isinstance(geo_y, np.ndarray):
            pass
        else:
            log_stream.error(' ===> Geographical object y format is not supported')
            raise NotImplemented('Case not implemented yet')

        if time is None:
            data_da = xr.DataArray(data,
                                   dims=dims_order,
                                   coords={coord_name_x: (dim_name_x, geo_x),
                                           coord_name_y: (dim_name_y, geo_y)})
        elif isinstance(time, pd.DatetimeIndex):

            if data.shape.__len__() == 2:
                data = np.expand_dims(data, axis=-1)

            data_da = xr.DataArray(data,
                                   dims=dims_order,
                                   coords={coord_name_x: (dim_name_x, geo_x),
                                           coord_name_y: (dim_name_y, geo_y),
                                           coord_name_time: (dim_name_time, time)})
        else:
            log_stream.error(' ===> Time obj is in wrong format')
            raise IOError('Variable time format not valid')

    else:
        log_stream.error(' ===> Longitude and Latitude must be 1d')
        raise IOError('Variable shape is not valid')

    return data_da
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create a data array
def create_darray_3d(data, time, geo_x, geo_y, geo_1d=True,
                     coord_name_x='west_east', coord_name_y='south_north', coord_name_time='time',
                     dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                     dims_order=None):

    if dims_order is None:
        dims_order = [dim_name_y, dim_name_x, dim_name_time]

    if geo_1d:
        if geo_x.shape.__len__() == 2:
            geo_x = geo_x[0, :]
        if geo_y.shape.__len__() == 2:
            geo_y = geo_y[:, 0]

        if isinstance(geo_x, xr.DataArray):
            geo_x = geo_x.values
        elif isinstance(geo_x, np.ndarray):
            pass
        else:
            log_stream.error(' ===> Geographical object x format is not supported')
            raise NotImplemented('Case not implemented yet')
        if isinstance(geo_y, xr.DataArray):
            geo_y = geo_y.values
        elif isinstance(geo_y, np.ndarray):
            pass
        else:
            log_stream.error(' ===> Geographical object y format is not supported')
            raise NotImplemented('Case not implemented yet')

        data_da = xr.DataArray(data,
                               dims=dims_order,
                               coords={coord_name_time: (dim_name_time, time),
                                       coord_name_x: (dim_name_x, geo_x),
                                       coord_name_y: (dim_name_y, geo_y)})
    else:
        log_stream.error(' ===> Longitude and Latitude must be 1d')
        raise IOError('Variable shape is not valid')

    return data_da
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to select attributes
def select_attrs(var_attrs_raw):

    var_attrs_select = {}
    if var_attrs_raw:
        for var_key, var_value in var_attrs_raw.items():
            if var_value is not None:
                if var_key not in attrs_decoded:
                    if isinstance(var_value, list):
                        var_string = [str(value) for value in var_value]
                        var_value = ','.join(var_string)
                    if isinstance(var_value, dict):
                        var_string = json.dumps(var_value)
                        var_value = var_string
                    if var_key in attrs_reserved:
                        var_value = None
                    if var_value is not None:
                        var_attrs_select[var_key] = var_value

    return var_attrs_select
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create datasets obj
def create_dset(var_data_values, var_geo_values, var_geo_x, var_geo_y,
                var_data_time=None,
                var_data_name='variable', var_data_attrs=None, var_geo_name='terrain', var_geo_attrs=None,
                coord_name_x='longitude', coord_name_y='latitude', coord_name_time='time',
                dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                dims_order_2d=None, dims_order_3d=None):

    if (var_geo_x.ndim == 2) and (var_geo_y.ndim == 2):
        var_geo_x_2d, var_geo_y_2d = deepcopy(var_geo_x), deepcopy(var_geo_y)
    elif (var_geo_x.ndim == 1) and (var_geo_y.ndim == 1):
        var_geo_x_2d, var_geo_y_2d = np.meshgrid(var_geo_x, var_geo_y)
    else:
        log_stream.error(' ===> Geographical arrays are expected 1d or 2d')
        raise NotImplementedError('Case not implemented yet')

    var_geo_y_upper, var_geo_y_lower = var_geo_y_2d[0, 0], var_geo_y_2d[-1, 0]
    if var_geo_y_lower > var_geo_y_upper:
        var_data_values = np.flipud(var_data_values)
        var_geo_values = np.flipud(var_geo_values)
        var_geo_y_2d = np.flipud(var_geo_y_2d)

    var_geo_x_1d, var_geo_y_1d = var_geo_x_2d[0, :], var_geo_y_2d[:, 0]

    if dims_order_2d is None:
        dims_order_2d = [dim_name_y, dim_name_x]
    if dims_order_3d is None:
        dims_order_3d = [dim_name_y, dim_name_x, dim_name_time]

    if not isinstance(var_data_time, list):
        var_data_time = [var_data_time]

    if var_data_values.ndim == 2:
        var_dset = xr.Dataset(coords={coord_name_time: ([dim_name_time], var_data_time)})
        var_dset.coords[coord_name_time] = var_dset.coords[coord_name_time].astype('datetime64[ns]')
    elif var_data_values.ndim == 3:
        var_dset = xr.Dataset(coords={coord_name_x: ([dim_name_x], var_geo_x_1d),
                                      coord_name_y: ([dim_name_y], var_geo_y_1d),
                                      coord_name_time: ([dim_name_time], var_data_time)})
        var_dset.coords[coord_name_time] = var_dset.coords[coord_name_time].astype('datetime64[ns]')
    else:
        log_stream.error(' ===> Datasets group object is expected 2d or 3d')
        raise NotImplementedError('Case not implemented yet')

    if var_geo_values.ndim == 2:
        var_da_terrain = xr.DataArray(var_geo_values,  name=var_geo_name, dims=dims_order_2d,
                                      coords={coord_name_x: ([dim_name_x], var_geo_x_1d),
                                              coord_name_y: ([dim_name_y], var_geo_y_1d)})
    else:
        log_stream.error(' ===> Datasets geographical object is expected 2d')
        raise NotImplementedError('Case not implemented yet')
    var_dset[var_geo_name] = var_da_terrain
    if var_geo_attrs is not None:
        var_dset[var_geo_name].attrs = var_geo_attrs

    if var_data_values.ndim == 2:
        var_da_data = xr.DataArray(var_data_values, name=var_data_name,
                                   dims=dims_order_2d,
                                   coords={coord_name_x: ([dim_name_x], var_geo_x_1d),
                                           coord_name_y: ([dim_name_y], var_geo_y_1d)})
    elif var_data_values.ndim == 3:
        var_da_data = xr.DataArray(var_data_values, name=var_data_name,
                                   dims=dims_order_3d,
                                   coords={coord_name_time: ([dim_name_time], var_data_time),
                                           coord_name_x: ([dim_name_x], var_geo_x_1d),
                                           coord_name_y: ([dim_name_y], var_geo_y_1d)})
    else:
        log_stream.error(' ===> Datasets variable object is expected 1d or 2d')
        raise NotImplementedError('Case not implemented yet')

    var_dset[var_data_name] = var_da_data
    if var_data_attrs is not None:
        var_dset[var_data_name].attrs = var_data_attrs

    return var_dset
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create dataset
def create_dset_OLD(var_data_values,
                var_geo_values, var_geo_x, var_geo_y,
                var_data_time=None,
                var_data_name='variable', var_geo_name='terrain', var_data_attrs=None, var_geo_attrs=None,
                var_geo_1d=False,
                coord_name_x='longitude', coord_name_y='latitude', coord_name_time='time',
                dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                dims_order_2d=None, dims_order_3d=None):

    var_geo_x_tmp = var_geo_x
    var_geo_y_tmp = var_geo_y
    if var_geo_1d:
        if (var_geo_x.shape.__len__() == 2) and (var_geo_y.shape.__len__() == 2):
            var_geo_x_tmp = var_geo_x[0, :]
            var_geo_y_tmp = var_geo_y[:, 0]
    else:
        if (var_geo_x.shape.__len__() == 1) and (var_geo_y.shape.__len__() == 1):
            var_geo_x_tmp, var_geo_y_tmp = np.meshgrid(var_geo_x, var_geo_y)

    if dims_order_2d is None:
        dims_order_2d = [dim_name_y, dim_name_x]
    if dims_order_3d is None:
        dims_order_3d = [dim_name_y, dim_name_x, dim_name_time]

    if not isinstance(var_data_time, list):
        var_data_time = [var_data_time]

    if var_data_values.shape.__len__() == 2:
        var_dset = xr.Dataset(coords={coord_name_time: ([dim_name_time], var_data_time)})
        var_dset.coords[coord_name_time] = var_dset.coords[coord_name_time].astype('datetime64[ns]')
    elif var_data_values.shape.__len__() == 3:
        var_dset = xr.Dataset(coords={coord_name_x: ([dim_name_y, dim_name_x], var_geo_x_tmp),
                                      coord_name_y: ([dim_name_y, dim_name_x], np.flipud(var_geo_y_tmp)),
                                      coord_name_time: ([dim_name_time], var_data_time)})
        var_dset.coords[coord_name_time] = var_dset.coords[coord_name_time].astype('datetime64[ns]')
    else:
        raise NotImplemented

    var_da_terrain = xr.DataArray(np.flipud(var_geo_values),  name=var_geo_name,
                                  dims=dims_order_2d,
                                  coords={coord_name_x: ([dim_name_y, dim_name_x], var_geo_x_tmp),
                                          coord_name_y: ([dim_name_y, dim_name_x], np.flipud(var_geo_y_tmp))})
    var_dset[var_geo_name] = var_da_terrain
    var_geo_attrs_select = select_attrs(var_geo_attrs)

    if var_geo_attrs_select is not None:
        var_dset[var_geo_name].attrs = var_geo_attrs_select

    if var_data_values.shape.__len__() == 2:
        var_da_data = xr.DataArray(np.flipud(var_data_values), name=var_data_name,
                                   dims=dims_order_2d,
                                   coords={coord_name_x: ([dim_name_y, dim_name_x], var_geo_x_tmp),
                                           coord_name_y: ([dim_name_y, dim_name_x], np.flipud(var_geo_y_tmp))})
    elif var_data_values.shape.__len__() == 3:
        var_da_data = xr.DataArray(np.flipud(var_data_values), name=var_data_name,
                                   dims=dims_order_3d,
                                   coords={coord_name_time: ([dim_name_time], var_data_time),
                                           coord_name_x: ([dim_name_y, dim_name_x], var_geo_x_tmp),
                                           coord_name_y: ([dim_name_y, dim_name_x], np.flipud(var_geo_y_tmp))})
    else:
        raise NotImplemented

    if var_data_attrs is not None:
        if attr_valid_range in list(var_data_attrs.keys()):
            valid_range = var_data_attrs[attr_valid_range]
            var_da_data = clip_map(var_da_data, valid_range)

        if attr_missing_value in list(var_data_attrs.keys()):
            missing_value = var_data_attrs[attr_missing_value]
            var_da_data = var_da_data.where(var_da_terrain > 0, other=missing_value)

    var_dset[var_data_name] = var_da_data
    if var_data_attrs is not None:
        var_data_attrs_select = select_attrs(var_data_attrs)
    else:
        var_data_attrs_select = None

    if var_data_attrs_select is not None:
        var_dset[var_data_name].attrs = var_data_attrs_select

    return var_dset

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to write dataset obj
def write_dset(file_name,
               dset_data, dset_mode='w', dset_engine='netcdf4', dset_compression=0, dset_format='NETCDF4',
               dim_key_time='time', no_data=-9999.0):

    dset_encoded = dict(zlib=True, complevel=dset_compression)

    dset_encoding = {}
    for var_name in dset_data.data_vars:

        if isinstance(var_name, bytes):
            var_name_upd = var_name.decode("utf-8")
            dset_data = var_name.rename({var_name: var_name_upd})
            var_name = var_name_upd

        var_data = dset_data[var_name]
        var_attrs = dset_data[var_name].attrs
        if len(var_data.dims) > 0:
            dset_encoding[var_name] = deepcopy(dset_encoded)

        if var_attrs:
            for attr_key, attr_value in var_attrs.items():
                if attr_key in attrs_decoded:

                    dset_encoding[var_name][attr_key] = {}

                    if isinstance(attr_value, list):
                        attr_string = [str(value) for value in attr_value]
                        attr_value = ','.join(attr_string)

                    dset_encoding[var_name][attr_key] = attr_value

            if '_FillValue' not in list(dset_encoding[var_name].keys()):
                dset_encoding[var_name]['_FillValue'] = no_data

    if dim_key_time in list(dset_data.coords):
        dset_encoding[dim_key_time] = {'calendar': 'gregorian'}

    dset_data.to_netcdf(path=file_name, format=dset_format, mode=dset_mode, engine=dset_engine,
                        encoding=dset_encoding)

# -------------------------------------------------------------------------------------
