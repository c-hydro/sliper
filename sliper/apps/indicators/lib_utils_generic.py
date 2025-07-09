"""
Library Features:

Name:          lib_utils_generic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
import tempfile
import numpy as np
import xarray as xr
import pandas as pd

from datetime import datetime
from typing import Dict, Any, Optional, Union, List
from copy import deepcopy

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# initialize variables
attrs_decoded = []
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to add fields to a flattened dictionary
def fields2dict(fields: dict, extra_fields: dict = None, extra_formats: dict = None) -> Dict[str, Any]:
    # Add extra fields
    if (extra_fields is not None) and extra_fields:
        for key, value in extra_fields.items():

            fmt = None
            if key in list(extra_formats.keys()):
                fmt = extra_formats[key]

            if isinstance(value, (datetime, pd.Timestamp)):
                if fmt is not None:
                    value = value.strftime(fmt)
            fields[key] = value

    return fields
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert a nested dictionary to a flattened dictionary
def dict2flat(d, parent_key='', sep=':'):
    """Flatten a nested dictionary using a delimiter between keys."""
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(dict2flat(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert a flattened dictionary back to a nested dictionary
def flat2dict(d, sep=':'):
    """Convert a flattened dictionary back to a nested dictionary."""
    result = {}
    for flat_key, value in d.items():
        keys = flat_key.split(sep)
        current = result
        for part in keys[:-1]:
            if part not in current or not isinstance(current[part], dict):
                current[part] = {}
            current = current[part]
        current[keys[-1]] = value
    return result
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to extract subpart of a dictionary
def extract_subpart(group: Dict[str, Dict[str, Any]], sub_keys: Union[str, List[str]]) -> Dict[str, Any]:
    if isinstance(sub_keys, str):
        sub_keys = [sub_keys]

    result = {}
    for key, area in group.items():
        current = area
        for sub_key in sub_keys:
            if isinstance(current, dict) and sub_key in current:
                current = current[sub_key]
            else:
                log_stream.warning(f" ===> Key path {sub_keys} not found for group '{key}' at '{sub_key}'")
                current = {}
                break
        result[key] = current

    return result
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to create a temporary file name
def create_filename_tmp(prefix='tmp_', suffix='.tiff', folder=None):
    if folder is None:
        folder = '/tmp'
    with tempfile.NamedTemporaryFile(dir=folder, prefix=prefix, suffix=suffix, delete=False) as tmp:
        temp_file_name = tmp.name
    return temp_file_name
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to create a data array
def create_darray(data: np.ndarray,
                  geo_x: Union[np.ndarray, xr.DataArray],
                  geo_y: Union[np.ndarray, xr.DataArray],
                  geo_1d: bool = True,
                  time: Optional[pd.DatetimeIndex] = None,
                  coord_name_x: str = 'longitude',
                  coord_name_y: str = 'latitude',
                  coord_name_time: str = 'time',
                  dim_name_x: str = 'longitude',
                  dim_name_y: str = 'latitude',
                  dim_name_time: str = 'time',
                  dims_order: Optional[List[str]] = None) -> xr.DataArray:

    if dims_order is None:
        dims_order = [dim_name_y, dim_name_x]
    if time is not None:
        if dims_order is None:
            dims_order = [dim_name_time, dim_name_y, dim_name_x]

    if time is not None:
        time_detected = False
        for dim_len in list(data.shape):
            if dim_len == len(time):
                time_detected = True
    else:
        time_detected = None

    if time_detected is not None:
        if not time_detected:
            log_stream.error(' ===> Data time dimension does not match time coordinates')
            raise ValueError('Mismatch between data and time dimension')

    if geo_1d:
        if geo_x.ndim == 2:
            geo_x = geo_x[0, :]
        if geo_y.ndim == 2:
            geo_y = geo_y[:, 0]

        if isinstance(geo_x, xr.DataArray):
            geo_x = geo_x.values
        elif not isinstance(geo_x, np.ndarray):
            log_stream.error(' ===> Geographical object x format is not supported')
            raise NotImplementedError('Case not implemented yet')

        if isinstance(geo_y, xr.DataArray):
            geo_y = geo_y.values
        elif not isinstance(geo_y, np.ndarray):
            log_stream.error(' ===> Geographical object y format is not supported')
            raise NotImplementedError('Case not implemented yet')

        # check if time is defined or not
        if time is None:

            # create 2d data array
            data_da = xr.DataArray(data,
                                   dims=dims_order,
                                   coords={coord_name_x: (dim_name_x, geo_x),
                                           coord_name_y: (dim_name_y, geo_y)})
        elif isinstance(time, pd.DatetimeIndex) or isinstance(time, list):

            if isinstance(time, list):
                time = pd.DatetimeIndex(time)

            if data.ndim == 2:
                data = np.expand_dims(data, axis=0)

            # create 3d data array
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
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to create datasets
def create_dset_OLD(var_data_values, var_geo_values, var_geo_x, var_geo_y,
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
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to write dataset
def write_dset_OLD(file_name,
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

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to fill template string
def fill_template_string(template_str: str,
                         template_map: dict,
                         value_map: dict) -> str:
    """
    Fill a template string using values from template_map (providing formats) and
    value_map (providing actual values). Only placeholders present in both maps are used.

    Parameters:
        template_str (str): Template string with placeholders (e.g., '{source_date}', '{region}')
        template_map (dict): Dict with keys as placeholders and values as format strings or literals
        value_map (dict): Dict with keys and actual values to be formatted

    Returns:
        str: Final string with placeholders filled. Skips unresolved keys with warnings.

    Raises:
        ValueError: If template_str is None.
    """

    if template_str is None:
        log_stream.error(" ===> The variable 'template_str' should not be None.")
        raise ValueError("Check the template_str variable and provide a valid string")

    matches = re.findall(r"{(\w+)}", template_str)
    filled_values = {}

    for key in matches:
        if key in template_map and key in value_map:
            fmt = template_map[key]
            val = value_map[key]

            if isinstance(fmt, str) and "%" in fmt:
                # Treat as datetime formatting
                if isinstance(val, pd.Timestamp):
                    filled_values[key] = val.strftime(fmt)
                else:
                    log_stream.warning(
                        f" ===> Expected pd.Timestamp for key '{key}' to apply datetime format '{fmt}'."
                    )
                    filled_values[key] = str(val)
            else:
                # Use as literal format override
                filled_values[key] = val
        else:
            log_stream.warning(f" ===> Skipping placeholder '{{{key}}}' — not present in both template_map and value_map.")
            filled_values[key] = '{' + key + '}'  # Keep unresolved placeholders as-is

    try:
        return template_str.format(**filled_values)
    except KeyError as e:
        log_stream.error(f" ===> Formatting error: missing key {e}")
        raise
# ----------------------------------------------------------------------------------------------------------------------

