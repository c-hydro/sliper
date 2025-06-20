"""
Library Features:

Name:          lib_utils_data_grid_rain
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import os
import rasterio
import numpy as np
import pandas as pd
import xarray as xr

from copy import deepcopy
from rasterio.crs import CRS
from osgeo import ogr

from lib_data_io_tiff import write_file_tiff

from lib_utils_time import split_time_window
from lib_utils_io_obj import write_dset, create_darray_2d, create_darray_3d
from lib_analysis_interpolation_point import interp_point2map
from lib_analysis_interpolation_grid import interp_grid2map

from lib_info_args import proj_wkt as proj_default_wkt
from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Default variable(s)
lut_season_default = {
    1: 'DJF', 2: 'DJF', 3: 'MAM', 4: 'MAM', 5: 'MAM', 6: 'JJA',
    7: 'JJA', 8: 'JJA', 9: 'SON', 10: 'SON', 11: 'SON', 12: 'DJF'
}

# Debug
import matplotlib.pylab as plt
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to get data tiff
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
# Method to save grid data in geotiff format
def save_data_tiff(file_name, file_data, file_geo_x, file_geo_y, file_metadata=None, file_epsg_code=None):

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


# -------------------------------------------------------------------------------------
# Method to get data netcdf
def save_data_nc(file_name, file_dset):
    write_dset(
        file_name,
        file_dset, dset_mode='w', dset_engine='netcdf4', dset_compression=0,
        dset_format='NETCDF4', dim_key_time='time', no_data=-9999.0)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to filter rain dataframe
def filter_rain_dataframe(df_rain, dict_point_static=None, tag_filter_column='code'):
    dict_point_dynamic = {}
    for point_reference, point_fields in dict_point_static.items():
        point_neighbour_code = point_fields[tag_filter_column]
        df_select = df_rain.loc[df_rain[tag_filter_column].isin(point_neighbour_code)]
        dict_point_dynamic[point_reference] = df_select
    return dict_point_dynamic
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to compute rain accumulated maps over time period
def compute_rain_maps_accumulated(var_da_source,  coord_name_time='time', time_window=None, time_direction=None):

    if time_window is None:
        time_window = '3H'

    if time_direction is None:
        time_direction = 'right'

    if coord_name_time in list(var_da_source.coords):
        time_coords = var_da_source[coord_name_time]
    else:
        log_stream.error(' ===> Time coord "' + coord_name_time + '" must be defined in the source DataArray object')
        raise RuntimeError('Check your source DataArray object and include the time coord "' + coord_name_time + '"')

    time_period, time_frequency = split_time_window(time_window)
    time_stamp_start, time_stamp_end = pd.Timestamp(time_coords.values[0]), pd.Timestamp(time_coords.values[-1])

    if time_direction == 'left':
        var_da_sorted = var_da_source.sortby(time_coords, ascending=False)
        # var_da_test = var_da_sorted.resample(time=time_window, label='right', closed='right').sum(coord_name_time)
        var_da_resampled = var_da_sorted.rolling(time=time_period, center=False).sum()
    elif time_direction == 'right':
        var_da_sorted = var_da_source.sortby(time_coords, ascending=True)
        # var_da_test = var_da_sorted.resample(time=time_window, label='right', closed='right').sum(coord_name_time)
        var_da_resampled = var_da_sorted.rolling(time=time_period, center=False).sum()
    else:
        log_stream.error(' ===> Time direction "' + time_direction + '" flag is not allowed')
        raise IOError('Available flags for temporal direction are: "right" and "left"')

    var_da_resampled.attrs = {'time_from': time_stamp_start, 'time_to': time_stamp_end,
                              'time_window': time_window, 'time_direction': time_direction}

    # Debug
    # plt.figure()
    # plt.imshow(var_da_resampled.values[:,:,0])
    # plt.colorbar()
    # plt.show()

    return var_da_resampled

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to compute rain averaged time-series over time period
def compute_rain_ts_averaged(dframe_var, column_name=None,
                             time_window='3H', time_direction='right', time_inverse=True):
    if column_name is None:
        column_name = 'data_time_series'
    if isinstance(column_name, list):
        column_name = column_name[0]

    time_period, time_frequency = split_time_window(time_window)

    if time_inverse:
        # series_var = dframe_var[column_name].resample(time_window, label=time_direction).mean()[:-1]
        series_var = dframe_var[column_name].rolling(time_period, center=False).mean()[::-1]
    else:
        # series_var = dframe_var[column_name].resample(time_window, label=time_direction).mean()
        series_var = dframe_var[column_name].rolling(time_period, center=False).mean()

    series_var = series_var.dropna(how='all')

    return series_var
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to compute rain accumulated time-series over time period
def compute_rain_ts_accumulated(dframe_var, column_name=None,
                                time_window='3H', time_direction='right', time_inverse=True):
    if column_name is None:
        column_name = 'data_time_series'
    if isinstance(column_name, list):
        column_name = column_name[0]

    time_period, time_frequency = split_time_window(time_window)

    if time_inverse:
        # series_var = dframe_var[column_name].resample(time_window, label=time_direction).sum()[:-1]
        series_var = dframe_var[column_name].rolling(time_period, center=False).sum()[::-1]
    else:
        # series_var = dframe_var[column_name].resample(time_window, label=time_direction).sum()
        series_var = dframe_var[column_name].rolling(time_period, center=False).sum()

    series_var = series_var.dropna(how='all')

    return series_var
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to compute rain statistics
def compute_rain_statistics(da_var, column_name=None,
                            tag_first_value=False, tag_last_value=False, tag_avg_value=True,
                            tag_max_value=True, tag_min_value=False):

    if column_name is None:
        column_name = ['data_time_series']
    if not isinstance(column_name, list):
        column_name = [column_name]

    value_avg, value_max, value_min, value_first, value_last = None, None, None, None, None
    if tag_avg_value:
        value_avg = float(da_var[column_name].mean())
    if tag_max_value:
        value_max = float(da_var[column_name].max())
    if tag_min_value:
        value_min = float(da_var[column_name].min())
    if tag_first_value:
        time_first = list(da_var.index)[0]
        value_first = float(da_var.loc[time_first].values[0])
    if tag_last_value:
        time_last = list(da_var.index)[-1]
        value_last = float(da_var.loc[time_last].values[0])

    return value_first, value_last, value_avg, value_max, value_min
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to compute rain peaks
def compute_rain_peaks(var_da, var_point_collections, var_analysis='max'):

    var_time = var_da['time'].values
    var_data_3d = var_da.values

    obj_point_time_stamp, obj_point_collections = [], {}
    for time_id, date_time_step in enumerate(var_time):
        var_data_2d = var_data_3d[:, :, time_id]

        time_stamp_step = pd.Timestamp(date_time_step)

        for point_key, point_idxs in var_point_collections.items():
            var_data_1d = var_data_2d[point_idxs]

            value_max, value_avg = np.nanmax(var_data_1d), np.nanmean(var_data_1d)

            if point_key not in list(obj_point_collections.keys()):
                obj_point_collections[point_key] = [value_max]
            else:
                value_tmp = obj_point_collections[point_key]
                value_tmp.append(value_max)
                obj_point_collections[point_key] = value_tmp

        obj_point_time_stamp.append(time_stamp_step)

    peaks_dframe = pd.DataFrame(index=obj_point_time_stamp, data=obj_point_collections)
    peaks_dframe = peaks_dframe.dropna(how='all')

    peaks_max = peaks_dframe.to_numpy().max()

    return peaks_max, peaks_dframe

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to reproject rain map to time-series
def reproject_rain_map2ts(da_var, column_name=None, dim_name_time='time',
                          dim_name_geo_x='west_east', dim_name_geo_y='south_north'):

    if column_name is None:
        column_name = ['data_time_series']
    if not isinstance(column_name, list):
        column_name = [column_name]

    times_period_index = pd.DatetimeIndex(da_var[dim_name_time].values)
    values_period_avg = da_var.mean(dim=[dim_name_geo_y, dim_name_geo_x]).values

    dframe_var = pd.DataFrame(index=times_period_index, data=values_period_avg,
                              columns=column_name).fillna(value=pd.NA)

    return dframe_var
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to reproject rain source to map destination
def reproject_rain_source2map(da_var_in, da_mask_out, da_geo_x_out, da_geo_y_out,
                              coord_name_time_in='time',
                              coord_name_geo_x_in='west_east', coord_name_geo_y_in='south_north',
                              dim_name_time_in='time',
                              dim_name_geo_x_in='west_east', dim_name_geo_y_in='south_north',
                              coord_name_time_out='time',
                              coord_name_geo_x_out='west_east', coord_name_geo_y_out='south_north',
                              dim_name_time_out='time',
                              dim_name_geo_x_out='west_east', dim_name_geo_y_out='south_north',
                              mask_out_condition=False, interp_method='nearest',
                              interp_mode='xarray', interp_index=None):

    if not isinstance(da_var_in, xr.DataArray):
        log_stream.error(' ===> Source object must be "DataArray" or "numpy.array"')
        raise RuntimeError('The format of the source object is not supported')

    if interp_mode == 'xarray':
        if coord_name_time_in in list(da_var_in.coords):
            time_n = da_var_in[coord_name_time_in].shape[0]
            time_index = pd.DatetimeIndex(da_var_in[coord_name_time_in].values)
        else:
            time_n, time_index = 0, None
        if dim_name_time_in in list(da_var_in.dims):
            log_stream.warning(' ===> Dimension "' + dim_name_time_in + '" is not defined in the source data')

        if (coord_name_geo_x_in in list(da_var_in.coords)) and (coord_name_geo_x_in in list(da_var_in.coords)):
            geo_x_tmp_in = da_var_in[coord_name_geo_x_in].values
            geo_y_tmp_in = da_var_in[coord_name_geo_y_in].values
            if geo_x_tmp_in.ndim == 1 and geo_y_tmp_in.ndim == 1:
                geo_x_2d_in, geo_y_2d_in = np.meshgrid(geo_x_tmp_in, geo_y_tmp_in)
            elif geo_x_tmp_in.ndim == 2 and geo_y_tmp_in.ndim == 2:
                geo_x_2d_in, geo_y_2d_in = deepcopy(geo_x_tmp_in), deepcopy(geo_y_tmp_in)
            else:
                log_stream.error(' ===> Geographical objects is expected 1d or 2d')
                raise RuntimeError('Check the obj dimensions')
        else:
            log_stream.error(' ===> Geographical objects must be defined in the source data')
            raise RuntimeError('Check the obj structure')
        if dim_name_geo_x_in in list(da_var_in.dims):
            log_stream.warning(' ===> Dimension "' + dim_name_geo_x_in + '" is not defined in the source data')
        if dim_name_geo_y_in in list(da_var_in.dims):
            log_stream.warning(' ===> Dimension "' + dim_name_geo_y_in + '" is not defined in the source data')

        geo_y_upper_in, geo_y_lower_in = geo_y_2d_in[0, 0], geo_y_2d_in[-1, 0]
        if geo_y_lower_in > geo_y_upper_in:
            values_var_tmp = np.flipud(da_var_in.values)
            geo_y_2d_tmp = np.flipud(geo_y_2d_in)
            geo_x_2d_tmp = deepcopy(geo_x_2d_in)
            if values_var_tmp.ndim == 2:
                da_var_tmp = create_darray_2d(
                    values_var_tmp, geo_x_2d_tmp[0, :], geo_y_2d_tmp[:, 0],
                    coord_name_x=coord_name_geo_x_out, coord_name_y=coord_name_geo_y_out,
                    dim_name_x=dim_name_geo_x_out, dim_name_y=dim_name_geo_y_out)
            elif values_var_tmp.ndim == 3:
                da_var_tmp = create_darray_3d(
                    values_var_tmp, time_index, geo_x_2d_tmp[0, :], geo_y_2d_tmp[:, 0],
                    coord_name_time=coord_name_time_out,
                    coord_name_x=coord_name_geo_x_out, coord_name_y=coord_name_geo_y_out,
                    dim_name_time=dim_name_time_out,
                    dim_name_x=dim_name_geo_x_out, dim_name_y=dim_name_geo_y_out)
            else:
                log_stream.error(' ===> Dataset obj is expected 2d or 3d')
                raise RuntimeError('Check the obj dimensions')
        else:
            da_var_tmp = deepcopy(da_var_in)

        da_var_interp = da_var_tmp.interp(
            south_north=da_geo_y_out, west_east=da_geo_x_out, method=interp_method)

        if mask_out_condition:
            mask_out_2d = da_mask_out.values
            if time_n > 0:
                mask_out_3d = np.repeat(mask_out_2d[:, :, np.newaxis], time_n, axis=2)
                da_var_out = da_var_interp.where(mask_out_3d > 0, np.nan)
            else:
                da_var_out = da_var_interp.where(mask_out_2d > 0, np.nan)
        else:
            da_var_out = deepcopy(da_var_interp)

        obj_var_out = deepcopy(da_var_out)

        ''' Debug
        plt.figure()
        plt.imshow(da_var_in.values[:, :, 0])
        plt.colorbar()
        plt.figure()
        plt.imshow(da_var_tmp.values[:, :, 0])
        plt.colorbar()
        plt.figure()
        plt.imshow(da_var_interp.values[:, :, 0])
        plt.colorbar()
        plt.figure()
        plt.imshow(da_var_out.values[:, :, 0])
        plt.colorbar()
        plt.show()
        '''

    elif interp_mode == 'numpy':

        geox_in_1d = da_var_in['west_east'].values
        geoy_in_1d = da_var_in['south_north'].values
        data_in_2d = da_var_in.values

        mask_out_2d = da_mask_out.values
        geox_out_1d = da_geo_x_out['west_east'].values
        geoy_out_1d = da_geo_y_out['south_north'].values

        geox_in_2d, geoy_in_2d = np.meshgrid(geox_in_1d, geoy_in_1d)
        geox_out_2d, geoy_out_2d = np.meshgrid(geox_out_1d, geoy_out_1d)

        data_out_2d = interp_grid2map(geox_in_2d, geoy_in_2d, data_in_2d,
                                      geox_out_2d, geoy_out_2d,
                                      interp_method='nearest',
                                      interpolating_max_distance=10000,
                                      interpolating_fill_value=0)

        tmp_out_2d = deepcopy(data_out_2d)
        if mask_out_condition:
            data_out_2d[mask_out_2d == 0] = np.nan

        obj_var_out = deepcopy(data_out_2d)

    else:
        log_stream.error(' ===> Reproject method "' + interp_mode + '" is not supported.')
        raise NotImplementedError('Case not implemented yet')

    return obj_var_out
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to reproject rain source to map (data_array format)
def reproject_rain_source2map_OLD(da_rain, mask_out_2d, geo_x_out_2d, geo_y_out_2d,
                              index_out_1d=None, mask_out_condition=True):

    geox_in_1d = da_rain['west_east'].values
    geoy_in_1d = da_rain['south_north'].values
    data_in_2d = da_rain.values

    geox_in_2d, geoy_in_2d = np.meshgrid(geox_in_1d, geoy_in_1d)

    data_out_2d = interp_grid2map(geox_in_2d, geoy_in_2d, data_in_2d,
                                  geo_x_out_2d, geo_y_out_2d,
                                  nodata=-9999, interp_method='nearest',
                                  index_out=index_out_1d)
    if mask_out_condition:
        data_out_2d[mask_out_2d == 0] = np.nan

    # Debug
    # plt.figure()
    # plt.imshow(data_in_2d)
    # plt.colorbar()
    # plt.figure()
    # plt.imshow(data_out_2d)
    # plt.colorbar()
    # plt.show()

    return data_out_2d

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to interpolate rain points to map (data_frame format)
def interpolate_rain_points2map(df_rain, mask_out_2d, geox_out_2d, geoy_out_2d, folder_tmp=None):

    geox_in_1d = df_rain['longitude'].values
    geoy_in_1d = df_rain['latitude'].values
    data_in_1d = df_rain['data'].values

    data_out_2d = interp_point2map(
        data_in_1d, geox_in_1d, geoy_in_1d, geox_out_2d, geoy_out_2d, epsg_code='4326',
        interp_no_data=-9999.0, interp_radius_x=0.2, interp_radius_y=0.2,
        interp_method='idw', var_name_data='values', var_name_geox='x', var_name_geoy='y',
        folder_tmp=folder_tmp)

    data_out_2d[mask_out_2d == 0] = np.nan

    return data_out_2d
# -------------------------------------------------------------------------------------

