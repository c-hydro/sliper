"""
Library Features:

Name:          lib_utils_data_grid_sm
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

from lib_data_io_nc import read_file_nc
from lib_data_io_binary import read_file_binary
from lib_data_io_tiff_OLD import write_file_tiff
from lib_utils_io_obj import write_dset, create_darray_2d

#from lib_utils_geo import convert_cn2s

from lib_info_args import proj_wkt as proj_default_wkt
from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

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
        file_dset, dset_mode='w', dset_engine='netcdf4', dset_compression=0, dset_format='NETCDF4',
        dim_key_time='time', no_data=-9999.0)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get data binary
def get_data_binary(file_name, da_geo, da_cn, da_cnet=None, mask_cnet=True, mask_limits=True,
                    value_cnet_mask=1, value_sm_mask=-1, value_sm_min=0, value_sm_max=1):

    geo_x_1d = da_geo['west_east']
    geo_y_1d = da_geo['south_north']
    geo_values = da_geo.values
    cn_values = da_cn.values
    cnet_values = da_cnet.values

    vtot_values = read_file_binary(file_name, data_geo=geo_values)
    vmax_values = convert_cn2s(cn_values, geo_values)
    sm_values = vtot_values / vmax_values

    if mask_cnet:
        sm_values[cnet_values == value_cnet_mask] = value_sm_mask

    if mask_limits:
        sm_values[sm_values < value_sm_min] = np.nan
        sm_values[sm_values > value_sm_max] = np.nan

    da_sm = create_darray_2d(sm_values, geo_x_1d, geo_y_1d,
                             coord_name_x='west_east', coord_name_y='south_north',
                             dim_name_x='west_east', dim_name_y='south_north')

    return da_sm
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get data netcdf
def get_data_nc(file_name, da_geo, da_cn, da_cnet=None, mask_cnet=True, mask_limits=True,
                value_cnet_mask=1, value_sm_mask=-1, value_sm_min=0, value_sm_max=1):

    geo_x_1d = da_geo['west_east'].values
    geo_y_1d = da_geo['south_north'].values
    geo_values = da_geo.values
    cn_values = da_cn.values
    cnet_values = da_cnet.values

    sm_values, geo_x_values, geo_y_values = read_file_nc(file_name)

    if mask_cnet:
        sm_values[cnet_values == value_cnet_mask] = value_sm_mask

    if mask_limits:
        sm_values[sm_values < value_sm_min] = np.nan
        sm_values[sm_values > value_sm_max] = np.nan

    da_sm = create_darray_2d(sm_values, geo_x_1d, geo_y_1d,
                             coord_name_x='west_east', coord_name_y='south_north',
                             dim_name_x='west_east', dim_name_y='south_north')

    return da_sm

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to filter scenarios dataframe
def filter_scenarios_dataframe(df_scenarios,
                               tag_column_rain='rain_accumulated_3H', tag_time='time',filter_rain=True,
                               value_min_rain=0, value_max_rain=None,
                               tag_column_sm='sm_max', filter_sm=True,
                               value_min_sm=0, value_max_sm=1,
                               tag_column_event='event_n', filter_event=True,
                               value_min_event=1, value_max_event=None,
                               tag_column_season='seasons', filter_season=True,
                               season_lut=None, season_name='ALL'):

    dframe_scenarios = deepcopy(df_scenarios)

    if not isinstance(tag_column_rain, list):
        tag_column_rain = [tag_column_rain]
    if not isinstance(tag_column_sm, list):
        tag_column_sm = [tag_column_sm]

    if filter_season:
        if season_lut is not None:
            grp_season = [season_lut.get(pd.Timestamp(t_stamp).month) for t_stamp in dframe_scenarios[tag_time].values]
            dframe_scenarios[tag_column_season] = grp_season
        else:
            dframe_scenarios[tag_column_season] = 'ALL'
    else:
        dframe_scenarios[tag_column_season] = 'ALL'

    # Filter by rain not valid values
    if filter_rain:
        for tag_column_step in tag_column_rain:
            logging.info(' -------> Filter variable ' + tag_column_step + ' ... ')
            if tag_column_step in list(dframe_scenarios.columns):
                if value_min_rain is not None:
                    dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_step] < value_min_rain].index)
                if value_max_rain is not None:
                    dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_step] > value_max_rain].index)
                log_stream.info(' -------> Filter variable ' + tag_column_step + ' ... DONE')
            else:
                log_stream.info(' -------> Filter variable ' + tag_column_step + ' ... FAILED')
                log_stream.warning(' ===> Filter rain datasets failed. Variable ' + tag_column_step +
                                   ' is not in the selected dataframe')

    # Filter by soil moisture not valid values
    if filter_sm:
        for tag_column_step in tag_column_sm:
            log_stream.info(' -------> Filter variable ' + tag_column_step + ' ... ')
            if tag_column_step in list(dframe_scenarios.columns):
                if value_min_sm is not None:
                    dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_step] < value_min_sm].index)
                if value_max_sm is not None:
                    dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_step] > value_max_sm].index)
                log_stream.info(' -------> Filter variable ' + tag_column_step + ' ... DONE')
            else:
                log_stream.info(' -------> Filter variable ' + tag_column_step + ' ... FAILED')
                log_stream.warning(' ===> Filter soil moisture datasets failed. Variable ' + tag_column_step +
                                   ' is not in the selected dataframe')

    # Filter by event n
    if filter_event:
        if value_min_event is not None:
            dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_event] < value_min_event].index)
        if value_max_event is not None:
            dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_event] > value_max_event].index)

    # Filter by season name
    if filter_season:
        dframe_scenarios = dframe_scenarios.loc[dframe_scenarios[tag_column_season] == season_name]

    return dframe_scenarios
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to compute sm statistics
def compute_sm_statistics(da_var, column_name=None,
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
# Method to reproject sm map to time-series
def reproject_sm_map2ts(da_var, column_name=None, dim_name_time='time',
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
# Method to reproject soil moisture source to map (data_array format)
def reproject_sm_source2map(da_sm_in, da_mask_out, da_geo_x_out, da_geo_y_out,
                            mask_out_condition=True):

    da_sm_tmp = da_sm_in.interp(
        south_north=da_geo_y_out, west_east=da_geo_x_out, method='nearest')

    if mask_out_condition:
        da_sm_out = da_sm_tmp.where(da_mask_out > 0, np.nan)
    else:
        da_sm_out = deepcopy(da_sm_tmp)

    # Debug
    # plt.figure()
    # plt.imshow(da_sm_in.values)
    # plt.colorbar()
    # plt.figure()
    # plt.imshow(da_sm_tmp.values)
    # plt.colorbar()
    # plt.figure()
    # plt.imshow(da_sm_out.values)
    # plt.colorbar()
    # plt.figure()

    return da_sm_out

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to merge map datasets
def merge_sm_list2map(da_map_list, da_mask_reference):

    array_merge = np.zeros([da_mask_reference.values.shape[0] * da_mask_reference.values.shape[1]])
    array_merge[:] = np.nan

    for da_map_step in da_map_list:
        array_values = da_map_step.values.ravel()
        idx_finite = np.isfinite(array_values)

        array_merge[idx_finite] = array_values[idx_finite]

        # debug
        # grid_merge = np.reshape(array_merge, [da_mask_reference.values.shape[0], da_mask_reference.values.shape[1]])
        # grid_values = np.reshape(array_values, [da_mask_reference.values.shape[0], da_mask_reference.values.shape[1]])
        # plt.figure()
        # plt.imshow(grid_values)
        # plt.colorbar()
        # plt.figure()
        # plt.imshow(grid_merge)
        # plt.colorbar()
        # plt.show()

    grid_merge = np.reshape(array_merge, [da_mask_reference.values.shape[0], da_mask_reference.values.shape[1]])
    idx_choice = np.where(grid_merge == -1)

    grid_merge[idx_choice[0], idx_choice[1]] = np.nan

    idx_filter = np.where((da_mask_reference.values == 1) & (np.isnan(grid_merge)))
    grid_merge[idx_filter[0], idx_filter[1]] = np.nanmean(grid_merge)
    grid_merge[(da_mask_reference.values == 0)] = np.nan

    grid_merge[idx_choice[0], idx_choice[1]] = np.nan

    return grid_merge
# -------------------------------------------------------------------------------------
