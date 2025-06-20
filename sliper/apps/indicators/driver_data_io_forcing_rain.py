"""
Class Features

Name:          driver_data_io_forcing_rain
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '202200502'
Version:       '1.5.0'
"""

######################################################################################
# Library
import logging
import os
import numpy as np
import pandas as pd
import xarray as xr

from copy import deepcopy

from lib_utils_system import fill_tags2string, make_folder

from lib_data_io_csv_scenarios import read_file_csv
from lib_data_io_xlsx import read_file_xlsx

from lib_utils_data_table import read_table_obj, select_table_obj, get_table_value
from lib_utils_data_grid_rain import interpolate_rain_points2map, reproject_rain_source2map, \
    get_data_tiff, save_data_tiff, save_data_nc

from lib_utils_io_obj import create_dset, write_dset
from lib_utils_time import search_time_features

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverForcing for rain datasets
class DriverForcing:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, time_step, src_dict, anc_dict, dst_dict=None, tmp_dict=None,
                 alg_ancillary=None, alg_template_tags=None,
                 time_data=None, table_data='table_variables.json',
                 collections_data_group=None,
                 collections_data_geo_grid=None, collections_data_geo_pnt=None,
                 flag_data_src='rain_data', flag_data_anc='rain_data',
                 flag_ancillary_updating=True):

        self.time_step = pd.Timestamp(time_step)

        self.flag_data_src = flag_data_src
        self.flag_data_anc = flag_data_anc

        self.obj_file_name_tag = 'file_name'
        self.obj_folder_name_tag = 'folder_name'
        self.obj_type_tag = 'obj_type'
        self.obj_geo_reference_tag = 'obj_geo_reference'

        self.reference_tag = 'reference'
        self.region_tag = 'region'
        self.region_pivot_name_primary = 'region:primary_data:rain_data'
        self.region_pivot_name_index = 'region:index_data:rain_data'

        self.alg_ancillary = alg_ancillary
        self.alg_template_tags = alg_template_tags

        self.data_group = collections_data_group
        self.data_geo_reference_grid = collections_data_geo_grid[self.reference_tag]['reference']
        self.data_geo_region_grid = collections_data_geo_grid[self.region_tag][self.region_pivot_name_primary]
        self.data_geo_region_index = collections_data_geo_grid[self.region_tag][self.region_pivot_name_index]
        self.data_geo_pnt = collections_data_geo_pnt

        # time object(s)
        self.time_data = time_data[self.flag_data_src]
        self.time_period_max, self.time_frequency_max, self.time_period_type = search_time_features(
            self.data_group, data_key='rain_datasets')
        self.time_range = self.collect_file_time()

        # source object(s)
        self.type_src = src_dict[self.flag_data_src][self.obj_type_tag]
        self.table_src = select_table_obj(read_table_obj(table_data), ['source', self.flag_data_src, self.type_src])
        self.geo_reference_src = src_dict[self.flag_data_src][self.obj_geo_reference_tag]

        self.file_name_src_raw = src_dict[self.flag_data_src][self.obj_file_name_tag]
        self.folder_name_src_raw = src_dict[self.flag_data_src][self.obj_folder_name_tag]
        self.file_path_src_list = self.collect_file_list(self.folder_name_src_raw, self.file_name_src_raw,
                                                         table_data=self.table_src)

        # ancillary object(s)
        self.file_name_anc_raw = anc_dict[self.flag_data_anc][self.obj_file_name_tag]
        self.folder_name_anc_raw = anc_dict[self.flag_data_anc][self.obj_folder_name_tag]
        self.file_path_anc_list = self.collect_file_list(self.folder_name_anc_raw, self.file_name_anc_raw)

        # tmp object(s)
        self.folder_name_tmp_raw = tmp_dict[self.obj_folder_name_tag]
        self.file_name_tmp_raw = tmp_dict[self.obj_file_name_tag]

        self.folder_name_tmp = None
        if self.folder_name_tmp_raw is not None:
            self.file_path_tmp_list = self.collect_file_list(self.folder_name_tmp_raw, self.file_name_tmp_raw)
            self.folder_name_tmp = list(set(self.file_path_tmp_list))[0]

        self.flag_ancillary_updating = flag_ancillary_updating

        self.file_path_processed = []

        # variable(s) to keep in memory the active filename and dataframe
        self.file_name_active = None
        self.file_dframe_active = None
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect time(s)
    def collect_file_time(self):

        # get generic variable features
        time_period = self.time_data["time_period"]
        time_frequency = self.time_data["time_frequency"]
        time_rounding = self.time_data["time_rounding"]

        # case of time period
        if isinstance(self.time_period_max, (float, int)):
            if time_period < self.time_period_max:
                log_stream.warning(' ===> Obj "time_period" is less then "time_period_max". '
                                   'Set "time_period" == "time_period_max"')
                time_period = self.time_period_max
        elif isinstance(self.time_period_max, list):

            time_period_max = np.max(np.asarray(self.time_period_max))

            if time_period < time_period_max:
                log_stream.warning(' ===> Obj "time_period" is less then "time_period_max". '
                                   'Set "time_period" == "time_period_max"')
                time_period = time_period_max

        else:
            log_stream.error(' ===> Obj "time_period" format is not expected.')
            raise NotImplementedError('Case not implemented yet')

        # case of time frequency
        if isinstance(self.time_frequency_max, str):
            if time_frequency != self.time_frequency_max:
                log_stream.error(' ===> Obj "time_frequency" is not equal to "time_frequency_max".')
                raise NotImplementedError('Case not implemented yet')
        elif isinstance(self.time_frequency_max, list):
            for time_frequency_step in self.time_frequency_max:
                if time_frequency != time_frequency_step:
                    log_stream.error(' ===> Obj "time_frequency" is not equal to "time_frequency_max" in all cases.')
                    raise NotImplementedError('Case not implemented yet')
        else:
            log_stream.error(' ===> Obj "time_frequency" format is not expected.')
            raise NotImplementedError('Case not implemented yet')

        # time step, time_step_left, time_step_right information
        time_step = self.time_step.floor(time_rounding)
        time_step_left = time_step
        time_step_right = pd.date_range(start=time_step, periods=2, freq=time_frequency)[1]
        # search type information (left, right, both, [left, right], [right, left])
        search_type = self.time_period_type

        # case 'left' -> time range from time_start_left
        if search_type == 'left':
            time_range = pd.date_range(end=time_step_left, periods=time_period, freq=time_frequency)

        # case 'right' -> time range from time_end_right
        elif search_type == 'right':
            time_range = pd.date_range(start=time_step_right, periods=time_period, freq=time_frequency)

        # case 'both' -> time range from time_start_left and time_end_right (same length)
        elif search_type == 'both':
            time_range_left = pd.date_range(end=time_step_left, periods=time_period, freq=time_frequency)
            time_range_right = pd.date_range(start=time_step_right, periods=time_period, freq=time_frequency)
            time_range = time_range_left.union(time_range_right)

        # case 'left' and 'right' -> time range from time_step_left and time_step_right (different length)
        elif search_type == ['left', 'right']:
            time_period_left, time_period_right = self.time_period_max[0], self.time_period_max[1]
            time_frequency_left, time_frequency_right = self.time_frequency_max[0], self.time_frequency_max[1]
            time_range_left = pd.date_range(end=time_step_left, periods=time_period_left, freq=time_frequency_left)
            time_range_right = pd.date_range(start=time_step_right, periods=time_period_right, freq=time_frequency_right)
            time_range = time_range_left.union(time_range_right)

        # case 'right' and 'left' -> time range from time_step_left and time_step_right (different length)
        elif search_type == ['right', 'left']:
            time_period_left, time_period_right = self.time_period_max[1], self.time_period_max[0]
            time_frequency_left, time_frequency_right = self.time_frequency_max[1], self.time_frequency_max[0]
            time_range_left = pd.date_range(end=time_step_left, periods=time_period_left, freq=time_frequency_left)
            time_range_right = pd.date_range(start=time_step_left, periods=time_period_right, freq=time_frequency_right)
            time_range = time_range_left.union(time_range_right)

        else:
            log_stream.error(' ===> Obj "search_type" for "time_range" selection is not allowed')
            raise RuntimeError('Obj "search_type" must be equal to "left", "right", "both", '
                               '["left", "right"] or ["right", "left"]')

        return time_range
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect ancillary file
    def collect_file_list(self, folder_name_raw, file_name_raw, table_data=None):

        month_lut = get_table_value(table_data, 'month_lut')

        file_name_list = []
        for datetime_step in self.time_range:

            date_month_step = str(datetime_step.month).zfill(2)

            if month_lut is not None:
                if date_month_step in list(month_lut.keys()):
                    name_month_step = month_lut[date_month_step]
                else:
                    log_stream.error(' ===> Obj "date_month" is not available in the "month_lut" obj')
                    raise IOError('Check the "month_lut" to use the correct "date_month" string')
            else:
                name_month_step = datetime_step.month_name()

            alg_template_values_step = {
                'month_name': name_month_step,
                'source_rain_datetime': datetime_step, 'source_rain_sub_path_time': datetime_step,
                'ancillary_rain_datetime': datetime_step, 'ancillary_rain_sub_path_time': datetime_step,
                'destination_rain_datetime': datetime_step, 'destination_rain_sub_path_time': datetime_step}

            folder_name_def = fill_tags2string(
                folder_name_raw, self.alg_template_tags, alg_template_values_step)
            if file_name_raw is not None:
                file_name_def = fill_tags2string(
                    file_name_raw, self.alg_template_tags, alg_template_values_step)
                file_path_def = os.path.join(folder_name_def, file_name_def)
            else:
                file_path_def = folder_name_def

            file_name_list.append(file_path_def)

        return file_name_list

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize forcing
    def organize_forcing(self, var_name='rain', var_min=0, var_max=None):

        log_stream.info(' ----> Organize rain forcing ... ')

        type_src = self.type_src

        data_geo_reference_grid = self.data_geo_reference_grid
        data_geo_region_grid = self.data_geo_region_grid
        data_geo_region_index = self.data_geo_region_index
        data_geo_pnt = self.data_geo_pnt

        for datetime_step, file_path_src, file_path_anc in zip(
                self.time_range, self.file_path_src_list, self.file_path_anc_list):

            log_stream.info(' -----> TimeStep: ' + str(datetime_step) + ' ... ')

            if self.flag_ancillary_updating:
                if os.path.exists(file_path_anc):
                    os.remove(file_path_anc)

            if not os.path.exists(file_path_anc):

                if os.path.exists(file_path_src):

                    if file_path_src.endswith('.csv'):

                        # Read datasets point file
                        file_dframe = read_file_csv(
                            file_path_src, datetime_step,
                            file_header=self.table_src['file_columns_src'],
                            file_sep=self.table_src['file_columns_sep'],
                            scale_factor_longitude=self.table_src['file_scale_factor_longitude'],
                            scale_factor_latitude=self.table_src['file_scale_factor_latitude'],
                            scale_factor_data=self.table_src['file_scale_factor_data'])

                        # Filter data using variable limits (if defined)
                        if var_min is not None:
                            file_dframe = file_dframe[(file_dframe['data'] >= var_min)]
                        if var_max is not None:
                            file_dframe = file_dframe[(file_dframe['data'] <= var_max)]

                        if file_dframe is not None:
                            file_time_src = file_dframe.index.unique()
                        else:
                            file_time_src = None

                        file_obj = deepcopy(file_dframe)

                    elif file_path_src.endswith('.xlsx'):

                        # manage the active filename and dataframe
                        if self.file_name_active is None:
                            self.file_name_active = deepcopy(file_path_src)
                            file_dframe_active = None
                        elif self.file_name_active != file_path_src:
                            self.file_name_active = deepcopy(file_path_src)
                            file_dframe_active = None
                        elif self.file_name_active == file_path_src:
                            file_dframe_active = deepcopy(self.file_dframe_active)
                        else:
                            log_stream.error(' ===> Checking active data source mode is not supported')
                            raise NotImplemented('Case not implemented yet')

                        # Read datasets point file
                        file_dframe, file_dframe_active = read_file_xlsx(
                            file_path_src, datetime_step,
                            file_header=self.table_src['file_columns_src'],
                            scale_factor_longitude=self.table_src['file_scale_factor_longitude'],
                            scale_factor_latitude=self.table_src['file_scale_factor_latitude'],
                            scale_factor_data=self.table_src['file_scale_factor_data'],
                            file_dframe_active=file_dframe_active)

                        # Update active dataframe
                        if self.file_dframe_active is None:
                            self.file_dframe_active = deepcopy(file_dframe_active)

                        # Filter data using variable limits (if defined)
                        if var_min is not None:
                            file_dframe = file_dframe[(file_dframe['data'] >= var_min)]
                        if var_max is not None:
                            file_dframe = file_dframe[(file_dframe['data'] <= var_max)]

                        if file_dframe is not None:
                            file_time_src = file_dframe.index.unique()
                        else:
                            file_time_src = None

                        file_obj = deepcopy(file_dframe)

                    elif file_path_src.endswith('.tiff') or file_path_src.endswith('.tif'):

                        # Read datasets map file
                        file_darray, file_proj, file_transform = get_data_tiff(file_path_src)

                        # Filter data using variable limits (if defined)
                        if var_min is not None:
                            file_darray = file_darray.where(file_darray.values < var_min, file_darray.values, var_min)
                        if var_max is not None:
                            file_darray = file_darray.where(file_darray.values > var_max, file_darray.values, var_max)

                        if file_darray is not None:
                            file_time_src = pd.DatetimeIndex([datetime_step])
                        else:
                            file_time_src = None

                        file_obj = deepcopy(file_darray)
                    else:
                        log_stream.error(' ===> Source data format is not supported. Check your source datasets')
                        raise NotImplementedError('Only "csv" or "tiff" formats are available.')

                else:
                    file_obj = None
                    file_time_src = None
                    log_stream.warning(' ===> File datasets of rain weather stations is not available.')

                if (file_time_src is not None) and (file_time_src.__len__() > 1):
                    log_stream.warning(' ===> Time step selected are greater than 1. Errors could arise in the script')

                if file_obj is not None:

                    if isinstance(file_obj, pd.DataFrame):

                        log_stream.info(' ------> Interpolate points to map datasets ... ')

                        geoy_out_1d = data_geo_reference_grid['south_north'].values
                        geox_out_1d = data_geo_reference_grid['west_east'].values
                        mask_out_2d = data_geo_reference_grid.values
                        geox_out_2d, geoy_out_2d = np.meshgrid(geox_out_1d, geoy_out_1d)

                        map_out_2d = interpolate_rain_points2map(
                            file_obj, mask_out_2d, geox_out_2d, geoy_out_2d,
                            folder_tmp=self.folder_name_tmp)

                        log_stream.info(' ------> Interpolate points to map datasets ... DONE')

                    elif isinstance(file_obj, xr.DataArray):

                        log_stream.info(' ------> Reproject source datasets to map datasets ... ')

                        da_geoy_out = data_geo_reference_grid['south_north']
                        da_geox_out = data_geo_reference_grid['west_east']
                        da_mask_out = data_geo_reference_grid

                        map_out_2d = reproject_rain_source2map(
                            file_obj, da_mask_out, da_geox_out, da_geoy_out,
                            mask_out_condition=True,
                            interp_index=data_geo_region_index, interp_mode='numpy')
                        log_stream.info(' ------> Reproject source datasets to map datasets ... DONE')

                    else:
                        log_stream.error(' ===> Filename format is not allowed')
                        raise NotImplementedError('Format is not implemented yet')

                    log_stream.info(' ------> Check map datasets limits ... ')
                    if var_min is not None:
                        check_idxs_var_min = np.argwhere(map_out_2d.flatten() < var_min).tolist()
                    else:
                        check_idxs_var_min = []
                    if var_max is not None:
                        check_idxs_var_max = np.argwhere(map_out_2d.flatten() > var_max).tolist()
                    else:
                        check_idxs_var_max = []

                    if check_idxs_var_min.__len__() > 0:
                        log_stream.warning(' ===> Some values are less then the "var_min" limit ... ')
                        map_out_2d[map_out_2d < var_min] = var_min
                        log_stream.warning(' ===> Some values are less then the "var_min" limit ... '
                                           'FIX MAP VALUES TO MIN LIMIT')

                    if check_idxs_var_max.__len__() > 0:
                        logging.warning(' ===> Some values are greater then the "var_max" limit ...')
                        map_out_2d[map_out_2d > var_max] = var_max
                        log_stream.warning(' ===> Some values are less then the "var_max" limit ... '
                                           'FIX MAP VALUES TO MAX LIMIT')
                    log_stream.info(' ------> Check map datasets limits ... DONE')

                    log_stream.info(' ------> Save map datasets ... ')

                    folder_name_anc, file_name_anc = os.path.split(file_path_anc)
                    make_folder(folder_name_anc)

                    if file_path_anc.endswith('.nc'):

                        geoy_out_1d = data_geo_reference_grid['south_north'].values
                        geox_out_1d = data_geo_reference_grid['west_east'].values
                        mask_out_2d = data_geo_reference_grid.values

                        dset_out = create_dset(
                            map_out_2d,
                            mask_out_2d, geox_out_1d, geoy_out_1d,
                            var_data_time=datetime_step,
                            var_data_name=var_name,
                            var_geo_name='mask', var_data_attrs=None, var_geo_attrs=None,
                            coord_name_x='west_east', coord_name_y='south_north', coord_name_time='time',
                            dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                            dims_order_2d=None, dims_order_3d=None)

                        # CREATE DSET IS CHANGED CHECK WRITE DSET FOR LAT AND DATA DIRECTION
                        save_data_nc(file_path_anc, dset_out)

                        log_stream.info(' ------> Save map datasets ... DONE. [NETCDF]')

                    elif file_path_anc.endswith('.tiff'):

                        geoy_out_1d = data_geo_reference_grid['south_north'].values
                        geox_out_1d = data_geo_reference_grid['west_east'].values
                        mask_out_2d = data_geo_reference_grid.values
                        geox_out_2d, geoy_out_2d = np.meshgrid(geox_out_1d, geoy_out_1d)

                        # ERROR ON SERVER
                        # error in saving ERROR 1: Only OGC WKT Projections supported for writing to GeoTIFF.
                        # EPSG:4326 not supported.
                        save_data_tiff(file_path_anc,
                                       map_out_2d, geox_out_2d, geoy_out_2d,
                                       file_metadata=self.table_src['file_metadata'],
                                       file_epsg_code=self.table_src['file_epsg_code'])

                        log_stream.info(' ------> Save map datasets ... DONE. [GEOTIFF]')

                    else:
                        log_stream.info(' ------> Save map datasets ... FAILED')
                        log_stream.error(' ===> Filename format is not allowed')
                        raise NotImplementedError('Format is not implemented yet')

                    self.file_path_processed.append(file_path_anc)

                    log_stream.info(' -----> TimeStep: ' + str(datetime_step) + ' ... DONE')

                else:
                    log_stream.info(' -----> TimeStep: ' + str(datetime_step) + ' ... FAILED')
                    log_stream.warning(' ===> File datasets of rain weather stations is not available.')
            else:
                log_stream.info(' -----> TimeStep: ' + str(datetime_step) + ' ... PREVIOUSLY DONE')

        log_stream.info(' ----> Organize rain forcing ... DONE')

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
