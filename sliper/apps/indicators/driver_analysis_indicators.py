"""
Class Features

Name:          driver_analysis_indicators
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20200515'
Version:       '1.0.0'
"""

######################################################################################
# Library
import logging
import os

import numpy as np
import pandas as pd
import xarray as xr

from lib_data_io_pickle import write_obj
from lib_utils_time import find_time_maximum_delta, split_time_window, search_time_features
from lib_utils_system import fill_tags2string, make_folder
from lib_utils_io_obj import filter_obj_variables, filter_obj_datasets, create_dset

from lib_utils_data_point_soil_slips import select_point_by_time

from lib_utils_data_grid_rain import reproject_rain_source2map, reproject_rain_map2ts, \
    compute_rain_maps_accumulated, compute_rain_peaks, \
    compute_rain_ts_averaged, compute_rain_ts_accumulated, compute_rain_statistics
from lib_utils_data_grid_rain import get_data_tiff as get_data_tiff_rain
from lib_utils_data_grid_sm import reproject_sm_map2ts, compute_sm_statistics
from lib_utils_data_grid_sm import get_data_tiff as get_data_tiff_sm

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverAnalysis for indicators
class DriverAnalysis:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, time_step, anc_dict, dst_dict,
                 file_list_rain, file_list_sm,
                 alg_ancillary=None, alg_template_tags=None,
                 time_data=None,
                 collections_data_geo_grid=None,
                 collections_data_geo_pnt=None,
                 collections_data_group=None,
                 flag_time_rain='rain_data',
                 flag_time_soil_moisture='soil_moisture_data',
                 flag_data_anc_rain='rain_data', flag_data_anc_sm='soil_moisture_data',
                 flag_data_dst_indicators='indicators_data',
                 flag_dst_updating=True):

        self.time_step = pd.Timestamp(time_step)

        self.anc_dict = anc_dict
        self.dst_dict = dst_dict

        self.file_name_tag = 'file_name'
        self.folder_name_tag = 'folder_name'

        self.reference_tag = 'reference'
        self.region_tag = 'region'
        self.alert_area_tag = 'alert_area'

        self.region_pivot_name_primary = 'region:primary_data:rain_data'
        self.region_pivot_name_index = 'region:index_data:rain_data'
        self.alert_area_pivot_name_mask = 'alert_area:mask_data:{:}'
        self.alert_area_pivot_name_idx_grid = 'alert_area:index_grid_data:{:}'
        self.alert_area_pivot_name_idx_circle = 'alert_area:index_circle_data:{:}'

        self.flag_time_rain = flag_time_rain
        self.flag_time_soil_moisture = flag_time_soil_moisture

        self.flag_data_anc_rain = flag_data_anc_rain
        self.flag_data_anc_sm = flag_data_anc_sm

        self.flag_data_dst_indicators = flag_data_dst_indicators

        self.file_list_rain = file_list_rain
        self.file_list_sm = file_list_sm

        self.data_group = collections_data_group
        self.data_geo_reference_grid = collections_data_geo_grid[self.reference_tag]['reference']
        self.data_geo_region_grid = collections_data_geo_grid[self.region_tag][self.region_pivot_name_primary]
        self.data_geo_region_index = collections_data_geo_grid[self.region_tag][self.region_pivot_name_index]
        self.data_geo_pnt = collections_data_geo_pnt

        data_vars_alert_area_mask = filter_obj_variables(
            list(collections_data_geo_grid[self.alert_area_tag].keys()), self.alert_area_pivot_name_mask)
        self.data_geo_alert_area_mask = filter_obj_datasets(
            collections_data_geo_grid[self.alert_area_tag], data_vars_alert_area_mask)
        data_vars_alert_area_idx_grid = filter_obj_variables(
            list(collections_data_geo_grid[self.alert_area_tag].keys()), self.alert_area_pivot_name_idx_grid)
        self.data_geo_alert_area_idx_grid = filter_obj_datasets(
            collections_data_geo_grid[self.alert_area_tag], data_vars_alert_area_idx_grid)
        data_vars_alert_area_idx_circle = filter_obj_variables(
            list(collections_data_geo_grid[self.alert_area_tag].keys()), self.alert_area_pivot_name_idx_circle)
        self.data_geo_alert_area_idx_circle = filter_obj_datasets(
            collections_data_geo_grid[self.alert_area_tag], data_vars_alert_area_idx_circle)

        self.alg_template_tags = alg_template_tags

        self.time_data_rain = time_data[self.flag_time_rain]
        self.time_data_sm = time_data[self.flag_time_soil_moisture]

        self.var_name_x = 'west_east'
        self.var_name_y = 'south_north'

        self.file_name_anc_rain_raw = anc_dict[self.flag_data_anc_rain][self.file_name_tag]
        self.folder_name_anc_rain_raw = anc_dict[self.flag_data_anc_rain][self.folder_name_tag]
        self.file_name_anc_sm_raw = anc_dict[self.flag_data_anc_sm][self.file_name_tag]
        self.folder_name_anc_sm_raw = anc_dict[self.flag_data_anc_sm][self.folder_name_tag]

        self.file_name_dst_indicators_raw = dst_dict[self.flag_data_dst_indicators][self.file_name_tag]
        self.folder_name_dst_indicators_raw = dst_dict[self.flag_data_dst_indicators][self.folder_name_tag]

        self.flag_dst_updating = flag_dst_updating

        file_path_dst_indicators_expected = []
        for group_key in self.data_group.keys():

            file_path_dst_step = collect_file_list(
                time_step, self.folder_name_dst_indicators_raw, self.file_name_dst_indicators_raw,
                self.alg_template_tags, alert_area_name=group_key)[0]

            if self.flag_dst_updating:
                if os.path.exists(file_path_dst_step):
                    os.remove(file_path_dst_step)

            file_path_dst_indicators_expected.append(file_path_dst_step)

        self.file_path_dst_indicators_expected = file_path_dst_indicators_expected

        self.template_struct_ts_datasets = 'time_series_datasets'
        self.template_struct_ts_analysis = 'time_series_analysis'
        self.template_struct_ts_peaks = 'time_series_peaks'

        self.template_rain_ts_accumulated = 'rain_accumulated_{:}'
        self.template_rain_ts_avg = 'rain_average_{:}'
        self.template_rain_ts_peak = 'rain_peak_{:}'

        self.template_sm_point_first = 'sm_value_first'
        self.template_sm_point_last = 'sm_value_last'
        self.template_sm_point_max = 'sm_value_max'
        self.template_sm_point_avg = 'sm_value_avg'

        self.template_indicators_domain = 'domain'
        self.template_indicators_time = 'time'
        self.template_indicators_event = 'event'
        self.template_indicators_data = 'data'

        self.analysis_event_undefined = {'event_n': -9999, 'event_threshold': 'NA', 'event_index': -9999}
        self.analysis_event_not_found = {'event_n': 0, 'event_threshold': 'white', 'event_index': 0}

        self.tag_sep = ':'

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define time range
    @staticmethod
    def compute_time_range(time, time_period_lenght=1, time_period_type='both', time_frequency='H'):

        if time_period_type == 'left':

            time_start_left = pd.date_range(start=time, periods=2, freq=time_frequency)[1]
            time_end_right = time
            time_range = pd.date_range(start=time_start_left, periods=time_period_lenght, freq=time_frequency)

        elif time_period_type == 'right':

            time_start_left = pd.date_range(start=time, periods=2, freq=time_frequency)[1]
            time_end_right = time
            time_range = pd.date_range(end=time_end_right, periods=time_period_lenght, freq=time_frequency)

        elif time_period_type == 'both':

            time_start_left = pd.date_range(start=time, periods=2, freq=time_frequency)[1]
            time_end_right = time
            time_range_left = pd.date_range(start=time_start_left, periods=time_period_lenght, freq=time_frequency)
            time_range_right = pd.date_range(end=time_end_right, periods=time_period_lenght, freq=time_frequency)
            time_range = time_range_right.union(time_range_left)

        # case 'left' and 'right' -> time range from time_step_left and time_step_right (different length)
        elif time_period_type == ['left', 'right']:

            time_period_left, time_period_right = time_period_lenght[0], time_period_lenght[1]
            time_frequency_left, time_frequency_right = time_frequency[0], time_frequency[1]
            time_range_left = pd.date_range(end=time, periods=time_period_left, freq=time_frequency_left)
            time_range_right = pd.date_range(start=time, periods=time_period_right, freq=time_frequency_right)
            time_range = time_range_left.union(time_range_right)

        # case 'right' and 'left' -> time range from time_step_left and time_step_right (different length)
        elif time_period_type == ['right', 'left']:

            time_period_left, time_period_right = time_period_lenght[0], time_period_lenght[1]
            time_frequency_left, time_frequency_right = time_frequency[0], time_frequency[1]
            time_range_left = pd.date_range(end=time, periods=time_period_left, freq=time_frequency_left)
            time_range_right = pd.date_range(start=time, periods=time_period_right, freq=time_frequency_right)
            time_range = time_range_left.union(time_range_right)

        else:
            log_stream.error(' ===> Bad definition for time_period_type')
            raise NotImplementedError('Case not allowed.')

        return time_range

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to unpack analysis in dictionary format
    @staticmethod
    def unpack_analysis(data_obj, data_keys=None):

        data_dict = {}
        if data_keys is None:
            if isinstance(data_obj, pd.Series):
                data_keys = list(data_obj.index)
                create_dict = True
            elif isinstance(data_obj, dict):
                create_dict = False
            else:
                logging.error(' ===> DataType not allowed in case of keys are null')
                raise NotImplementedError('Case not implemented yet')
        elif data_keys is not None:
            if isinstance(data_obj, pd.Series):
                create_dict = True
            else:
                logging.error(' ===> DataType not allowed in case of keys are not null')
                raise NotImplementedError('Case not implemented yet')
        else:
            logging.error(' ===> Columns format not allowed')
            raise NotImplementedError('Case not implemented yet')

        if create_dict:
            for key in data_keys:
                values = data_obj[key]
                data_dict[key] = values
        else:
            data_dict = data_obj

        return data_dict

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to dump analysis
    def save_analysis(self, group_analysis_sm, group_analysis_rain, group_soil_slips):

        log_stream.info(' ----> Save analysis [' + str(self.time_step) + '] ... ')

        time_step = self.time_step
        group_data = self.data_group

        for group_key, group_items in group_data.items():

            log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... ')

            # debug for testing alert_area_c (some soilslips are defined for 2023-01-08 --> to test if getting data works fine)
            # group_key = 'alert_area_c'

            file_path_dst = collect_file_list(
                time_step, self.folder_name_dst_indicators_raw, self.file_name_dst_indicators_raw,
                self.alg_template_tags, alert_area_name=group_key)[0]

            if not os.path.exists(file_path_dst):

                if group_analysis_sm[group_key] is not None:
                    group_analysis_sm_select = group_analysis_sm[group_key][self.template_struct_ts_analysis]
                else:
                    group_analysis_sm_select = None

                if group_analysis_rain[group_key] is not None:
                    group_analysis_rain_select = group_analysis_rain[group_key][self.template_struct_ts_analysis]
                else:
                    group_analysis_rain_select = None

                if group_analysis_sm_select is not None:
                    group_soil_slips_select, \
                        time_soil_slips_select_from, time_soil_slips_select_to = select_point_by_time(
                            time_step, group_soil_slips[group_key])
                else:
                    group_soil_slips_select, time_soil_slips_select_from, time_soil_slips_select_to = None, None, None

                if (group_analysis_sm_select is not None) and (group_analysis_rain_select is not None):
                    analysis_sm = self.unpack_analysis(group_analysis_sm_select)
                    analysis_rain = self.unpack_analysis(group_analysis_rain_select)
                    analysis_data = {**analysis_sm, **analysis_rain}
                else:
                    analysis_data = None
                    if (group_analysis_sm_select is None) and (group_analysis_rain_select is not None):
                        log_stream.warning(' ===> "Soil moisture" datasets is undefined')
                    elif (group_analysis_rain_select is None) and (group_analysis_sm_select is not None):
                        log_stream.warning(' ===> "Rain" datasets is undefined')
                    else:
                        log_stream.warning(' ===> "Rain" and "Soil moisture" datasets are undefined')

                if group_soil_slips_select is not None:
                    analysis_event = self.unpack_analysis(group_soil_slips_select,
                                                          ['event_n', 'event_threshold', 'event_index'])
                else:
                    if (time_soil_slips_select_from is not None) and (time_soil_slips_select_to is not None):
                        if time_soil_slips_select_from <= time_step <= time_soil_slips_select_to:
                            analysis_event = self.analysis_event_not_found
                            log_stream.warning(' ===> Soil slips events are not selected for the analysis time step')
                        else:
                            analysis_event = self.analysis_event_undefined
                            log_stream.warning(' ===> Soil slips events are not defined for the analysis time step')
                    else:
                        log_stream.warning(' ===> Soil slips events period do not include the analysis time step')
                        analysis_event = self.analysis_event_undefined

                if (analysis_data is not None) and (analysis_event is not None):

                    analysis_obj = {
                        self.template_indicators_domain: group_key,
                        self.template_indicators_time: time_step,
                        self.template_indicators_data: analysis_data,
                        self.template_indicators_event: analysis_event}

                    folder_name_dst, file_name_dst = os.path.split(file_path_dst)
                    make_folder(folder_name_dst)

                    write_obj(file_path_dst, analysis_obj)

                    log_stream.info(' -----> Alert Area ' + group_key + ' ... DONE')

                else:

                    log_stream.info(' -----> Alert Area ' + group_key + ' ... SKIPPED. Some datasets are undefined')

            else:
                log_stream.info(' -----> Alert Area ' + group_key + ' ... SKIPPED. Analysis file created previously')

        log_stream.info(' ----> Save analysis [' + str(self.time_step) + '] ... DONE')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize analysis for soil moisture datasets
    def organize_analysis_sm(self, var_name='soil_moisture'):

        log_stream.info(' ----> Compute soil moisture analysis [' + str(self.time_step) + '] ... ')

        time_step = self.time_step

        group_data = self.data_group

        time_period_max, time_frequency_max, time_period_type = search_time_features(
            group_data, data_key='sm_datasets')

        data_geo_reference_grid = self.data_geo_reference_grid
        data_geo_alert_area_mask = self.data_geo_alert_area_mask
        data_geo_alert_area_idx_grid = self.data_geo_alert_area_idx_grid
        data_geo_region_grid = self.data_geo_region_grid
        data_geo_region_index = self.data_geo_region_index
        data_geo_pnt = self.data_geo_pnt

        geo_y_ref_1d = data_geo_reference_grid['south_north'].values
        geo_x_ref_1d = data_geo_reference_grid['west_east'].values
        mask_ref_2d = data_geo_reference_grid.values
        geo_x_ref_2d, geo_y_ref_2d = np.meshgrid(geo_x_ref_1d, geo_y_ref_1d)

        var_ts_group = {}
        for group_key, group_items in group_data.items():

            log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... ')

            file_path_dst = collect_file_list(
                time_step, self.folder_name_dst_indicators_raw, self.file_name_dst_indicators_raw,
                self.alg_template_tags, alert_area_name=group_key)[0]

            # create the reference geographical name
            name_key = self.alert_area_pivot_name_mask.format(group_key)

            # get the reference geographical datasets
            data_geo_reference_grid = data_geo_alert_area_mask[name_key]

            var_dset_obj = None
            if not os.path.exists(file_path_dst):

                # Get subdomain mask, longitudes and latitudes
                mask_out_da = data_geo_reference_grid
                geo_x_out_da = data_geo_reference_grid['west_east']
                geo_y_out_da = data_geo_reference_grid['south_north']
                mask_out_2d = data_geo_reference_grid.values
                geo_y_out_1d = geo_y_out_da.values
                geo_x_out_1d = geo_x_out_da.values
                geo_x_out_2d, geo_y_out_2d = np.meshgrid(geo_x_out_1d, geo_y_out_1d)

                # Get times features
                time_range = self.compute_time_range(time_step, time_period_max, time_period_type, time_frequency_max)

                # Get ancillary file list
                file_list_anc = collect_file_list(
                    time_range, self.folder_name_anc_sm_raw, self.file_name_anc_sm_raw,
                    self.alg_template_tags, alert_area_name=group_key)

                file_list_check, time_range_check = [], []
                for file_anc_step, timestamp_step in zip(file_list_anc, time_range):
                    if os.path.exists(file_anc_step):
                        file_list_check.append(file_anc_step)
                        time_range_check.append(timestamp_step)
                file_analysis = False
                if file_list_check.__len__() >= 1:
                    file_analysis = True

                if file_analysis:

                    var_ts_analysis = {}
                    if file_list_check[0].endswith('.nc'):
                        if var_dset_obj is None:
                            var_dset_obj = xr.open_mfdataset(file_list_check, combine='by_coords')
                    elif file_list_check[0].endswith('.tiff'):

                        if var_dset_obj is None:
                            data_in_3d = np.zeros(
                                shape=[geo_x_out_2d.shape[0], geo_y_out_2d.shape[1], file_list_check.__len__()])
                            data_in_3d[:, :, :] = np.nan
                            data_time = []
                            for file_id, (file_name_step, time_stamp_step) in enumerate(zip(file_list_check, time_range)):
                                da_in_2d, proj, geotrans = get_data_tiff_sm(file_name_step, file_mandatory=False)

                                if da_in_2d is not None:
                                    data_in_2d = da_in_2d.values
                                    data_in_2d[mask_out_2d == 0] = np.nan

                                    data_in_3d[:, :, file_id] = data_in_2d
                                    data_time.append(time_stamp_step)

                            var_dset_obj = create_dset(
                                data_in_3d, mask_out_da, geo_x_out_2d, geo_y_out_2d,
                                var_data_time=data_time, var_data_name=var_name,
                                var_geo_name='mask', var_data_attrs=None, var_geo_attrs=None,
                                coord_name_x='west_east', coord_name_y='south_north', coord_name_time='time',
                                dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                                dims_order_2d=None, dims_order_3d=None)

                    else:
                        log_stream.error(' ===> Filename format is not allowed')
                        raise NotImplementedError('Format is not implemented yet')

                    var_da_maps = var_dset_obj[var_name]
                    var_ts_datasets = reproject_sm_map2ts(var_da_maps, column_name=self.template_struct_ts_datasets)

                    # Debug
                    # plt.figure()
                    # plt.imshow(var_da_maps.values[:, :, 0])
                    # plt.colorbar()
                    # plt.show()

                    var_value_first, var_value_last, \
                        var_value_avg, var_value_max, var_value_min = compute_sm_statistics(
                            var_ts_datasets, column_name=self.template_struct_ts_datasets,
                            tag_first_value=True, tag_last_value=True,
                            tag_avg_value=True, tag_max_value=True, tag_min_value=True)

                    var_ts_analysis[self.template_sm_point_avg] = var_value_avg
                    var_ts_analysis[self.template_sm_point_max] = var_value_max
                    var_ts_analysis[self.template_sm_point_first] = var_value_first
                    var_ts_analysis[self.template_sm_point_last] = var_value_last

                    var_ts_collections = {self.template_struct_ts_datasets: var_ts_datasets,
                                          self.template_struct_ts_analysis: var_ts_analysis}

                    log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... DONE')

                else:
                    var_ts_collections = None
                    log_stream.warning(' ===> Datasets are not available')
                    log_stream.info(' -----> Get datasets for reference area "' + group_key +
                                    '" ... SKIPPED. Datasets are not available.')

                var_ts_group[group_key] = var_ts_collections

            else:
                log_stream.info(' -----> Get datasets for reference area "' + group_key +
                                '" ... SKIPPED. Analysis file created previously')

        log_stream.info(' ----> Compute soil moisture analysis [' + str(self.time_step) + '] ... DONE')

        return var_ts_group

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize analysis for rain datasets
    def organize_analysis_rain(self, var_name='rain'):

        log_stream.info(' ----> Compute rain analysis [' + str(self.time_step) + '] ... ')

        time_step = self.time_step

        group_data = self.data_group

        data_geo_reference_grid = self.data_geo_reference_grid
        data_geo_alert_area_mask = self.data_geo_alert_area_mask
        data_geo_alert_area_idx_grid = self.data_geo_alert_area_idx_grid
        data_geo_alert_area_idx_circle = self.data_geo_alert_area_idx_circle
        data_geo_region_grid = self.data_geo_region_grid
        data_geo_region_index = self.data_geo_region_index
        data_geo_pnt = self.data_geo_pnt

        geo_y_ref_1d = data_geo_reference_grid['south_north'].values
        geo_x_ref_1d = data_geo_reference_grid['west_east'].values
        mask_ref_2d = data_geo_reference_grid.values
        geo_x_ref_2d, geo_y_ref_2d = np.meshgrid(geo_x_ref_1d, geo_y_ref_1d)

        var_ts_group, var_dset_obj = {}, None
        for group_key, group_items in group_data.items():

            log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... ')

            file_path_dest = collect_file_list(
                time_step, self.folder_name_dst_indicators_raw, self.file_name_dst_indicators_raw,
                self.alg_template_tags, alert_area_name=group_key)[0]

            # create the reference geographical name
            name_key_mask = self.alert_area_pivot_name_mask.format(group_key)
            name_key_idx_circle = self.alert_area_pivot_name_idx_circle.format(group_key)

            # get the reference geographical datasets
            data_geo_reference_mask = data_geo_alert_area_mask[name_key_mask]
            data_geo_reference_idx_circle = data_geo_alert_area_idx_circle[name_key_idx_circle]

            if not os.path.exists(file_path_dest):

                # Get subdomain mask, longitudes and latitudes
                mask_out_da = data_geo_reference_mask
                geo_x_out_da = data_geo_reference_mask['west_east']
                geo_y_out_da = data_geo_reference_mask['south_north']
                geo_y_out_1d = geo_y_out_da.values
                geo_x_out_1d = geo_x_out_da.values
                geo_x_out_2d, geo_y_out_2d = np.meshgrid(geo_x_out_1d, geo_y_out_1d)

                # Get times features
                time_delta_max = find_time_maximum_delta(group_items['rain_datasets']['search_period'])
                time_period_type = group_items['rain_datasets']['search_type'][0]
                time_period_max, time_frequency_max = split_time_window(time_delta_max)
                time_range = self.compute_time_range(time_step, time_period_max, time_period_type, time_frequency_max)

                # Get ancillary file list
                file_list_anc = collect_file_list(
                    time_range, self.folder_name_anc_rain_raw, self.file_name_anc_rain_raw,
                    self.alg_template_tags)

                file_analysis = True
                for file_anc_step in file_list_anc:
                    if not os.path.exists(file_anc_step):
                        logging.warning(' ===> Filename ' + file_anc_step + ' does not exist')
                        file_analysis = False
                        break

                if file_analysis:

                    if file_list_anc[0].endswith('.nc'):
                        if var_dset_obj is None:
                            var_dset_obj = xr.open_mfdataset(file_list_anc, combine='by_coords')
                    elif file_list_anc[0].endswith('.tiff'):

                        if file_list_anc.__len__() > 1:

                            if var_dset_obj is None:
                                data_in_3d = np.zeros(
                                    shape=[geo_x_ref_2d.shape[0], geo_y_ref_2d.shape[1], file_list_anc.__len__()])
                                data_in_3d[:, :, :] = np.nan
                                data_time = []
                                for file_id, (file_name_step, time_stamp_step) in enumerate(zip(file_list_anc, time_range)):
                                    da_in_2d, proj, geotrans = get_data_tiff_rain(file_name_step)
                                    data_in_2d = da_in_2d.values
                                    data_in_2d[mask_ref_2d == 0] = np.nan
                                    data_in_3d[:, :, file_id] = data_in_2d
                                    data_time.append(time_stamp_step)

                                var_dset_obj = create_dset(
                                    data_in_3d, mask_ref_2d, geo_x_ref_2d, geo_y_ref_2d,
                                    var_data_time=data_time, var_data_name=var_name,
                                    var_geo_name='mask', var_data_attrs=None, var_geo_attrs=None,
                                    coord_name_x='west_east', coord_name_y='south_north', coord_name_time='time',
                                    dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                                    dims_order_2d=None, dims_order_3d=None)

                        else:
                            log_stream.error(' ===> Length of file list is not allowed')
                            raise NotImplementedError('Case is not implemented yet')

                    else:
                        log_stream.error(' ===> Filename format is not allowed')
                        raise NotImplementedError('Format is not implemented yet')

                    # Compute maps values
                    var_da_obj = var_dset_obj[var_name]

                    # Compute maps values
                    var_da_maps = reproject_rain_source2map(
                        var_da_obj, mask_out_da, geo_x_out_da, geo_y_out_da, mask_out_condition=True)

                    # Compute time-series values
                    var_ts_datasets = reproject_rain_map2ts(var_da_maps, column_name=self.template_struct_ts_datasets)

                    var_ts_analysis, var_ts_peaks = {}, {}
                    for time_window in group_items['rain_datasets']['search_period']:

                        log_stream.info(' ------> Compute analysis over time "' + time_window + '" window ... ')

                        # Compute maps accumulated values and peaks
                        log_stream.info(' -------> Rain peaks ... ')
                        tag_ts_series_peaks = self.template_rain_ts_peak.format(time_window)

                        var_map_accumulated = compute_rain_maps_accumulated(
                            var_da_maps, time_window=time_window, time_direction='right')

                        var_value_peak, var_dframe_peaks = compute_rain_peaks(
                            var_map_accumulated, data_geo_reference_idx_circle)
                        var_ts_peaks[tag_ts_series_peaks] = var_dframe_peaks
                        var_ts_analysis[tag_ts_series_peaks] = var_value_peak
                        log_stream.info(' -------> Rain peaks ... DONE')

                        # Compute time-series averaged values
                        log_stream.info(' -------> Rain averaged time-series  ... ')
                        tag_ts_series_avg = self.template_rain_ts_avg.format(time_window)
                        var_ts_series_avg = compute_rain_ts_averaged(
                            var_ts_datasets, column_name=self.template_struct_ts_datasets,
                            time_window=time_window, time_direction='right')
                        var_ts_datasets[tag_ts_series_avg] = var_ts_series_avg

                        var_value_first, var_value_last, \
                            var_value_avg, var_value_max, var_value_min = compute_rain_statistics(
                                var_ts_datasets, column_name=tag_ts_series_avg,
                                tag_first_value=False, tag_last_value=False,
                                tag_avg_value=True, tag_max_value=False, tag_min_value=True)
                        var_ts_analysis[tag_ts_series_avg] = var_value_avg
                        log_stream.info(' -------> Rain averaged time-series  ... DONE')

                        # Compute time-series accumulated values
                        log_stream.info(' -------> Rain accumulated time-series  ... ')
                        tag_ts_series_accumulated = self.template_rain_ts_accumulated.format(time_window)
                        var_ts_series_accumulated = compute_rain_ts_accumulated(
                            var_ts_datasets, column_name=self.template_struct_ts_datasets,
                            time_window=time_window, time_direction='right')
                        var_ts_datasets[tag_ts_series_accumulated] = var_ts_series_accumulated

                        var_value_first, var_value_last, \
                            var_value_avg, var_value_max, var_value_min = compute_rain_statistics(
                                var_ts_datasets, column_name=tag_ts_series_accumulated,
                                tag_first_value=False, tag_last_value=False,
                                tag_avg_value=False, tag_max_value=True, tag_min_value=True)
                        var_ts_analysis[tag_ts_series_accumulated] = var_value_max
                        log_stream.info(' -------> Rain accumulated time-series  ... DONE')

                        log_stream.info(' ------> Compute analysis over time "' + time_window + '" window ... DONE')

                    var_ts_collections = {self.template_struct_ts_datasets: var_ts_datasets,
                                          self.template_struct_ts_analysis: var_ts_analysis,
                                          self.template_struct_ts_peaks: var_ts_peaks}

                    log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... DONE')

                else:
                    var_ts_collections = None
                    log_stream.warning(' ===> Datasets are not available')
                    log_stream.info(' -----> Get datasets for reference area "' + group_key +
                                    '" ... SKIPPED. Datasets are not available.')

                var_ts_group[group_key] = var_ts_collections

            else:
                log_stream.info(' -----> Get datasets for reference area "' + group_key +
                                '" ... SKIPPED. Analysis file created previously')

        log_stream.info(' ----> Compute rain analysis [' + str(self.time_step) + '] ... DONE')

        return var_ts_group

    # -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Method to collect ancillary file
def collect_file_list(time_range, folder_name_raw, file_name_raw, template_tags, alert_area_name=None):
    if (not isinstance(time_range, pd.DatetimeIndex)) and (isinstance(time_range, pd.Timestamp)):
        time_range = pd.DatetimeIndex([time_range])

    file_name_list = []
    for datetime_step in time_range:
        template_values_step = {
            'alert_area_name': alert_area_name,
            'source_rain_datetime': datetime_step, 'source_rain_sub_path_time': datetime_step,
            'source_sm_datetime': datetime_step, 'source_sm_sub_path_time': datetime_step,
            'ancillary_rain_datetime': datetime_step, 'ancillary_rain_sub_path_time': datetime_step,
            'ancillary_sm_datetime': datetime_step, 'ancillary_sm_sub_path_time': datetime_step,
            'destination_indicators_datetime': datetime_step, 'destination_indicators_sub_path_time': datetime_step
        }

        folder_name_def = fill_tags2string(folder_name_raw, template_tags, template_values_step)
        file_name_def = fill_tags2string(file_name_raw, template_tags, template_values_step)

        file_path_def = os.path.join(folder_name_def, file_name_def)

        file_name_list.append(file_path_def)

    return file_name_list

# -------------------------------------------------------------------------------------
