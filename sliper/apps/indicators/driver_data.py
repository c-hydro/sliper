"""
Class Features

Name:          driver_data
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250618'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os

import numpy as np
import pandas as pd
import xarray as xr

from copy import deepcopy

from lib_data_io_csv import write_file_csv
from lib_data_io_pickle import read_obj, write_obj
from lib_data_io_tiff import read_file_tiff

from lib_utils_time import divide_time_range

from lib_utils_data_analysis import compute_data_metrics
from lib_utils_geo import resample_data, mask_data, transform_data2ts
from lib_utils_generic import fill_template_string, create_darray
from lib_utils_data_indicators import search_period_by_type, search_window_by_type
from lib_utils_generic import extract_subpart, dict2flat, flat2dict, fields2dict

from lib_utils_data_rain import (compute_rain_maps_accumulated, compute_rain_peaks,
                                 compute_rain_ts_averaged, compute_rain_ts_accumulated)

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Class DriverData
class DriverData:

    # ------------------------------------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, time_run, time_range,
                 src_dict, anc_dict, dst_dict,
                 tags_dict=None,
                 collections_data_geo_grid_ref=None,
                 collections_data_geo_grid_other=None,
                 collections_data_geo_pnt_other=None,
                 collections_data_group=None,
                 flag_update_anc_grid=True, flag_update_anc_ts=True,
                 flag_update_dst=True):

        self.time_run = pd.Timestamp(time_run)
        self.time_range = time_range

        self.src_dict = src_dict
        self.anc_dict = anc_dict
        self.dst_dict = dst_dict
        self.tags_dict = tags_dict

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.format_tag, self.type_tag = 'format', 'type'
        self.var_name_x_tag, self.var_name_y_tag = 'longitude', 'latitude'

        self.reference_tag = 'reference'
        self.alert_area_tag = 'alert_area'

        # get geographical information
        self.data_group = collections_data_group
        self.data_geo_grid_ref = collections_data_geo_grid_ref
        self.data_geo_grid_other = collections_data_geo_grid_other
        self.data_geo_pnt_other = collections_data_geo_pnt_other

        # get file names and folder names source
        self.file_name_src = src_dict[self.file_name_tag]
        self.folder_name_src = src_dict[self.folder_name_tag]
        self.path_name_src = os.path.join(self.folder_name_src, self.file_name_src)
        self.format_src = src_dict[self.format_tag]
        self.type_src = src_dict[self.type_tag]

        # get file names and folder names ancillary grid
        self.file_name_anc_grid = anc_dict['grid'][self.file_name_tag]
        self.folder_name_anc_grid = anc_dict['grid'][self.folder_name_tag]
        self.path_name_anc_grid = os.path.join(self.folder_name_anc_grid, self.file_name_anc_grid)
        # get file names and folder names ancillary ts
        self.file_name_anc_ts = anc_dict['ts'][self.file_name_tag]
        self.folder_name_anc_ts = anc_dict['ts'][self.folder_name_tag]
        self.path_name_anc_ts = os.path.join(self.folder_name_anc_ts, self.file_name_anc_ts)

        # get file names and folder names destination
        self.file_name_dst = dst_dict[self.file_name_tag]
        self.folder_name_dst = dst_dict[self.folder_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        self.format_dst = dst_dict[self.format_tag]
        self.type_dst = dst_dict[self.type_tag]

        # flags to update datasets ancillary and destination
        self.flag_update_anc_grid = flag_update_anc_grid
        self.flag_update_anc_ts = flag_update_anc_ts
        self.flag_update_dst = flag_update_dst

        # template for time series analysis
        self.template_struct_ts_datasets = 'time_series_datasets'
        self.template_struct_ts_analysis = 'time_series_analysis'
        self.template_struct_ts_peaks = 'time_series_peaks'

        self.template_ts_accumulated = 'rain_accumulated_{:}'
        self.template_ts_avg = 'rain_average_{:}'
        self.template_ts_peak = 'rain_peak_{:}'

        self.tag_sep = ':'

        self.group_name = extract_subpart(self.data_group, 'name')
        self.group_dset_period = extract_subpart(self.data_group, ['datasets', 'search_period'])
        self.group_dset_type = extract_subpart(self.data_group, ['datasets', 'search_type'])

        self.group_time_windows = search_window_by_type(self.group_dset_type, self.group_dset_period)

        self.group_time_types = search_period_by_type(self.group_dset_type, self.group_dset_period)
        self.group_time_periods = divide_time_range(
            time_range,
            ref_time=time_run, ref_frequency='H',
            observed_hours=self.group_time_types['max_search_period_left'],
            forecast_hours=self.group_time_types['max_search_period_right'],
            observed_label='Observed', forecast_label='Forecast')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to dump data
    def dump_data(self, time_run: pd.Timestamp, analysis_collections: dict, var_name='rain'):

        # info method start
        log_stream.info(' ----> Dump dynamic data [' + str(time_run) + '] ... ')

        # get group information
        group_info = self.data_group
        group_time_types = self.group_time_types
        group_time_periods = self.group_time_periods
        group_time_windows = self.group_time_windows

        # get time information
        time_start, time_end = self.time_range[0], self.time_range[-1]

        # iterate over group(s)
        for group_key, group_items in group_info.items():

            # info dump alart area information start
            log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... ')

            # define destination path names
            path_name_dst = fill_template_string(
                template_str=deepcopy(self.path_name_dst),
                template_map=self.tags_dict,
                value_map={'destination_sub_path_time': time_run, "destination_datetime_run": time_run,
                           'destination_datetime_start': time_start, 'destination_datetime_end': time_end,
                           'alert_area_name': group_key})

            # apply flags (to update datasets source and destination)
            if self.flag_update_anc_grid:
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)
            if self.flag_update_anc_ts:
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)
            if self.flag_update_dst:
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)

            # check if destination file exists
            if not os.path.exists(path_name_dst):

                # time periods type (observed or forecast)
                group_collections = pd.DataFrame()
                for sub_time_periods in group_time_periods.to_dict(orient="records"):

                    # get sub time start and end
                    period_tag, period_type = sub_time_periods['period_tag'], sub_time_periods['period_type']
                    period_id = sub_time_periods['period_id']
                    period_time_key = sub_time_periods['time_key']
                    period_time_start, period_time_end = sub_time_periods['time_start'], sub_time_periods['time_end']
                    period_time_range = sub_time_periods['time_range']

                    # info source data start
                    log_stream.info(' ------> Type "' + period_tag + '" ... ')

                    # get period datasets
                    period_collections = analysis_collections[period_tag][group_key]

                    # get information about group
                    group_analysis = period_collections['time_series_analysis']
                    group_datasets = period_collections['time_series_datasets']
                    group_peaks = period_collections['time_series_peaks']

                    # convert group datasets to flat dictionary
                    group_obj = dict2flat(group_analysis, sep=self.tag_sep)

                    # add extra fields to analysis object
                    group_dict = fields2dict(
                        group_obj,
                        extra_fields={
                            'time_run': period_time_key, 'time_start': period_time_start, 'time_end': period_time_end,
                            'period_tag': period_tag, 'period_type': period_type, 'period_id': period_id},
                        extra_formats={
                            'time_run': '%Y-%m-%d', 'time_start': '%Y-%m-%d %H:%M', 'time_end': '%Y-%m-%d %H:%M'})

                    # create a DataFrame with the group data
                    group_timestamp = pd.Timestamp(period_time_key)
                    group_row = pd.DataFrame([group_dict], index=[group_timestamp])
                    group_row.index.name = 'time'

                    # Append the new row to the empty DataFrame
                    group_collections = pd.concat([group_collections, group_row])

                    # info source data start
                    log_stream.info(' ------> Type "' + period_tag + '" ... DONE')

                # dump datasets
                folder_name_dst, file_name_dst = os.path.split(path_name_dst)
                os.makedirs(folder_name_dst, exist_ok=True)

                write_file_csv(group_collections, filename=path_name_dst, orientation='cols')

                # info dump alart area information end (DONE)
                log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... DONE')

            else:
                # info dump alart area information end (SKIPPED)
                log_stream.info(' -----> Save datasets for reference area "' + group_key +
                                '" SKIPPED. Analysis file created previously')

        # info method end
        log_stream.info(' ----> Dump dynamic data [' + str(time_run) + '] ... DONE')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to analyze data
    def analyze_data(self, time_run: pd.Timestamp, data_collections: dict, var_name='rain'):

        # info method start
        log_stream.info(' ----> Analyze dynamic data [' + str(time_run) + '] ... ')

        # get group information
        group_info = self.data_group
        group_time_types = self.group_time_types
        group_time_periods = self.group_time_periods
        group_time_windows = self.group_time_windows

        # get geographical information
        data_geo_grid_reference = self.data_geo_grid_ref
        data_geo_grid_other = self.data_geo_grid_other
        data_geo_pnt_other = self.data_geo_pnt_other

        # get geographical reference datasets
        geo_y_ref_1d = data_geo_grid_reference['geo_dst']['latitude'].values
        geo_x_ref_1d = data_geo_grid_reference['geo_dst']['longitude'].values
        mask_ref_2d = data_geo_grid_reference['geo_dst'].values
        geo_x_ref_2d, geo_y_ref_2d = np.meshgrid(geo_x_ref_1d, geo_y_ref_1d)

        # time periods type (observed or forecast)
        analysis_collections = {}
        for sub_time_periods in group_time_periods.to_dict(orient="records"):

            # get sub time start and end
            period_tag, period_type = sub_time_periods['period_tag'], sub_time_periods['period_type']
            period_time_start, period_time_end = sub_time_periods['time_start'], sub_time_periods['time_end']
            period_time_range = sub_time_periods['time_range']

            # get period datasets
            period_collections = data_collections[period_tag]

            # info source data start
            log_stream.info(' -----> Type "' + period_tag + '" ... ')

            # define ancillary path names (ts)
            path_name_anc_ts = fill_template_string(
                template_str=deepcopy(self.path_name_anc_ts),
                template_map=self.tags_dict,
                value_map={'ancillary_sub_path_time': time_run, "ancillary_datetime_run": time_run,
                           "ancillary_datetime_start": period_time_start, "ancillary_datetime_end": period_time_end})

            # apply flags (to update datasets source and destination)
            if self.flag_update_anc_ts:
                if os.path.exists(path_name_anc_ts):
                    os.remove(path_name_anc_ts)

            # check if ancillary file exists
            if not os.path.exists(path_name_anc_ts):

                # info get datasets start
                log_stream.info(' ------> Get datasets ... ')

                # iterate over group(s)
                group_ts_collections = {}
                for group_key, group_items in group_info.items():

                    # debug
                    #group_key = 'alert_area_c'  # debug for testing alert_area_c (some soilslips are defined for 2023-01-08)

                    # info analysis start
                    log_stream.info(' ------> Reference area "' + group_key + '" ... ')

                    # get geographical datasets
                    geo_da = data_geo_grid_other[group_key]['geo_dst']
                    geo_idx_array = data_geo_grid_other[group_key]['index_array']
                    geo_idx_valid_output = data_geo_grid_other[group_key]['valid_output_index']
                    geo_pnt = data_geo_pnt_other[group_key]

                    # get datasets information
                    if period_type == 'Observed':
                        var_time_window = group_time_windows['left']
                    elif period_type == 'Forecast':
                        var_time_window = group_time_windows['right']
                    else:
                        # error message (period type is not allowed)
                        log_stream.error(' ===> Period type "' + period_type + '" is not allowed')
                        raise NotImplementedError('Period type is not implemented yet')

                    # extract data array values
                    var_da_raw = deepcopy(period_collections)
                    # resample data to geographical reference
                    var_da_resampled = resample_data(
                        var_da_raw, geo_da, name_da=var_name, method='nearest',
                        index_array=geo_idx_array, valid_output_index=geo_idx_valid_output)
                    # mask data with geographical mask
                    var_da_masked = mask_data(var_da_resampled, geo_da, mask_value=1)

                    ''' debug
                    plt.figure()
                    plt.imshow(var_da_raw.values[:, :, 5])
                    plt.colorbar()
    
                    plt.figure()
                    plt.imshow(var_da_resampled.values[:, :, 5])
                    plt.colorbar()
    
                    plt.figure()
                    plt.imshow(geo_da.values)
                    plt.colorbar()
    
                    plt.figure()
                    plt.imshow(var_da_masked.values[:, :, 5])
                    plt.colorbar()
    
                    plt.show(block=False)
                    '''

                    # transform data to time-series
                    var_ts_data = transform_data2ts(var_da_masked, column_name=self.template_struct_ts_datasets)

                    # iterate over expected time windows
                    var_ts_analysis, var_ts_peaks = {}, {}
                    for time_window in var_time_window:

                        # info analysis start
                        log_stream.info(' -------> Compute analysis over time "' + time_window + '" window ... ')

                        # compute maps accumulated values and peaks
                        log_stream.info(' --------> Rain peaks ... ')
                        tag_ts_peaks = self.template_ts_peak.format(time_window)

                        # compute rain accumulated map (over defined time window)
                        var_map_accumulated = compute_rain_maps_accumulated(
                            var_da_masked, time_window=time_window, time_direction='right')

                        # computer rain peaks
                        var_dframe_peaks, var_metrics_peaks = compute_rain_peaks(
                            var_map_accumulated, geo_pnt)
                        var_ts_peaks[tag_ts_peaks] = var_dframe_peaks
                        var_ts_analysis[tag_ts_peaks] = var_metrics_peaks
                        log_stream.info(' --------> Rain peaks ... DONE')

                        # compute time-series data averaged values
                        log_stream.info(' --------> Rain averaged time-series  ... ')
                        tag_ts_avg = self.template_ts_avg.format(time_window)
                        var_ts_avg = compute_rain_ts_averaged(
                            var_ts_data, column_name=self.template_struct_ts_datasets,
                            time_window=time_window, time_direction='right')
                        var_ts_data[tag_ts_avg] = var_ts_avg

                        # compute time-series data statistics
                        var_metrics_avg = compute_data_metrics(
                            var_ts_data, column_name=tag_ts_avg,
                            metrics=['avg', 'max', 'min'])
                        var_ts_analysis[tag_ts_avg] = var_metrics_avg
                        log_stream.info(' --------> Rain averaged time-series  ... DONE')

                        # Compute time-series accumulated values
                        log_stream.info(' --------> Rain accumulated time-series  ... ')

                        tag_ts_accumulated = self.template_ts_accumulated.format(time_window)
                        var_ts_accumulated = compute_rain_ts_accumulated(
                            var_ts_data, column_name=self.template_struct_ts_datasets,
                            time_window=time_window, time_direction='right')
                        var_ts_data[tag_ts_accumulated] = var_ts_accumulated

                        var_metrics_accumulated = compute_data_metrics(
                            var_ts_data, column_name=tag_ts_accumulated,
                            metrics=['max', 'min'])
                        var_ts_analysis[tag_ts_accumulated] = var_metrics_accumulated

                        log_stream.info(' --------> Rain accumulated time-series  ... DONE')

                        # info analysis end
                        log_stream.info(' -------> Compute analysis over time "' + time_window + '" window ... DONE')

                    # organize time-series collections
                    var_ts_collections = {self.template_struct_ts_datasets: var_ts_data,
                                          self.template_struct_ts_analysis: var_ts_analysis,
                                          self.template_struct_ts_peaks: var_ts_peaks}

                    # add time-series collections to group
                    group_ts_collections[group_key] = var_ts_collections

                    # info analysis end
                    log_stream.info(' ------> Reference area "' + group_key + '" ... DONE')

                # info get datasets end
                log_stream.info(' ------> Get datasets ... DONE')

                # info save datasets start
                log_stream.info(' ------> Save datasets ... ')

                # define path name for time-series collections
                folder_name_anc, file_name_anc = os.path.split(path_name_anc_ts)
                os.makedirs(folder_name_anc, exist_ok=True)
                # save time-series collections in workspace object
                write_obj(path_name_anc_ts, group_ts_collections)

                # info save datasets end
                log_stream.info(' ------> Save datasets ... DONE')

            else:
                # info data already exists
                group_ts_collections = read_obj(path_name_anc_ts)

            # store data collections by the time period tag
            analysis_collections[period_tag] = group_ts_collections

            # info source data end
            log_stream.info(' -----> Type "' + period_tag + '" ... DONE')

        # info method end
        log_stream.info(' ----> Analyze dynamic data [' + str(time_run) + '] ... DONE')

        return analysis_collections
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize data
    def organize_data(self, time_run: pd.Timestamp, time_all=True, var_name='rain', ):

        # info method start
        log_stream.info(' ----> Organize dynamic data [' + str(time_run) + '] ... ')

        # get group information
        group_info = self.data_group
        group_time_type = self.group_time_types
        group_time_periods = self.group_time_periods

        # define common time start and end
        common_time_start, common_time_end = self.time_range[0], self.time_range[-1]

        # get geographical information
        data_geo_grid_reference = self.data_geo_grid_ref
        data_geo_grid_other = self.data_geo_grid_other
        data_geo_pnt_other = self.data_geo_pnt_other

        # get geographical reference datasets
        geo_y_ref_1d = data_geo_grid_reference['geo_dst']['latitude'].values
        geo_x_ref_1d = data_geo_grid_reference['geo_dst']['longitude'].values
        mask_ref_2d = data_geo_grid_reference['geo_dst'].values
        geo_x_ref_2d, geo_y_ref_2d = np.meshgrid(geo_x_ref_1d, geo_y_ref_1d)

        # time periods type (observed or forecast)
        datasets_collections = {}
        for sub_time_periods in group_time_periods.to_dict(orient="records"):

            # get sub time start and end
            period_tag = sub_time_periods['period_tag']
            period_time_start, period_time_end = sub_time_periods['time_start'], sub_time_periods['time_end']
            period_time_range = sub_time_periods['time_range']

            # info source data start
            log_stream.info(' -----> Type "' + period_tag + '" ... ')

            # define ancillary path names (grid)
            path_name_anc_grid = fill_template_string(
                template_str=deepcopy(self.path_name_anc_grid),
                template_map=self.tags_dict,
                value_map={'ancillary_sub_path_time': time_run, "ancillary_datetime_run": time_run,
                           "ancillary_datetime_start": period_time_start, "ancillary_datetime_end": period_time_end})

            # define ancillary path names (ts)
            path_name_anc_ts = fill_template_string(
                template_str=deepcopy(self.path_name_anc_ts),
                template_map=self.tags_dict,
                value_map={'ancillary_sub_path_time': time_run, "ancillary_datetime_run": time_run,
                           "ancillary_datetime_start": period_time_start, "ancillary_datetime_end": period_time_end})

            # apply flags (to update datasets source and destination)
            if self.flag_update_anc_grid:
                if os.path.exists(path_name_anc_grid):
                    os.remove(path_name_anc_grid)
                if os.path.exists(path_name_anc_ts):
                    os.remove(path_name_anc_ts)
            if self.flag_update_anc_ts:
                if os.path.exists(path_name_anc_ts):
                    os.remove(path_name_anc_ts)

            # check if ancillary file exists
            if not os.path.exists(path_name_anc_grid) or not os.path.exists(path_name_anc_ts):

                # info check data start
                log_stream.info(' ------> Check source data ... ')

                # check path list source
                check_list_src, path_list_src, data_da = True, [], None
                for period_time_step in period_time_range:

                    # info time start
                    log_stream.info(' -------> Time ' + str(period_time_step) + ' ... ')

                    # get path(s) index circle
                    path_name_src = fill_template_string(
                        template_str=deepcopy(self.path_name_src),
                        template_map=self.tags_dict,
                        value_map={'source_sub_path_time': period_time_step, "source_datetime": period_time_step})

                    # check if path name source exists
                    if not os.path.exists(path_name_src):
                        # check if time_all is True
                        if time_all:
                            # file does not exist, and time_all is False
                            log_stream.error(' ===> Filename ' + path_name_src + ' does not exist')
                            # file does not exist, but time_all is True
                            log_stream.info(' -------> Time ' + str(period_time_step) + ' ... FAILED')

                            # info data end
                            check_list_src = False
                            break
                        else:
                            # file does not exist, and time_all is False
                            log_stream.warning(' ===> Filename ' + path_name_src + ' does not exist')
                            # info time start
                            log_stream.info(' -------> Time ' + str(period_time_step) + ' ... SKIPPED')

                    else:
                        path_list_src.append(path_name_src)
                        # info time start
                        log_stream.info(' -------> Time ' + str(period_time_step) + ' ... DONE')

                # info check data end
                if check_list_src:
                    log_stream.info(' ------> Check source data ... DONE')
                else:
                    log_stream.info(' ------> Check source data ... FAILED')
                    # error message (format is not allowed)
                    log_stream.error(' ===> All source files must be available')
                    raise IOError('One or more source files are not available. Check your datasets')

                # info merge data start
                log_stream.info(' ------> Merge source data ... ')

                # merge path list source
                if self.type_src == 'grid':

                    # check format of source data
                    if self.format_src == 'netcdf' or self.format_src == 'nc':
                        if data_da is None:
                            data_da = xr.open_mfdataset(path_list_src, combine='by_coords')
                    elif self.format_src == 'tiff' or self.format_src == 'tif':

                        # check if path list source has more than one file
                        if path_list_src.__len__() > 1:

                            # check if object data is not defined
                            if data_da is None:

                                # create 3D array to store data
                                data_in_3d = np.zeros(
                                    shape=[geo_x_ref_2d.shape[0], geo_y_ref_2d.shape[1], path_list_src.__len__()])
                                data_in_3d[:, :, :] = np.nan

                                # iterate over path list source
                                data_time = []
                                for file_id, (file_name_step, time_stamp_step) in enumerate(zip(path_list_src, period_time_range)):
                                    da_in_2d = read_file_tiff(file_name_step)
                                    data_in_2d = da_in_2d.values
                                    data_in_2d[mask_ref_2d == 0] = np.nan
                                    data_in_3d[:, :, file_id] = data_in_2d
                                    data_time.append(time_stamp_step)

                                    ''' debug
                                    plt.figure()
                                    plt.imshow(data_in_2d)
                                    plt.colorbar()
                                    plt.figure()
                                    plt.imshow(data_in_3d[:, :, file_id])
                                    plt.colorbar()
                                    plt.show()
                                    '''

                                # create data array object
                                data_da = create_darray(
                                    data=data_in_3d, geo_x=geo_x_ref_2d, geo_y=geo_y_ref_2d,
                                    time=data_time, dims_order=['latitude', 'longitude', 'time'])

                        else:
                            # check if object data is not defined
                            if time_all:
                                # error message (file list is not allowed)
                                log_stream.error(' ===> Length of file list is not allowed')
                                raise NotImplementedError('Case is not implemented yet')
                            else:
                                # warning message (file list is not expected)
                                log_stream.warning(' ===> Length of file list is not expected')
                                log_stream.info(' ------> Merge source data ... SKIPPED')
                                data_da = None

                    else:
                        # error message (format is not allowed)
                        log_stream.error(' ===> Filename format "'+ self.format_src + '" is not allowed')
                        raise NotImplementedError('Format is not implemented yet')

                else:
                    # error message (type is not allowed)
                    log_stream.error(' ===> Filename type "'+ self.type_src + '" is not allowed')
                    raise NotImplementedError('Format is not implemented yet')

                # info merge data end
                log_stream.info(' ------> Merge source data ... DONE')

                # info save data end
                log_stream.info(' ------> Save source data ... ')

                # check if ancillary object is defined
                if data_da is not None:

                    # save data in workspace object
                    folder_name_anc, file_name_anc = os.path.split(path_name_anc_grid)
                    os.makedirs(folder_name_anc, exist_ok=True)

                    write_obj(path_name_anc_grid, data_da)

                    # info save data end
                    log_stream.info(' ------> Save source data ... DONE')

                else:
                    # warning message (data object is not defined)
                    log_stream.warning(' ===> Data object is not defined')
                    # info save data end
                    log_stream.info(' ------> Save source data ... SKIPPED')

            else:
                # info data already exists
                data_da = read_obj(path_name_anc_grid)

            # store datasets collections by the time period tag
            datasets_collections[period_tag] = data_da

            # info source data end
            log_stream.info(' -----> Type "' + period_tag + '" ... DONE')

        # info method end
        log_stream.info(' ----> Organize dynamic data [' + str(time_run) + '] ... DONE')

        return datasets_collections

    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
