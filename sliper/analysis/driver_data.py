"""
Class Features

Name:          driver_data
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20231010'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
from copy import deepcopy

from driver_plot import DriverPlot

from lib_data_io_pickle import read_obj, write_obj
from lib_data_io_csv import read_file_csv, filter_file_csv_by_time

from lib_utils_plot import configure_time_series_info, view_time_series
from lib_utils_generic import fill_template_string, extract_subkeys

from lib_utils_time import extract_time_info

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class driver data
class DriverData:

    # ------------------------------------------------------------------------------------------------------------------
    # initialize class
    def __init__(self, time_run, time_range,
                 src_dict, anc_dict, dst_dict=None,
                 tags_dict=None, tmp_dict=None,
                 geo_data=None,
                 flag_update_src=True, flag_update_dst=True):

        # time object(s)
        self.time_run = time_run
        self.time_range = time_range

        # geographical object(s)
        self.geo_data = geo_data

        # args object(s)
        self.src_dict = src_dict
        self.anc_dict = anc_dict
        self.dst_dict = dst_dict
        self.tags_dict = tags_dict
        self.tmp_dict = tmp_dict

        # data tag(s)
        self.file_name_tag, self.folder_name_tag, self.path_name_tag = 'file_name', 'folder_name', 'path_name'
        self.fields_tag, self.format_tag, self.type_tag = 'fields', 'format', 'type'
        self.delimiter_tag, self.settings_tag = 'delimiter', 'settings'

        # DA CONTROLALRE
        self.value_min_tag, self.value_max_tag, self.scale_factor_tag = 'value_min', 'value_max', 'scale_factor'
        self.value_nodata_tag = 'value_no_data'

        # get source data
        self.file_name_src = src_dict[self.file_name_tag]
        self.folder_name_src = src_dict[self.folder_name_tag]
        self.path_name_src = os.path.join(self.folder_name_src, self.file_name_src)
        self.fields_src = src_dict[self.fields_tag]
        self.format_src = src_dict[self.format_tag] if self.format_tag in list(src_dict.keys()) else 'csv'
        self.type_src = src_dict[self.type_tag] if self.type_tag in list(src_dict.keys()) else 'vector'
        self.settings_src = src_dict[self.settings_tag] if self.settings_tag in list(src_dict.keys()) else {}
        self.delimiter_src = src_dict[self.delimiter_tag] if self.delimiter_tag in list(src_dict.keys()) else ';'

        # get ancillary data
        self.file_name_anc = anc_dict[self.file_name_tag]
        self.folder_name_anc = anc_dict[self.folder_name_tag]
        self.path_name_anc = os.path.join(self.folder_name_anc, self.file_name_anc)

        # get destination data
        self.file_name_dst = dst_dict[self.file_name_tag]
        self.folder_name_dst = dst_dict[self.folder_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        self.fields_dst = dst_dict[self.fields_tag]
        self.format_dst = dst_dict[self.format_tag] if self.format_tag in list(dst_dict.keys()) else 'jpeg'
        self.type_dst = dst_dict[self.type_tag] if self.type_tag in list(dst_dict.keys()) else 'figure'
        self.settings_dst = dst_dict[self.settings_tag] if self.settings_tag in list(dst_dict.keys()) else {}
        self.delimiter_dst = dst_dict[self.delimiter_tag] if self.delimiter_tag in list(dst_dict.keys()) else ';'

        # initialize plot object
        self.driver_plot = DriverPlot(time_run=self.time_run, config_dict=self.settings_dst)

        self.geo_range_thresholds = extract_subkeys(
            self.geo_data, subkeys=['white_range', 'green_range', 'yellow_range', 'orange_range', 'red_range'],
            keep_keys=True)
        self.geo_range_id = extract_subkeys(
            self.geo_data, subkeys=['white_id', 'green_id', 'yellow_id', 'orange_id', 'red_id'],
            keep_keys=True)
        self.geo_range_rgb = extract_subkeys(
            self.geo_data, subkeys=['white_rgb', 'green_rgb', 'yellow_rgb', 'orange_rgb', 'red_rgb'],
            keep_keys=True)

        # get reset flags
        self.flag_update_src, self.flag_update_dst = flag_update_src, flag_update_dst

    # ------------------------------------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to view data
    def view_data(self, data_workspace):

        # method start info
        log_stream.info(' ----> View time-series object(s) [' + self.time_run.strftime('%Y-%d-%m') + '] ... ')

        # get time(s)
        time_run = self.time_run
        time_start, time_end, time_freq = extract_time_info(self.time_range)
        # get geographical data
        geo_data = self.geo_data

        # iterate over geographical areas
        for geo_key, geo_info in geo_data.items():

            # info area start
            log_stream.info(' -----> Area "' + geo_key + '" ... ')

            # get destination filename(s)
            path_name_dst = fill_template_string(
                template_str=deepcopy(self.path_name_dst),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_run, "run_datetime": time_run,
                    'destination_sub_path_time_run': time_run, "destination_datetime_run": time_run,
                    "destination_sub_path_time": time_run, "destination_datetime": time_run,
                    'alert_area_name': geo_key, 'registry_name': geo_key})

            # get point data
            data_collections = data_workspace[geo_key]

            # check data availability
            if data_collections is not None:

                # view point time-series
                if self.format_dst == 'jpeg' or self.format_dst == 'jpg':

                    # get geographical information
                    geo_name, geo_idx, geo_catchment = geo_info['name'], geo_info['index'], geo_info['catchment']
                    # get warning thresholds, id and rgb
                    warning_thr = self.geo_range_thresholds[geo_key]
                    warning_id = self.geo_range_id[geo_key]
                    warning_rgb = self.geo_range_rgb[geo_key]

                    # method to create figure
                    self.driver_plot.view_ts(
                        file_name=path_name_dst, ts_data=data_collections,
                        ts_name=geo_name, ts_index=geo_idx, ts_catchment=geo_catchment,
                        warning_thresholds=warning_thr,
                        warning_index=warning_id, warning_rgb=warning_rgb)

                else:
                    # error message for unsupported format
                    log_stream.error(' ===> Destination data type "' + self.format_dst + '" is not supported.')
                    raise NotImplemented('Case not implemented yet')

                # info area end
                log_stream.info(' -----> Area "' + geo_key + '" ... DONE')

            else:
                # info area end
                log_stream.info(' -----> Area "' + geo_key + '" ... SKIPPED. Data not available')

        # method end info
        log_stream.info(' ----> View time-series object(s) [' + self.time_run.strftime('%Y-%d-%m') + '] ... DONE.')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize data
    def organize_data(self):

        # method start info
        log_stream.info(' ----> Organize dynamic datasets [' + self.time_run.strftime('%Y-%d-%m') + ']... ')

        # get time(s)
        time_run = self.time_run
        time_start, time_end, time_freq = extract_time_info(self.time_range)
        # get geographical data
        geo_data = self.geo_data

        # iterate over geographical areas
        data_workspace = {}
        for geo_key, geo_info in geo_data.items():

            # info area start
            log_stream.info(' -----> Area "' + geo_key + '" ... ')

            # define source, ancillary and destination filename(s)
            path_name_src = fill_template_string(
                template_str=deepcopy(self.path_name_src),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_run, "run_datetime": time_run,
                    'source_sub_path_time_run': time_run, "source_datetime_run": time_run,
                    "source_sub_path_time": time_run, "source_datetime": time_run,
                    'alert_area_name': geo_key, 'registry_name': geo_key})
            path_name_anc = fill_template_string(
                template_str=deepcopy(self.path_name_anc),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_run, "run_datetime": time_run,
                    'ancillary_sub_path_time_run': time_run, "ancillary_datetime_run": time_run,
                    "ancillary_sub_path_time": time_run, "ancillary_datetime": time_run,
                    'alert_area_name': geo_key, 'registry_name': geo_key})
            path_name_dst = fill_template_string(
                template_str=deepcopy(self.path_name_dst),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_run, "run_datetime": time_run,
                    'destination_sub_path_time_run': time_run, "destination_datetime_run": time_run,
                    "destination_sub_path_time": time_run, "destination_datetime": time_run,
                    'alert_area_name': geo_key, 'registry_name': geo_key})

            # reset source file
            if self.flag_update_src:
                if os.path.exists(path_name_anc):
                    os.remove(path_name_anc)
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)

            # check ancillary file availability
            if not os.path.exists(path_name_anc):

                # get data start
                log_stream.info(' ------> Get datasets ... ')

                # check source reference file availability
                if os.path.exists(path_name_src):

                    # check source file type
                    if self.type_src == 'vector':
                        # check source file format
                        if self.format_src == 'csv':

                            # method to get data
                            data_raw = read_file_csv(
                                path_name_src,
                                delimiter=self.delimiter_src, fields=self.fields_src,
                                time_col='time', time_index=True, result_format='dataframe',
                                key_column=None, allowed_prefix=['sm', 'rain', 'slips'], prefix_key=None)

                            # method to filter data
                            data_collections = filter_file_csv_by_time(
                                data_raw, datetime_col='time', start=time_start, end=time_end)

                        else:
                            log_stream.error(' ===> Source data type "' + self.format_src + '" is not supported.')
                            raise NotImplemented('Case not implemented yet')
                    else:
                        log_stream.error(' ===> Source data type "' + self.type_src + '" is not supported.')
                        raise NotImplemented('Case not implemented yet')

                else:
                    log_stream.warning(' ===> Datasets file "' + path_name_src + '" was not available.')
                    data_collections = None

                # get data end
                log_stream.info(' ------> Get datasets ... DONE')

                # dump data start
                log_stream.info(' ------> Dump datasets ... ')
                # check data availability
                if data_collections is not None:

                    # method to dump data
                    folder_name_anc, file_name_anc = os.path.split(path_name_anc)
                    os.makedirs(folder_name_anc, exist_ok=True)

                    # collect data and metrics
                    write_obj(path_name_anc, data_collections)

                    # dump data end
                    log_stream.info(' ------> Dump datasets ... DONE')

                else:
                    # dump data end
                    data_collections = None
                    log_stream.info(' ------> Dump datasets ... SKIPPED. Datasets is not available')

                # info area end
                log_stream.info(' -----> Area "' + geo_key + '" ... DONE')

            else:

                # point info end
                data_collections = read_obj(path_name_anc)
                log_stream.info(' -----> Area "' + geo_key + '" ... SKIPPED. Data previously saved')

            # add data to workspace
            data_workspace[geo_key] = data_collections

        # method end info
        log_stream.info(' ----> Organize dynamic datasets [' + self.time_run.strftime('%Y-%d-%m') + '] ... DONE')

        return data_workspace

    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
