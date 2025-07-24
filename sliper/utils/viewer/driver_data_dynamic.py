"""
Class Features

Name:          driver_data_dynamic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20231010'
Version:       '1.0.0'
"""

# -------------------------------------------------------------------------------------
# libraries
import logging
import os
import numpy as np
import pandas as pd
from copy import deepcopy

from lib_data_io_pickle import read_obj, write_obj
from lib_data_io_generic import convert_data_to_vars
from lib_data_io_csv import read_file_csv

from lib_utils_graph import configure_time_series_info, view_time_series
from lib_utils_obj import create_dict_from_list
from lib_utils_system import fill_tags2string, make_folder

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# class driver data
class DriverData:

    # -------------------------------------------------------------------------------------
    # initialize class
    def __init__(self, time_run,
                 static_obj, source_dict, ancillary_dict, destination_dict=None,
                 flags_dict=None, info_dict=None, template_dict=None, tmp_dict=None):

        # time object(s)
        self.time_run = time_run

        # static object(s)
        self.static_obj = static_obj

        # args object(s)
        self.source_dict = source_dict
        self.ancillary_dict = ancillary_dict
        self.destination_dict = destination_dict

        self.flags_dict = flags_dict
        self.info_dict = info_dict
        self.template_dict = template_dict
        self.tmp_dict = tmp_dict

        # data tag(s)
        self.file_name_tag, self.folder_name_tag, self.path_name_tag = 'file_name', 'folder_name', 'path_name'
        self.value_min_tag, self.value_max_tag, self.scale_factor_tag = 'value_min', 'value_max', 'scale_factor'
        self.value_nodata_tag = 'value_no_data'
        self.fields_tag, self.format_tag = 'fields', 'format'
        # time tag(s)
        self.time_start_tag, self.time_end_tag = 'time_start', 'time_end'
        self.time_reference_tag, self.time_period_tag = "time_reference", "time_period",
        self.time_freq_tag, self.time_rounding_tag = 'time_frequency', 'time_rounding'
        self.time_range_tag = 'time_range'

        # figure tag(s)
        self.title_tag = 'title'
        self.label_axis_x_sm_tag, self.label_axis_y_sm_tag = 'label_axis_x_soil_moisture', 'label_axis_y_soil_moisture'
        self.label_axis_x_rain_tag, self.label_axis_y_rain_tag = 'label_axis_x_rain', 'label_axis_y_rain'
        self.legend_tag, self.style_tag = 'legend', "style"

        # get reset flags
        self.reset_src = flags_dict['reset_dynamic_source']
        self.reset_dst = flags_dict['reset_dynamic_destination']

        # get static data and info
        self.obj_data, self.obj_registry = self.static_obj['data'], self.static_obj['registry']

        # get source data
        (folder_name_src, file_name_src, fields_src, format_src, scale_factor_src,
         value_min_src, value_max_src, value_no_data_src) = self.get_info_data(
            self.source_dict)
        file_path_src = os.path.join(folder_name_src, file_name_src)
        # zip source data
        self.dset_obj_src = self.zip_info_data(
            folder_name_src, file_name_src, file_path_src,
            fields_src, format_src, scale_factor_src,
            value_min_src, value_max_src, value_no_data_src)
        # get source time
        (time_ref_src, time_period_src, time_round_src, time_freq_src,
         time_start_src, time_end_src, time_range_src) = self.get_info_time(self.source_dict)
        # zip source time
        self.time_obj_src = self.zip_info_time(
            time_ref_src, time_period_src, time_round_src, time_freq_src,
            time_start_src, time_end_src, time_range_src)

        # get ancillary data
        (folder_name_anc, file_name_anc, _, _, _, _, _, _) = self.get_info_data(self.ancillary_dict)
        file_path_anc = os.path.join(folder_name_anc, file_name_anc)
        # zip ancillary data
        self.dset_obj_anc = self.zip_info_data(folder_name_anc, file_name_anc, file_path_anc)

        # get destination figure
        (folder_name_dst_fig, file_name_dst_fig, fields_dst_fig, format_dst_fig, scale_factor_dst_fig,
         _, _, _) = self.get_info_data(self.destination_dict)
        file_path_dst_fig = os.path.join(folder_name_dst_fig, file_name_dst_fig)
        # zip destination figure
        self.dset_obj_dst_file = self.zip_info_data(
            folder_name_dst_fig, file_name_dst_fig, file_path_dst_fig,
            fields_dst_fig, format_dst_fig)

        (title_dst_fig, label_axis_x_dst_sm_fig, label_axis_y_dst_sm_fig,
         label_axis_x_dst_rain_fig, label_axis_y_dst_rain_fig,
         fig_legend, fig_style) = self.get_info_figure(self.destination_dict)
        # zip tmp data
        self.dset_obj_dst_fig = self.zip_info_figure(
            title_dst_fig, label_axis_x_dst_sm_fig, label_axis_y_dst_sm_fig,
            label_axis_x_dst_rain_fig, label_axis_y_dst_rain_fig,
            fig_legend, fig_style)

        # get tmp data
        (folder_name_tmp, file_name_tmp, _, _, _, _, _, _) = self.get_info_data(self.tmp_dict)
        # zip tmp data
        self.dset_obj_tmp = self.zip_info_data(folder_name_tmp, file_name_tmp, None)

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to zip info data
    def zip_info_data(self, folder_name, file_name, path_name, fields=None, file_format=None, scale_factor=None,
                      value_min=None, value_max=None, value_no_data=None):
        info_obj = {self.folder_name_tag: folder_name, self.file_name_tag: file_name, self.path_name_tag: path_name,
                    self.fields_tag: fields, self.format_tag: file_format, self.scale_factor_tag: scale_factor,
                    self.value_min_tag: value_min, self.value_max_tag: value_max, self.value_nodata_tag: value_no_data}
        return info_obj
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to get info data
    def get_info_data(self, obj_data):

        folder_name = check_key_of_obj(self.folder_name_tag, obj_data, value_data_default=None)
        file_name = check_key_of_obj(self.file_name_tag, obj_data, value_data_default=None)
        fields = check_key_of_obj(self.fields_tag, obj_data, value_data_default={})
        scale_factor = check_key_of_obj(self.scale_factor_tag, obj_data, value_data_default=1)
        file_format = check_key_of_obj(self.format_tag, obj_data, value_data_default=None)
        v_min = check_key_of_obj(self.value_min_tag, obj_data, value_data_default=None)
        v_max = check_key_of_obj(self.value_max_tag, obj_data, value_data_default=None)
        v_no_data = check_key_of_obj(self.value_nodata_tag, obj_data, value_data_default=np.nan)

        if v_no_data is None:
            v_no_data = np.nan

        return folder_name, file_name, fields, file_format, scale_factor, v_min, v_max, v_no_data
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to zip info time
    def zip_info_time(self, time_ref, time_period, time_round, time_freq, time_start, time_end, time_range):
        time_obj = {self.time_period_tag: time_period, self.time_reference_tag: time_ref,
                    self.time_freq_tag: time_freq, self.time_rounding_tag: time_round,
                    self.time_start_tag: time_start, self.time_end_tag: time_end,
                    self.time_range_tag: time_range}
        return time_obj
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to get info time
    def get_info_time(self, obj_data):

        time_reference = check_key_of_obj(self.time_reference_tag, obj_data, value_data_default=self.time_run)
        time_period = check_key_of_obj(self.time_period_tag, obj_data, value_data_default=None)
        time_rounding = check_key_of_obj(self.time_rounding_tag, obj_data, value_data_default=None)
        time_freq = check_key_of_obj(self.time_freq_tag, obj_data, value_data_default=None)
        time_start = check_key_of_obj(self.time_start_tag, obj_data, value_data_default=None)
        time_end = check_key_of_obj(self.time_end_tag, obj_data, value_data_default=None)

        if time_start is None and time_end is None:
            if time_period is not None and time_freq is not None:
                time_range = pd.date_range(end=time_reference, periods=time_period, freq=time_freq)
                time_start, time_end = time_range[0], time_range[-1]
            else:
                log_stream.error(' ===> The variables "time_period" and "time_frequency" are both undefined')
                raise RuntimeError('The variables "time_period" and "time_frequency" must be defined')
        elif time_start is not None and time_end is not None:
            time_start, time_end = pd.Timestamp(time_start), pd.Timestamp(time_end)
            time_range = pd.date_range(start=time_start, end=time_end, freq=time_freq)
        else:
            log_stream.error(' ===> The variables "time_start" and "time_end" must be both defined or undefined')
            raise NotImplemented('Case not implemented yet')

        return time_reference, time_period, time_rounding, time_freq, time_start, time_end, time_range

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to zip info figure
    def zip_info_figure(self, fig_title, fig_label_axis_sm_x, fig_label_axis_sm_y,
                        fig_label_axis_rain_x, fig_label_axis_rain_y,
                        fig_legend, fig_style):
        info_obj = {self.title_tag: fig_title,
                    self.label_axis_x_sm_tag: fig_label_axis_sm_x,
                    self.label_axis_y_sm_tag: fig_label_axis_sm_y,
                    self.label_axis_x_rain_tag: fig_label_axis_rain_x,
                    self.label_axis_y_rain_tag: fig_label_axis_rain_y,
                    self.legend_tag: fig_legend, self.style_tag: fig_style}
        return info_obj
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to get info data
    def get_info_figure(self, obj_data):

        fig_title = check_key_of_obj(self.title_tag, obj_data, value_data_default='title')
        fig_label_axis_x_sm = check_key_of_obj(self.label_axis_x_sm_tag, obj_data, value_data_default='x sm [-]')
        fig_label_axis_y_sm = check_key_of_obj(self.label_axis_y_sm_tag, obj_data, value_data_default='y sm [-]')
        fig_label_axis_x_rain = check_key_of_obj(self.label_axis_x_rain_tag, obj_data, value_data_default='x rain [-]')
        fig_label_axis_y_rain = check_key_of_obj(self.label_axis_y_rain_tag, obj_data, value_data_default='y rain [-]')
        fig_legend = check_key_of_obj(self.legend_tag, obj_data, value_data_default=None)
        fig_style = check_key_of_obj(self.style_tag, obj_data, value_data_default=None)

        return (fig_title, fig_label_axis_x_sm, fig_label_axis_y_sm, fig_label_axis_x_rain, fig_label_axis_y_rain,
                fig_legend, fig_style)

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to define file name
    def define_file_name(self, file_path_raw,
                         time_step=None, time_start=None, time_end=None,
                         registry_name=None, registry_code=None, registry_tag=None):

        if time_step is None:
            time_step = deepcopy(time_end)

        template_time_generic = self.template_dict['time']
        template_datasets_generic = self.template_dict['datasets']

        generic_time_list = list(template_time_generic.keys())
        values_time_dict_generic = create_dict_from_list(generic_time_list, time_step)

        if time_end is not None:
            time_end_list = [key for key, value in values_time_dict_generic.items() if 'end' in key.lower()]
            values_dict_tmp = create_dict_from_list(time_end_list, time_end)
            values_time_dict_generic.update(**values_dict_tmp)

        if time_start is not None:
            time_start_list = [key for key, value in values_time_dict_generic.items() if 'start' in key.lower()]
            values_dict_tmp = create_dict_from_list(time_start_list, time_start)
            values_time_dict_generic.update(**values_dict_tmp)

        # generic_datasets_list = list(template_datasets_generic.keys())
        values_datasets_dict_generic = {}
        if registry_name is not None:
            values_datasets_dict_generic['registry_name'] = registry_name
        if registry_code is not None:
            values_datasets_dict_generic['registry_code'] = str(registry_code)
        if registry_tag is not None:
            values_datasets_dict_generic['registry_tag'] = registry_tag

        file_path_def = fill_tags2string(file_path_raw, template_time_generic, values_time_dict_generic)[0]
        file_path_def = fill_tags2string(file_path_def, template_datasets_generic, values_datasets_dict_generic)[0]

        return file_path_def

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to view data
    def view_data(self, point_data_collections):

        # method start info
        time_str = self.time_run.strftime('%Y-%d-%m')
        log_stream.info(' ----> View time-series object(s) [' + time_str + '] ... ')

        # get time
        time_run = self.time_run

        # get static object
        obj_registry = self.obj_registry

        # get file settings
        file_path_dst_raw = self.dset_obj_dst_file[self.path_name_tag]
        file_format_dst = self.dset_obj_dst_file[self.format_tag]
        file_fields_dst = self.dset_obj_dst_file[self.fields_tag]
        file_format_dst = self.dset_obj_dst_file[self.format_tag]

        # get figure settings
        fig_legend = self.dset_obj_dst_fig[self.legend_tag]
        fig_style = self.dset_obj_dst_fig[self.style_tag]
        fig_title = self.dset_obj_dst_fig[self.title_tag]
        fig_label_x_sm_tag = self.dset_obj_dst_fig[self.label_axis_x_sm_tag]
        fig_label_y_sm_tag = self.dset_obj_dst_fig[self.label_axis_y_sm_tag]
        fig_label_x_rain_tag = self.dset_obj_dst_fig[self.label_axis_x_rain_tag]
        fig_label_y_rain_tag = self.dset_obj_dst_fig[self.label_axis_y_rain_tag]

        # iterate over point(s)
        for registry_key, registry_fields in obj_registry.items():

            # get point information
            registry_name, registry_idx = registry_fields['name'], registry_fields['index']
            registry_basin = registry_fields['basin']
            registry_warn_thr = registry_fields['warning_threshold']
            registry_warn_idx = registry_fields['warning_index']

            # point info start
            log_stream.info(' -----> Registry "' + registry_key + '" ... ')

            # define file name
            file_path_dst_def = self.define_file_name(
                file_path_dst_raw,
                registry_name=registry_key, registry_code=registry_idx,
                time_step=time_run, time_start=None, time_end=None)

            # get point data
            data_raw = point_data_collections[registry_key]['data']

            # check data availability
            if data_raw is not None:

                # select point data
                data_selected = configure_time_series_info(data_raw, fields=self.dset_obj_dst_file['fields'])

                # view point time-series
                if file_format_dst == 'jpeg' or file_format_dst == 'jpg':

                    # create figure
                    view_time_series(file_path_dst_def,
                                     ts_data=data_selected, ts_name=registry_key,
                                     ts_registry=registry_fields,
                                     fig_title=fig_title,
                                     fig_legend=fig_legend, fig_style=fig_style,
                                     fig_label_axis_sm_x=fig_label_x_sm_tag,
                                     fig_label_axis_sm_y=fig_label_y_sm_tag,
                                     fig_label_axis_rain_x=fig_label_x_rain_tag,
                                     fig_label_axis_rain_y=fig_label_y_rain_tag,
                                     fig_dpi=150)

                else:
                    log_stream.error(' ===> Destination data type "' + file_format_dst + '" is not supported.')
                    raise NotImplemented('Case not implemented yet')

                # point info end
                log_stream.info(' -----> Registry "' + registry_key + '" ... DONE')

            else:
                # point info end
                log_stream.info(' -----> Registry "' + registry_key + '" ... SKIPPED. Data not available')

        # method end info
        log_stream.info(' ----> View time-series object(s) [' + time_str + '] ... DONE.')

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to organize data
    def organize_data(self):

        # method start info
        time_str = self.time_run.strftime('%Y-%d-%m')
        log_stream.info(' ----> Organize time-series object(s) [' + time_str + ']... ')

        # get time(s)
        time_run = self.time_run
        time_start = self.time_obj_src['time_start']
        time_end = self.time_obj_src['time_end']
        time_freq = self.time_obj_src['time_frequency']

        # get static object
        obj_registry = self.obj_registry

        # get path(s)
        file_path_src_raw = self.dset_obj_src[self.path_name_tag]
        file_path_anc_raw = self.dset_obj_anc[self.path_name_tag]
        file_path_dst_raw = self.dset_obj_dst_file[self.path_name_tag]

        # get format(s)
        file_format_src = self.dset_obj_src[self.format_tag]

        # get flag(s)
        reset_src, reset_dst = self.reset_src, self.reset_dst

        # iterate over point(s)
        obj_data_collections = {}
        for registry_key, registry_fields in obj_registry.items():

            # get point information
            registry_name, registry_idx = registry_fields['name'], registry_fields['index']
            registry_basin = registry_fields['basin']
            registry_warn_thr = registry_fields['warning_threshold']
            registry_warn_idx = registry_fields['warning_index']

            # point info start
            log_stream.info(' -----> Registry "' + registry_key + '" ... ')

            # define source, ancillary and destination filename(s)
            file_path_src_def = self.define_file_name(
                file_path_src_raw,
                time_step=time_run, time_start=None, time_end=None)

            file_path_anc_def = self.define_file_name(
                file_path_anc_raw,
                time_step=time_run, time_start=None, time_end=None)

            file_path_dst_def = self.define_file_name(
                file_path_dst_raw,
                time_step=time_run, time_start=None, time_end=None)

            # reset source file
            if reset_src:
                if os.path.exists(file_path_anc_def):
                    os.remove(file_path_anc_def)
                if os.path.exists(file_path_dst_def):
                    os.remove(file_path_dst_def)

            # check ancillary file availability
            if not os.path.exists(file_path_anc_def):

                # get data start
                log_stream.info(' ------> Get datasets ... ')

                # check source reference file availability
                if os.path.exists(file_path_src_def):

                    # check source file format
                    if file_format_src == 'csv':

                        # method to get data
                        data_raw = read_file_csv(file_path_src_def)
                        # method to convert data
                        data_converted = convert_data_to_vars(
                            data_raw, obj_row_key=registry_key, obj_column_key='soil_slips_domain_name',
                            obj_fields=self.dset_obj_src['fields'],
                            time_start=time_start, time_end=time_end, time_freq=time_freq)

                    else:
                        log_stream.error(' ===> Source data type "' + file_format_src + '" is not supported.')
                        raise NotImplemented('Case not implemented yet')

                else:
                    log_stream.warning(' ===> Datasets file "' + file_path_src_def + '" was not available.')
                    data_converted = None

                # get data end
                log_stream.info(' ------> Get datasets ... DONE')

                # dump data start
                log_stream.info(' ------> Dump datasets ... ')
                # check data availability
                if data_converted is not None:

                    # method to dump data
                    folder_name_anc_def, file_name_anc_def = os.path.split(file_path_anc_def)
                    make_folder(folder_name_anc_def)
                    # collect data and metrics
                    data_obj = {'data': data_converted}
                    write_obj(file_path_anc_def, data_obj)

                    # dump data end
                    log_stream.info(' ------> Dump datasets ... DONE')

                else:
                    # dump data end
                    data_obj = {'data': None}
                    log_stream.info(' ------> Dump datasets ... SKIPPED. Datasets is not available')

                # point info end
                log_stream.info(' -----> Registry "' + registry_key + '" ... DONE')

            else:

                # point info end
                data_obj = read_obj(file_path_anc_def)
                log_stream.info(' -----> Registry "' + registry_key + '" ... SKIPPED. Data previously saved')

            obj_data_collections[registry_key] = data_obj

        # method end info
        log_stream.info(' ----> Organize time-series object(s) [' + time_str + '] ... DONE')

        return obj_data_collections

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# method to check key in a dictionary obj
def check_key_of_obj(key_data, obj_data, value_data_default=None):
    if key_data in list(obj_data.keys()):
        return obj_data[key_data]
    else:
        return value_data_default
# -------------------------------------------------------------------------------------
