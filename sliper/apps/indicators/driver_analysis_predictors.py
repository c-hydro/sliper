"""
Class Features

Name:          driver_analysis_predictors
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

######################################################################################
# Library
import logging
import os
import glob
import pandas as pd

from copy import deepcopy

from lib_data_io_pickle import read_obj, write_obj
from lib_utils_system import fill_tags2string, make_folder
from lib_utils_data_point_scenarios import read_file_scenarios

from lib_data_io_csv_predictors import write_file_csv
from lib_analysis_predictors_alert import compute_alert_level

from driver_fx_configuration import DriverFx

from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
# Debug
import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverAnalysis for predictors
class DriverAnalysis:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, time_step,
                 info_dict, src_dict, anc_dict, dst_dict,
                 ancillary_dict=None, fx_dict=None,
                 template_dict=None,
                 flag_fx='fx_kernel',
                 flag_static_src_matrix_center='training_matrix_center',
                 flag_static_src_matrix_max='training_matrix_max',
                 flag_static_src_matrix_mean='training_matrix_mean',
                 flag_static_src_coeff='training_coefficient',
                 flag_dynamic_info='predictors_data',
                 flag_dynamic_src='predictors_data',
                 flag_dynamic_anc='predictors_data',
                 flag_dynamic_dst='predictors_data',
                 flag_anc_updating=True, flag_dst_updating=True):

        self.time_step = pd.Timestamp(time_step)

        self.info_dict = info_dict
        self.src_dict = src_dict
        self.anc_dict = anc_dict
        self.dst_dict = dst_dict

        self.ancillary_dict = ancillary_dict
        self.fx_dict = fx_dict
        self.template_dict = template_dict

        self.file_name_tag = 'file_name'
        self.folder_name_tag = 'folder_name'
        self.names_columns_tag = 'columns_names'

        self.flag_fx = flag_fx
        self.flag_static_src_matrix_center = flag_static_src_matrix_center
        self.flag_static_src_matrix_max = flag_static_src_matrix_max
        self.flag_static_src_matrix_mean = flag_static_src_matrix_mean
        self.flag_static_src_coeff = flag_static_src_coeff
        self.flag_dynamic_info = flag_dynamic_info
        self.flag_dynamic_src = flag_dynamic_src
        self.flag_dynamic_anc = flag_dynamic_anc
        self.flag_dynamic_dst = flag_dynamic_dst

        self.flag_anc_updating = flag_anc_updating
        self.flag_dst_updating = flag_dst_updating

        self.ancillary_dict_info = ancillary_dict['info']
        self.ancillary_dict_group = ancillary_dict['group']

        self.ancillary_dict_methods = self.ancillary_dict_info['methods']
        self.ancillary_dict_group_index = self.map_group_field(self.ancillary_dict_group, group_field='index')
        self.ancillary_dict_warn_thr = self.map_group_field(self.ancillary_dict_group, group_field='warning_threshold')
        self.ancillary_dict_warn_index = self.map_group_field(self.ancillary_dict_group, group_field='warning_index')

        self.info_fields = info_dict[self.flag_dynamic_info]
        self.info_analysis_time_period = self.define_analysis_period(self.time_step, self.info_fields)

        self.file_columns_src = src_dict[self.flag_dynamic_src][self.names_columns_tag]
        self.file_name_src = src_dict[self.flag_dynamic_src][self.file_name_tag]
        self.folder_name_src = src_dict[self.flag_dynamic_src][self.folder_name_tag]
        file_path_src = os.path.join(self.folder_name_src, self.file_name_src)
        file_path_src = self.define_file_name(self.time_step, file_path_src)
        self.file_path_src = self.search_file_name(file_path_src)

        self.file_name_anc = anc_dict[self.flag_dynamic_anc][self.file_name_tag]
        self.folder_name_anc = anc_dict[self.flag_dynamic_anc][self.folder_name_tag]
        file_path_anc = os.path.join(self.folder_name_anc, self.file_name_anc)
        file_path_anc = self.define_file_name(self.time_step, file_path_anc)
        self.file_path_anc = self.search_file_name(file_path_anc)

        self.file_columns_dst = dst_dict[self.flag_dynamic_dst][self.names_columns_tag]
        self.file_name_dst = dst_dict[self.flag_dynamic_dst][self.file_name_tag]
        self.folder_name_dst = dst_dict[self.flag_dynamic_dst][self.folder_name_tag]
        file_path_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        file_path_dst = self.define_file_name(self.time_step, file_path_dst)
        self.file_path_dst = self.search_file_name(file_path_dst)

        self.fx_attrs = self.define_fx_attrs(self.flag_fx, self.fx_dict)

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define fx attrs
    def define_fx_attrs(self, fx_flag, fx_datasets):

        flag_static_src_matrix_center = self.flag_static_src_matrix_center
        flag_static_src_matrix_max = self.flag_static_src_matrix_max
        flag_static_src_matrix_mean = self.flag_static_src_matrix_mean
        flag_static_src_coeff = self.flag_static_src_coeff

        fx_attrs = {}
        if fx_flag == 'fx_kernel':

            flag_fx_matrix_center, flag_fx_coeff = 'fx_training_matrix_center', 'fx_training_coefficient'
            flag_fx_matrix_max, flag_fx_matrix_mean = 'fx_training_matrix_max', 'fx_training_matrix_mean'
            flag_fx_param_type, flag_fx_param_exponent = 'fx_parameters_type', 'fx_parameters_exponent'
            flag_fx_filter_columns, flag_fx_filter_group_idx = 'fx_filter_columns', 'fx_filter_group_index'
            flag_fx_filter_warning_thr, flag_fx_filter_warning_idx = 'fx_filter_warning_threshold', 'fx_filter_warning_threshold'

            if flag_static_src_matrix_center not in list(fx_datasets.keys()):
                log_stream.error(' ===> Fx datasets "' + flag_static_src_matrix_center +
                                 '" is not available in the reference obj')
                raise IOError('Datasets is mandatory to apply the method. Exit.')
            else:
                fx_attrs[flag_fx_matrix_center] = deepcopy(fx_datasets[flag_static_src_matrix_center])
            if flag_static_src_matrix_max not in list(fx_datasets.keys()):
                log_stream.error(' ===> Fx datasets "' + flag_static_src_matrix_max +
                                 '" is not available in the reference obj')
                raise IOError('Datasets is mandatory to apply the method. Exit.')
            else:
                fx_attrs[flag_fx_matrix_max] = deepcopy(fx_datasets[flag_static_src_matrix_max])
            if flag_static_src_matrix_mean not in list(fx_datasets.keys()):
                log_stream.error(' ===> Fx datasets "' + flag_static_src_matrix_mean +
                                 '" is not available in the reference obj')
                raise IOError('Datasets is mandatory to apply the method. Exit.')
            else:
                fx_attrs[flag_fx_matrix_mean] = deepcopy(fx_datasets[flag_static_src_matrix_mean])

            if flag_static_src_coeff not in list(fx_datasets.keys()):
                log_stream.error(' ===> Fx datasets "' + flag_static_src_coeff +
                                 '" is not available in the reference obj')
                raise IOError('Datasets is mandatory to apply the method. Exit.')
            else:
                fx_attrs[flag_fx_coeff] = deepcopy(fx_datasets[flag_static_src_coeff])

            if fx_flag in list(self.ancillary_dict_methods.keys()):
                fx_attrs[flag_fx_param_type] = self.ancillary_dict_methods[fx_flag]['kernel_type']
                fx_attrs[flag_fx_param_exponent] = self.ancillary_dict_methods[fx_flag]['kernel_exponent']
            else:
                log_stream.error(' ===> Fx parameters "' + fx_flag + '" are not correctly defined.')
                raise RuntimeError('Fx parameters must be defined in the settings file')

            fx_attrs[flag_fx_filter_columns] = self.file_columns_src
            fx_attrs[flag_fx_filter_group_idx] = self.ancillary_dict_group_index
            fx_attrs[flag_fx_filter_warning_thr] = self.ancillary_dict_warn_thr
            fx_attrs[flag_fx_filter_warning_idx] = self.ancillary_dict_warn_index

        else:
            log_stream.error(' ===> Fx name "' + fx_flag + '" is not expected by the procedure.')
            raise NotImplementedError('Fx not defined yet')

        return fx_attrs

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to map group field(s)
    @staticmethod
    def map_group_field(group_dict, group_field='index'):
        map_dict = {}
        for group_key, group_fields in group_dict.items():
            map_dict[group_key] = None
            if group_field in list(group_fields.keys()):
                group_value = group_fields[group_field]
                map_dict[group_key] = group_value

        return map_dict
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define analysis period
    @staticmethod
    def define_analysis_period(time_reference, time_fields):

        time_period, time_frequency, time_rounding = None, 'D', 'D'
        if 'time_period' in list(time_fields.keys()):
            time_period = time_fields['time_period']
        if 'time_frequency' in list(time_fields.keys()):
            time_frequency = time_fields['time_frequency']
        if 'time_rounding' in list(time_fields.keys()):
            time_rounding = time_fields['time_rounding']

        time_reference = time_reference.round(time_rounding)

        time_start, time_end = None, None
        if 'time_start' in list(time_fields.keys()):
            time_start = time_fields['time_start']
            if not isinstance(time_start, pd.Timestamp):
                time_start = pd.Timestamp(time_start)
            time_start = time_start.round(time_rounding)
        if 'time_end' in list(time_fields.keys()):
            time_end = time_fields['time_end']
            if not isinstance(time_end, pd.Timestamp):
                time_end = pd.Timestamp(time_end)
            time_end = time_end.round(time_rounding)

        time_range = None
        if (time_period is not None) and (time_reference is not None):
            time_range = pd.date_range(end=time_reference, periods=time_period, freq=time_frequency)
        elif (time_start is not None) and (time_end is not None):
            time_range = pd.date_range(start=time_start, end=time_end, freq=time_frequency)

        return time_range
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to search filename(s)
    @staticmethod
    def search_file_name(file_path_in):

        folder_name_in, file_name_in = os.path.split(file_path_in)

        if '*' in file_path_in:
            file_path_out = glob.glob(file_path_in)
        else:
            file_path_out = deepcopy(file_path_in)

        if isinstance(file_path_out, list):
            if file_path_out.__len__() == 1:
                file_path_out = file_path_out[0]
            elif file_path_out.__len__() == 0:
                log_stream.warning(' ===> Folder location and/or template file name are/is wrong')
                log_stream.warning(' ===> Folder location: "' + folder_name_in + '"')
                log_stream.warning(' ===> File template: "' + file_name_in + '"')
                file_path_out = None
            else:
                log_stream.error(' ===> File name is expected in list format with length equal to 1 ')
                log_stream.error(' ===> Folder location: "' + folder_name_in + '"')
                log_stream.error(' ===> File template: "' + file_name_in + '"')
                raise NotImplementedError('Case not implemented yet')

        return file_path_out
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define filename(s)
    def define_file_name(self, time_reference, file_name_raw):

        if not isinstance(time_reference, pd.Timestamp):
            time_reference = pd.Timestamp(time_reference)

        template_dict_default = self.template_dict
        template_dict_values = {
            'source_datetime': time_reference, 'source_sub_path_time': time_reference,
            'ancillary_datetime': time_reference, 'ancillary_sub_path_time': time_reference,
            'destination_datetime': time_reference, 'destination_sub_path_time': time_reference
        }
        file_name_def = fill_tags2string(file_name_raw, template_dict_default, template_dict_values)

        return file_name_def
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to dump analysis datasets
    def dump_analysis(self, file_data):

        log_stream.info(' ----> Dump analysis [' + str(self.time_step) + '] ... ')

        file_path_dst = self.file_path_dst
        flag_dst_updating = self.flag_dst_updating

        if flag_dst_updating:
            if os.path.exists(file_path_dst):
                os.remove(file_path_dst)

        log_stream.info(' ----> Save predictors datasets ... ')
        if not os.path.exists(file_path_dst):

            if file_data is not None:

                folder_name_dst, file_name_dst = os.path.split(file_path_dst)
                make_folder(folder_name_dst)

                write_file_csv(file_path_dst, file_data,
                               file_tag_columns=self.file_columns_dst, file_tag_index='time')

                log_stream.info(' ----> Save predictors datasets ... DONE')
            else:
                log_stream.info(' ----> Save predictors datasets ... SKIPPED. Datasets are undefined')

        else:
            log_stream.info(' ----> Save predictors datasets ... SKIPPED. Datasets are previously saved')

        log_stream.info(' ----> Dump analysis [' + str(self.time_step) + '] ... DONE')
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to compute analysis datasets
    def compute_analysis(self, file_data_in):

        log_stream.info(' ----> Compute analysis [' + str(self.time_step) + '] ... ')

        log_stream.info(' -----> Apply fx predictors ...')
        if file_data_in is not None:

            driver_fx = DriverFx(
                self.time_step,
                fx_name=self.flag_fx, fx_attrs=self.fx_attrs)

            fx_obj_datasets_in = driver_fx.organize_fx_datasets_in(file_data_in)
            fx_obj_attributes = driver_fx.organize_fx_parameters()
            fx_obj_output = driver_fx.exec_fx(fx_obj_datasets_in, fx_obj_attributes)
            fx_obj_datasets_out = driver_fx.organize_fx_datasets_out(fx_obj_output, fx_obj_datasets_in)

            file_data_out = compute_alert_level(fx_obj_datasets_out,
                                                self.ancillary_dict_warn_thr, self.ancillary_dict_warn_index)
            log_stream.info(' -----> Apply fx predictors ... DONE')

        else:

            file_data_out = None
            log_stream.info(' -----> Apply fx predictors ... FAILED. Datasets is NoneType')

        log_stream.info(' ----> Compute analysis [' + str(self.time_step) + '] ... DONE')

        return file_data_out

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize analysis datasets
    def organize_analysis(self):

        log_stream.info(' ----> Organize analysis [' + str(self.time_step) + '] ... ')

        file_path_src = self.file_path_src
        file_path_anc = self.file_path_anc

        flag_anc_updating = self.flag_anc_updating

        if flag_anc_updating:
            if os.path.exists(file_path_anc):
                os.remove(file_path_anc)

        log_stream.info(' -----> Collect and filter datasets ... ')
        if not os.path.exists(file_path_anc):

            if (file_path_src is not None) and (os.path.exists(file_path_src)):
                file_data_anc = read_file_scenarios(
                    file_path_src, file_time_range=self.info_analysis_time_period)

                folder_name_anc, file_name_anc = os.path.split(file_path_anc)
                make_folder(folder_name_anc)
                write_obj(file_path_anc, file_data_anc)

                log_stream.info(' -----> Collect and filter datasets ... DONE')

            else:
                log_stream.info(' -----> Collect and filter datasets ... FAILED.')
                if file_path_src is None:
                    log_stream.warning(' ===> File source is defined by NoneType ')
                elif (file_path_src is not None) and (not os.path.exists(file_path_src)):
                    log_stream.warning(' ===> File source "' + file_path_src + '" does not exist')
                else:
                    log_stream.warning(' ===> File source is not correctly open')
                file_data_anc = None
        else:

            file_data_anc = read_obj(file_path_anc)
            log_stream.info(' -----> Collect and filter datasets ... SKIPPED. Datasets previously prepared.')

        log_stream.info(' ----> Organize analysis [' + str(self.time_step) + '] ... DONE')

        return file_data_anc

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
