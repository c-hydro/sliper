"""
Class Features

Name:          driver_predictors
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250725'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os

import pandas as pd

from copy import deepcopy

from lib_data_io_csv import read_file_csv, filter_file_csv_by_time, write_file_csv
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_data_predictors import define_analysis_period, remap_data
from lib_utils_fx_configuration import select_fx_method, organize_fx_args
from lib_utils_fx_data import ensure_time_index, ensure_time_doy

from lib_utils_fx_analysis import compute_alert_info

from driver_fx import DriverFx

from lib_utils_generic import fill_template_string, extract_subkeys, extract_subpart

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# debugging
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class DriverPredictors
class DriverPredictors:

    # ------------------------------------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self,
                 time_run, time_range,
                 src_dict, anc_dict, dst_dict,
                 geo_data, training_data,
                 parameters_dict=None, tags_dict=None,
                 flag_update_anc=True, flag_update_dst=True):

        self.time_run = pd.Timestamp(time_run)
        self.time_range = time_range

        self.src_dict = src_dict
        self.anc_dict = anc_dict
        self.dst_dict = dst_dict
        self.tags_dict = tags_dict

        self.parameters_methods = parameters_dict['methods']
        self.parameters_anls_variables = parameters_dict['analysis']['variables']
        self.parameters_anls_time = parameters_dict['analysis']['time']

        self.geo_data = geo_data
        self.training_data = training_data

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.format_tag, self.type_tag, self.delimiter_tag = 'format', 'type', 'delimiter'
        self.fields_tag, self.prefix_tag, self.variable_tag = 'fields', 'prefix', 'variable'

        self.training_matrix_center_tag = 'training_matrix_center'
        self.training_matrix_max_tag = 'training_matrix_max'
        self.training_matrix_mean_tag = 'training_matrix_mean'
        self.training_coefficient_tag = 'training_coefficient'

        self.file_name_src = src_dict[self.file_name_tag]
        self.folder_name_src = src_dict[self.folder_name_tag]
        self.path_name_src = os.path.join(self.folder_name_src, self.file_name_src)
        self.delimiter_src = src_dict[self.delimiter_tag]
        self.fields_src = src_dict[self.fields_tag]
        self.prefix_src = src_dict[self.prefix_tag]
        self.variable_src = src_dict[self.variable_tag]

        self.file_name_anc = anc_dict[self.file_name_tag]
        self.folder_name_anc = anc_dict[self.folder_name_tag]
        self.path_name_anc = os.path.join(self.folder_name_anc, self.file_name_anc)

        self.file_name_dst = dst_dict[self.file_name_tag]
        self.folder_name_dst = dst_dict[self.folder_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        self.delimiter_dst = dst_dict[self.delimiter_tag]
        self.fields_dst = dst_dict[self.fields_tag]
        self.prefix_dst = dst_dict[self.prefix_tag]
        self.variable_dst = dst_dict[self.variable_tag]

        self.flag_update_anc = flag_update_anc
        self.flag_update_dst = flag_update_dst

        self.geo_dset_name = extract_subpart(self.geo_data, 'name')
        self.geo_dset_code = extract_subpart(self.geo_data, 'alert_area')
        self.geo_dset_catchments = extract_subpart(self.geo_data, 'catchment')

        self.geo_range_thresholds = extract_subkeys(
            self.geo_data, subkeys=['white_range', 'green_range', 'yellow_range', 'orange_range', 'red_range'],
            keep_keys=True)
        self.geo_range_id = extract_subkeys(
            self.geo_data, subkeys=['white_id', 'green_id', 'yellow_id', 'orange_id', 'red_id'],
            keep_keys=True)
        self.geo_range_rgb = extract_subkeys(
            self.geo_data, subkeys=['white_rgb', 'green_rgb', 'yellow_rgb', 'orange_rgb', 'red_rgb'],
            keep_keys=True)

        self.time_analysis = define_analysis_period(
            time_reference=self.time_run, **self.parameters_anls_time)

        self.fx_name, self.fx_parameters = select_fx_method(self.parameters_methods)
        self.fx_args = organize_fx_args(
            self.fx_name, self.fx_parameters, training_data,
            self.geo_range_thresholds, self.geo_range_id, self.geo_range_rgb)

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Method to dump analysis datasets
    def dump_data(self, analysis_workspace):

        # info method start
        log_stream.info(' ----> Dump analysis [' + str(self.time_run) + '] ... ')

        # get time info
        time_run = self.time_run
        # get geo data
        geo_data = self.geo_data

        # get flags
        flag_update_dst = self.flag_update_dst

        # organize workflow to apply fx predictors
        if analysis_workspace is not None:

            # iterate over geographical areas
            for geo_key, geo_info in geo_data.items():

                # info area start
                log_stream.info(' -----> Area "' + str(geo_key) + '" ...')

                # define destination path names
                path_name_dst = fill_template_string(
                    template_str=deepcopy(self.path_name_dst),
                    template_map=self.tags_dict,
                    value_map={'destination_sub_path_time_run': time_run, "destination_datetime_run": time_run,
                               "destination_sub_path_time": time_run, "destination_datetime": time_run,
                               'alert_area_name': geo_key})

                # remove file if it is needed by the procedure
                if flag_update_dst:
                    if os.path.exists(path_name_dst):
                        os.remove(path_name_dst)

                # info save predictors datasets start
                log_stream.info(' ------> Save predictors datasets ... ')
                if not os.path.exists(path_name_dst):

                    # check if destination file exists
                    if not os.path.exists(path_name_dst):

                        # get analysis collections
                        analysis_collections = analysis_workspace[geo_key]
                        # rename analysis collections
                        analysis_collections = remap_data(analysis_collections, rename_map=self.fields_dst)

                        # check if analysis collections is not None
                        if analysis_collections is not None:

                            # save data in file object
                            folder_name_dst, file_name_dst = os.path.split(path_name_dst)
                            os.makedirs(folder_name_dst, exist_ok=True)

                            write_file_csv(analysis_collections,
                                           filename=path_name_dst, orientation='cols', float_format='%.2f')

                            # info area end
                            log_stream.info(' -----> Area "' + str(geo_key) + '" ... DONE')

                        else:
                            # info area end
                            log_stream.info(' -----> Area "' + str(geo_key) + '" ... FAILED. Datasets are undefined')

                    else:
                        # info area end
                        log_stream.info(' -----> Area "' + str(geo_key) + '" ... SKIPPED. Datasets previously created')

                    # info save predictors datasets end
                    log_stream.info(' ----> Save predictors datasets ... DONE')

                else:
                    # info save predictors datasets end
                    log_stream.info(' ----> Save predictors datasets ... SKIPPED. Datasets are previously saved')

        # info method end
        log_stream.info(' ----> Dump analysis [' + str(self.time_run) + '] ... DONE')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to compute analysis datasets
    def compute_data(self, datasets_workspace):

        # info method start
        log_stream.info(' ----> Compute analysis [' + str(self.time_run) + '] ... ')

        # get time info
        time_run = self.time_run
        # get geo data
        geo_data = self.geo_data

        # info method start
        log_stream.info(' -----> Apply fx predictors ...')

        # organize workflow to apply fx predictors
        analysis_workspace = {}
        if datasets_workspace is not None:

            # iterate over geographical areas
            for geo_key, geo_info in geo_data.items():

                # info area start
                log_stream.info(' -----> Area "' + str(geo_key) + '" ...')
                analysis_workspace[geo_key] = {}

                # get datasets for the area
                fx_obj_datasets = datasets_workspace[geo_key]

                # get ancillary thresholds and ids for the area
                geo_obj_thresholds = self.geo_range_thresholds[geo_key]
                geo_obj_ids = self.geo_range_id[geo_key]

                # configure fx driver
                driver_fx = DriverFx(
                    self.time_run,
                    fx_name=self.fx_name, fx_attrs=self.fx_args)

                # organize fx datasets
                fx_obj_datasets = driver_fx.organize_fx_datasets_in(
                    fx_obj_datasets, fx_variables_src=self.parameters_anls_variables)
                # organize fx parameters
                fx_obj_attributes = driver_fx.organize_fx_parameters()

                # execute fx
                fx_obj_output = driver_fx.exec_fx(fx_obj_datasets, fx_obj_attributes)
                # organize fx analysis
                fx_obj_analysis = driver_fx.organize_fx_datasets_out(fx_obj_output, fx_obj_datasets)

                # add alert info to the analysis
                file_obj_analysis = compute_alert_info(
                    fx_obj_analysis,
                    column_data='slips_pred_n',
                    column_id='slips_pred_id',
                    column_color='slips_pred_alert_color', column_rgba='slip_pred_alert_rgb',
                    color_ids=geo_obj_ids, color_ranges=geo_obj_thresholds)

                # save output datasets
                analysis_workspace[geo_key] = file_obj_analysis

                # info area end
                log_stream.info(' -----> Area "' + str(geo_key) + '" ... DONE')

            # info method end (done
            log_stream.info(' -----> Apply fx predictors ... DONE')

        else:

            # info method end (failed)
            analysis_workspace = None
            log_stream.info(' -----> Apply fx predictors ... FAILED. Datasets is NoneType')

        # info method end (done)
        log_stream.info(' ----> Compute analysis [' + str(self.time_run) + '] ... DONE')

        return analysis_workspace

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize analysis datasets
    def organize_data(self):

        # info method start
        log_stream.info(' ----> Organize dynamic data [' + str(self.time_run) + '] ... ')

        # get time info
        time_run = self.time_run
        # get geo data
        geo_data = self.geo_data

        # get flags
        flag_update_anc = self.flag_update_anc
        flag_update_dst = self.flag_update_dst

        # iterate over groups
        datasets_workspace = {}
        for geo_key, geo_info in geo_data.items():

            # info time start
            log_stream.info(' -----> Area "' + str(geo_key) + '" ...' )
            datasets_workspace[geo_key] = {}

            # define source path names (rain, soil moisture, soil slips)
            path_name_src = fill_template_string(
                template_str=deepcopy(self.path_name_src),
                template_map=self.tags_dict,
                value_map={'source_sub_path_time_run': time_run, "source_datetime_run": time_run,
                           "source_sub_path_time": time_run, "source_datetime": time_run,
                           'alert_area_name': geo_key})
            # define ancillary path names
            path_name_anc = fill_template_string(
                template_str=deepcopy(self.path_name_anc),
                template_map=self.tags_dict,
                value_map={'ancillary_sub_path_time_run': time_run, "ancillary_datetime_run": time_run,
                           "ancillary_sub_path_time": time_run, "ancillary_datetime": time_run,
                           'alert_area_name': geo_key})
            # define destination path names
            path_name_dst = fill_template_string(
                template_str=deepcopy(self.path_name_dst),
                template_map=self.tags_dict,
                value_map={'destination_sub_path_time_run': time_run, "destination_datetime_run": time_run,
                           "destination_sub_path_time": time_run, "destination_datetime": time_run,
                           'alert_area_name': geo_key})

            # remove file if it is needed by the procedure
            if flag_update_anc:
                if os.path.exists(path_name_anc):
                    os.remove(path_name_anc)
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)
            if flag_update_dst:
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)

            # info data collection start
            log_stream.info(' -----> Collect and filter datasets ... ')
            if not os.path.exists(path_name_anc):

                # check if source is defined and exists
                if (path_name_src is not None) and (os.path.exists(path_name_src)):

                    # read data from source file
                    df_data_raw = read_file_csv(
                        path_name_src, delimiter=self.delimiter_src, fields=self.fields_src,
                        time_col='time', time_index=True,
                        allowed_prefix=self.prefix_src, prefix_key=None, result_format='dataframe')

                    # filter data by time
                    if self.time_analysis is not None:
                        start, end = self.time_analysis[0], self.time_analysis[-1]
                        df_data_filtered = filter_file_csv_by_time(
                            df_data_raw, datetime_col='time', start=start, end=end
                        )
                    else:
                        # No filtering, keep original
                        df_data_filtered = deepcopy(df_data_raw)

                    # ensure time doy
                    df_data_filtered = ensure_time_doy(df_data_filtered)
                    # ensure time index
                    df_data_filtered = ensure_time_index(df_data_filtered)

                    # save filtered data
                    folder_name_anc, file_name_anc = os.path.split(path_name_anc)
                    os.makedirs(folder_name_anc, exist_ok=True)
                    write_obj(path_name_anc, df_data_filtered)

                    # info data collection end (done)
                    log_stream.info(' -----> Collect and filter datasets ... DONE')

                else:

                    # info data collection end (failed)
                    log_stream.info(' -----> Collect and filter datasets ... FAILED.')
                    if path_name_src is None:
                        log_stream.warning(' ===> File source is defined by NoneType ')
                    elif (path_name_src is not None) and (not os.path.exists(path_name_src)):
                        log_stream.warning(' ===> File source "' + path_name_src + '" does not exist')
                    else:
                        log_stream.warning(' ===> File source is not correctly open')
                    df_data_filtered = None
            else:

                # info data collection end (previously prepared)
                df_data_filtered = read_obj(path_name_src)
                log_stream.info(' -----> Collect and filter datasets ... SKIPPED. Datasets previously prepared.')

            # store in workspace
            datasets_workspace[geo_key] = df_data_filtered

            # info method end
            log_stream.info(' ----> Organize dynamic data [' + str(self.time_run) + '] ... DONE')

        return datasets_workspace

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
