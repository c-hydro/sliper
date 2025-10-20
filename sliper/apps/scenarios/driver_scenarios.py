"""
Class Features

Name:          driver_scenarios
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250618'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import pandas as pd

from copy import deepcopy

from lib_data_io_csv import write_file_csv
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_data_scenarios import (
    read_data, merge_data_by_time, merge_data_by_vars, memorize_data, fill_data,
    analyze_data_alignment, analyze_time_summary, add_missing_days_with_nodata, ensure_time_index)

from lib_utils_generic import (
    extract_subpart, detect_token, validate_time_index,
    fill_template_string, get_common_time_index, get_files_with_tags, format_sub_path_by_time,
    extract_timestamps_from_paths, extract_timestamps_from_filenames)

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class DriverScenarios
class DriverScenarios:

    # ------------------------------------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, time_ref, time_run, time_range,
                 src_dict, anc_dict, dst_dict,
                 tags_dict=None, geo_dict=None,
                 flag_update_anc_datasets=True, flag_update_anc_analysis=True,
                 flag_update_dst=True):

        self.time_ref = pd.Timestamp(time_ref)
        self.time_run = pd.Timestamp(time_run)
        self.time_range = time_range

        self.src_dict = src_dict
        self.anc_dict = anc_dict
        self.dst_dict = dst_dict
        self.tags_dict = tags_dict

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.format_tag, self.type_tag, self.delimiter_tag = 'format', 'type', 'delimiter'
        self.fields_tag, self.prefix_tag, self.variable_tag = 'fields', 'prefix', 'variable'
        self.var_name_x_tag, self.var_name_y_tag = 'longitude', 'latitude'

        self.name_rain_tag = 'rain'
        self.name_sm_tag = 'soil_moisture'
        self.name_slips_tag = 'soil_slips'

        # get geographical information
        self.geo_data = geo_dict

        # get file names and folder names source (rain, sm, soil_slips)
        self.file_name_src_rain = src_dict[self.name_rain_tag][self.file_name_tag]
        self.folder_name_src_rain = src_dict[self.name_rain_tag][self.folder_name_tag]
        self.path_name_src_rain = os.path.join(self.folder_name_src_rain, self.file_name_src_rain)
        self.format_src_rain = src_dict[self.name_rain_tag][self.format_tag]
        self.type_src_rain = src_dict[self.name_rain_tag][self.type_tag]
        self.fields_src_rain = src_dict[self.name_rain_tag][self.fields_tag]
        self.delimiter_src_rain = src_dict[self.name_rain_tag][self.delimiter_tag]
        self.fields_src_rain = src_dict[self.name_rain_tag][self.fields_tag]
        self.prefix_src_rain = src_dict[self.name_rain_tag][self.prefix_tag]
        self.variable_src_rain = src_dict[self.name_rain_tag][self.variable_tag]

        self.file_name_src_sm = src_dict[self.name_sm_tag][self.file_name_tag]
        self.folder_name_src_sm = src_dict[self.name_sm_tag][self.folder_name_tag]
        self.path_name_src_sm = os.path.join(self.folder_name_src_sm, self.file_name_src_sm)
        self.format_src_sm = src_dict[self.name_sm_tag][self.format_tag]
        self.type_src_sm = src_dict[self.name_sm_tag][self.type_tag]
        self.delimiter_src_sm = src_dict[self.name_sm_tag][self.delimiter_tag]
        self.fields_src_sm = src_dict[self.name_sm_tag][self.fields_tag]
        self.prefix_src_sm = src_dict[self.name_sm_tag][self.prefix_tag]
        self.variable_src_sm = src_dict[self.name_sm_tag][self.variable_tag]

        self.file_name_src_slips = src_dict[self.name_slips_tag][self.file_name_tag]
        self.folder_name_src_slips = src_dict[self.name_slips_tag][self.folder_name_tag]
        self.path_name_src_slips = os.path.join(self.folder_name_src_slips, self.file_name_src_slips)
        self.format_src_slips = src_dict[self.name_slips_tag][self.format_tag]
        self.type_src_slips = src_dict[self.name_slips_tag][self.type_tag]
        self.delimiter_src_slips = src_dict[self.name_slips_tag][self.delimiter_tag]
        self.fields_src_slips = src_dict[self.name_slips_tag][self.fields_tag]
        self.prefix_src_slips = src_dict[self.name_slips_tag][self.prefix_tag]
        self.variable_src_slips = src_dict[self.name_slips_tag][self.variable_tag]

        # get file names and folder names ancillary datasets and analysis
        self.file_name_anc_dset = anc_dict['datasets'][self.file_name_tag]
        self.folder_name_anc_dset = anc_dict['datasets'][self.folder_name_tag]
        self.path_name_anc_dset = os.path.join(self.folder_name_anc_dset, self.file_name_anc_dset)

        self.file_name_anc_anls = anc_dict['analysis'][self.file_name_tag]
        self.folder_name_anc_anls = anc_dict['analysis'][self.folder_name_tag]
        self.path_name_anc_anls = os.path.join(self.folder_name_anc_anls, self.file_name_anc_anls)

        # get file names and folder names destination
        self.file_name_dst = dst_dict[self.file_name_tag]
        self.folder_name_dst = dst_dict[self.folder_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        self.format_dst = dst_dict[self.format_tag]
        self.type_dst = dst_dict[self.type_tag]
        self.delimiter_dst = dst_dict[self.delimiter_tag]
        self.fields_dst = dst_dict[self.fields_tag]
        self.prefix_dst = dst_dict[self.prefix_tag]
        self.variable_dst = dst_dict[self.variable_tag]

        # flags to update datasets ancillary and destination
        self.flag_update_anc_dset = flag_update_anc_datasets
        self.flag_update_anc_anls = flag_update_anc_analysis
        self.flag_update_dst = flag_update_dst

        self.geo_dset_name = extract_subpart(self.geo_data, 'name')
        self.geo_dset_code = extract_subpart(self.geo_data, 'alert_area')
        self.geo_dset_catchments = extract_subpart(self.geo_data, 'catchment')

        # set token pattern
        self.token_sub_path_pattern = ['search_sub_path_time', 'run_sub_path_time']
        # set extra fields to be insert in the df source (if needed)
        self.fields_src_extra = {'time_pivot': self.time_ref.strftime("%Y-%m-%d %H:%M")}

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to dump data
    def dump_data(self, time_pivot: pd.Timestamp, analysis_workspace: dict):

        # info method start
        log_stream.info(' ----> Dump dynamic data [' + str(time_pivot) + '] ... ')

        # get time info
        time_run = self.time_run
        time_range = self.time_range
        time_start, time_end = time_range[0], time_range[-1]

        # get group information
        geo_data = self.geo_data

        # iterate over groups
        for geo_key, geo_info in geo_data.items():

            # info area start
            log_stream.info(' -----> Area "' + str(geo_key) + '" ...' )

            # define destination path names
            path_name_dst = fill_template_string(
                template_str=deepcopy(self.path_name_dst),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_pivot, 'run_datetime': time_pivot,
                    'destination_sub_path_time_run': time_run, "destination_datetime_run": time_pivot,
                    "destination_sub_path_time": time_pivot, "destination_datetime": time_pivot,
                    'alert_area_name': geo_key})

            # apply flags (to update datasets destination)
            if self.flag_update_dst:
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)

            # check if destination file exists
            if not os.path.exists(path_name_dst):

                # get analysis collections
                analysis_collections = analysis_workspace[geo_key]

                # manage time column/index
                analysis_collections = ensure_time_index(analysis_collections, time_col='time')

                # save data in file object
                folder_name_dst, file_name_dst = os.path.split(path_name_dst)
                os.makedirs(folder_name_dst, exist_ok=True)

                write_file_csv(analysis_collections,
                               filename=path_name_dst, orientation='cols', float_format='%.3f')

                # info area end
                log_stream.info(' -----> Area "' + str(geo_key) + '" ... DONE')

            else:
                # info area start end
                log_stream.info(' -----> Area "' + str(geo_key) + '" ... SKIPPED. Datasets previously created')

        # info method end
        log_stream.info(' ----> Dump dynamic data [' + str(time_pivot) + '] ... DONE')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to analyze data
    def analyze_data(self, time_pivot: pd.Timestamp, datasets_workspace: dict, time_workspace: dict):

        # info method start
        log_stream.info(' ----> Analyze dynamic data [' + str(time_pivot) + '] ... ')

        # get time info
        time_run = self.time_run
        # get group information
        geo_data = self.geo_data

        # iterate over groups
        analysis_workspace = {}
        for geo_key, geo_info in geo_data.items():

            # info area start
            log_stream.info(' -----> Area "' + str(geo_key) + '" ...' )

            # define ancillary path names
            path_name_anc_anls = fill_template_string(
                template_str=deepcopy(self.path_name_anc_anls),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_pivot, 'run_datetime': time_pivot,
                    'ancillary_sub_path_time_run': time_pivot, "ancillary_datetime_run": time_pivot,
                    "ancillary_sub_path_time": time_pivot, "ancillary_datetime": time_pivot,
                    'alert_area_name': geo_key})

            # define destination path names
            path_name_dst = fill_template_string(
                template_str=deepcopy(self.path_name_dst),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_pivot, 'run_datetime': time_pivot,
                    'destination_sub_path_time_run': time_pivot, "destination_datetime_run": time_pivot,
                    "destination_sub_path_time": time_pivot, "destination_datetime": time_pivot,
                    'alert_area_name': geo_key})

            # apply flags (to update datasets source and destination)
            if self.flag_update_anc_anls:
                if os.path.exists(path_name_anc_anls):
                    os.remove(path_name_anc_anls)
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)
            if self.flag_update_dst:
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)

            # check if destination file exists
            if not os.path.exists(path_name_anc_anls):

                # info get analysis start
                log_stream.info(' ------> Get analysis data ... ')

                # get data collections
                datasets_collections = datasets_workspace[geo_key]
                # get time collections
                time_collections = time_workspace[geo_key]

                # get data variables
                df_rain = datasets_collections[self.variable_src_rain]
                df_sm = datasets_collections[self.variable_src_sm]
                df_slips = datasets_collections[self.variable_src_slips]

                # get time variables
                time_period_data, time_period_ref = time_collections['time_period_data'], time_collections['time_period_ref']

                # info get analysis end
                log_stream.info(' ------> Get analysis data ... DONE')

                # info compute analysis start
                log_stream.info(' ------> Compute analysis data ... ')

                # define analysis collections
                analysis_collections_merged = merge_data_by_vars(
                    time_period_data[0], time_period_data[-1],
                    df_rain, df_sm, df_slips,
                    rain_var=self.variable_src_rain, sm_var=self.variable_src_sm, slips_var=self.variable_src_slips,
                    domain_var=geo_key, domain_label='domain',
                    time_label='time', time_frequency='D')

                # fill analysis collections fill row value nans
                analysis_collections_filled = fill_data(analysis_collections_merged)
                # fill analysis collections fill row empty
                analysis_collections_filled = add_missing_days_with_nodata(
                    analysis_collections_filled, start_date=self.time_range.min(), end_date=None)

                # organize datasets workspace
                analysis_workspace[geo_key] = analysis_collections_filled

                # info compute analysis end
                log_stream.info(' ------> Compute analysis data ... DONE')

                # info save analysis start
                log_stream.info(' ------> Save analysis data ... ')

                # save data in workspace object
                folder_name_anc, file_name_anc = os.path.split(path_name_anc_anls)
                os.makedirs(folder_name_anc, exist_ok=True)

                write_obj(path_name_anc_anls, analysis_collections_filled)

                # info save analysis end
                log_stream.info(' ------> Save analysis data ... DONE')

                # info area end
                log_stream.info(' -----> Area "' + str(geo_key) + '" ... DONE')

            else:

                # info data already exists
                analysis_collections = read_obj(path_name_anc_anls)
                # organize df workspace
                analysis_workspace[geo_key] = analysis_collections

                # info data not available
                log_stream.info(' -----> Area "' + str(geo_key) + '" ... SKIPPED. Datasets previously created')

        # info method end
        log_stream.info(' ----> Analyze dynamic data [' + str(time_pivot) + '] ... DONE')

        return analysis_workspace
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize data
    def organize_data(self):

        # info method start
        log_stream.info(' ----> Organize dynamic data [' + str(self.time_run) + '] ... ')

        # get time info
        time_run = self.time_run

        # get group information
        geo_data = self.geo_data

        # iterate over groups
        datasets_workspace, time_workspace, tp_workspace = {}, {}, {}
        for geo_key, geo_info in geo_data.items():

            # info time start
            log_stream.info(' -----> Area "' + str(geo_key) + '" ...' )
            datasets_workspace[geo_key], time_workspace[geo_key] = {}, {}

            # define ancillary path names
            path_name_anc_dset = fill_template_string(
                template_str=deepcopy(self.path_name_anc_dset),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_run, 'run_datetime': time_run,
                    'ancillary_sub_path_time_run': time_run, "ancillary_datetime_run": time_run,
                    "ancillary_sub_path_time": time_run, "ancillary_datetime": time_run,
                    'alert_area_name': geo_key})
            path_name_anc_anls = fill_template_string(
                template_str=deepcopy(self.path_name_anc_anls),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_run, 'run_datetime': time_run,
                    'ancillary_sub_path_time_run': time_run, "ancillary_datetime_run": time_run,
                    "ancillary_sub_path_time": time_run, "ancillary_datetime": time_run,
                    'alert_area_name': geo_key})

            # define ancillary path names
            path_name_dst = fill_template_string(
                template_str=deepcopy(self.path_name_dst),
                template_map=self.tags_dict,
                value_map={
                    'run_sub_path_time': time_run, 'run_datetime': time_run,
                    'destination_sub_path_time_run': time_run, "destination_datetime_run": time_run,
                    "destination_sub_path_time": time_run, "destination_datetime": time_run,
                    'alert_area_name': geo_key})

            # apply flags (to update datasets source and destination)
            if self.flag_update_anc_dset:
                if os.path.exists(path_name_anc_dset):
                    os.remove(path_name_anc_dset)
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)
            if self.flag_update_anc_anls:
                if os.path.exists(path_name_anc_anls):
                    os.remove(path_name_anc_anls)
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)
            if self.flag_update_dst:
                if os.path.exists(path_name_dst):
                    os.remove(path_name_dst)

            # check if ancillary file exists
            if not os.path.exists(path_name_anc_dset):

                # initialize data collections
                df_common_rain, df_common_sm, df_common_slips = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                memory_rain, memory_sm, memory_slips = {}, {}, {}

                # iterate over time steps (observed or forecast)
                time_summary = {}
                folder_time_index_common, file_time_index_common = None, None
                folder_time_tag, file_time_tag = None, None
                for time_step in self.time_range:

                    # info time start
                    log_stream.info(' ------> Time "' + str(time_step) + '" ...')

                    # detect folder token rain
                    folder_token_rain = detect_token(self.folder_name_src_rain, self.token_sub_path_pattern)
                    # define folder time pattern rain
                    folder_time_pattern_rain = format_sub_path_by_time(
                        ts=time_step, pattern=self.tags_dict[folder_token_rain])
                    # detect folder time index rain
                    folder_time_obj_src_rain = extract_timestamps_from_paths(
                        path_template=self.folder_name_src_rain, path_format=self.tags_dict[folder_token_rain],
                        tokens={folder_token_rain: folder_time_pattern_rain})

                    # detect folder token sm
                    folder_token_sm = detect_token(self.folder_name_src_sm, self.token_sub_path_pattern)
                    # define folder time pattern sm
                    folder_time_pattern_sm = format_sub_path_by_time(
                        ts=time_step, pattern=self.tags_dict[folder_token_sm])
                    # detect folder time index rain
                    folder_time_obj_src_sm = extract_timestamps_from_paths(
                        path_template=self.folder_name_src_sm, path_format=self.tags_dict[folder_token_sm],
                        tokens={folder_token_sm: folder_time_pattern_sm})

                    # check folders (if not found continue)
                    if (folder_time_obj_src_rain is None) or (folder_time_obj_src_sm is None):
                        # info time end (skipped)
                        log_stream.info(' ------> Time "' + str(time_step) + '" ... SKIPPED. Dataset is not available')
                        continue

                    # search the common folder time index
                    folder_time_index_common = get_common_time_index(
                        pd.DatetimeIndex(list(folder_time_obj_src_rain.keys())),
                        pd.DatetimeIndex(list(folder_time_obj_src_sm.keys()))
                    )

                    # info get data start
                    log_stream.info(' -------> Get source data ... ')

                    # iterate over folder time
                    for folder_time_tag in folder_time_index_common:

                        # info time tag start
                        log_stream.info(' --------> Search folder time "' + str(folder_time_tag) + '" ... ')

                        # check if time tag is suitable to the time reference
                        if folder_time_tag <= self.time_ref:

                            # define folder name rain
                            folder_name_src_rain = folder_time_obj_src_rain[folder_time_tag]
                            # detect file list rain
                            file_list_src_rain = get_files_with_tags(folder_name_src_rain, tags=[geo_key])
                            # detect file time rain
                            file_time_src_rain = extract_timestamps_from_filenames(
                                file_list_src_rain,
                                date_ref=self.time_ref, date_ascending=False, date_format='%Y%m%d%H%M')

                            # define folder name sm
                            folder_name_src_sm = folder_time_obj_src_sm[folder_time_tag]
                            # detect file list sm
                            file_list_src_sm = get_files_with_tags(folder_name_src_sm, tags=[geo_key])
                            # detect file time sm
                            file_time_src_sm = extract_timestamps_from_filenames(
                                file_list_src_sm,
                                date_ref=self.time_ref, date_ascending=False, date_format='%Y%m%d%H%M')

                            # check folders (if not found continue)
                            if (file_time_src_rain is None) or (file_time_src_sm is None):
                                # info time start
                                log_stream.info(' --------> Search folder time "' + str(folder_time_tag) +
                                                '" ... SKIPPED. Folder is empty.')
                                continue

                            # search the common time index
                            file_time_index_common = get_common_time_index(file_time_src_rain, file_time_src_sm)

                            # iterate over file time
                            for file_time_tag in file_time_index_common:

                                # info file time tag start
                                log_stream.info(' --------> Search file time "' + str(file_time_tag) + '" ... ')

                                file_name_src_rain = fill_template_string(
                                    template_str=deepcopy(self.file_name_src_rain),
                                    template_map=self.tags_dict,
                                    value_map={
                                        'search_sub_path_time': folder_time_tag, 'search_datetime': folder_time_tag,
                                        'run_sub_path_time': self.time_ref, 'run_datetime': self.time_ref,
                                        'source_sub_path_time_run': file_time_tag, "source_datetime_run": file_time_tag,
                                        "source_sub_path_time_rain": file_time_tag, "source_datetime_rain": file_time_tag,
                                        'alert_area_name': geo_key})
                                path_name_src_rain = os.path.join(folder_name_src_rain, file_name_src_rain)

                                # define sm path name
                                folder_name_src_sm = folder_time_obj_src_sm[folder_time_tag]
                                file_name_src_sm = fill_template_string(
                                    template_str=deepcopy(self.file_name_src_sm),
                                    template_map=self.tags_dict,
                                    value_map={
                                        'search_sub_path_time': folder_time_tag, 'search_datetime': folder_time_tag,
                                        'run_sub_path_time': self.time_ref, 'run_datetime': self.time_ref,
                                        'source_sub_path_time_run': file_time_tag, "source_datetime_run": file_time_tag,
                                        "source_sub_path_time_sm": file_time_tag, "source_datetime_sm": file_time_tag,
                                        'alert_area_name': geo_key})
                                path_name_src_sm = os.path.join(folder_name_src_sm, file_name_src_sm)

                                # define slips path name
                                path_name_src_slips = fill_template_string(
                                    template_str=deepcopy(self.path_name_src_slips),
                                    template_map=self.tags_dict,
                                    value_map={
                                        'search_sub_path_time': folder_time_tag, 'search_datetime': folder_time_tag,
                                        'run_sub_path_time':  self.time_ref, 'run_datetime':  self.time_ref,
                                        'source_sub_path_time_run': file_time_tag, "source_datetime_run": file_time_tag,
                                        "source_sub_path_time_soil_slips": file_time_tag,
                                        "source_datetime_soil_slips": file_time_tag,
                                        'alert_area_name': geo_key})

                                # method to memorize data rain (processed or not)
                                memory_rain, status_rain = memorize_data(memory_rain, path_name_src_rain)
                                # check if the file has the same name
                                if not status_rain:
                                    if path_name_src_rain in list(memory_rain.keys()):
                                        status_tmp = memory_rain[path_name_src_rain]
                                        if status_tmp:
                                            status_rain = True

                                # check data status (before reading)
                                df_step_rain = None
                                if status_rain:

                                    # method to read data rain
                                    df_step_rain = read_data(
                                        file_data=path_name_src_rain, var_data=self.variable_src_rain,
                                        type_data=self.type_src_rain, format_data=self.format_src_rain,
                                        fields_data=self.fields_src_rain, delimiter_data=self.delimiter_src_rain,
                                        fields_extra=self.fields_src_extra,
                                        prefix_key=self.prefix_src_rain, prefix_delimiter='_')

                                    # check data rain
                                    if df_step_rain is None:
                                        log_stream.warning(f' ===> File {path_name_src_rain} not correctly loaded.')

                                    # check data status (after reading)
                                    memory_rain, _ = memorize_data(memory_rain, path_name_src_rain)

                                # method to memorize data sm (processed or not)
                                memory_sm, status_sm = memorize_data(memory_sm, path_name_src_sm)
                                # check if the file has the same name
                                if not status_sm:
                                    if path_name_src_sm in list(memory_sm.keys()):
                                        status_tmp = memory_sm[path_name_src_sm]
                                        if status_tmp:
                                            status_sm = True

                                # check data status (before reading)
                                df_step_sm = None
                                if status_sm:

                                    # method to read data soil moisture
                                    df_step_sm = read_data(
                                        file_data=path_name_src_sm, var_data=self.variable_src_sm,
                                        type_data=self.type_src_sm, format_data=self.format_src_sm,
                                        fields_data=self.fields_src_sm, delimiter_data=self.delimiter_src_sm,
                                        fields_extra=self.fields_src_extra,
                                        prefix_key=self.prefix_src_sm, prefix_delimiter='_')

                                    # check data sm
                                    if df_step_sm is None:
                                        log_stream.warning(f' ===> File {path_name_src_sm} not correctly loaded.')

                                    # check data status (after reading)
                                    memory_sm, _ = memorize_data(memory_sm, path_name_src_sm)

                                # method to memorize data soil slips (processed or not)
                                memory_slips, status_slips = memorize_data(memory_slips, path_name_src_slips)
                                # check if the file has the same name (soils can be taken from csv file)
                                if not status_slips:
                                    if path_name_src_slips in list(memory_slips.keys()):
                                        status_tmp = memory_slips[path_name_src_slips]
                                        if status_tmp:
                                            status_slips = True

                                # check data status (before reading)
                                df_step_slips = None
                                if status_slips:

                                    # method to read data soil slips
                                    df_step_slips = read_data(
                                        file_data=path_name_src_slips, var_data=self.variable_src_slips,
                                        type_data=self.type_src_slips, format_data=self.format_src_slips,
                                        fields_data=self.fields_src_slips, delimiter_data=self.delimiter_src_slips,
                                        fields_extra= {},
                                        prefix_key=self.prefix_src_slips, prefix_delimiter='_')

                                    # check data soil slips
                                    if df_step_slips is None:
                                        log_stream.warning(f' ===> File {path_name_src_slips} not correctly loaded.')

                                    # check data status (after reading)
                                    memory_slips, _ = memorize_data(memory_slips, path_name_src_slips)

                                # info file time tag end (done)
                                log_stream.info(' --------> Search file time "' + str(file_time_tag) + '" ... DONE')

                                # check data (and exit from file cycle(s)
                                log_stream.info(' --------> Check data availability ... ')
                                if (df_step_rain is not None) and (df_step_sm is not None) and (
                                        df_step_slips is not None):
                                    # info file time tag end (done)
                                    log_stream.info(' --------> Check data availability ... DONE')
                                    break
                                else:
                                    # info file time tag end (continue ...)
                                    log_stream.info(' --------> Check data availability ' +
                                                    ' ... CONTINUE TO THE NEXT. ONE OR MORE DATASETS IS/ARE EMTPY')

                            # info folder time tag end
                            log_stream.info(' --------> Search folder time "' + str(folder_time_tag) + '" ... DONE')

                            # exit from the folder cycle(s)
                            if (df_step_rain is not None) and (df_step_sm is not None) and (df_step_slips is not None):
                                break

                        else:
                            # info folder time tag end (skipped ...)
                            log_stream.info(' --------> Search path time "' +
                                '" ... SKIPPED. TimeTag "' +str(folder_time_tag)+ '" >= TimeRef "' + str(self.time_ref) + '"')

                    # validate time index (if data is available or not)
                    if validate_time_index(folder_time_index_common, file_time_index_common):

                        # info get data end
                        log_stream.info(' -------> Get source data ... DONE')

                    else:

                        # initialize using NoneType
                        df_step_rain, df_step_sm, df_step_slips = None, None, None
                        log_stream.warning(' ===> Source data are not available for "' + str(time_step) + '"')
                        # info get data end
                        log_stream.info(' -------> Get source data ... SKIPPED')

                    # info get merge start
                    log_stream.info(' -------> Merge source data ...')

                    # validate time index (if data is available or not)
                    if validate_time_index(folder_time_index_common, file_time_index_common):

                        # method to merge data rain
                        if df_step_rain is not None:
                            df_common_rain = merge_data_by_time(
                                df_common_rain, df_step_rain, key_cols=['{:}_time_start', '{:}_time_end'],
                                prefix_keys=self.prefix_src_rain)
                        # method to merge data soil moisture
                        if df_step_sm is not None:
                            df_common_sm = merge_data_by_time(
                                df_common_sm, df_step_sm, key_cols=['{:}_time_start', '{:}_time_end'],
                                prefix_keys=self.prefix_src_sm)
                        # method to merge data soil slips (at the moment not realtime information)
                        if df_step_slips is not None:
                            df_common_slips = merge_data_by_time(
                                df_common_slips, df_step_slips, key_cols=None,
                                prefix_keys=self.prefix_src_slips)

                        # info get merge end
                        log_stream.info(' -------> Merge source data ... DONE')

                    else:

                        # info get merge end
                        log_stream.info(' -------> Merge source data ... SKIPPED')

                    # store time information
                    time_summary[time_step] = {
                        'time_tag_folder' : folder_time_tag, 'time_tag_file': file_time_tag,
                        'time_ref' : self.time_ref}

                    # info time start
                    log_stream.info(' ------> Time "' + str(time_step) + '" ... DONE')

                # manage dataframe rows filter by time
                common_times = set(df_common_rain["time"]).intersection(df_common_sm["time"])
                # Keep only rows with common time values
                df_common_rain = df_common_rain[df_common_rain["time"].isin(common_times)]
                df_common_sm = df_common_sm[df_common_sm["time"].isin(common_times)]

                # organize time collections
                time_collections = analyze_data_alignment(
                    time_run, self.time_range,
                    df_common_rain, df_common_sm, df_common_slips,
                    dn1=self.variable_src_rain, dn2=self.variable_src_sm, dn3=self.variable_src_slips,
                    use1=True, use2=True, use3=False,
                    time_col= 'time')

                # fill data for the missing days (if needed)
                df_common_rain = add_missing_days_with_nodata(
                    df_common_rain, start_date=self.time_range.min(), end_date=None)
                df_common_sm = add_missing_days_with_nodata(
                    df_common_sm, start_date=self.time_range.min(), end_date=None)

                # get the time that summarize newest run and available data
                time_pivot = analyze_time_summary(time_summary, mode='newest', tag='time_tag_file')

                # organize datasets collections
                datasets_collections = {
                    'tp': time_pivot,
                    'summary': time_summary,
                    self.variable_src_rain: df_common_rain,
                    self.variable_src_sm: df_common_sm,
                    self.variable_src_slips: df_common_slips}

                # organize datasets workspace
                datasets_workspace[geo_key] = datasets_collections
                # organize time workspace
                time_workspace[geo_key] = time_collections
                # organize summary workspace
                tp_workspace[geo_key] = time_pivot

                # info save data start
                log_stream.info(' ------> Save source data ... ')

                # save data in workspace object
                folder_name_anc, file_name_anc = os.path.split(path_name_anc_dset)
                os.makedirs(folder_name_anc, exist_ok=True)

                # organize objects collections
                obj_collections = {
                    'tp': time_pivot,
                    'datasets': datasets_collections, 'time': time_collections}

                write_obj(path_name_anc_dset, obj_collections)

                # info save data end
                log_stream.info(' ------> Save source data ... DONE')

            else:

                # info data already exists
                obj_collections = read_obj(path_name_anc_dset)
                datasets_collections, time_collections = obj_collections['datasets'], obj_collections['time']
                tp_collections = obj_collections['tp']

                # organize datasets workspace
                datasets_workspace[geo_key] = datasets_collections
                # organize time workspace
                time_workspace[geo_key] = time_collections
                # organize tp workspace
                tp_workspace[geo_key] = tp_collections

            # info area end
            log_stream.info(' -----> Area "' + str(geo_key) + '" ... DONE')

        # extract unique time pivot
        time_pivot = set(tp_workspace.values())
        if len(time_pivot) != 1:
            log_stream.error(' ===> TimePivot is not unique')
            raise ValueError(f"Not all timestamps are equal: {time_pivot}")
        # get the single time pivot
        time_pivot = time_pivot.pop()

        # write time pivot report
        log_stream.info(' ----> Time Report - Remap from "time_run" to "time_pivot" ... ')
        log_stream.info(' ::: time_run "' + str(self.time_run) + '" -- time_pivot "' + str(time_pivot) + '"')
        log_stream.info(' ----> Time Report - Remap from "time_run" to "time_pivot" ... DONE')

        # info method end
        log_stream.info(' ----> Organize dynamic data [' + str(self.time_run) + '] ... DONE')

        return time_pivot, datasets_workspace, time_workspace

    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
