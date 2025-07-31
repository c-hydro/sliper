"""
Class Features

Name:          driver_data
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.5.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import numpy as np
import pandas as pd
import xarray as xr

from copy import deepcopy

from lib_data_io_csv import read_file_csv, write_file_csv
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_data_alert_area import (translate_points_object, filter_points_object,
                                       join_points_and_grid, count_points_and_thresholds)
from lib_utils_generic import fill_template_string, extract_subkeys

from lib_info_args import logger_name


# logging
log_stream = logging.getLogger(logger_name)

# debugging
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# DriverData class
class DriverData:

    # ------------------------------------------------------------------------------------------------------------------
    # class constructor
    def __init__(self, time_run,
                 src_dict, anc_dict, dst_dict, tags_dict=None, tmp_dict=None,
                 collections_data_geo_ref=None,
                 collections_data_geo_info=None, collections_data_geo_areas=None,
                 flag_update=True):

        # check if time_run is a pd.Timestamp object
        if not isinstance(time_run, pd.Timestamp):
            time_run = pd.Timestamp(time_run)
        self.time_run = time_run

        self.src_dict = src_dict
        self.anc_dict = anc_dict
        self.dst_dict = dst_dict
        self.tags_dict = tags_dict

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.format_tag, self.type_tag, self.delimiter_tag, self.fields_tag = 'format', 'type', 'delimiter', 'fields'

        # get geographical information
        self.data_group = collections_data_geo_info
        self.data_geo_grid_ref = collections_data_geo_ref
        self.data_geo_grid_areas = collections_data_geo_areas

        # source object(s)
        self.folder_name_src, self.file_name_src = src_dict[self.folder_name_tag], src_dict[self.file_name_tag]
        self.path_name_src = os.path.join(self.folder_name_src, self.file_name_src)
        self.type_src = src_dict.get(self.type_tag, 'vector')
        self.format_src = src_dict.get(self.format_tag, 'csv')
        self.delimiter_src = src_dict.get(self.delimiter_tag, ';')
        self.fields_src = src_dict.get(self.fields_tag, None)

        # destination object(s)
        self.folder_name_anc, self.file_name_anc = anc_dict[self.folder_name_tag], anc_dict[self.file_name_tag]
        self.path_name_anc = os.path.join(self.folder_name_anc, self.file_name_anc)

        # destination object(s)
        self.folder_name_dst, self.file_name_dst = dst_dict[self.folder_name_tag], dst_dict[self.file_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        self.type_dst = dst_dict.get(self.type_tag, 'vector')
        self.format_dst = dst_dict.get(self.format_tag, 'csv')
        self.delimiter_dst = dst_dict.get(self.delimiter_tag, ';')
        self.fields_dst = dst_dict.get(self.fields_tag, None)

        # tmp object(s)
        self.folder_name_tmp, self.file_name_tmp = tmp_dict[self.folder_name_tag], tmp_dict[self.file_name_tag]

        # flags for updating dataset(s)
        self.flag_update = flag_update

        # group names
        self.group_area_names = extract_subkeys(self.data_group, subkeys='alert_area', keep_keys=False)
        self.group_range_thresholds = extract_subkeys(
            self.data_group, subkeys=['white_range', 'green_range', 'yellow_range', 'orange_range', 'red_range'],
            keep_keys=True)
        self.group_range_id = extract_subkeys(
            self.data_group, subkeys=['white_id', 'green_id', 'yellow_id', 'orange_id', 'red_id'],
            keep_keys=True)
        self.group_range_rgb = extract_subkeys(
            self.data_group, subkeys=['white_rgb', 'green_rgb', 'yellow_rgb', 'orange_rgb', 'red_rgb'],
            keep_keys=True)

        # variable to set and keep the data
        self.active_data, self.active_file = None, None

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to dump data
    def dump_data(self, data: pd.DataFrame, var_name: str ='soil_slips'):

        # info method start
        log_stream.info(' ----> Dump dynamic data [' + str(self.time_run) + '] ... ')

        # get time information
        time_run = self.time_run

        # get group information
        group_info = self.data_group

        # get the type and format of source data
        type_dst, format_dst, fields_dst = self.type_dst, self.format_dst, self.fields_dst

        # iterate over group(s)
        for group_key, group_items in group_info.items():

            # info dump alart area information start
            log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... ')

            # check if group_key exists in data
            if group_key in list(data.keys()):

                # get group data information
                group_data_common = data[group_key]
                # filter group data by fields
                group_data_filter, group_time_filter = filter_points_object(group_data_common, fields=fields_dst)

                # get group time information
                if group_data_filter is not None:
                    # get time information
                    time_start, time_end = group_time_filter.min(), group_time_filter.max()
                else:
                    # if no data is available, set time_start and time_end to None
                    log_stream.warning(' ===> No time available for group "' + group_key + '"')
                    time_start, time_end = time_run, time_run

                # define destination path names
                path_name_dst = fill_template_string(
                    template_str=deepcopy(self.path_name_dst),
                    template_map=self.tags_dict,
                    value_map={
                        'destination_datetime': time_run, 'destination_sub_path': group_key,
                        'destination_sub_path_time': time_run, "destination_datetime_run": time_run,
                        'destination_datetime_start': time_start, 'destination_datetime_end': time_end,
                        'alert_area_name': group_key})

                # apply flags (to update datasets source and destination)
                if self.flag_update:
                    if os.path.exists(path_name_dst):
                        os.remove(path_name_dst)

                # check if destination file exists
                if not os.path.exists(path_name_dst):

                    # check type (only grid)
                    if type_dst == 'vector':

                        # check format (nc or tiff)
                        if format_dst == 'csv':

                            # dump datasets
                            folder_name_dst, file_name_dst = os.path.split(path_name_dst)
                            os.makedirs(folder_name_dst, exist_ok=True)

                            write_file_csv(group_data_filter, filename=path_name_dst, orientation='cols')

                            # info dump alart area information end (DONE)
                            log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... DONE')

                        else:
                            # if the destination data format is not supported, raise an error
                            log_stream.error(' ===> Filename format is not allowed')
                            log_stream.info(' ------> Save datasets ... FAILED')
                            raise NotImplementedError('Only "csv" format is available.')

                    else:
                        # if the destination data type is not supported, raise an error
                        log_stream.error(' ===> Destination data type is not supported')
                        log_stream.info(' ------> Save datasets ... FAILED')
                        raise NotImplementedError('Only "vector" type is available.')

                else:
                    # if the destination file exists, log a warning
                    log_stream.warning(' ===> File "' + path_name_dst + '" is already available.')
                    log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... SKIPPED')

            else:
                # if the group_key does not exist in data, log a warning
                log_stream.warning(' ===> Group "' + group_key + '" is not available in data')
                log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... SKIPPED')

        # info method end
        log_stream.info(' ----> Dump dynamic data [' + str(self.time_run) + '] ... DONE')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize data
    def organize_data(self, var_name='soil_slips'):

        # info method start
        log_stream.info(' ----> Organize dynamic data [' + str(self.time_run) + '] ... ')

        # get time info
        time_run = self.time_run

        # get geographical data
        geo_info_generic = self.data_group
        geo_info_names = self.group_area_names
        geo_range_thr = self.group_range_thresholds
        geo_range_id = self.group_range_id
        geo_range_rgb = self.group_range_rgb

        geo_grid_ref = self.data_geo_grid_ref
        geo_grid_areas = self.data_geo_grid_areas

        # get the type and format of source data
        type_src, format_src = self.type_src, self.format_src
        fields_src = self.fields_src

        # compose the path names using the template strings
        path_name_src = fill_template_string(
            template_str=deepcopy(self.path_name_src),
            template_map=self.tags_dict,
            value_map={'source_sub_path_time': time_run, "source_datetime": time_run})

        path_name_anc = fill_template_string(
            template_str=deepcopy(self.path_name_anc),
            template_map=self.tags_dict,
            value_map={'destination_sub_path_time': time_run, "destination_datetime": time_run})

        # apply the update flag (if set by the user)
        if self.flag_update:
            if os.path.exists(path_name_anc):
                os.remove(path_name_anc)

        # check if ancillary file exists
        if not os.path.exists(path_name_anc):

            # info get data start
            log_stream.info(' -----> Get source data ... ')

            # check if source file exists
            if os.path.exists(path_name_src):

                # check the type of source data (point or grid)
                if type_src == 'vector' or type_src == 'point':

                    # check the source data format (csv or xlsx)
                    if format_src == 'csv':

                        # read soil slips datasets
                        file_dframe = read_file_csv(
                            path_name_src, fields=fields_src, key_column=None,
                            delimiter=self.delimiter_src, encoding='latin', result_format='dataframe')

                        # method to translate the soil slips datasets
                        file_dframe, file_collections, file_geoms = translate_points_object(file_dframe)

                        # join points, areas and information
                        file_dframe = join_points_and_grid(
                            file_dframe, geo_grid_areas,
                            point_area_tag='alert_area_code',
                            point_longitude_tag='longitude', point_latitude_tag='latitude')

                        # count points by data and alert area
                        count_dframe = count_points_and_thresholds(
                            file_dframe, geo_info_names,
                            geo_range_thr, geo_range_id, geo_range_rgb)

                        # info get data end
                        log_stream.info(' -----> Get source data ... DONE')

                    else:
                        # info get data end
                        log_stream.error(' ===> Source data format is not supported. Check your source datasets')
                        log_stream.info(' -----> Get source data ... FAILED')
                        raise NotImplementedError('Only "csv" formats are available.')

                else:
                    # if the source data type is not supported, raise an error
                    log_stream.error(' ===> Source data type is not supported. Check your source datasets')
                    log_stream.info(' -----> Get source data ... FAILED')
                    raise NotImplementedError('Only "vector" types are available.')

            else:
                # if the source file does not exist, set file_obj to None and log a warning
                count_dframe = None
                log_stream.warning(' ===> File "' + path_name_src + '" is not available.')
                log_stream.info(' -----> Get source data ... SKIPPED')

            # info save data start
            log_stream.info(' -----> Save source data ... ')

            # check if file_obj is not None (i.e., source file exists)
            if count_dframe is not None:

                # save data in workspace object
                folder_name_anc, file_name_anc = os.path.split(path_name_anc)
                os.makedirs(folder_name_anc, exist_ok=True)

                write_obj(path_name_anc, count_dframe)

                # info save data end
                log_stream.info(' -----> Save source data ... DONE')

            else:

                # warning message (data object is not defined)
                log_stream.warning(' ===> Data object is not defined')
                # info save data end
                log_stream.info(' -----> Save source data ... SKIPPED')

        else:
            # if the ancillary file exists, read it from the workspace (previously saved)
            count_dframe = read_obj(path_name_anc)
            log_stream.info(' -----> Get source data ... DONE. Data previously saved: ' + path_name_anc)

        # info method end
        log_stream.info(' ----> Organize dynamic data [' + str(self.time_run) + '] ... DONE')

        return count_dframe

# ----------------------------------------------------------------------------------------------------------------------
