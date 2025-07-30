"""
Class Features

Name:          driver_data
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

from lib_data_io_csv import read_file_csv, write_file_csv
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_data import merge_data

from lib_utils_generic import fill_template_string, extract_subkeys, extract_subpart

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# debugging
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class DriverData
class DriverData:

    # ------------------------------------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self,
                 time_run, time_range,
                 src_dict, anc_dict, dst_dict,
                 geo_data,
                 tags_dict=None,
                 flag_update_anc=True, flag_update_dst=True):

        self.time_run = pd.Timestamp(time_run)
        self.time_range = time_range

        self.src_dict = src_dict
        self.anc_dict = anc_dict
        self.dst_dict = dst_dict
        self.tags_dict = tags_dict

        self.geo_data = geo_data

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.format_tag, self.type_tag, self.delimiter_tag = 'format', 'type', 'delimiter'

        self.obj_src = deepcopy(self.src_dict)

        self.file_name_src = src_dict[self.file_name_tag]
        self.folder_name_src = src_dict[self.folder_name_tag]
        self.path_name_src = os.path.join(self.folder_name_src, self.file_name_src)
        self.delimiter_src = src_dict[self.delimiter_tag]

        self.file_name_anc = anc_dict[self.file_name_tag]
        self.folder_name_anc = anc_dict[self.folder_name_tag]
        self.path_name_anc = os.path.join(self.folder_name_anc, self.file_name_anc)

        self.file_name_dst = dst_dict[self.file_name_tag]
        self.folder_name_dst = dst_dict[self.folder_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        self.delimiter_dst = dst_dict[self.delimiter_tag]

        self.flag_update_anc = flag_update_anc
        self.flag_update_dst = flag_update_dst

        self.geo_dset_name = extract_subpart(self.geo_data, 'name')
        self.geo_dset_code = extract_subpart(self.geo_data, 'alert_area')
        self.geo_dset_catchments = extract_subpart(self.geo_data, 'catchment')

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
    # method to organize analysis datasets
    def organize_data(self):

        # info method start
        log_stream.info(' ----> Organize dynamic data [' + str(self.time_run) + '] ... ')

        # get time info
        time_run = self.time_run
        # get geo data
        geo_data = self.geo_data

        # get flags
        flag_update_anc, flag_update_dst = self.flag_update_anc, self.flag_update_dst

        # define ancillary path names
        path_name_anc = fill_template_string(
            template_str=deepcopy(self.path_name_anc),
            template_map=self.tags_dict,
            value_map={'ancillary_sub_path_time_run': time_run, "ancillary_datetime_run": time_run,
                       "ancillary_sub_path_time": time_run, "ancillary_datetime": time_run})

        # define destination path names
        path_name_dst = fill_template_string(
            template_str=deepcopy(self.path_name_dst),
            template_map=self.tags_dict,
            value_map={'destination_sub_path_time_run': time_run, "destination_datetime_run": time_run,
                       "destination_sub_path_time": time_run, "destination_datetime": time_run})

        # remove file if it is needed by the procedure
        if flag_update_anc:
            if os.path.exists(path_name_anc):
                os.remove(path_name_anc)
            if os.path.exists(path_name_dst):
                os.remove(path_name_dst)
        if flag_update_dst:
            if os.path.exists(path_name_dst):
                os.remove(path_name_dst)

        # check if the ancillary file exists
        if not os.path.exists(path_name_anc):

            # info collect data start
            log_stream.info(' -----> Collect datasets ... ')

            # iterate over groups
            df_common = None
            for geo_key, geo_info in geo_data.items():

                # info domain start
                log_stream.info(' -----> Area "' + str(geo_key) + '" ...' )

                # define source path names (rain, soil moisture, soil slips)
                path_name_src = fill_template_string(
                    template_str=deepcopy(self.path_name_src),
                    template_map=self.tags_dict,
                    value_map={'source_sub_path_time_run': time_run, "source_datetime_run": time_run,
                               "source_sub_path_time": time_run, "source_datetime": time_run,
                               'alert_area_name': geo_key})

                # check if source is defined and exists
                if os.path.exists(path_name_src):

                    # read data from source file
                    df_raw = read_file_csv(
                        path_name_src, delimiter=self.delimiter_src, fields=None,
                        time_col='time', time_index=True,
                        allowed_prefix=None, prefix_key=None, result_format='dataframe')

                    # info domain end (done)
                    log_stream.info(' -----> Area "' + str(geo_key) + '" ... DONE')

                else:
                    # info data collection end (failed)
                    df_raw = None
                    # info domain end (done)
                    log_stream.info(' -----> Area "' + str(geo_key) + '" ... FAILED. Source file not found: ' + path_name_src)

                # save data in workspace
                df_common = merge_data(df_common, df_raw)

            # info collect data end
            log_stream.info(' -----> Collect datasets ... DONE')

            # info merge data start
            log_stream.info(' -----> Merge datasets ... ')



            # info merge data end
            log_stream.info(' -----> Merge datasets ... DONE')
        else:

            # read datasets from file
            datasets_workspace = read_obj(path_name_anc)

        # info method end
        log_stream.info(' ----> Organize dynamic data [' + str(self.time_run) + '] ... DONE')

        return datasets_workspace

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
