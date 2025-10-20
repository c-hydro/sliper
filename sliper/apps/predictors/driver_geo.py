"""
Class Features

Name:          driver_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os

from copy import deepcopy

from lib_data_io_csv import read_file_csv
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_data_geo import translate_geo_object
from lib_utils_generic import fill_template_string

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Class DriverGeo
class DriverGeo:

    # ------------------------------------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, src_dict, dst_dict, tmp_dict,
                 tags_dict=None, flag_update=True):

        self.tags_dict = tags_dict

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.type_tag, self.format_tag = 'type', 'format'

        # source object(s)
        self.folder_name_src, self.file_name_src = src_dict[self.folder_name_tag], src_dict[self.file_name_tag]
        self.path_name_src = os.path.join(self.folder_name_src, self.file_name_src)
        self.type_src = src_dict[self.type_tag] if self.type_tag in list(src_dict.keys()) else 'vector'
        self.format_src = src_dict[self.format_tag] if self.format_tag in list(src_dict.keys()) else 'csv'

        # destination object(s)
        self.folder_name_dst, self.file_name_dst = dst_dict[self.folder_name_tag], dst_dict[self.file_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        self.type_dst = dst_dict[self.type_tag] if self.type_tag in list(src_dict.keys()) else 'vector'
        self.format_dst = dst_dict[self.format_tag] if self.format_tag in list(src_dict.keys()) else 'workspace'

        # tmp object(s)
        self.folder_name_tmp, self.file_name_tmp = tmp_dict[self.folder_name_tag], tmp_dict[self.file_name_tag]

        # flags for updating dataset(s)
        self.flag_update = flag_update

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to wrap geographical data
    def wrap_geo_data(self, file_name: str, file_type: str, file_format: str, file_mandatory: bool = True):

        # info start
        log_stream.info(' -----> Compute geo data ... ')

        # check data availability
        if os.path.exists(file_name):

            if file_format == 'csv':

                if file_type == 'vector':
                    # read csv file
                    tmp_obj = read_file_csv(
                        self.path_name_src,
                        key_column='name', delimiter=',')
                    # translate object to the correct format
                    geo_obj = translate_geo_object(tmp_obj, id_range=None, rgb_range=None)

                else:
                    log_stream.error(' ===> File type "' + file_type + '" for alert areas info is not supported')
                    raise IOError('Check your configuration file')

            else:
                log_stream.error(' ===> File format "' + file_format + '" for alert areas is not supported')
                raise IOError('Check your configuration file')

        else:
            if file_mandatory:
                log_stream.error(' ===> File "' + file_name + '" is not available')
                raise IOError('Check your configuration file')
            else:
                # if file is not mandatory, return None
                log_stream.warning(' ===> File "' + file_name + '" is not available')
                geo_obj = None

        # info end
        log_stream.info(' -----> Compute geo data ... DONE')

        return geo_obj

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Method to organize geographical data
    def organize_data(self):

        # info start
        log_stream.info(' ----> Organize alert area geographical data ... ')

        # define path name destination
        path_name_dst = fill_template_string(
            template_str=deepcopy(self.path_name_dst),
            template_map=self.tags_dict, value_map={})

        # update datasets if necessary
        if self.flag_update:
            if os.path.exists(path_name_dst):
                os.remove(path_name_dst)

        # check if the destination file exists
        if not os.path.exists(path_name_dst):

            # read geo data info source
            geo_info = self.wrap_geo_data(
                file_name=self.path_name_src, file_type=self.type_src, file_format=self.format_src,
                file_mandatory=True)

            # organize geo data
            geo_collections = {'info': geo_info}

            # save geo data
            folder_name_dst, file_name_dst = os.path.split(path_name_dst)
            os.makedirs(folder_name_dst, exist_ok=True)

            write_obj(path_name_dst, geo_collections)

        else:

            # read geo data from file
            geo_collections = read_obj(path_name_dst)
            geo_info = geo_collections['info']

        # info end
        log_stream.info(' ----> Organize alert area geographical data ... DONE')

        return geo_info
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
