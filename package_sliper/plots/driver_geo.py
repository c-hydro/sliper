"""
Class Features

Name:          driver_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250731'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os

from copy import deepcopy

from lib_data_io_csv import read_file_csv
from lib_data_io_shp import read_file_shp, convert_shp_2_tiff
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_geo import translate_geo_object
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

        # source object(s) (info and shapefile)
        src_dict_info = src_dict['info']
        self.folder_name_src_info, self.file_name_src_info = src_dict_info[self.folder_name_tag], src_dict_info[self.file_name_tag]
        self.path_name_src_info = os.path.join(self.folder_name_src_info, self.file_name_src_info)
        self.type_src_info = src_dict_info[self.type_tag] if self.type_tag in list(src_dict_info.keys()) else 'vector'
        self.format_src_info = src_dict_info[self.format_tag] if self.format_tag in list(src_dict_info.keys()) else 'csv'

        src_dict_shp = src_dict['shapefile']
        self.folder_name_src_shp, self.file_name_src_shp = src_dict_shp[self.folder_name_tag], src_dict_shp[self.file_name_tag]
        self.path_name_src_shp = os.path.join(self.folder_name_src_shp, self.file_name_src_shp)
        self.type_src_shp = src_dict_shp[self.type_tag] if self.type_tag in list(src_dict_shp.keys()) else 'vector'
        self.format_src_shp = src_dict_shp[self.format_tag] if self.format_tag in list(src_dict_shp.keys()) else 'shapefile'

        # destination object(s)
        self.folder_name_dst, self.file_name_dst = dst_dict[self.folder_name_tag], dst_dict[self.file_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)

        # tmp object(s)
        self.folder_name_tmp, self.file_name_tmp = tmp_dict[self.folder_name_tag], tmp_dict[self.file_name_tag]

        # flags for updating dataset(s)
        self.flag_update = flag_update

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to wrap geographical data
    def wrap_geo_data(self, file_name: str, file_type: str, file_format: str, file_mandatory: bool = True):

        # info start
        log_stream.info(' -----> Get source ... ')

        # check data availability
        if os.path.exists(file_name):

            if file_format == 'shapefile' or file_format == 'shp':

                if file_type == 'vector':
                    # get file data
                    geo_dframe, geo_collections, geo_geoms = read_file_shp(file_name)

                    # convert shapefile to raster
                    geo_da = convert_shp_2_tiff(
                        file_name, pixel_size=0.001, burn_value=1, epsg=4326, folder_tmp=self.folder_name_tmp)

                    geo_obj = {'geo_da': geo_da, 'geo_dframe': geo_dframe,
                               'geo_polygons': geo_collections, 'geo_geoms': geo_geoms}

                else:
                    log_stream.error(' ===> File type "' + file_type + '" for alert areas domains is not supported')
                    raise IOError('Check your configuration file')

            elif file_format == 'csv':

                if file_type == 'vector':
                    # read csv file
                    tmp_obj = read_file_csv(
                        self.path_name_src_info, key_column='name', delimiter=',')
                    # translate object to the correct format
                    geo_obj = translate_geo_object(tmp_obj)
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
        log_stream.info(' -----> Get source ... DONE')

        return geo_obj

    # ------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------------------------------------------
    # Method to organize geographical data
    def organize_data(self):

        # info method start
        log_stream.info(' ----> Organize geographical datasets ... ')

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
                file_name=self.path_name_src_info, file_type=self.type_src_info, file_format=self.format_src_info,
                file_mandatory=True)

            # read geo data shapefile source
            geo_shp = self.wrap_geo_data(
                file_name=self.path_name_src_shp, file_type=self.type_src_shp, file_format=self.format_src_shp,
                file_mandatory=True)
            geo_da, geo_dframe = geo_shp['geo_da'], geo_shp['geo_dframe']
            geo_polygons, geo_geoms = geo_shp['geo_polygons'], geo_shp['geo_geoms']

            # organize geo data
            geo_collections = {'info': geo_info, 'geo_dframe': geo_dframe, 'geo_darray': geo_da}

            # save geo data
            folder_name_dst, file_name_dst = os.path.split(path_name_dst)
            os.makedirs(folder_name_dst, exist_ok=True)

            write_obj(path_name_dst, geo_collections)

        else:

            # read geo data from file
            geo_collections = read_obj(path_name_dst)
            geo_info = geo_collections['info']
            geo_da, geo_dframe = geo_collections['geo_darray'], geo_collections['geo_dframe']

        # info method end
        log_stream.info(' ----> Organize geographical datasets ... DONE')

        return geo_info, geo_da, geo_dframe
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
