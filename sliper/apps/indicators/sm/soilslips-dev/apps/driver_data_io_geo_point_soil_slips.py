"""
Class Features

Name:          driver_data_io_geo_point_soil_slips
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20200515'
Version:       '1.0.0'
"""

######################################################################################
# Library
import logging
import os

from copy import deepcopy

from lib_data_io_pickle import read_obj, write_obj
from lib_utils_system import make_folder
from lib_utils_io_obj import filter_obj_variables, filter_obj_datasets

from lib_utils_geo import km_2_degree
from lib_utils_data_point_soil_slips import read_point_file, collect_point_data, join_point_and_grid

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
# import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverGeo
class DriverGeoPoint:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, src_dict, dst_dict=None,
                 collections_data_geo=None, collections_data_group=None,
                 flag_point_data_src='soil_slips', flag_point_data_dst='soil_slips',
                 flag_geo_updating=True, search_radius_km=5):

        self.flag_point_data_src = flag_point_data_src
        self.flag_point_data_dst = flag_point_data_dst

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.file_type_tag, self.file_delimiter_tag = 'type', 'delimiter'

        self.point_db_data_tag = 'database_data'

        self.structure_group_tag_name_src = 'name'
        self.structure_group_tag_threshold_src = 'warning_threshold'
        self.structure_group_tag_index_src = 'warning_index'
        self.structure_group_tag_features_src = None
        self.structure_group_tag_name_dst = None
        self.structure_group_tag_threshold_dst = 'event_threshold'
        self.structure_group_tag_index_dst = 'event_index'
        self.structure_group_tag_features_dst = 'event_features'

        self.column_db_list_alert_area = ['ZONA_ALLER', 'ALLERTA']
        self.column_db_tag_alert_area = None
        self.column_db_tag_time = 'DATA'
        self.column_db_point_code_tag = 'id_frana'
        self.column_db_point_name_tag = 'id_frana'
        self.column_db_point_longitude_tag = 'lon'
        self.column_db_point_latitude_tag = 'lat'

        self.alert_area_tag = 'alert_area'
        self.alert_area_pivot_name_vector = 'alert_area:vector_data'
        self.alert_area_pivot_name_mask = 'alert_area:mask_data'

        self.geo_data = collections_data_geo
        self.group_data = collections_data_group

        self.flag_geo_updating = flag_geo_updating
        # search radius (to define searched area)
        self.search_radius_km = search_radius_km

        self.file_name_src = src_dict[self.flag_point_data_src][self.point_db_data_tag][self.file_name_tag]
        self.folder_name_src = src_dict[self.flag_point_data_src][self.point_db_data_tag][self.folder_name_tag]
        self.file_path_src = os.path.join(self.folder_name_src, self.file_name_src)

        self.file_name_dst = dst_dict[self.flag_point_data_dst][self.point_db_data_tag][self.file_name_tag]
        self.folder_name_dst = dst_dict[self.flag_point_data_dst][self.point_db_data_tag][self.folder_name_tag]
        self.file_path_dst = os.path.join(self.folder_name_dst, self.file_name_dst)

        self.file_type_src, self.file_delimiter_src = 'shapefile', ';'
        if self.file_type_tag in list(src_dict[self.flag_point_data_dst][self.point_db_data_tag].keys()):
            self.file_type_src = src_dict[self.flag_point_data_dst][self.point_db_data_tag][self.file_type_tag]
        if self.file_delimiter_tag in list(src_dict[self.flag_point_data_dst][self.point_db_data_tag].keys()):
            self.file_delimiter = src_dict[self.flag_point_data_dst][self.point_db_data_tag][self.file_delimiter_tag]

        # select alert area vector and mask dataset(s)
        vars_alert_area_vector = filter_obj_variables(
            list(self.geo_data[self.alert_area_tag].keys()), self.alert_area_pivot_name_vector)
        vars_alert_area_mask = filter_obj_variables(
            list(self.geo_data[self.alert_area_tag].keys()), self.alert_area_pivot_name_mask)

        self.geo_alert_area_vector = filter_obj_datasets(self.geo_data[self.alert_area_tag], vars_alert_area_vector)
        self.geo_alert_area_mask = filter_obj_datasets(self.geo_data[self.alert_area_tag], vars_alert_area_mask)

        self.time_format = '%Y-%m-%d'

        # define radius for searching object(s)
        self.radius_distance_max = km_2_degree(self.search_radius_km)
        self.radius_distance_inf = float("inf")

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize data
    def organize_data(self):

        # Starting info
        log_stream.info(' ----> Organize soil slips point information ... ')

        group_data = self.group_data

        file_path_src = self.file_path_src
        file_path_dst = self.file_path_dst

        if self.flag_geo_updating:
            if os.path.exists(file_path_dst):
                os.remove(file_path_dst)

        log_stream.info(' -----> Define registry points data ... ')
        if not os.path.exists(file_path_dst):

            # read the soil-slips collections
            obj_point_registry = read_point_file(
                file_path_src, file_type=self.file_type_src, file_delimiter=self.file_delimiter_src)

            # set alert_area tag [ZONA_ALLER, ALLERTA]
            for column_db_tmp_alert_area in self.column_db_list_alert_area:
                if column_db_tmp_alert_area in obj_point_registry.columns:
                    self.column_db_tag_alert_area = deepcopy(column_db_tmp_alert_area)
                    break
            if self.column_db_tag_alert_area is None:
                log_stream.error(' ===> Alert area tag not found in soil slips data')
                raise IOError('Alert area tag could be defined with a mismatching tag name')

            # fill the soil-slips collections
            obj_point_registry = join_point_and_grid(
                obj_point_registry, self.geo_alert_area_vector,
                max_distance=self.radius_distance_max,
                point_code_tag=self.column_db_point_code_tag,
                point_area_tag=self.column_db_tag_alert_area,
                point_longitude_tag=self.column_db_point_longitude_tag,
                point_latitude_tag=self.column_db_point_latitude_tag
            )

            # collect and organize the soil-slips collections
            obj_point_collection = collect_point_data(
                obj_point_registry, group_data,
                point_time_format=self.time_format,
                point_dframe_alert_area_tag=self.column_db_tag_alert_area,
                point_dframe_time_tag=self.column_db_tag_time,
                point_group_name_tag_src=self.structure_group_tag_name_src,
                point_group_threshold_tag_src=self.structure_group_tag_threshold_src,
                point_group_index_tag_src=self.structure_group_tag_index_src,
                point_group_feature_tag_src=self.structure_group_tag_name_src,
                point_group_name_tag_dst=self.structure_group_tag_name_dst,
                point_group_threshold_tag_dst=self.structure_group_tag_threshold_dst,
                point_group_index_tag_dst=self.structure_group_tag_index_dst,
                point_group_feature_tag_dst=self.structure_group_tag_features_dst,
            )

            # save the soil-slips collections
            folder_name_dst, file_name_dst = os.path.split(file_path_dst)
            make_folder(folder_name_dst)
            write_obj(file_path_dst, obj_point_collection)

            log_stream.info(' -----> Define registry points data ... DONE')
        else:

            # read soil-slips collections from disk
            obj_point_collection = read_obj(file_path_dst)
            log_stream.info(' -----> Define registry points data ... SKIPPED. Datasets was previously computed.')

        log_stream.info(' ----> Organize soil slips point information ... DONE')

        return obj_point_collection

# -------------------------------------------------------------------------------------
