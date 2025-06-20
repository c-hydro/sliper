"""
Class Features

Name:          driver_data_io_geo_point_weather_stations
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220428'
Version:       '2.0.0'
"""

######################################################################################
# Library
import logging
import os

from lib_utils_data_point_weather_stations import read_point_file, \
    join_point_and_grid, join_point_and_neighbours, \
    save_point_dframe2csv, read_point_csv2dframe
from lib_data_io_pickle import write_obj, read_obj
from lib_utils_geo import km_2_degree
from lib_utils_system import make_folder
from lib_utils_io_obj import filter_obj_variables, filter_obj_datasets

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverGeo
class DriverGeoPoint:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, src_dict, dst_dict=None, tmp_dict=None,
                 collections_data_geo=None, collections_data_group=None,
                 flag_point_data_src='weather_stations', flag_point_data_dst='weather_stations',
                 flag_grid_data='geo',
                 alg_template_tags=None, flag_geo_updating=True,
                 search_radius_km=10):

        self.flag_point_data_src = flag_point_data_src
        self.flag_point_data_dst = flag_point_data_dst
        self.flag_grid_data = flag_grid_data

        self.file_name_tag = 'file_name'
        self.folder_name_tag = 'folder_name'

        self.point_registry_data_tag = 'registry_data'
        self.point_registry_tree_tag = 'registry_tree'

        self.point_code_tag = 'code'
        self.point_name_tag = 'name'
        self.point_longitude_tag = 'longitude'
        self.point_latitude_tag = 'latitude'

        self.alert_area_tag = 'alert_area'
        self.alert_area_pivot_name_vector = 'alert_area:vector_data'
        self.alert_area_pivot_name_mask = 'alert_area:mask_data'

        self.geo_data = collections_data_geo
        self.group_data = collections_data_group
        self.alg_template_tags = alg_template_tags

        # flags for updating dataset(s)
        self.flag_geo_updating = flag_geo_updating
        # search radius (to define searched area)
        self.search_radius_km = search_radius_km

        self.file_name_point_registry_data_src = src_dict[
            self.flag_point_data_src][self.point_registry_data_tag][self.file_name_tag]
        self.folder_name_point_registry_data_src = src_dict[
            self.flag_point_data_src][self.point_registry_data_tag][self.folder_name_tag]
        self.file_path_point_registry_data_src = os.path.join(
            self.folder_name_point_registry_data_src, self.file_name_point_registry_data_src)

        self.file_name_point_registry_data_dst = dst_dict[
            self.flag_point_data_dst][self.point_registry_data_tag][self.file_name_tag]
        self.folder_name_point_registry_data_dst = dst_dict[
            self.flag_point_data_dst][self.point_registry_data_tag][self.folder_name_tag]
        self.file_path_point_registry_data_dst = os.path.join(
            self.folder_name_point_registry_data_dst, self.file_name_point_registry_data_dst)

        self.file_name_point_registry_tree_dst = dst_dict[
            self.flag_point_data_dst][self.point_registry_tree_tag][self.file_name_tag]
        self.folder_name_point_registry_tree_dst = dst_dict[
            self.flag_point_data_dst][self.point_registry_tree_tag][self.folder_name_tag]
        self.file_path_point_registry_tree_dst = os.path.join(
            self.folder_name_point_registry_tree_dst, self.file_name_point_registry_tree_dst)

        # select alert area vector and mask dataset(s)
        vars_alert_area_vector = filter_obj_variables(
            list(self.geo_data[self.alert_area_tag].keys()), self.alert_area_pivot_name_vector)
        vars_alert_area_mask = filter_obj_variables(
            list(self.geo_data[self.alert_area_tag].keys()), self.alert_area_pivot_name_mask)

        self.geo_alert_area_vector = filter_obj_datasets(self.geo_data[self.alert_area_tag], vars_alert_area_vector)
        self.geo_alert_area_mask = filter_obj_datasets(self.geo_data[self.alert_area_tag], vars_alert_area_mask)

        # tmp object(s)
        self.folder_name_tmp = tmp_dict[self.folder_name_tag]
        self.file_name_tmp = tmp_dict[self.file_name_tag]

        # define radius for searching object(s)
        self.radius_distance_max = km_2_degree(self.search_radius_km)
        self.radius_distance_inf = float("inf")

        self.tag_sep = ':'

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize data
    def organize_data(self):

        # Starting info
        log_stream.info(' ----> Organize weather stations point information ... ')

        file_path_point_registry_data_src = self.file_path_point_registry_data_src
        file_path_point_registry_data_dst = self.file_path_point_registry_data_dst
        file_path_point_registry_tree_dst = self.file_path_point_registry_tree_dst

        if self.flag_geo_updating:
            if os.path.exists(file_path_point_registry_data_dst):
                os.remove(file_path_point_registry_data_dst)
            if os.path.exists(file_path_point_registry_tree_dst):
                os.remove(file_path_point_registry_tree_dst)

        log_stream.info(' -----> Define registry points data ... ')
        if not os.path.exists(file_path_point_registry_data_dst):

            obj_point_registry = read_point_file(file_path_point_registry_data_src)
            obj_point_registry = join_point_and_grid(
                obj_point_registry, self.geo_alert_area_vector, max_distance=self.radius_distance_max,
                point_code_tag=self.point_code_tag, point_area_tag=self.alert_area_tag,
                point_longitude_tag=self.point_longitude_tag, point_latitude_tag=self.point_latitude_tag
            )

            folder_name, file_name = os.path.split(file_path_point_registry_data_dst)
            make_folder(folder_name)
            save_point_dframe2csv(file_path_point_registry_data_dst, obj_point_registry)

            log_stream.info(' -----> Define registry points data ... DONE')
        else:
            obj_point_registry = read_point_csv2dframe(file_path_point_registry_data_dst)
            log_stream.info(' -----> Define registry points data ... SKIPPED. Datasets was previously computed.')

        log_stream.info(' -----> Define registry points tree ... ')
        if not os.path.exists(file_path_point_registry_tree_dst):

            obj_point_tree = join_point_and_neighbours(
                obj_point_registry,
                max_distance=self.radius_distance_max, inf_distance=self.radius_distance_inf,
                point_code_tag=self.point_code_tag, point_name_tag=self.point_name_tag,
                point_area_tag=self.alert_area_tag,
                point_longitude_tag=self.point_longitude_tag, point_latitude_tag=self.point_latitude_tag
            )

            folder_name, file_name = os.path.split(file_path_point_registry_tree_dst)
            make_folder(folder_name)
            write_obj(file_path_point_registry_tree_dst, obj_point_tree)

            log_stream.info(' -----> Define registry points tree ... DONE')

        else:
            obj_point_tree = read_obj(file_path_point_registry_tree_dst)
            log_stream.info(' -----> Define registry points tree ... SKIPPED. Datasets was previously computed.')

        return obj_point_tree

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
