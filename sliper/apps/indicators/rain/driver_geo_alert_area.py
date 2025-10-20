"""
Class Features

Name:          driver_geo_alert_area
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os

import numpy as np
import xarray as xr

from copy import deepcopy

from lib_data_io_csv import read_file_csv
from lib_data_io_shp import read_file_shp, convert_shp_2_tiff, convert_polygons_2_shp
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_data_alert_area import translate_geo_object, merge_geo_objects
from lib_utils_geo import resample_index, resample_data, compute_grid_from_bounds, find_points_with_buffer
from lib_utils_generic import create_filename_tmp, fill_template_string

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Class DriverGeo
class DriverGeoAlertArea:

    # ------------------------------------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, src_dict, dst_dict, anc_dict, info_dict, tmp_dict,
                 tags_dict=None,
                 flag_update=True):

        self.tags_dict = tags_dict

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.type_tag, self.format_tag = 'type', 'format'
        self.xll_corner_tag, self.yll_corner_tag = 'xll_corner', 'yll_corner'
        self.cell_size_tag, self.rows_tag, self.cols_tag   = 'cell_size', 'rows', 'cols'
        self.geo_src_tag, self.geo_dst_tag = 'geo_src', 'geo_dst'

        self.obj_fields_file = [self.file_name_tag, self.folder_name_tag]
        self.obj_fields_grid = [self.xll_corner_tag, self.yll_corner_tag,
                                self.cell_size_tag, self.rows_tag, self.cols_tag]
        self.obj_keys_delimiter = ':'

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

        # ancillary object(s) (vector, mask, index grid, index circle)
        anc_dict_vec = anc_dict['vector_data']
        self.folder_name_anc_vec, self.file_name_anc_vec = anc_dict_vec[self.folder_name_tag], anc_dict_vec[self.file_name_tag]
        self.path_name_anc_vec = os.path.join(self.folder_name_anc_vec, self.file_name_anc_vec)

        anc_dict_mask = anc_dict['mask_data']
        self.folder_name_anc_mask, self.file_name_anc_mask = anc_dict_mask[self.folder_name_tag], anc_dict_mask[self.file_name_tag]
        self.path_name_anc_mask = os.path.join(self.folder_name_anc_mask, self.file_name_anc_mask)

        anc_dict_idx_grid = anc_dict['index_grid_data']
        self.folder_name_anc_idx_grid, self.file_name_anc_idx_grid = anc_dict_idx_grid[self.folder_name_tag], anc_dict_idx_grid[self.file_name_tag]
        self.path_name_anc_idx_grid = os.path.join(self.folder_name_anc_idx_grid, self.file_name_anc_idx_grid)

        anc_dict_idx_circle = anc_dict['index_circle_data']
        self.folder_name_anc_idx_circle, self.file_name_anc_idx_circle = anc_dict_idx_circle[self.folder_name_tag], anc_dict_idx_circle[self.file_name_tag]
        self.path_name_anc_idx_circle = os.path.join(self.folder_name_anc_idx_circle, self.file_name_anc_idx_circle)

        # destination object(s)
        self.folder_name_dst, self.file_name_dst = dst_dict[self.folder_name_tag], dst_dict[self.file_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)

        # tmp object(s)
        self.folder_name_tmp, self.file_name_tmp = tmp_dict[self.folder_name_tag], tmp_dict[self.file_name_tag]

        # info object(s)
        self.info_parameters = info_dict['parameters'] if 'parameters' in list(info_dict.keys()) else None
        self.info_group = info_dict['group'] if 'group' in list(info_dict.keys()) else None

        if self.info_group is None:
            log_stream.error(' ===> Info group dictionary is not defined')
            raise IOError('Check your configuration file')
        if self.info_parameters is None:
            log_stream.error(' ===> Info parameters dictionary is not defined')
            raise IOError('Check your configuration file')

        # flags for updating dataset(s)
        self.flag_update = flag_update

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to wrap geographical data
    def wrap_geo_data(self, file_name: str, file_type: str, file_format: str, file_mandatory: bool = True):

        # info start
        log_stream.info(' -----> Compute geo datasets ... ')

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
                        self.path_name_src_info,
                        key_column='name', delimiter=',')
                    # translate object to the correct format
                    tmp_obj = translate_geo_object(tmp_obj)
                    # merge geo objects
                    geo_obj = merge_geo_objects(tmp_obj, self.info_group, name_object_sort=True)

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
        log_stream.info(' -----> Compute geo datasets ... DONE')

        return geo_obj

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to wrap geographical ancillary data
    def wrap_geo_ancillary(self, geo_info_src: dict, geo_da_src: xr.DataArray,
                           geo_polygons_src: dict = None, geo_mandatory: bool = True):

        # info start
        log_stream.info(' -----> Compute geo ancillary ... ')

        if geo_da_src is None or geo_polygons_src is None:
            if geo_mandatory:
                log_stream.error(' ===> Source or polygons geographical data are not available')
                raise IOError('Check your configuration file')
            else:
                log_stream.warning(' ===> Source or polygons geographical data are not available')
                return None, None, None, None

        # get parameters
        point_circle_radius = self.info_parameters['point_circle_radius']
        point_grid_cell_size = self.info_parameters['point_grid_cell_size']

        # iterate over group(s)
        geo_data_group, geo_point_group = {}, {}
        for group_name, group_settings in geo_info_src.items():

            # info domain start
            log_stream.info(' ------> Analyze "' + group_name + '" ... ')

            # extract group information
            group_info = group_settings['info']
            # get geo information
            geo_polygons_group = geo_polygons_src[group_info['alert_area']]

            # get path(s) vector
            path_name_vec = fill_template_string(
                template_str=deepcopy(self.path_name_anc_vec),
                template_map=self.tags_dict, value_map={'alert_area_name': group_info['name']})
            folder_name_vec, file_name_vec = os.path.split(path_name_vec)
            os.makedirs(folder_name_vec, exist_ok=True)

            # get path(s) mask
            path_name_mask = fill_template_string(
                template_str=deepcopy(self.path_name_anc_mask),
                template_map=self.tags_dict, value_map={'alert_area_name': group_info['name']})
            folder_name_mask, file_name_mask = os.path.split(path_name_mask)
            os.makedirs(folder_name_mask, exist_ok=True)

            # get path(s) index grid
            path_name_index_grid = fill_template_string(
                template_str=deepcopy(self.path_name_anc_idx_grid),
                template_map=self.tags_dict, value_map={'alert_area_name': group_info['alert_area']})
            folder_name_index_grid, file_name_index_grid = os.path.split(path_name_index_grid)
            os.makedirs(folder_name_index_grid, exist_ok=True)

            # get path(s) index circle
            path_name_index_circle = fill_template_string(
                template_str=deepcopy(self.path_name_anc_idx_circle),
                template_map=self.tags_dict, value_map={'alert_area_name': group_info['alert_area']})
            folder_name_index_circle, file_name_index_circle = os.path.split(path_name_index_circle)
            os.makedirs(folder_name_index_circle, exist_ok=True)

            # get path(s) mask
            path_name_mask = fill_template_string(
                template_str=deepcopy(self.path_name_anc_mask),
                template_map=self.tags_dict, value_map={'alert_area_name': group_info['alert_area']})
            folder_name_mask, file_name_mask = os.path.split(path_name_mask)
            os.makedirs(folder_name_mask, exist_ok=True)

            # update datasets if necessary
            if self.flag_update:
                if os.path.exists(path_name_vec):
                    os.remove(path_name_vec)
                if os.path.exists(path_name_mask):
                    os.remove(path_name_mask)
                if os.path.exists(path_name_index_grid):
                    os.remove(path_name_index_grid)
                if os.path.exists(path_name_index_circle):
                    os.remove(path_name_index_circle)

            # info alert area mask start
            log_stream.info(' -------> Create alert area mask ... ')

            # convert polygons to shapefile
            convert_polygons_2_shp(geo_polygons_group, path_name_vec)
            # convert shapefile to raster
            geo_mask_alert_area = convert_shp_2_tiff(path_name_vec, path_name_mask, tiff_remove=False,
                                               pixel_size=0.001, burn_value=1, epsg=4326)
            geo_mask_alert_area.attrs = {'file_name': path_name_mask}

            """ debugging plots
            import matplotlib.pylab as plt
            plt.figure()
            plt.imshow(geo_mask_alert_area.values)
            plt.colorbar()
            plt.show()
            """

            # info alert area mask end
            log_stream.info(' -------> Create alert area mask ... DONE')

            # info alert area polygons start
            log_stream.info(' -------> Create alert area polygons ... ')
            geo_polygons_alert_area, _, _ = read_file_shp(path_name_vec)
            geo_polygons_alert_area.attrs = {'file_name': path_name_vec}
            # info alert area polygons end
            log_stream.info(' -------> Create alert area polygons ... DONE')

            # info alert area index grid start
            log_stream.info(' -------> Create index grid datasets ... ')
            if not os.path.exists(path_name_index_grid):

                # compute resampling index
                (valid_input_index, valid_output_index,
                 index_array, weight_array) = resample_index(geo_da_src, geo_mask_alert_area)

                # create geo grid collections
                geo_data_collections = {
                    self.geo_src_tag: geo_da_src,
                    self.geo_dst_tag: geo_mask_alert_area,
                    'valid_input_index': valid_input_index,
                    'valid_output_index': valid_output_index,
                    'index_array': index_array,
                    'weight_array': weight_array
                }

                # dump geo data collections
                write_obj(path_name_index_grid, geo_data_collections)

                # info alert area index grid end
                log_stream.info(' -------> Create index grid datasets ... DONE')

            else:

                # info alert area index grid end (previously created)
                log_stream.info(
                    ' -------> Create index grid datasets ... SKIPPED. Datasets previously created.')
                geo_data_collections = read_obj(path_name_index_grid)

            # info alert area index circle start
            log_stream.info(' -------> Create index circle datasets ... ')
            if not os.path.exists(path_name_index_circle):

                # organize geo data
                geo_data = geo_mask_alert_area.values
                geo_x_data_1d = geo_mask_alert_area['longitude'].values
                geo_y_data_1d = geo_mask_alert_area['latitude'].values
                geo_x_data_2d, geo_y_data_2d = np.meshgrid(geo_x_data_1d, geo_y_data_1d)

                # compute grid from bounds
                points_values, points_x_values_2d, points_y_values_2d = compute_grid_from_bounds(
                    geo_data, geo_x_data_2d, geo_y_data_2d, km=point_grid_cell_size)

                # find area points with buffer
                geo_points_collection = find_points_with_buffer(
                    points_values, points_x_values_2d, points_y_values_2d,
                    geo_data, geo_x_data_2d, geo_y_data_2d,
                    point_buffer=point_circle_radius)

                # dump geo data collections
                write_obj(path_name_index_circle, geo_points_collection)

                # info alert area index circle end
                log_stream.info(' -------> Create index circle datasets ... DONE')

            else:
                # info alert area index circle end (previously created)
                log_stream.info(
                    ' -------> Create index circle datasets ... SKIPPED. Datasets previously created.')
                geo_points_collection = read_obj(path_name_index_grid)

            # organize geo data and points groups
            geo_data_group[group_name] = geo_data_collections
            geo_point_group[group_name] = geo_points_collection

            # info domain end
            log_stream.info(' ------> Analyze "' + group_name + '" ... DONE')

        # info end
        log_stream.info(' -----> Compute geo ancillary ... DONE')

        return geo_data_group, geo_point_group
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
                file_name=self.path_name_src_info, file_type=self.type_src_info, file_format=self.format_src_info,
                file_mandatory=True)

            # read geo data shapefile source
            geo_shp = self.wrap_geo_data(
                file_name=self.path_name_src_shp, file_type=self.type_src_shp, file_format=self.format_src_shp,
                file_mandatory=True)
            geo_da, geo_dframe = geo_shp['geo_da'], geo_shp['geo_dframe']
            geo_polygons, geo_geoms = geo_shp['geo_polygons'], geo_shp['geo_geoms']

            # get geo data ancillary
            geo_data, geo_point = self.wrap_geo_ancillary(geo_info, geo_da, geo_polygons)

            # organize geo data
            geo_collections = {
                'info': geo_info, 'geo_data': geo_data, 'geo_point': geo_point}

            # save geo data
            folder_name_dst, file_name_dst = os.path.split(path_name_dst)
            os.makedirs(folder_name_dst, exist_ok=True)

            write_obj(path_name_dst, geo_collections)

        else:

            # read geo data from file
            geo_collections = read_obj(path_name_dst)
            geo_info = geo_collections['info']
            geo_data, geo_point = geo_collections['geo_data'], geo_collections['geo_point']

        # info end
        log_stream.info(' ----> Organize alert area geographical data ... DONE')

        return geo_info, geo_data, geo_point
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
