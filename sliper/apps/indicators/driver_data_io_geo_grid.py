"""
Class Features

Name:          driver_data_io_geo_grid
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20200515'
Version:       '1.0.0'
"""

######################################################################################
# Library
import logging
import numpy as np
import os
from copy import deepcopy

from lib_analysis_interpolation_grid import interp_grid2index
from lib_data_io_ascii import read_file_raster
from lib_data_io_shp import read_file_point as read_file_shp, convert_polygons_2_shp
from lib_utils_geo import create_grid, compute_grid_from_bounds, find_points_with_buffer, get_data_tiff
from lib_data_io_pickle import read_obj, write_obj
from lib_utils_system import get_dict_nested_value, get_dict_nested_all_values, get_dict_nested_all_keys, \
    create_filename_tmp, fill_tags2string, make_folder, join_path

from lib_utils_io_tiff import convert_shp_2_tiff

from lib_utils_io_obj import filter_obj_variables

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
# import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverGeo
class DriverGeoGrid:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, src_dict, dst_dict, tmp_dict,
                 collections_data_group=None, collections_options=None, alg_template_tags=None,
                 flag_geo_data_src='geo', flag_geo_data_dst='geo',
                 flag_geo_updating=True):

        self.flag_geo_data_src = flag_geo_data_src
        self.flag_geo_data_dst = flag_geo_data_dst
        self.alg_template_tags = alg_template_tags

        self.file_name_tag = 'file_name'
        self.folder_name_tag = 'folder_name'
        self.xll_corner_tag = 'xll_corner'
        self.yll_corner_tag = 'yll_corner'
        self.cell_size_tag = 'cell_size'
        self.rows_tag = 'rows'
        self.cols_tag = 'cols'

        self.obj_fields_file = [self.file_name_tag, self.folder_name_tag]
        self.obj_fields_grid = [self.xll_corner_tag, self.yll_corner_tag,
                                self.cell_size_tag, self.rows_tag, self.cols_tag]
        self.obj_keys_delimiter = ':'

        self.reference_tag = 'reference'
        self.region_tag = 'region'
        self.alert_area_tag = 'alert_area'
        self.catchment_tag = 'catchment'

        self.region_pivot_name_src = 'region:primary_data'
        self.region_pivot_name_dst_index = 'region:index_data'
        self.alert_area_pivot_name_src = 'alert_area:primary_data'
        self.alert_area_pivot_name_dst_vector = 'alert_area:vector_data'
        self.alert_area_pivot_name_dst_mask = 'alert_area:mask_data'
        self.alert_area_pivot_name_dst_idx_grid = 'alert_area:index_grid_data'
        self.alert_area_pivot_name_dst_idx_circle = 'alert_area:index_circle_data'

        self.group_data_structure = collections_data_group
        self.group_name_alert_area = self.define_obj_group(collections_data_group, tag_group='name')
        self.group_name_catchment = self.define_obj_group(collections_data_group, tag_group='catchment')

        self.point_circle_radius = collections_options['point_circle_radius']
        self.point_grid_cell_size = collections_options['point_grid_cell_size']

        # source object(s)
        self.src_dict_geo = src_dict[self.flag_geo_data_src]
        self.src_collection_geo = self.flat_obj_structure(
            self.src_dict_geo, obj_type_expected=['file', 'grid'], obj_keys_delimiter=self.obj_keys_delimiter,
            obj_collections=True)
        self.src_vars_geo = list(self.src_collection_geo.keys())

        # destination object(s)
        self.dst_dict_geo = dst_dict[self.flag_geo_data_dst]
        self.dst_collection_geo = self.flat_obj_structure(
            self.dst_dict_geo, obj_type_expected=['file'], obj_keys_delimiter=self.obj_keys_delimiter,
            obj_collections=True)
        self.dset_vars_geo = list(self.dst_collection_geo.keys())

        # tmp object(s)
        self.folder_name_tmp = tmp_dict[self.folder_name_tag]
        self.file_name_tmp = tmp_dict[self.file_name_tag]

        # flags for updating dataset(s)
        self.flag_geo_updating = flag_geo_updating

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define obj group
    @staticmethod
    def define_obj_group(data_group, tag_group='catchment'):
        list_group = []
        for key_group, fields_group in data_group.items():
            values_group = get_dict_nested_value(fields_group, [tag_group])
            list_group.extend(values_group)
        return list_group
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to map object keys
    def map_obj_keys(self, dict_structure, dict_keys_map=None, dict_keys_pivot=None):

        for dict_key, dict_fields in dict_structure.items():

            if dict_fields is None:
                dict_fields = {}
                dict_fields_first = []
            else:
                dict_fields_first = list(dict_fields.values())[0]

            if dict_fields_first is None:
                dict_fields_first = {}

            if isinstance(dict_fields_first, dict):

                dict_keys_tmp_in, list_extend = [], False
                if dict_keys_map is not None:
                    list_extend = True
                elif dict_keys_map is None:
                    list_extend = False

                if dict_keys_pivot is None:
                    dict_keys_pivot = dict_key
                if dict_keys_pivot != dict_key:
                    dict_keys_pivot = dict_key

                dict_keys_tmp_out = deepcopy(self.map_obj_keys(
                    dict_fields, dict_keys_tmp_in, dict_keys_pivot=dict_keys_pivot))

                if not list_extend:
                    dict_keys_map = deepcopy(dict_keys_tmp_out)
                else:
                    dict_keys_tmp = [dict_keys_map, dict_keys_tmp_out]
                    dict_keys_map = deepcopy(dict_keys_tmp)

            else:

                if dict_keys_pivot is None:
                    dict_keys_pivot = []
                if dict_keys_map is None:
                    dict_keys_map = []

                if dict_keys_pivot.__len__() == 0:
                    dict_keys_tmp = [dict_key]
                else:
                    dict_keys_tmp = [dict_keys_pivot, dict_key]
                dict_keys_map.append(dict_keys_tmp)

        return dict_keys_map

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define dictionary type
    def define_obj_type(self, obj_fields, obj_type_expected=None):

        if obj_type_expected is None:
            obj_type_expected = ['file', 'grid']
        obj_data, obj_type = None, None
        if (obj_fields is not None) and (isinstance(obj_fields, dict)):
            if all(key in list(obj_fields.keys()) for key in self.obj_fields_file):
                obj_type = 'file'
                folder_name_obj = obj_fields[self.folder_name_tag]
                file_name_obj = obj_fields[self.file_name_tag]
                if (folder_name_obj is not None) and (file_name_obj is not None):
                    obj_data = join_path(folder_name_obj, file_name_obj)
            elif all(key in list(obj_fields.keys()) for key in self.obj_fields_grid):
                obj_type = 'grid'
                obj_data = deepcopy(obj_fields)

        if obj_type is not None:
            if obj_type not in obj_type_expected:
                log_stream.error(' ===> Object type "' + obj_type + '" is not expected')
                raise RuntimeError('Expected type for this object could be "' + str(obj_type_expected) + '"')
        else:
            log_stream.warning(' ===> Object type is defined by NoneType. Check is this settings is correctly defined')

        return obj_type, obj_data

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to flat obj level(s)
    def flat_obj_structure(self, obj_fields,
                           obj_type_expected=None, obj_collections=False, obj_keys_delimiter=':'):

        if obj_type_expected is None:
            obj_type_expected = ['file']

        if not isinstance(obj_fields, dict):
            log_stream.error(' ===> Expected object must be a dictionary')
            raise RuntimeError('Other object type are not supported')

        map_keys_collections, key_pivot = {}, list(obj_fields.keys())
        for step_pivot in key_pivot:
            pivot_fields = obj_fields[step_pivot]
            items_fields = get_dict_nested_all_keys(pivot_fields, key_collection={}, key_list=[step_pivot])[1]

            map_keys_collections[step_pivot] = list(items_fields.values())

        obj_fields_collections = {}
        for map_key_pivot, map_key_list in map_keys_collections.items():

            if not isinstance(map_key_list[0], list):
                map_key_list = [map_key_list]

            for obj_keys_sublist in map_key_list:
                obj_values_nested = get_dict_nested_value(obj_fields, obj_keys_sublist)
                obj_values_type = self.define_obj_type(
                    obj_values_nested, obj_type_expected=obj_type_expected)
                obj_values_structure = self.define_obj_structure(
                    obj_values_type, obj_collections=obj_collections)
                obj_map_sublist = obj_keys_delimiter.join(obj_keys_sublist)
                obj_fields_collections[obj_map_sublist] = obj_values_structure

        return obj_fields_collections

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to define object structure
    def define_obj_structure(self, obj_fields, obj_collections=False):
        obj_type, obj_struct = obj_fields[0], None
        if (obj_type is not None) and (obj_type == 'file'):
            obj_file_path = obj_fields[1]
            if obj_file_path is not None:
                obj_folder_name, obj_file_name = os.path.split(obj_file_path)
                if obj_collections:
                    obj_struct = self.collect_obj_file(obj_folder_name, obj_file_name)
                else:
                    obj_struct = deepcopy(obj_file_path)
            else:
                log_stream.warning(' ===> Obj "file_path" is defined by "NoneType"')
                obj_struct = None
        elif (obj_type is not None) and (obj_type == 'grid'):
            obj_struct = deepcopy(obj_fields[1])
        elif obj_type is None:
            log_stream.warning(' ===> Obj type is defined by "NoneType"')
        else:
            log_stream.error(' ===> Obj type "' + obj_fields[0] + '" not accepted')
            raise NotImplementedError('Case not implemented yet')

        return obj_type, obj_struct
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect file
    def collect_obj_file(self, folder_name_raw, file_name_raw):

        data_group = self.group_data_structure

        if ('alert_area_name' in file_name_raw) or ('catchment_name' in file_name_raw):

            if 'alert_area_name' in file_name_raw:
                data_tag_src, data_tag_dst, data_level = 'name', 'alert_area_name', 1
            elif 'catchment_name' in file_name_raw:
                data_tag_src, data_tag_dst, data_level = 'catchment', 'catchment_name', 2

            file_name_obj = {}
            for group_key, group_data in data_group.items():
                group_name = group_data[data_tag_src]

                if not isinstance(group_name, list):
                    group_name = [group_name]

                for group_step in group_name:

                    if data_level == 1:
                        file_name_obj[group_key] = {}
                    elif data_level == 2:
                        if group_key not in list(file_name_obj.keys()):
                            file_name_obj[group_key] = {}
                        file_name_obj[group_key][group_step] = {}

                    alg_template_values_step = {data_tag_dst: group_step}

                    folder_name_def = fill_tags2string(
                        folder_name_raw, self.alg_template_tags, alg_template_values_step)
                    file_name_def = fill_tags2string(
                        file_name_raw, self.alg_template_tags, alg_template_values_step)
                    file_path_def = os.path.join(folder_name_def, file_name_def)

                    if data_level == 1:
                        file_name_obj[group_key] = file_path_def
                    elif data_level == 2:
                        file_name_obj[group_key][group_step] = file_path_def

        else:
            file_name_obj = os.path.join(folder_name_raw, file_name_raw)

        return file_name_obj

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to get geographical dataset(s)
    def get_geo_data(self, data_collections_src, data_collections_dst, data_level=1,
                     data_map_pivot='region', data_keys_delimiter=':', default_key_dst=None):

        log_stream.info(' -----> Get geographical datasets "' + data_map_pivot + '" ... ')

        group_obj = {}
        for data_key, data_group_src in data_collections_src.items():

            data_map_list = data_key.split(sep=data_keys_delimiter)

            if data_map_pivot == data_map_list[0]:

                log_stream.info(' ------> Get datasets "' + data_key + '" ... ')

                data_type_src, data_fields_src = data_group_src[0], data_group_src[1]

                if data_key in list(data_collections_dst.keys()):
                    data_group_dst = data_collections_dst[data_key]
                    data_type_dst, data_fields_dst = data_group_dst[0], data_group_dst[1]
                elif (default_key_dst is not None) and (default_key_dst in list(data_collections_dst.keys())):
                    data_group_dst = data_collections_dst[default_key_dst]
                    data_type_dst, data_fields_dst = data_group_dst[0], data_group_dst[1]
                else:
                    data_type_dst, data_fields_dst = None, None

                if data_type_src is not None:

                    if data_type_src == 'file' and data_type_dst == 'file':

                        data_obj_src = deepcopy(data_fields_src)
                        data_obj_dst = deepcopy(data_fields_dst)

                        keys_collections_src, keys_collections_dst = [], []
                        file_path_collections_src, file_path_collections_dst = [], []
                        if isinstance(data_obj_src, dict) and (isinstance(data_obj_dst, dict)):
                            for (key_src, fields_src), (key_dst, fields_dst) in zip(
                                    data_obj_src.items(), data_obj_dst.items()):
                                assert key_src == key_dst

                                keys_src_tmp = list(fields_src.keys())
                                keys_dst_tmp = list(fields_dst.keys())

                                file_path_src_tmp = list(get_dict_nested_all_values(fields_src))
                                file_path_dst_tmp = list(get_dict_nested_all_values(fields_dst))

                                for keys_src_step, keys_dst_step, file_path_src_step, file_path_dst_step in zip(
                                        keys_src_tmp, keys_dst_tmp, file_path_src_tmp, file_path_dst_tmp):
                                    keys_collections_src.append(keys_src_step)
                                    keys_collections_dst.append(keys_dst_step)
                                    file_path_collections_src.append(file_path_src_step)
                                    file_path_collections_dst.append(file_path_dst_step)

                        elif isinstance(data_obj_src, str) and (isinstance(data_obj_dst, str)):
                            if data_map_list.__len__() == 1:
                                key_src = data_map_list[0]
                            elif data_map_list.__len__() > 1:
                                key_src = data_map_list[-1]
                            keys_collections_src.append(key_src)
                            keys_collections_dst.append(key_src)
                            file_path_collections_src.append(data_obj_src)
                            file_path_collections_dst.append(data_obj_dst)
                        elif (data_obj_src is None) and (isinstance(data_obj_dst, str)):
                            keys_collections_src.append(None)
                            keys_collections_dst.append(None)
                            file_path_collections_src.append(None)
                            file_path_collections_dst.append(None)
                        else:
                            log_stream.error(' ===> Data format for "' + data_key + '" is not supported')
                            raise NotImplementedError('Case not implemented yet')

                        for key_src, key_dst, file_path_src, file_path_dst in zip(
                                keys_collections_src, keys_collections_dst,
                                file_path_collections_src, file_path_collections_dst):

                            if self.flag_geo_updating:
                                if (file_path_dst is not None) and (os.path.exists(file_path_dst)):
                                    os.remove(file_path_dst)

                            if (file_path_src is not None) and (os.path.exists(file_path_src)):

                                if file_path_src.endswith('.shp'):

                                    if not os.path.exists(file_path_dst):

                                        shape_dframe, shape_collections, shape_geoms = read_file_shp(file_path_src)

                                        file_path_tmp = create_filename_tmp(folder=self.folder_name_tmp)
                                        convert_shp_2_tiff(file_path_src, file_path_tmp,
                                                           pixel_size=0.001, burn_value=1, epsg=4326)

                                        da_geo, _, _ = get_data_tiff(file_path_tmp)
                                        da_geo.attrs = {
                                            'file_name': file_path_src,
                                            'shape_dframe': shape_dframe,
                                            'shape_collections': shape_collections}

                                        folder_name_dst, file_name_dst = os.path.split(file_path_dst)
                                        make_folder(folder_name_dst)
                                        write_obj(file_path_dst, da_geo)

                                        if os.path.exists(file_path_tmp):
                                            os.remove(file_path_tmp)

                                    else:
                                        da_geo = read_obj(file_path_dst)

                                elif file_path_src.endswith('.tiff') or file_path_src.endswith('tif'):

                                    if not os.path.exists(file_path_dst):

                                        da_geo, _, _ = get_data_tiff(file_path_src)
                                        da_geo.attrs = {'file_name': file_path_src}

                                        folder_name_dst, file_name_dst = os.path.split(file_path_dst)
                                        make_folder(folder_name_dst)
                                        write_obj(file_path_dst, da_geo)

                                    else:
                                        da_geo = read_obj(file_path_dst)

                                elif file_path_src.endswith('txt') or file_path_src.endswith('asc'):
                                    if not os.path.exists(file_path_dst):
                                        da_geo = read_file_raster(file_path_src, output_format='data_array')

                                        folder_name_dst, file_name_dst = os.path.split(file_path_dst)
                                        make_folder(folder_name_dst)
                                        write_obj(file_path_dst, da_geo)

                                    else:
                                        da_geo = read_obj(file_path_dst)

                                else:
                                    log_stream.error(' ===> Data format "' + file_path_src + '" is not supported')
                                    raise NotImplementedError('Case not implemented yet')

                                if data_level == 1:
                                    group_obj[data_key] = da_geo
                                elif data_level == 2:
                                    if data_key not in list(group_obj.keys()):
                                        group_obj[data_key] = {}
                                    group_obj[data_key][key_dst] = {}
                                    group_obj[data_key][key_dst] = da_geo
                                else:
                                    log_stream.error(' ===> Data object must be level=1 or level=2')
                                    raise NotImplementedError('Case not implemented yet')

                            else:
                                if file_path_src is not None:
                                    log_stream.error(' ===> Data file "' + file_path_src + '" is not available')
                                    raise IOError('Check your configuration file')
                                elif file_path_src is None:
                                    log_stream.error(' ===> Data file is defined by NoneType')
                                    raise IOError('Check your configuration file')
                                else:
                                    log_stream.error(' ===> Data file is define by unsupported type')
                                    raise IOError('Check your configuration file')

                    elif data_type_src == 'grid' and data_type_dst == 'file':

                        file_grid_src = deepcopy(data_fields_src)
                        file_path_dst = deepcopy(data_fields_dst)

                        if self.flag_geo_updating:
                            if os.path.exists(file_path_dst):
                                os.remove(file_path_dst)

                        if not os.path.exists(file_path_dst):

                            da_geo = create_grid(
                                xll_corner=file_grid_src['xll_corner'], yll_corner=file_grid_src['yll_corner'],
                                rows=file_grid_src['rows'], cols=file_grid_src['cols'],
                                cell_size=file_grid_src['cell_size'])
                            da_geo.attrs = {'file_name': None}

                            folder_name_dst, file_name_dst = os.path.split(file_path_dst)
                            make_folder(folder_name_dst)
                            write_obj(file_path_dst, da_geo)

                        else:
                            da_geo = read_obj(file_path_dst)

                        if data_level == 1:
                            group_obj[data_key] = da_geo
                        else:
                            log_stream.error(' ===> Data object must be level=1')
                            raise NotImplementedError('Case not implemented yet')

                    else:
                        log_stream.error(' ===> Data type "' + data_type_src + '" is not supported')
                        raise NotImplementedError('Case not implemented yet')
                else:
                    log_stream.warning(' ===> Data type for layer "' + data_key + '" is NoneType')
                    group_obj[data_key] = None

                log_stream.info(' ------> Get datasets "' + data_key + '" ... DONE')

        log_stream.info(' -----> Get geographical datasets "' + data_map_pivot + '" ... DONE')

        return group_obj

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to compute ancillary region datasets
    def compute_ancillary_region(self, dset_collections, dset_reference,
                                 data_collections,
                                 pivot_name_src='region:primary_data',
                                 pivot_name_dst_index='region:index_data'):

        log_stream.info(' -----> Compute ancillary region datasets ... ')

        if pivot_name_src is None:
            pivot_name_src = 'region:primary_data'

        data_name_src = filter_obj_variables(dset_collections, pivot_name_src)
        data_group_dst_index = data_collections[pivot_name_dst_index]
        type_dst_index, file_path_dst_index = data_group_dst_index[0], data_group_dst_index[1]

        data_reference = dset_reference[self.reference_tag]

        geo_x_ref_1d = data_reference['west_east'].values
        geo_y_ref_1d = data_reference['south_north'].values
        geo_x_ref_2d, geo_y_ref_2d = np.meshgrid(geo_x_ref_1d, geo_y_ref_1d)

        if self.flag_geo_updating:
            if os.path.exists(file_path_dst_index):
                os.remove(file_path_dst_index)

        if not os.path.exists(file_path_dst_index):

            dset_obj = {}
            for step_name_src in data_name_src:

                log_stream.info(' ------> Analyze dataset for "' + step_name_src + '" ... ')

                data_group_src = data_collections[step_name_src]
                data_type_src, data_fields_src = data_group_src[0], data_group_src[1]

                step_name_obj = step_name_src.split(self.obj_keys_delimiter)

                if step_name_obj.__len__() == 3:
                    step_pivot_src = step_name_obj[0]
                    step_mid_src, step_var_src = step_name_obj[1], step_name_obj[2]
                    step_name_dst = self.obj_keys_delimiter.join([pivot_name_dst_index, step_var_src])
                else:
                    log_stream.error(' ===> Variable name format is not supported')
                    raise NotImplementedError('Case not implemented yet')

                if data_type_src == 'file':

                    data_file_path_src = data_fields_src

                    if os.path.exists(data_file_path_src):

                        log_stream.info(' -------> Get region datasets ... ')
                        da_frame = read_obj(data_file_path_src)
                        log_stream.info(' -------> Get region datasets ... DONE')

                        log_stream.info(' -------> Create index datasets ... ')

                        geo_x_data_1d = da_frame['west_east'].values
                        geo_y_data_1d = da_frame['south_north'].values
                        geo_x_data_2d, geo_y_data_2d = np.meshgrid(geo_x_data_1d, geo_y_data_1d)

                        index_data_1d = interp_grid2index(
                            geo_x_data_2d, geo_y_data_2d, geo_x_ref_2d, geo_y_ref_2d,
                            nodata=-9999, interp_method='nearest')

                        log_stream.info(' -------> Create index datasets ... DONE')

                    else:
                        log_stream.warning(' ===> Region data file ' + data_file_path_src + ' is not available')
                        index_data_1d = None

                    dset_obj[step_name_dst] = {}
                    dset_obj[step_name_dst] = index_data_1d

                else:
                    log_stream.error(' ===> Region data type ' + data_type_src + ' is not supported')
                    raise IOError('Check your configuration file')

                log_stream.info(' ------> Analyze dataset for "' + step_name_src + '" ... DONE')

            folder_name_dst_index, file_name_dst_index = os.path.split(file_path_dst_index)
            make_folder(folder_name_dst_index)
            write_obj(file_path_dst_index, dset_obj)

        else:
            dset_obj = read_obj(file_path_dst_index)

        dset_collections = {**dset_collections, **dset_obj}

        log_stream.info(' -----> Compute ancillary region datasets ... DONE')

        return dset_collections

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to compute ancillary alert area datasets
    def compute_ancillary_alert_area(self, dset_collections, dset_reference,
                                     data_collections,
                                     pivot_name_src='alert_area:primary_data',
                                     pivot_name_dst_vector='alert_area:vector_data',
                                     pivot_name_dst_mask='alert_area:mask_data',
                                     pivot_name_dst_idx_grid='alert_area:index_grid_data',
                                     pivot_name_dst_idx_circle='alert_area:index_circle_data'):

        log_stream.info(' -----> Compute ancillary alert area datasets ... ')

        data_reference = dset_reference[self.reference_tag]

        geo_x_ref_1d = data_reference['west_east'].values
        geo_y_ref_1d = data_reference['south_north'].values
        geo_x_ref_2d, geo_y_ref_2d = np.meshgrid(geo_x_ref_1d, geo_y_ref_1d)

        data_group_info = self.group_data_structure

        data_group_src = data_collections[pivot_name_src]
        data_group_dst_vector = data_collections[pivot_name_dst_vector]
        data_group_dst_mask = data_collections[pivot_name_dst_mask]
        data_group_dst_idx_grid = data_collections[pivot_name_dst_idx_grid]
        data_group_dst_idx_circle = data_collections[pivot_name_dst_idx_circle]

        data_type_src, data_fields_src = data_group_src[0], data_group_src[1]

        dset_obj = {}
        if data_type_src == 'file':

            data_file_path_src = data_fields_src

            if os.path.exists(data_file_path_src):

                obj_workspace_src = read_obj(data_file_path_src)
                data_values_src = obj_workspace_src.values
                data_attrs_src = obj_workspace_src.attrs

                data_attrs_shp_dframe_src = data_attrs_src['shape_dframe']
                data_attrs_shp_collections_src = data_attrs_src['shape_collections']

                for data_shp, (data_key, data_fields) in zip(data_attrs_shp_collections_src, data_group_info.items()):

                    log_stream.info(' ------> Analyze dataset for "' + data_key + '" ... ')

                    name_shp, polygons_shp = data_shp[0], data_shp[1]

                    assert name_shp == data_fields['name']

                    type_dst_vector, file_path_dst_vector = data_group_dst_vector[0], data_group_dst_vector[1][data_key]
                    type_dst_mask, file_path_dst_mask = data_group_dst_mask[0], data_group_dst_mask[1][data_key]
                    type_dst_idx_grid, file_path_dst_idx_grid = data_group_dst_idx_grid[0], data_group_dst_idx_grid[1][data_key]
                    type_dst_idx_circle, file_path_dst_idx_circle = data_group_dst_idx_circle[0], data_group_dst_idx_circle[1][data_key]

                    if self.flag_geo_updating:
                        if os.path.exists(file_path_dst_vector):
                            os.remove(file_path_dst_vector)
                        if os.path.exists(file_path_dst_mask):
                            os.remove(file_path_dst_mask)
                        if os.path.exists(file_path_dst_idx_grid):
                            os.remove(file_path_dst_idx_grid)
                        if os.path.exists(file_path_dst_idx_circle):
                            os.remove(file_path_dst_idx_circle)

                    log_stream.info(' -------> Create vector and mask datasets ... ')
                    if (not os.path.exists(file_path_dst_vector)) or (not os.path.exists(file_path_dst_mask)):

                        folder_name_dst_vector, file_name_dst_vector = os.path.split(file_path_dst_vector)
                        make_folder(folder_name_dst_vector)

                        convert_polygons_2_shp(polygons_shp, file_path_dst_vector,
                                               template_file_name=data_reference.attrs['file_name'])

                        folder_name_dst_area, file_name_dst_area = os.path.split(file_path_dst_mask)
                        make_folder(folder_name_dst_area)

                        convert_shp_2_tiff(file_path_dst_vector, file_path_dst_mask,
                                           pixel_size=0.001, burn_value=1, epsg=4326)

                        log_stream.info(' -------> Create vector and mask datasets ... DONE')

                    else:
                        log_stream.info(' -------> Create vector and mask datasets ... SKIPPED. '
                                        'Datasets previously created.')

                    # Read alert area masked data
                    log_stream.info(' -------> Get vector datasets ... ')
                    polygons_frame, _, _ = read_file_shp(file_path_dst_vector)
                    polygons_frame.attrs = {'file_name': file_path_dst_vector}
                    log_stream.info(' -------> Get vector datasets ... ')

                    # Read alert area masked data
                    log_stream.info(' -------> Get mask datasets ... ')
                    da_frame, _, _ = get_data_tiff(file_path_dst_mask)
                    da_frame.attrs = {'file_name': file_path_dst_mask}
                    log_stream.info(' -------> Get mask datasets ... DONE')

                    log_stream.info(' -------> Create index grid datasets ... ')
                    if not os.path.exists(file_path_dst_idx_grid):

                        geo_x_data_1d = da_frame['west_east'].values
                        geo_y_data_1d = da_frame['south_north'].values
                        geo_x_data_2d, geo_y_data_2d = np.meshgrid(geo_x_data_1d, geo_y_data_1d)

                        index_data_2d = interp_grid2index(
                            geo_x_ref_2d, geo_y_ref_2d, geo_x_data_2d, geo_y_data_2d,
                            nodata=-9999, interp_method='nearest')

                        folder_name_dst_idx_grid, file_name_dst_idx_grid = os.path.split(file_path_dst_idx_grid)
                        make_folder(folder_name_dst_idx_grid)

                        write_obj(file_path_dst_idx_grid, index_data_2d)

                        log_stream.info(' -------> Create index grid datasets ... DONE')

                    else:
                        log_stream.info(
                            ' -------> Create index grid datasets ... SKIPPED. Datasets previously created.')
                        index_data_2d = read_obj(file_path_dst_idx_grid)

                    log_stream.info(' -------> Create index circle datasets ... ')
                    if not os.path.exists(file_path_dst_idx_circle):

                        geo_data = da_frame.values
                        geo_x_data_1d = da_frame['west_east'].values
                        geo_y_data_1d = da_frame['south_north'].values
                        geo_x_data_2d, geo_y_data_2d = np.meshgrid(geo_x_data_1d, geo_y_data_1d)

                        points_values, points_x_values_2d, points_y_values_2d = compute_grid_from_bounds(
                            geo_data, geo_x_data_2d, geo_y_data_2d, km=self.point_grid_cell_size)

                        points_collection = find_points_with_buffer(points_values, points_x_values_2d,
                                                                    points_y_values_2d,
                                                                    geo_data, geo_x_data_2d, geo_y_data_2d,
                                                                    point_buffer=self.point_circle_radius)

                        folder_name_dst_idx_circle, file_name_dst_idx_circle = os.path.split(file_path_dst_idx_circle)
                        make_folder(folder_name_dst_idx_circle)

                        write_obj(file_path_dst_idx_circle, points_collection)

                        log_stream.info(' -------> Create index circle datasets ... DONE')

                    else:
                        log_stream.info(
                            ' -------> Create index circle datasets ... SKIPPED. Datasets previously created.')
                        points_collection = read_obj(file_path_dst_idx_circle)

                    # data mask
                    data_name_dset_mask_key = self.obj_keys_delimiter.join([pivot_name_dst_mask, data_key])
                    dset_obj[data_name_dset_mask_key] = {}
                    dset_obj[data_name_dset_mask_key] = da_frame
                    # data vector
                    data_name_dset_vector_key = self.obj_keys_delimiter.join([pivot_name_dst_vector, data_key])
                    dset_obj[data_name_dset_vector_key] = {}
                    dset_obj[data_name_dset_vector_key] = polygons_frame
                    # data index grid
                    data_name_dset_idx_grid_key = self.obj_keys_delimiter.join([pivot_name_dst_idx_grid, data_key])
                    dset_obj[data_name_dset_idx_grid_key] = {}
                    dset_obj[data_name_dset_idx_grid_key] = index_data_2d
                    # data index circle
                    data_name_dset_idx_circle_key = self.obj_keys_delimiter.join([pivot_name_dst_idx_circle, data_key])
                    dset_obj[data_name_dset_idx_circle_key] = {}
                    dset_obj[data_name_dset_idx_circle_key] = points_collection

                    log_stream.info(' ------> Analyze dataset for "' + data_key + '" ... DONE')

                dset_collections = {**dset_collections, **dset_obj}

            else:
                log_stream.error(' ===> Alert area data file ' + data_file_path_src + ' is not available')
                raise IOError('Check your configuration file')
        else:
            log_stream.error(' ===> Alert area data type ' + data_type_src + ' is not supported')
            raise IOError('Check your configuration file')

        log_stream.info(' -----> Compute ancillary alert area datasets ... DONE')

        return dset_collections

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to select variables
    @staticmethod
    def select_vars(variable_list_all, variable_pivot='region'):
        variable_list_selected = []
        for variable_step in variable_list_all:
            if variable_pivot in variable_step:
                variable_list_selected.append(variable_step)
        return variable_list_selected
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize geographical data
    def organize_data(self):

        # Starting info
        log_stream.info(' ----> Organize grid information ... ')

        # Read geo catchment datasets
        dset_geo_catchment = self.get_geo_data(
            self.src_collection_geo, self.dst_collection_geo, data_map_pivot=self.catchment_tag,
            data_keys_delimiter=self.obj_keys_delimiter, data_level=2)
        # Read geo reference datasets
        dset_geo_reference = self.get_geo_data(
            self.src_collection_geo, self.dst_collection_geo, data_map_pivot=self.reference_tag,
            data_keys_delimiter=self.obj_keys_delimiter, data_level=1)
        # Read geo region datasets
        dset_geo_region = self.get_geo_data(
            self.src_collection_geo, self.dst_collection_geo, data_map_pivot=self.region_tag,
            data_keys_delimiter=self.obj_keys_delimiter, data_level=1)
        # Read geo alert area datasets
        dset_geo_alert_area = self.get_geo_data(
            self.src_collection_geo, self.dst_collection_geo, data_map_pivot=self.alert_area_tag,
            data_keys_delimiter=self.obj_keys_delimiter, data_level=1)

        # Compute ancillary region datasets (indexes)
        dset_geo_region = self.compute_ancillary_region(
            dset_geo_region, dset_geo_reference, self.dst_collection_geo,
            pivot_name_src=self.region_pivot_name_src, pivot_name_dst_index=self.region_pivot_name_dst_index)

        # Compute ancillary alert area datasets (mask, indexes ... )
        dset_geo_alert_area = self.compute_ancillary_alert_area(
            dset_geo_alert_area, dset_geo_reference, self.dst_collection_geo,
            pivot_name_src=self.alert_area_pivot_name_src,
            pivot_name_dst_vector=self.alert_area_pivot_name_dst_vector,
            pivot_name_dst_mask=self.alert_area_pivot_name_dst_mask,
            pivot_name_dst_idx_grid=self.alert_area_pivot_name_dst_idx_grid,
            pivot_name_dst_idx_circle=self.alert_area_pivot_name_dst_idx_circle)

        # Create geo data collections
        geo_data_collections = {self.region_tag: dset_geo_region,
                                self.alert_area_tag: dset_geo_alert_area,
                                self.reference_tag: dset_geo_reference,
                                self.catchment_tag: dset_geo_catchment}

        # Ending info
        log_stream.info(' ----> Organize grid information ... DONE')

        return geo_data_collections
    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
