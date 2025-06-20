"""
Class Features

Name:          driver_data_io_forcing_sm
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '202200502'
Version:       '1.5.0'
"""

######################################################################################
# Library
import logging
import os
import numpy as np
import pandas as pd

from lib_utils_data_grid_sm import get_data_nc, get_data_binary, \
    reproject_sm_source2map, merge_sm_list2map, save_data_tiff, save_data_nc
from lib_utils_data_table import read_table_obj, select_table_obj

from lib_utils_system import unzip_filename
from lib_utils_time import search_time_features
from lib_utils_io_obj import filter_obj_variables, filter_obj_datasets, create_dset

from lib_utils_system import fill_tags2string, make_folder, change_extension

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverForcing for soil moisture datasets
class DriverForcing:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, time_step, src_dict, anc_dict, dst_dict=None, tmp_dict=None,
                 alg_ancillary=None, alg_template_tags=None,
                 time_data=None, table_data='table_variables.json',
                 collections_data_group=None, collections_data_geo=None,
                 flag_data_src='soil_moisture_data',
                 flag_ancillary_updating=True):

        self.time_step = pd.Timestamp(time_step)

        self.flag_data_src = flag_data_src

        self.obj_file_name_tag = 'file_name'
        self.obj_folder_name_tag = 'folder_name'
        self.obj_type_tag = 'obj_type'
        self.obj_geo_reference_tag = 'obj_geo_reference'

        self.reference_tag = 'reference'
        self.region_tag = 'region'
        self.alert_area_tag = 'alert_area'
        self.region_pivot_name_primary = 'region:primary_data:soil_moisture_data'
        self.region_pivot_name_index = 'region:index_data:soil_moisture_data'
        self.alert_area_pivot_name_mask = 'alert_area:mask_data:{:}'
        self.catchment_tag = 'catchment'
        self.catchment_pivot_name_terrain = 'catchment:primary_data:terrain_data'
        self.catchment_pivot_name_cn = 'catchment:primary_data:cn_data'
        self.catchment_pivot_name_cnet = 'catchment:primary_data:channels_network_data'

        self.alg_template_tags = alg_template_tags

        # select geo datasets
        self.data_group = collections_data_group
        self.data_geo_reference_grid = collections_data_geo[self.reference_tag]['reference']
        self.data_geo_region_grid = collections_data_geo[self.region_tag][self.region_pivot_name_primary]
        self.data_geo_region_index = collections_data_geo[self.region_tag][self.region_pivot_name_index]
        self.data_geo_catchment_terrain = collections_data_geo[self.catchment_tag][self.catchment_pivot_name_terrain]
        self.data_geo_catchment_cn = collections_data_geo[self.catchment_tag][self.catchment_pivot_name_cn]
        self.data_geo_catchment_cnet = collections_data_geo[self.catchment_tag][self.catchment_pivot_name_cnet]

        data_vars_alert_area_mask = filter_obj_variables(
            list(collections_data_geo[self.alert_area_tag].keys()), self.alert_area_pivot_name_mask)
        self.data_geo_alert_area_mask = filter_obj_datasets(collections_data_geo[self.alert_area_tag],
                                                            data_vars_alert_area_mask)

        # time object(s)
        self.time_data = time_data[self.flag_data_src]
        self.time_period_max, self.time_frequency_max, self.time_period_type = search_time_features(
            self.data_group, data_key='sm_datasets')
        self.time_range = self.collect_file_time()

        # source object(s)
        self.type_src = src_dict[self.flag_data_src][self.obj_type_tag]
        self.table_src = select_table_obj(read_table_obj(table_data), ['source', self.flag_data_src, self.type_src])
        self.geo_reference_src = src_dict[self.flag_data_src][self.obj_geo_reference_tag]

        self.file_name_src_raw = src_dict[self.flag_data_src][self.obj_file_name_tag]
        self.folder_name_src_raw = src_dict[self.flag_data_src][self.obj_folder_name_tag]
        self.file_path_src_list = self.collect_file_list(self.folder_name_src_raw, self.file_name_src_raw)

        # ancillary object(s)
        self.file_name_anc_raw = anc_dict[self.flag_data_src][self.obj_file_name_tag]
        self.folder_name_anc_raw = anc_dict[self.flag_data_src][self.obj_folder_name_tag]
        self.file_path_anc_list = self.collect_file_list(self.folder_name_anc_raw, self.file_name_anc_raw)

        # tmp object(s)
        self.folder_name_tmp_raw = tmp_dict[self.obj_folder_name_tag]
        self.file_name_tmp_raw = tmp_dict[self.obj_file_name_tag]

        self.flag_ancillary_updating = flag_ancillary_updating

        self.file_extension_zip = self.table_src['file_extension_zip']
        self.file_extension_unzip = self.table_src['file_extension_unzip']

        self.var_name_x = 'west_east'
        self.var_name_y = 'south_north'

        self.file_path_processed = []

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect time(s)
    def collect_file_time(self):

        # get generic variable features
        time_period = self.time_data["time_period"]
        time_frequency = self.time_data["time_frequency"]
        time_rounding = self.time_data["time_rounding"]

        # case of time period
        if isinstance(self.time_period_max, (float, int)):
            if time_period < self.time_period_max:
                log_stream.warning(' ===> Obj "time_period" is less then "time_period_max". '
                                   'Set "time_period" == "time_period_max"')
                time_period = self.time_period_max
        elif isinstance(self.time_period_max, list):

            time_period_max = np.max(np.asarray(self.time_period_max))

            if time_period < time_period_max:
                log_stream.warning(' ===> Obj "time_period" is less then "time_period_max". '
                                   'Set "time_period" == "time_period_max"')
                time_period = time_period_max

        else:
            log_stream.error(' ===> Obj "time_period" format is not expected.')
            raise NotImplementedError('Case not implemented yet')

        # case of time frequency
        if isinstance(self.time_frequency_max, str):
            if time_frequency != self.time_frequency_max:
                log_stream.error(' ===> Obj "time_frequency" is not equal to "time_frequency_max".')
                raise NotImplementedError('Case not implemented yet')
        elif isinstance(self.time_frequency_max, list):
            for time_frequency_step in self.time_frequency_max:
                if time_frequency != time_frequency_step:
                    log_stream.error(' ===> Obj "time_frequency" is not equal to "time_frequency_max" in all cases.')
                    raise NotImplementedError('Case not implemented yet')
        else:
            log_stream.error(' ===> Obj "time_frequency" format is not expected.')
            raise NotImplementedError('Case not implemented yet')

        # time step, time_step_left, time_step_right information
        time_step = self.time_step.floor(time_rounding)
        time_step_left = time_step
        time_step_right = pd.date_range(start=time_step, periods=2, freq=time_frequency)[1]
        # search type information (left, right, both, [left, right], [right, left])
        search_type = self.time_period_type

        # case 'left' -> time range from time_start_left
        if search_type == 'left':
            time_range = pd.date_range(end=time_step_left, periods=time_period, freq=time_frequency)

        # case 'right' -> time range from time_end_right
        elif search_type == 'right':
            time_range = pd.date_range(start=time_step_right, periods=time_period, freq=time_frequency)

        # case 'both' -> time range from time_start_left and time_end_right (same length)
        elif search_type == 'both':
            time_range_left = pd.date_range(end=time_step_left, periods=time_period, freq=time_frequency)
            time_range_right = pd.date_range(start=time_step_right, periods=time_period, freq=time_frequency)
            time_range = time_range_left.union(time_range_right)

        # case 'left' and 'right' -> time range from time_step_left and time_step_right (different length)
        elif search_type == ['left', 'right']:
            time_period_left, time_period_right = self.time_period_max[0], self.time_period_max[1]
            time_frequency_left, time_frequency_right = self.time_frequency_max[0], self.time_frequency_max[1]
            time_range_left = pd.date_range(end=time_step_left, periods=time_period_left, freq=time_frequency_left)
            time_range_right = pd.date_range(start=time_step_right, periods=time_period_right, freq=time_frequency_right)
            time_range = time_range_left.union(time_range_right)

        # case 'right' and 'left' -> time range from time_step_left and time_step_right (different length)
        elif search_type == ['right', 'left']:
            time_period_left, time_period_right = self.time_period_max[1], self.time_period_max[0]
            time_frequency_left, time_frequency_right = self.time_frequency_max[1], self.time_frequency_max[0]
            time_range_left = pd.date_range(end=time_step_left, periods=time_period_left, freq=time_frequency_left)
            time_range_right = pd.date_range(start=time_step_left, periods=time_period_right, freq=time_frequency_right)
            time_range = time_range_left.union(time_range_right)

        else:
            log_stream.error(' ===> Obj "search_type" for "time_range" selection is not allowed')
            raise RuntimeError('Obj "search_type" must be equal to "left", "right", "both", '
                               '["left", "right"] or ["right", "left"]')

        return time_range
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect ancillary file
    def collect_file_list(self, folder_name_raw, file_name_raw):

        data_group = self.data_group
        data_time = self.time_range

        file_name_obj = {}
        for group_key, group_data in data_group.items():

            file_name_obj[group_key] = {}

            group_catchment = group_data['catchment']
            for catchment_name in group_catchment:

                file_name_list = []
                for time_step in data_time:

                    alg_template_values_step = {'source_sm_sub_path_time': time_step,
                                                'source_sm_datetime': time_step,
                                                'ancillary_sm_sub_path_time': time_step,
                                                'ancillary_sm_datetime': time_step,
                                                'catchment_name': catchment_name,
                                                'alert_area_name': group_key}

                    folder_name_def = fill_tags2string(
                        folder_name_raw, self.alg_template_tags, alg_template_values_step)
                    if file_name_raw is not None:
                        file_name_def = fill_tags2string(
                            file_name_raw, self.alg_template_tags, alg_template_values_step)
                        file_path_def = os.path.join(folder_name_def, file_name_def)
                    else:
                        file_path_def = folder_name_def

                    file_name_list.append(file_path_def)

                file_name_obj[catchment_name] = {}
                file_name_obj[catchment_name] = file_name_list

        return file_name_obj

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize forcing
    def organize_forcing(self, var_name='soil_moisture', var_min=0, var_max=1):

        log_stream.info(' ----> Organize soil moisture forcing ... ')

        time_range = self.time_range
        type_src = self.type_src

        data_group = self.data_group
        data_geo_alert_area_mask = self.data_geo_alert_area_mask
        data_geo_catchment_terrain = self.data_geo_catchment_terrain
        data_geo_catchment_cn = self.data_geo_catchment_cn
        data_geo_catchment_cnet = self.data_geo_catchment_cnet

        file_path_src_list = self.file_path_src_list
        file_path_anc_list = self.file_path_anc_list

        for group_key, group_fields in data_group.items():

            log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... ')

            # create the reference geographical name
            name_key = self.alert_area_pivot_name_mask.format(group_key)

            # get the reference geographical datasets
            data_geo_reference_grid = data_geo_alert_area_mask[name_key]

            da_geo_y_ref = data_geo_reference_grid['south_north']
            da_geo_x_ref = data_geo_reference_grid['west_east']
            da_mask_ref_2d = data_geo_reference_grid

            catchment_list = group_fields['catchment']

            data_anc_collections, file_anc_collections = {}, {}
            if catchment_list:
                for catchment_name in catchment_list:

                    log_stream.info(' ------> Catchment "' + catchment_name + '" ... ')

                    catchment_geo_terrain = data_geo_catchment_terrain[catchment_name]
                    catchment_geo_cn = data_geo_catchment_cn[catchment_name]
                    catchment_geo_cnet = data_geo_catchment_cnet[catchment_name]

                    file_src_list = file_path_src_list[catchment_name]
                    file_anc_list = file_path_anc_list[catchment_name]

                    for time_step, file_src_step, file_anc_step in zip(time_range, file_src_list, file_anc_list):

                        log_stream.info(' -------> Time "' + str(time_step) + '" ... ')

                        if self.flag_ancillary_updating:
                            if os.path.exists(file_anc_step):
                                os.remove(file_anc_step)

                        if not os.path.exists(file_anc_step):

                            if os.path.exists(file_src_step):

                                if file_src_step.endswith(self.file_extension_zip):
                                    file_tmp_step = change_extension(file_src_step, self.file_extension_unzip)
                                    unzip_filename(file_src_step, file_tmp_step)
                                else:
                                    file_tmp_step = file_src_step

                                if file_tmp_step.endswith('.bin'):

                                    da_sm_base = get_data_binary(
                                        file_tmp_step, da_geo=catchment_geo_terrain,
                                        da_cn=catchment_geo_cn, da_cnet=catchment_geo_cnet,
                                        value_sm_min=var_min, value_sm_max=var_max)

                                elif file_tmp_step.endswith('.nc'):

                                    da_sm_base = get_data_nc(
                                        file_tmp_step, da_geo=catchment_geo_terrain,
                                        da_cn=catchment_geo_cn, da_cnet=catchment_geo_cnet,
                                        value_sm_min=var_min, value_sm_max=var_max)

                                else:
                                    log_stream.error(
                                        ' ===> Source data format is not supported. Check your source datasets')
                                    raise NotImplementedError('Only "binary" or "nc" formats are available.')

                                # Interpolate data source to destination
                                da_sm_interp = reproject_sm_source2map(
                                    da_sm_base,
                                    da_mask_out=da_mask_ref_2d, da_geo_x_out=da_geo_x_ref, da_geo_y_out=da_geo_y_ref,
                                    mask_out_condition=True)

                                if time_step not in list(data_anc_collections.keys()):
                                    data_anc_collections[time_step] = [da_sm_interp]
                                    file_anc_collections[time_step] = [file_anc_step]
                                else:
                                    data_tmp = data_anc_collections[time_step]
                                    data_tmp.append(da_sm_interp)
                                    data_anc_collections[time_step] = data_tmp

                                    file_tmp = file_anc_collections[time_step]
                                    file_tmp.append(file_anc_step)
                                    file_tmp = list(set(file_tmp))
                                    file_anc_collections[time_step] = file_tmp

                                log_stream.info(' -------> Time "' + str(time_step) + '" ... DONE')
                            else:
                                log_stream.info(' -------> Time "' + str(time_step) + '" ... FAILED')
                                log_stream.warning(' ===> File: "' + file_src_step + '" does not exist')

                        else:
                            log_stream.info(' -------> Time "' + str(time_step) + '" ... PREVIOUSLY DONE')

                    log_stream.info(' ------> Catchment "' + catchment_name + '" ... DONE')

                log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... DONE')

            else:
                log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... SKIPPED')
                log_stream.warning(' ===> Datasets are not defined')

            log_stream.info(' -----> Compose datasets for reference area "' + group_key + '" ... ')
            for (time_step, da_data_list), file_path_anc in zip(
                    data_anc_collections.items(), file_anc_collections.values()):

                log_stream.info(' ------> Time "' + str(time_step) + '" ... ')

                if isinstance(file_path_anc, list) and file_path_anc.__len__() == 1:
                    file_path_anc = file_path_anc[0]
                else:
                    log_stream.error(' ===> Soil moisture ancillary file are not correctly defined.')
                    raise IOError('Ancillary file is not unique')

                if self.flag_ancillary_updating:
                    if os.path.exists(file_path_anc):
                        os.remove(file_path_anc)

                if not os.path.exists(file_path_anc):

                    log_stream.info(' -------> Merge grid datasets ... ')
                    grid_merge = merge_sm_list2map(da_data_list, da_mask_ref_2d)
                    log_stream.info(' -------> Merge grid datasets ... DONE')

                    log_stream.info(' -------> Save grid datasets ... ')

                    dset_merge = create_dset(
                        grid_merge,
                        da_mask_ref_2d.values, da_geo_x_ref.values, da_geo_y_ref.values,
                        var_data_time=time_step,
                        var_data_name=var_name,
                        var_geo_name='mask', var_data_attrs=None, var_geo_attrs=None,
                        coord_name_x='longitude', coord_name_y='latitude', coord_name_time='time',
                        dim_name_x='west_east', dim_name_y='south_north', dim_name_time='time',
                        dims_order_2d=None, dims_order_3d=None)

                    # Debug
                    # plt.figure()
                    # values_merged = dset_merge[var_name].values
                    # plt.imshow(values_merged)
                    # plt.colorbar()
                    # plt.show()

                    folder_name_anc, file_name_anc = os.path.split(file_path_anc)
                    make_folder(folder_name_anc)

                    if file_path_anc.endswith('.nc'):

                        save_data_nc(file_name_anc, dset_merge)

                        log_stream.info(' -------> Save grid datasets ... DONE. [NETCDF]')

                    elif file_path_anc.endswith('.tiff'):

                        map_out_2d = dset_merge[var_name].values

                        geox_out_1d = da_geo_x_ref.values
                        geoy_out_1d = da_geo_y_ref.values
                        geox_out_2d, geoy_out_2d = np.meshgrid(geox_out_1d, geoy_out_1d)

                        save_data_tiff(
                            file_path_anc, map_out_2d, geox_out_2d, geoy_out_2d,
                            file_metadata=self.table_src['file_metadata'],
                            file_epsg_code=self.table_src['file_epsg_code'])

                        log_stream.info(' -------> Save grid datasets ... DONE. [GEOTIFF]')

                    else:
                        log_stream.info(' -------> Save grid datasets ... FAILED')
                        log_stream.error(' ===> Filename format is not allowed')
                        raise NotImplementedError('Format is not implemented yet')

                    self.file_path_processed.append(file_path_anc)

                    log_stream.info(' ------> Time "' + str(time_step) + '" ... DONE')

                else:
                    log_stream.info(' ------> Time "' + str(time_step) + '" ... PREVIOUSLY DONE')

            log_stream.info(' -----> Compose datasets for reference area "' + group_key + '" ... DONE')

        log_stream.info(' ----> Organize soil moisture forcing ... DONE')

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
