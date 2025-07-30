"""
Class Features

Name:          driver_data_static
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20200515'
Version:       '1.0.0'
"""

# -------------------------------------------------------------------------------------
# libraries
import logging
import os

from lib_data_io_shp import read_file_shp
from lib_data_io_tiff import convert_shp_2_tiff
from lib_data_io_geo import read_grid_data
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_system import make_folder, create_filename_tmp

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Class DriverData
class DriverData:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, src_dict, dst_dict=None, tmp_dict=None,
                 template_dict=None, info_dict=None, flags_dict=None):

        # args object(s)
        self.src_dict = src_dict
        self.dst_dict = dst_dict
        self.tmp_dict = tmp_dict

        self.template_dict = template_dict
        self.info_dict = info_dict
        self.flags_dict = flags_dict

        # tags
        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'

        # flags
        self.reset_static = flags_dict['reset_static']

        # source object(s)
        folder_name_src = self.src_dict[self.folder_name_tag]
        file_name_src = self.src_dict[self.file_name_tag]
        self.file_path_src = os.path.join(folder_name_src, file_name_src)

        # destination object(s)
        folder_name_dst = self.dst_dict[self.folder_name_tag]
        file_name_dst = self.dst_dict[self.file_name_tag]
        self.file_path_dst = os.path.join(folder_name_dst, file_name_dst)

        # tmp object(s)
        self.folder_name_tmp = tmp_dict[self.folder_name_tag]
        self.file_name_tmp = tmp_dict[self.file_name_tag]

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to get geo point file
    def get_geo_data(self, file_name_src):

        log_stream.info(' -----> Read geographical file "' + file_name_src + '" ... ')

        if os.path.exists(file_name_src):

            shape_dframe, shape_collections, shape_geoms = read_file_shp(file_name_src)

            file_name_tmp = create_filename_tmp(folder=self.folder_name_tmp)
            convert_shp_2_tiff(file_name_src, file_name_tmp,
                               pixel_size=0.001, burn_value=1, epsg=4326)

            da_geo, da_attrs = read_grid_data(file_name_tmp)

            extended_attrs = {
                'file_name': file_name_src,
                'shape_dframe': shape_dframe,
                'shape_collections': shape_collections}
            da_attrs = {**da_attrs, **extended_attrs}

            da_geo.attrs = da_attrs

            if os.path.exists(file_name_tmp):
                os.remove(file_name_tmp)

            log_stream.info(' -----> Read geographical file "' + file_name_src + '" ... DONE')

        else:
            log_stream.info(' -----> Read geographical file "' + file_name_src + '" ... FAILED')
            log_stream.error(' ===> File is mandatory to run the application')
            raise FileNotFoundError('File not found')

        return da_geo
    # -------------------------------------------------------------------------------------

    # -------------1------------------------------------------------------------------------
    # method to organize data
    def organize_data(self):

        # method start info
        log_stream.info(' ----> Organize static information ... ')

        # get file path(s)
        file_path_src, file_path_dst = self.file_path_src, self.file_path_dst
        # get flag(s)
        reset_static = self.reset_static

        # reset destination file (if needed)
        if reset_static:
            if os.path.exists(file_path_dst):
                os.remove(file_path_dst)

        # check destination file availability
        if not os.path.exists(file_path_dst):

            # get data object
            obj_data = self.get_geo_data(file_path_src)
            # get info object
            obj_info = self.info_dict

            # merge data and info object(s)
            obj_collections = {'data': obj_data, 'registry': obj_info}

            # dump object to destination file
            folder_name_dst, file_name_dst = os.path.split(file_path_dst)
            make_folder(folder_name_dst)

            write_obj(file_path_dst, obj_collections)

        else:

            # get object from destination file
            obj_collections = read_obj(file_path_dst)

        # method end info
        log_stream.info(' ----> Organize static information ... DONE')

        return obj_collections
    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
