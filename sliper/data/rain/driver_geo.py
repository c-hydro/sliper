"""
Class Features

Name:          lib_utils_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import xarray as xr
import os

from lib_data_io_geo import read_file_grid
from lib_utils_geo import resample_index, resample_data

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
                 tags_dict=None,
                 flag_update=True):

        self.tags_dict = tags_dict

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.xll_corner_tag, self.yll_corner_tag = 'xll_corner', 'yll_corner'
        self.cell_size_tag, self.rows_tag, self.cols_tag   = 'cell_size', 'rows', 'cols'
        self.geo_src_tag, self.geo_dst_tag = 'geo_src', 'geo_dst'

        self.obj_fields_file = [self.file_name_tag, self.folder_name_tag]
        self.obj_fields_grid = [self.xll_corner_tag, self.yll_corner_tag,
                                self.cell_size_tag, self.rows_tag, self.cols_tag]
        self.obj_keys_delimiter = ':'

        # source object(s)
        self.folder_name_src, self.file_name_src = src_dict[self.folder_name_tag], src_dict[self.file_name_tag]
        self.path_name_src = os.path.join(self.folder_name_src, self.file_name_src)

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
    @staticmethod
    def wrap_geo_data(file_name: str, file_mandatory: bool = True):

        # check data availability
        if os.path.exists(file_name):
            geo_da = read_file_grid(file_name)
        else:
            if file_mandatory:
                log_stream.error(' ===> File "' + file_name + '" is not available')
                raise IOError('Check your configuration file')
            else:
                # if file is not mandatory, return None
                log_stream.warning(' ===> File "' + file_name + '" is not available')
                geo_da = None

        return geo_da

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to wrap geographical ancillary data
    @staticmethod
    def wrap_geo_ancillary(geo_da_src: xr.DataArray = None, geo_da_dst: xr.DataArray = None,
                           geo_mandatory: bool = True):

        if geo_da_src is None or geo_da_dst is None:
            if geo_mandatory:
                log_stream.error(' ===> Source or destination geographical data are not available')
                raise IOError('Check your configuration file')
            else:
                log_stream.warning(' ===> Source or destination geographical data are not available')
                return None, None, None, None

        valid_input_index, valid_output_index, index_array, weight_array = resample_index(geo_da_src, geo_da_dst)

        return valid_input_index, valid_output_index, index_array, weight_array
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Method to organize geographical data
    def organize_data(self):

        # Starting info
        log_stream.info(' ----> Organize geographical information ... ')

        # read geo data source
        geo_da_src = self.wrap_geo_data(file_name=self.path_name_src, file_mandatory=True)
        # read geo data destination
        geo_da_dst = self.wrap_geo_data(file_name=self.path_name_dst, file_mandatory=True)

        # compute ancillary data (for resampling)
        valid_input_index, valid_output_index, index_array, weight_array = (
            self.wrap_geo_ancillary(geo_da_src, geo_da_dst))

        # create geo grid collections
        geo_data_collections = {
            self.geo_src_tag: geo_da_src,
            self.geo_dst_tag: geo_da_dst,
            'valid_input_index': valid_input_index,
            'valid_output_index': valid_output_index,
            'index_array': index_array,
            'weight_array': weight_array
        }

        # Ending info
        log_stream.info(' ----> Organize geographical information ... DONE')

        return geo_data_collections
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
