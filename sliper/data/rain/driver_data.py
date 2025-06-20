"""
Class Features

Name:          driver_data
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.5.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import numpy as np
import pandas as pd
import xarray as xr

from copy import deepcopy

from lib_utils_geo import resample_data

from lib_data_io_tiff import read_file_tiff, save_file_tiff
from lib_data_io_nc import save_file_nc
from lib_data_io_xlsx import read_file_xlsx
from lib_data_io_pickle import read_obj, write_obj

from lib_utils_generic import fill_template_string

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# DriverData class
class DriverData:

    # ------------------------------------------------------------------------------------------------------------------
    # class constructor
    def __init__(self, src_dict, anc_dict, dst_dict=None, tmp_dict=None,
                 geo_dict=None, tags_dict=None,
                 flag_update=True):

        self.geo_dict = geo_dict
        self.tags_dict = tags_dict

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'
        self.format_tag, self.type_tag = 'format', 'type'

        # source object(s)
        self.folder_name_src, self.file_name_src = src_dict[self.folder_name_tag], src_dict[self.file_name_tag]
        self.path_name_src = os.path.join(self.folder_name_src, self.file_name_src)
        self.type_src = src_dict.get(self.type_tag, 'grid')
        self.format_src = src_dict.get(self.format_tag, 'tif')

        # ancillary object(s)
        self.folder_name_anc, self.file_name_anc = anc_dict[self.folder_name_tag], anc_dict[self.file_name_tag]
        self.path_name_anc = os.path.join(self.folder_name_anc, self.file_name_anc)

        # destination object(s)
        self.folder_name_dst, self.file_name_dst = dst_dict[self.folder_name_tag], dst_dict[self.file_name_tag]
        self.path_name_dst = os.path.join(self.folder_name_dst, self.file_name_dst)
        self.type_dst = dst_dict.get(self.type_tag, 'grid')
        self.format_dst = dst_dict.get(self.format_tag, 'tif')

        # tmp object(s)
        self.folder_name_tmp, self.file_name_tmp = tmp_dict[self.folder_name_tag], tmp_dict[self.file_name_tag]

        # flags for updating dataset(s)
        self.flag_update = flag_update

        # variable to set and keep the data
        self.active_data, self.active_file = None, None

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to dump data
    def dump_data(self, time_run: pd.Timestamp, var_name='rain'):

        # info start
        log_stream.info(' ----> Dump data ... ')

        # check if time_run is a pd.Timestamp object
        if not isinstance(time_run, pd.Timestamp):
            time_run = pd.Timestamp(time_run)

        # get the type and format of source data
        type_dst, format_dst = self.type_dst, self.format_dst

        # get the ancillary and destination path names
        path_name_anc, path_name_dst = self.path_name_anc, self.path_name_dst
        # compose the path names using the template strings
        path_name_anc = fill_template_string(
            template_str=path_name_anc, run_time=time_run, template_map=self.tags_dict)
        path_name_dst = fill_template_string(
            template_str=path_name_dst, run_time=time_run, template_map=self.tags_dict)

        # info save datasets start
        log_stream.info(' ------> Save datasets ... ')

        # check if destination file exists
        if not os.path.exists(path_name_dst):

            # check if destination file exists
            if os.path.exists(path_name_anc):

                # get the ancillary data
                map_data = read_obj(path_name_anc)

                # check type (only grid)
                if type_dst == 'grid':

                    # check format (nc or tiff)
                    if format_dst == 'nc':

                        # get reference data from the geo collections
                        ref_data = self.geo_dict['geo_dst'].values
                        ref_attrs = self.geo_dict['geo_dst'].attrs
                        ref_geo_x_1d = self.geo_dict['geo_dst']['longitude'].values
                        ref_geo_y_1d = self.geo_dict['geo_dst']['latitude'].values
                        # get reference attributes
                        ref_transform, ref_crs, ref_epsg = ref_attrs['transform'], ref_attrs['crs'], ref_attrs['epsg']

                        # create 2D meshgrid for geographical coordinates
                        ref_geo_x_2d, ref_geo_y_2d = np.meshgrid(ref_geo_x_1d, ref_geo_y_1d)

                        # create the destination folder if it does not exist
                        folder_name_dst, file_name_dst = os.path.split(path_name_dst)
                        os.makedirs(folder_name_dst, exist_ok=True)

                        # save the dataset to a NetCDF file
                        save_file_nc(path_name_dst, map_data, time_run,
                                     ref_data, ref_geo_x_2d, ref_geo_y_2d, var_name=var_name)

                        # info save datasets end (netcdf)
                        log_stream.info(' ------> Save map datasets ... DONE. [NETCDF]')

                    elif format_dst == 'tiff' or format_dst == 'tif':

                        # get reference data from the geo collections
                        ref_data = self.geo_dict['geo_dst'].values
                        ref_attrs = self.geo_dict['geo_dst'].attrs
                        ref_geo_x_1d = self.geo_dict['geo_dst']['longitude'].values
                        ref_geo_y_1d = self.geo_dict['geo_dst']['latitude'].values
                        # get reference attributes
                        ref_transform, ref_crs, ref_epsg = ref_attrs['transform'], ref_attrs['crs'], ref_attrs['epsg']
                        # create 2D meshgrid for geographical coordinates
                        ref_geo_x_2d, ref_geo_y_2d = np.meshgrid(ref_geo_x_1d, ref_geo_y_1d)

                        # create the destination folder if it does not exist
                        folder_name_dst, file_name_dst = os.path.split(path_name_dst)
                        os.makedirs(folder_name_dst, exist_ok=True)

                        # ERROR ON SERVER
                        # error in saving ERROR 1: Only OGC WKT Projections supported for writing to GeoTIFF.
                        # EPSG:4326 not supported.
                        save_file_tiff(path_name_dst,
                                       map_data, ref_geo_x_2d, ref_geo_y_2d,
                                       file_metadata=f'Variable: {var_name}',
                                       file_epsg_code=ref_epsg)

                        # info save datasets end (geotiff)
                        log_stream.info(' ------> Save datasets ... DONE. [GEOTIFF]')

                    else:
                        # if the destination data format is not supported, raise an error
                        log_stream.info(' ------> Save datasets ... FAILED')
                        log_stream.error(' ===> Filename format is not allowed')
                        raise NotImplementedError('Format is not implemented yet')

                else:
                    # if the destination data type is not supported, raise an error
                    log_stream.info(' ------> Save datasets ... FAILED')
                    log_stream.error(' ===> Destination data type is not supported')
                    raise NotImplementedError('Only "grid" type is available.')

            else:
                # if the ancillary file does not exist, log a warning and skip saving
                log_stream.warning(' ===> Ancillary file "' + path_name_anc + '" is not available.')
                log_stream.info(' ------> Save datasets ... SKIPPED. Ancillary file does not exist: ' + path_name_anc)

        else:
            # if the destination file already exists, log a message and skip saving
            log_stream.info(' ------> Save datasets ... SKIPPED. File already exists: ' + path_name_dst)

        # info end
        log_stream.info(' ----> Dump data ... DONE')

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to organize data
    def organize_data(self, time_run: pd.Timestamp,
                      var_name='rain', var_min=0, var_max=None):

        # info start
        log_stream.info(' ----> Organize data ... ')

        # check if time_run is a pd.Timestamp object
        if not isinstance(time_run, pd.Timestamp):
            time_run = pd.Timestamp(time_run)

        # get the type and format of source data
        type_src, format_src = self.type_src, self.format_src

        # get the source, ancillary and destination path names
        path_name_src, path_name_anc, path_name_dst = self.path_name_src, self.path_name_anc, self.path_name_dst
        # compose the path names using the template strings
        path_name_src = fill_template_string(
            template_str=path_name_src, run_time=time_run, template_map=self.tags_dict)
        path_name_anc = fill_template_string(
            template_str=path_name_anc, run_time=time_run, template_map=self.tags_dict)
        path_name_dst = fill_template_string(
            template_str=path_name_dst, run_time=time_run, template_map=self.tags_dict)

        # apply the update flag (if set by the user)
        if self.flag_update:
            if os.path.exists(path_name_anc):
                os.remove(path_name_anc)
            if os.path.exists(path_name_dst):
                os.remove(path_name_dst)

        # check if ancillary file exists
        if not os.path.exists(path_name_anc):

            # check if source file exists
            if os.path.exists(path_name_src):

                # check the type of source data (point or grid)
                if type_src == 'point':

                    # check the source data format (csv or xlsx)
                    if format_src('csv'):

                        # Read datasets point file
                        file_dframe = read_file_csv(path_name_src, time_run)

                        # Filter data using variable limits (if defined)
                        if var_min is not None:
                            file_dframe = file_dframe[(file_dframe['data'] >= var_min)]
                        if var_max is not None:
                            file_dframe = file_dframe[(file_dframe['data'] <= var_max)]

                        file_obj = deepcopy(file_dframe)

                    elif format_src('.xlsx'):

                        # manage the active filename and dataframe
                        if self.active_file is None:
                            self.active_file = deepcopy(path_name_src)
                            active_data = None
                        elif self.active_file != path_name_src:
                            self.active_file, self.active_data = deepcopy(path_name_src), None
                            active_data = None
                        elif self.active_file == path_name_src:
                            active_data = deepcopy(self.active_data)
                        else:
                            log_stream.error(' ===> Check active data and file source case is not supported')
                            raise NotImplemented('Case not implemented yet')

                        # read data from the xlsx file
                        file_dframe, file_dframe_active = read_file_xlsx(
                            path_name_src, time_run, file_dframe_active=active_data)

                        # update object data
                        if self.active_data is None:
                            self.active_data = deepcopy(file_dframe_active)

                        # Filter data using variable limits (if defined)
                        if var_min is not None:
                            file_dframe = file_dframe[(file_dframe['data'] >= var_min)]
                        if var_max is not None:
                            file_dframe = file_dframe[(file_dframe['data'] <= var_max)]

                        file_obj = deepcopy(file_dframe)

                    else:
                        log_stream.error(' ===> Source data format is not supported. Check your source datasets')
                        raise NotImplementedError('Only "csv" or "xlsx" formats are available.')

                elif type_src == 'grid':

                    # check the source data format (tiff or tif)
                    if format_src == 'tiff' or format_src == 'tif':

                        # Read datasets map file
                        file_darray = read_file_tiff(path_name_src)

                        # Filter data using variable limits (if defined)
                        if var_min is not None:
                            file_darray = file_darray.where(file_darray.values < var_min, file_darray.values, var_min)
                        if var_max is not None:
                            file_darray = file_darray.where(file_darray.values > var_max, file_darray.values, var_max)

                        file_obj = deepcopy(file_darray)

                    else:

                        # if the source data format is not supported, raise an error
                        log_stream.error(' ===> Source data format is not supported. Check your source datasets')
                        raise NotImplementedError('Only "tiff" or "tif" formats are available.')

                else:
                    # if the source data type is not supported, raise an error
                    log_stream.error(' ===> Source data type is not supported. Check your source datasets')
                    raise NotImplementedError('Only "point" or "grid" types are available.')

            else:
                # if the source file does not exist, set file_obj to None and log a warning
                file_obj = None
                log_stream.warning(' ===> File "' + path_name_src + '" is not available.')

        else:
            # if the ancillary file exists, read it from the workspace (previously saved)
            file_obj = None
            print('read from workspace')

        # check if file_obj is not None (i.e., source file exists)
        if file_obj is not None:

            # check data type (DataFrame or DataArray)
            if isinstance(file_obj, pd.DataFrame):

                log_stream.info(' ------> Interpolate points to reference datasets ... ')

                geox_out_1d = self.geo_dict['geo_dst']['longitude'].values
                geoy_out_1d = self.geo_dict['geo_dst']['latitude'].values
                mask_out_2d = self.geo_dict['geo_dst'].values
                geox_out_2d, geoy_out_2d = np.meshgrid(geox_out_1d, geoy_out_1d)

                map_out_2d = interpolate_rain_points2map(
                    file_obj, mask_out_2d, geox_out_2d, geoy_out_2d,
                    folder_tmp=self.folder_name_tmp)

                log_stream.info(' ------> Interpolate points to reference datasets ... DONE')

            elif isinstance(file_obj, xr.DataArray):

                log_stream.info(' ------> Reproject source datasets to reference datasets ... ')

                reference_obj = self.geo_dict['geo_dst']
                valid_index_out = self.geo_dict['valid_output_index']

                map_out_2d = resample_data(file_obj, reference_obj, valid_output_index=valid_index_out)

                """ debug resampling
                plt.figure()
                plt.imshow(map_out_2d, cmap='viridis')
                plt.colorbar()

                plt.figure()
                plt.imshow(file_obj.values, cmap='viridis')
                plt.colorbar()

                plt.show(block=False)
                """

                log_stream.info(' ------> Reproject source datasets to reference datasets ... DONE')

            else:
                # if the file_obj is not a DataFrame or DataArray, raise an error
                log_stream.error(' ===> Filename format is not allowed')
                raise NotImplementedError('Format is not implemented yet')

            # check map limits (if defined)
            log_stream.info(' ------> Check map datasets ...')

            # Ensure map_out_2d is a valid DataArray
            if map_out_2d is not None:

                # Initialize flags
                clipped_to_min, clipped_to_max = False, False

                # Handle var_min
                if var_min is not None:
                    below_min = map_out_2d < var_min
                    if below_min.any():
                        log_stream.warning(' ===> Some values are less than the "var_min" limit ...')
                        map_out_2d = map_out_2d.where(~below_min, var_min)
                        log_stream.warning(' ===> Fixed values below "var_min" to the minimum limit.')
                        clipped_to_min = True

                # Handle var_max
                if var_max is not None:
                    above_max = map_out_2d > var_max
                    if above_max.any():
                        log_stream.warning(' ===> Some values are greater than the "var_max" limit ...')
                        map_out_2d = map_out_2d.where(~above_max, var_max)
                        log_stream.warning(' ===> Fixed values above "var_max" to the maximum limit.')
                        clipped_to_max = True

                if not (clipped_to_min or clipped_to_max):
                    log_stream.info(' ------> Check map datasets ... DONE. All values are within the expected limits')
                else:
                    log_stream.info(' ------> Check map datasets ... DONE. Some values are not within the expected limits')

            else:
                log_stream.info(' ------> Check map datasets ... SKIPPED. Datasets is defined by NoneType or empty')

            # save map datasets
            log_stream.info(' ------> Save map datasets ... ')

            # ensure map_out_2d is a valid DataArray
            if map_out_2d is not None:

                folder_name_anc, file_name_anc = os.path.split(path_name_anc)
                os.makedirs(folder_name_anc, exist_ok=True)

                write_obj(path_name_anc, map_out_2d)

                log_stream.info(' ------> Save map datasets ... DONE')

            else:

                log_stream.info(' ------> Save map datasets ... SKIPPED. Datasets is defined by NoneType or empty')

            # info end
            log_stream.info(' ----> Organize data ... DONE')

        else:

            # info end
            log_stream.info(' ----> Organize data ... SKIPPED. Datasets was previously saved')

# ----------------------------------------------------------------------------------------------------------------------
