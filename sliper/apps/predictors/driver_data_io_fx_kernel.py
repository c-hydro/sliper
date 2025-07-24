"""
Class Features

Name:          driver_data_io_training
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

######################################################################################
# Library
import logging
import os
import pandas as pd

from lib_data_io_pickle import read_obj, write_obj
from lib_utils_system import make_folder

from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
# Debug
# import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverTraining
class DriverTraining:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, src_dict, dst_dict,
                 flag_training_src_matrix_center='training_matrix_center',
                 flag_training_src_matrix_max='training_matrix_max',
                 flag_training_src_matrix_mean='training_matrix_mean',
                 flag_training_src_coeff='training_coefficient',
                 flag_training_dst='training_datasets',
                 flag_training_updating=True):

        self.file_name_tag = 'file_name'
        self.folder_name_tag = 'folder_name'

        self.flag_training_src_matrix_center = flag_training_src_matrix_center
        self.flag_training_src_matrix_max = flag_training_src_matrix_max
        self.flag_training_src_matrix_mean = flag_training_src_matrix_mean
        self.flag_training_src_coeff = flag_training_src_coeff
        self.flag_training_dst = flag_training_dst

        self.flag_training_updating = flag_training_updating

        file_name_tmp = src_dict[self.flag_training_src_matrix_center][self.file_name_tag]
        folder_name_tmp = src_dict[self.flag_training_src_matrix_center][self.folder_name_tag]
        self.file_path_src_matrix_center = os.path.join(folder_name_tmp, file_name_tmp)

        file_name_tmp = src_dict[self.flag_training_src_matrix_max][self.file_name_tag]
        folder_name_tmp = src_dict[self.flag_training_src_matrix_max][self.folder_name_tag]
        self.file_path_src_matrix_max = os.path.join(folder_name_tmp, file_name_tmp)

        file_name_tmp = src_dict[self.flag_training_src_matrix_mean][self.file_name_tag]
        folder_name_tmp = src_dict[self.flag_training_src_matrix_mean][self.folder_name_tag]
        self.file_path_src_matrix_mean = os.path.join(folder_name_tmp, file_name_tmp)

        file_name_tmp = src_dict[self.flag_training_src_coeff][self.file_name_tag]
        folder_name_tmp = src_dict[self.flag_training_src_coeff][self.folder_name_tag]
        self.file_path_src_coeff = os.path.join(folder_name_tmp, file_name_tmp)

        self.file_name_dst = dst_dict[self.flag_training_dst][self.file_name_tag]
        self.folder_name_dst = dst_dict[self.flag_training_dst][self.folder_name_tag]
        self.file_path_dst = os.path.join(self.folder_name_dst, self.file_name_dst)

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to read training datasets in csv format
    def read_training_datasets(self, file_name, file_delimiter=',', file_header=None):

        if file_name.endswith('.csv'):
            csv_dset = pd.read_csv(file_name, delimiter=file_delimiter, header=file_header).to_numpy()
        else:
            log_stream.error(' ===> File "' + file_name + '" format is not supported')
            raise NotImplementedError('Case not implemented yet')

        return csv_dset
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize data
    def organize_data(self):

        # Starting info
        log_stream.info(' ----> Organize training information ... ')

        file_path_src_matrix_center = self.file_path_src_matrix_center
        file_path_src_matrix_max = self.file_path_src_matrix_max
        file_path_src_matrix_mean = self.file_path_src_matrix_mean
        file_path_src_coeff = self.file_path_src_coeff
        file_path_dst = self.file_path_dst

        if self.flag_training_updating:
            if os.path.exists(file_path_dst):
                os.remove(file_path_dst)

        if not os.path.exists(file_path_dst):

            if os.path.exists(file_path_src_matrix_center):
                training_datasets_matrix_center = self.read_training_datasets(
                    file_path_src_matrix_center, file_delimiter=',', file_header=None)
            else:
                log_stream.error(' ===> File training matrix center "' + file_path_src_matrix_center +
                                 '" does not exist.')
                raise IOError('File not found. Exit')
            if os.path.exists(file_path_src_matrix_max):
                training_datasets_matrix_max = self.read_training_datasets(
                    file_path_src_matrix_max, file_delimiter=';', file_header=None)
            else:
                log_stream.error(' ===> File training matrix max "' + file_path_src_matrix_max +
                                 '" does not exist.')
                raise IOError('File not found. Exit')
            if os.path.exists(file_path_src_matrix_mean):
                training_datasets_matrix_mean = self.read_training_datasets(
                    file_path_src_matrix_mean, file_delimiter=';', file_header=None)
            else:
                log_stream.error(' ===> File training matrix mean "' + file_path_src_matrix_mean +
                                 '" does not exist.')
                raise IOError('File not found. Exit')

            if os.path.exists(file_path_src_coeff):
                training_datasets_coeff = self.read_training_datasets(
                    file_path_src_coeff, file_delimiter=None, file_header=None)
            else:
                log_stream.error(' ===> File training coefficient "' + file_path_src_coeff + '" does not exist.')
                raise IOError('File not found. Exit')

            # Organize training collections
            training_collections = {
                self.flag_training_src_matrix_center: training_datasets_matrix_center,
                self.flag_training_src_matrix_max: training_datasets_matrix_max,
                self.flag_training_src_matrix_mean: training_datasets_matrix_mean,
                self.flag_training_src_coeff: training_datasets_coeff
            }

            # Write training collections
            folder_name_dst, file_name_dst = os.path.split(file_path_dst)
            make_folder(folder_name_dst)
            write_obj(file_path_dst, training_collections)

            # Ending info
            log_stream.info(' ----> Organize training information ... DONE')

        else:

            # Read soil slips collections from disk
            training_collections = read_obj(file_path_dst)
            log_stream.info(' ----> Organize training information ... LOADED. Datasets was previously computed.')

        return training_collections

# -------------------------------------------------------------------------------------
