"""
Class Features

Name:          driver_training
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250724'
Version:       '1.1.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import os
import pandas as pd

from lib_data_io_pickle import read_obj, write_obj

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# class DriverTraining
class DriverTraining:

    # ------------------------------------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, src_dict, dst_dict,
                 flag_update=True):

        self.file_name_tag, self.folder_name_tag = 'file_name', 'folder_name'

        self.training_matrix_center_tag = 'training_matrix_center'
        self.training_matrix_max_tag = 'training_matrix_max'
        self.training_matrix_mean_tag = 'training_matrix_mean'
        self.training_coefficient_tag = 'training_coefficient'

        self.flag_update = flag_update

        file_name_tmp = src_dict[self.training_matrix_center_tag][self.file_name_tag]
        folder_name_tmp = src_dict[self.training_matrix_center_tag][self.folder_name_tag]
        self.file_path_src_matrix_center = os.path.join(folder_name_tmp, file_name_tmp)

        file_name_tmp = src_dict[self.training_matrix_max_tag][self.file_name_tag]
        folder_name_tmp = src_dict[self.training_matrix_max_tag][self.folder_name_tag]
        self.file_path_src_matrix_max = os.path.join(folder_name_tmp, file_name_tmp)

        file_name_tmp = src_dict[self.training_matrix_mean_tag][self.file_name_tag]
        folder_name_tmp = src_dict[self.training_matrix_mean_tag][self.folder_name_tag]
        self.file_path_src_matrix_mean = os.path.join(folder_name_tmp, file_name_tmp)

        file_name_tmp = src_dict[self.training_coefficient_tag][self.file_name_tag]
        folder_name_tmp = src_dict[self.training_coefficient_tag][self.folder_name_tag]
        self.file_path_src_coeff = os.path.join(folder_name_tmp, file_name_tmp)

        self.file_name_dst = dst_dict[self.file_name_tag]
        self.folder_name_dst = dst_dict[self.folder_name_tag]
        self.file_path_dst = os.path.join(self.folder_name_dst, self.file_name_dst)

    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Method to read training datasets in csv format
    def read_training_datasets(self, file_name, file_delimiter=',', file_header=None):

        if file_name.endswith('.csv'):
            csv_dset = pd.read_csv(file_name, delimiter=file_delimiter, header=file_header).to_numpy()
        else:
            log_stream.error(' ===> File "' + file_name + '" format is not supported')
            raise NotImplementedError('Case not implemented yet')

        return csv_dset
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Method to organize data
    def organize_data(self):

        # info training information end (done)
        log_stream.info(' ----> Organize training information ... ')

        # get file paths
        file_path_src_matrix_center = self.file_path_src_matrix_center
        file_path_src_matrix_max = self.file_path_src_matrix_max
        file_path_src_matrix_mean = self.file_path_src_matrix_mean
        file_path_src_coeff = self.file_path_src_coeff
        file_path_dst = self.file_path_dst

        # flag to update training datasets
        if self.flag_update:
            if os.path.exists(file_path_dst):
                os.remove(file_path_dst)

        # check if training datasets exist
        if not os.path.exists(file_path_dst):

            if os.path.exists(file_path_src_matrix_center):
                training_matrix_center = self.read_training_datasets(
                    file_path_src_matrix_center, file_delimiter=',', file_header=None)
            else:
                log_stream.error(' ===> File training matrix center "' + file_path_src_matrix_center +
                                 '" does not exist.')
                raise IOError('File not found. Exit')
            if os.path.exists(file_path_src_matrix_max):
                training_matrix_max = self.read_training_datasets(
                    file_path_src_matrix_max, file_delimiter=';', file_header=None)
            else:
                log_stream.error(' ===> File training matrix max "' + file_path_src_matrix_max +
                                 '" does not exist.')
                raise IOError('File not found. Exit')
            if os.path.exists(file_path_src_matrix_mean):
                training_matrix_mean = self.read_training_datasets(
                    file_path_src_matrix_mean, file_delimiter=';', file_header=None)
            else:
                log_stream.error(' ===> File training matrix mean "' + file_path_src_matrix_mean +
                                 '" does not exist.')
                raise IOError('File not found. Exit')

            if os.path.exists(file_path_src_coeff):
                training_coefficient = self.read_training_datasets(
                    file_path_src_coeff, file_delimiter=None, file_header=None)
            else:
                log_stream.error(' ===> File training coefficient "' + file_path_src_coeff + '" does not exist.')
                raise IOError('File not found. Exit')

            # Organize training collections
            training_info = {
                self.training_matrix_center_tag: training_matrix_center,
                self.training_matrix_max_tag: training_matrix_max,
                self.training_matrix_mean_tag: training_matrix_mean,
                self.training_coefficient_tag: training_coefficient
            }

            # write training collections
            folder_name_dst, file_name_dst = os.path.split(file_path_dst)
            os.makedirs(folder_name_dst, exist_ok=True)

            write_obj(file_path_dst, training_info)

            # info training information end (done)
            log_stream.info(' ----> Organize training information ... DONE')

        else:

            # read training information from file
            training_info = read_obj(file_path_dst)

            # info training information end (previously computed)
            log_stream.info(' ----> Organize training information ... LOADED. Datasets was previously computed.')

        return training_info

# -------------------------------------------------------------------------------------
