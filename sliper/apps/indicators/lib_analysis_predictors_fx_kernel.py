"""
Library Features:

Name:          lib_analysis_predictors_fx_kernel
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

#######################################################################################
# Library
import logging
import pandas as pd

from lib_info_args import logger_name_predictors as logger_name

from lib_data_io_fx_kernel import convert_dframe2array, \
    filter_dataframe_columns, add_dataframe_columns, fill_dataframe_nan, \
    drop_dataframe_columns, order_dataframe_columns
from lib_utils_fx_kernel import center, normalize, regularizedKernLSTest

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to organize fx kernel datasets out
def organize_fx_kernel_datasets_out(fx_output, fx_datasets=None,
                                    fx_name_obj='kernel_dframe',
                                    fx_name_soil_slips_prediction='soil_slips_prediction'):
    if fx_datasets is not None:
        fx_dframe = fx_datasets[fx_name_obj]
        fx_output = fx_output.ravel()
        fx_output[fx_output < 0] = 0.0
        fx_output = fx_output.astype(int)
        fx_dframe[fx_name_soil_slips_prediction] = fx_output
    else:
        log_stream.error(' ===> DataFrame must be defined')
        raise IOError('Check the datasets and the procedure')
    return fx_dframe

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to configure fx kernel datasets in
def organize_fx_kernel_datasets_in(fx_dframe, fx_attrs=None,
                                   fx_name_kernel_datasets='kernel_datasets',
                                   fx_name_kernel_dframe='kernel_dframe'):

    if 'fx_filter_columns' in list(fx_attrs.keys()):
        filter_columns = fx_attrs['fx_filter_columns']
    else:
        log_stream.error(' ===> Attribute "fx_filter_columns" must be in the attributes obj')
        raise RuntimeError('Attribute is mandatory to apply the method')

    if 'fx_filter_group_index' in list(fx_attrs.keys()):
        filter_index = fx_attrs['fx_filter_group_index']
    else:
        log_stream.error(' ===> Attribute "fx_filter_index" must be in the attributes obj')
        raise RuntimeError('Attribute is mandatory to apply the method')

    fx_dframe_all = filter_dataframe_columns(fx_dframe, file_cols_filter=filter_columns)
    fx_dframe_all = fill_dataframe_nan(fx_dframe_all, column_list=['event_threshold'], fill_value_list=['NA'])
    fx_dframe_all = add_dataframe_columns(fx_dframe_all, obj_n_domain={'event_domain': filter_index})
    fx_dframe_all = order_dataframe_columns(
        fx_dframe_all, file_cols_order=['event_domain', 'day_of_the_year'], type_cols_order=['ascending', 'descending'])
    fx_dframe_numeric = drop_dataframe_columns(
        fx_dframe_all, file_cols_drop=["event_n", "event_threshold", "event_index", 'event_domain'])

    fx_obj = {fx_name_kernel_datasets: convert_dframe2array(fx_dframe_numeric),
              fx_name_kernel_dframe: fx_dframe_all}

    return fx_obj

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to organize fx kernel parameters
def organize_fx_kernel_parameters(
        fx_attrs,
        fx_name_kernel_matrix_center='kernel_matrix_center',
        fx_name_kernel_matrix_max='kernel_matrix_max',
        fx_name_kernel_matrix_mean='kernel_matrix_mean',
        fx_name_kernel_coefficient='kernel_coefficient',
        fx_name_kernel_type='kernel_type', fx_name_kernel_exponent='kernel_exponent'):

    if 'fx_training_matrix_center' in list(fx_attrs.keys()):
        fx_training_matrix_center = fx_attrs['fx_training_matrix_center']
    else:
        log_stream.error(' ===> Attribute "fx_training_matrix_center" must be in the attributes obj')
        raise RuntimeError('Attribute is mandatory to apply the method')
    if 'fx_training_matrix_max' in list(fx_attrs.keys()):
        fx_training_matrix_max = fx_attrs['fx_training_matrix_max']
    else:
        log_stream.error(' ===> Attribute "fx_training_matrix_max" must be in the attributes obj')
        raise RuntimeError('Attribute is mandatory to apply the method')
    if 'fx_training_matrix_mean' in list(fx_attrs.keys()):
        fx_training_matrix_mean = fx_attrs['fx_training_matrix_mean']
    else:
        log_stream.error(' ===> Attribute "fx_training_matrix_mean" must be in the attributes obj')
        raise RuntimeError('Attribute is mandatory to apply the method')

    if 'fx_training_coefficient' in list(fx_attrs.keys()):
        fx_training_coefficient = fx_attrs['fx_training_coefficient']
    else:
        log_stream.error(' ===> Attribute "fx_training_coefficient" must be in the attributes obj')
        raise RuntimeError('Attribute is mandatory to apply the method')

    if 'fx_parameters_type' in list(fx_attrs.keys()):
        fx_parameters_type = fx_attrs['fx_parameters_type']
    else:
        log_stream.error(' ===> Attribute "fx_parameters_type" must be in the attributes obj')
        raise RuntimeError('Attribute is mandatory to apply the method')
    if 'fx_parameters_exponent' in list(fx_attrs.keys()):
        fx_parameters_exponent = fx_attrs['fx_parameters_exponent']
    else:
        log_stream.error(' ===> Attribute "fx_parameters_exponent" must be in the attributes obj')
        raise RuntimeError('Attribute is mandatory to apply the method')

    fx_obj = {fx_name_kernel_matrix_center: fx_training_matrix_center,
              fx_name_kernel_matrix_max: fx_training_matrix_max,
              fx_name_kernel_matrix_mean: fx_training_matrix_mean,
              fx_name_kernel_coefficient: fx_training_coefficient,
              fx_name_kernel_type: fx_parameters_type, fx_name_kernel_exponent: fx_parameters_exponent}

    return fx_obj
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to execute fx kernel
def exec_fx_kernel(kernel_datasets,
                   kernel_matrix_center=None, kernel_matrix_max=None, kernel_matrix_mean=None,
                   kernel_coefficient=None,
                   kernel_type='polynomial', kernel_exponent=3):

    # Normalize and center the generated data
    # kernel_datasets_norm = center(normalize(kernel_datasets))

    # Normalize and center the generated data
    kernel_datasets_norm = (kernel_datasets / kernel_matrix_max)
    kernel_datasets_centered = kernel_datasets_norm - kernel_matrix_mean

    # Predict the output with the estimated model
    kernel_predictors = regularizedKernLSTest(
        kernel_coefficient, kernel_matrix_center, kernel_type, kernel_exponent,
        kernel_datasets_centered)

    return kernel_predictors

# -------------------------------------------------------------------------------------

