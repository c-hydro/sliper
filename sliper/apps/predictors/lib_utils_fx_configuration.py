"""
Library Features:

Name:          lib_utils_fx_configuration
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250725'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
from copy import deepcopy

from lib_utils_fx_data import convert_df2array
from lib_utils_fx_kernel import regularizedKernLSTest

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to select fx configuration
def select_fx_method(methods):
    """
    Searches for methods with active=True in the configuration dictionary.
    Ensures exactly one is active. Returns the method name and its config.
    Raises a ValueError if zero or multiple active methods are found.
    """

    # Find all active methods
    active_methods = [(name, mcfg) for name, mcfg in methods.items() if mcfg.get("active", False)]

    if len(active_methods) == 0:
        raise ValueError(" ===> No active methods found in the configuration.")
    elif len(active_methods) > 1:
        active_names = [name for name, _ in active_methods]
        raise ValueError(f" ===> Multiple active methods found: {active_names}. Only one can be active.")

    # select the active method
    fx_name, tmp_params = active_methods[0]
    fx_params = {k: v for k, v in tmp_params.items() if k != 'active'}

    # Only one active method
    return fx_name, fx_params
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to select fx configuration
def organize_fx_args(
    fx_name, fx_parameters,
    training_data,
    geo_data_alert_thr, geo_data_alert_index, geo_data_alert_color,
    training_matrix_center_tag = 'training_matrix_center',
    training_matrix_max_tag = 'training_matrix_max',
    training_matrix_mean_tag = 'training_matrix_mean',
    training_coefficient_tag = 'training_coefficient',
    ):

    """
    Build fx_attrs dictionary based on fx_flag and datasets.
    No class attributes (self) are used.
    """
    fx_attrs = {}

    # select fx method
    if fx_name == 'fx_kernel':

        # Map required dataset keys to their target attribute names
        key_map = {
            'fx_training_matrix_center': training_matrix_center_tag,
            'fx_training_matrix_max': training_matrix_max_tag,
            'fx_training_matrix_mean': training_matrix_mean_tag,
            'fx_training_coefficient': training_coefficient_tag,
        }

        # validata training data
        for target_key, source_key in key_map.items():
            if source_key not in training_data:
                log_stream.error(
                    f' ===> Fx datasets "{source_key}" is not available in the reference obj'
                )
                raise IOError('Datasets is mandatory to apply the method. Exit.')
            fx_attrs[target_key] = deepcopy(training_data[source_key])

        fx_attrs['fx_parameters_type'] = fx_parameters['kernel_type']
        fx_attrs['fx_parameters_exponent'] = fx_parameters['kernel_exponent']

        # add additional attributes
        fx_attrs['fx_filter_columns'] = None
        fx_attrs['fx_filter_group_index'] = None
        fx_attrs['fx_filter_warning_threshold'] = geo_data_alert_thr
        fx_attrs['fx_filter_warning_index'] = geo_data_alert_index
        fx_attrs['fx_filter_warning_color'] = geo_data_alert_color

    else:
        # if fx_flag is not recognized, raise an error
        log_stream.error(f' ===> Fx name "{fx_name}" is not expected by the procedure.')
        raise NotImplementedError('Fx not defined yet')

    return fx_attrs
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to organize fx kernel datasets out
def organize_fx_kernel_datasets_out(fx_output, fx_datasets=None,
                                    fx_name_obj='kernel_dframe',
                                    fx_name_soil_slips_prediction='slips_pred_n'):
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

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to configure fx kernel datasets in
def organize_fx_kernel_datasets_in(fx_dframe,
                                   fx_name_kernel_datasets='kernel_datasets',
                                   fx_name_kernel_dframe='kernel_dframe',
                                   fx_vars=None, **kwargs):

    # Check if fx_attrs is provided
    if fx_vars is None:
        fx_vars = {
            "var_1": "sm_value_first",
            "var_2": "rain_accumulated_12H",
            "var_3": "rain_peak_3H"}

    # copy the original dataframe
    fx_dframe_all = deepcopy(fx_dframe)

    # Base columns that must be included
    extra_vars = ["day_of_the_year"]

    # set desired order =  fx_vars + extra_vars
    required_vars = list(fx_vars.values()) + extra_vars
    # check if fx_dframe has all required variables
    missing_vars = [c for c in required_vars if c not in fx_dframe_all.columns]
    if missing_vars:
        raise ValueError(f"Missing variables in dataframe: {missing_vars}")

    # check available columns
    available_vars = [col for col in required_vars if col in fx_dframe_all.columns]
    # filter dataframe to only available columns
    fx_dframe_filter = fx_dframe_all[available_vars]
    # sort dataframe
    fx_dframe_order = fx_dframe_filter.sort_values(by="day_of_the_year", ascending=False).reset_index(drop=True)

    # keep only numeric columns
    fx_dframe_numeric = fx_dframe_order.select_dtypes(include=['number'])

    # organize the dataframe
    fx_dframe_numeric = fx_dframe_numeric[required_vars]

    fx_obj = {fx_name_kernel_datasets: convert_df2array(fx_dframe_numeric),
              fx_name_kernel_dframe: fx_dframe_all}

    return fx_obj

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
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
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to execute fx kernel
def exec_fx_kernel(kernel_datasets,
                   kernel_matrix_center=None, kernel_matrix_max=None, kernel_matrix_mean=None,
                   kernel_coefficient=None,
                   kernel_type='polynomial', kernel_exponent=3):

    # adapt the kernel parameters to the datasets
    # kernel_datasets_norm = center(normalize(kernel_datasets))
    kernel_matrix_max_adapted = kernel_matrix_max[:, :kernel_datasets.shape[1]]
    kernel_matrix_mean_adapted = kernel_matrix_mean[:, :kernel_datasets.shape[1]]# keep first 4
    kernel_matrix_center_adapted =  kernel_matrix_center[:, :kernel_datasets.shape[1]]

    # normalize and center the generated data
    kernel_datasets_norm = kernel_datasets / kernel_matrix_max_adapted
    # normalize and center the generated data
    kernel_datasets_centered = kernel_datasets_norm - kernel_matrix_mean_adapted

    # predict the output with the estimated model
    kernel_predictors = regularizedKernLSTest(
        kernel_coefficient, kernel_matrix_center_adapted, kernel_type, kernel_exponent,
        kernel_datasets_centered)

    return kernel_predictors

# ----------------------------------------------------------------------------------------------------------------------
