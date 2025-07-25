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
