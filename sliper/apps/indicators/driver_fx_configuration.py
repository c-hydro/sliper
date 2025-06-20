"""
Class Features

Name:          driver_fx_configuration
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

######################################################################################
# Library
import logging
import inspect
import pandas as pd

from copy import deepcopy

from lib_analysis_predictors_fx_kernel import exec_fx_kernel, organize_fx_kernel_datasets_in, organize_fx_kernel_datasets_out, \
    organize_fx_kernel_parameters

from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverFx configurations
class DriverFx:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, time_step, fx_name='fx_kernel', fx_attrs=None):

        self.time_step = pd.Timestamp(time_step)
        self.fx_name = fx_name
        self.fx_attrs = fx_attrs

        self.fx_methods = self.map_fx_methods()

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to map the fx methods
    def map_fx_methods(self):
        if self.fx_name == 'fx_kernel':
            fx_methods = {'fx_exec': exec_fx_kernel,
                          'fx_organize_datasets_in':  organize_fx_kernel_datasets_in,
                          'fx_organize_parameters': organize_fx_kernel_parameters,
                          'fx_organize_datasets_out': organize_fx_kernel_datasets_out}
        else:
            log_stream.error(' ===> Fx name "' + self.fx_name + '" is not expected')
            raise NotImplementedError('Fx method not implemented yet')
        return fx_methods
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize fx datasets out
    def organize_fx_datasets_out(self, fx_array, fx_datasets, tag_fx_organize='fx_organize_datasets_out'):
        fx_method = self.fx_methods[tag_fx_organize]
        fx_datasets = fx_method(fx_array, fx_datasets=fx_datasets)
        return fx_datasets
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to configure fx datasets in
    def organize_fx_datasets_in(self, fx_datasets_src, tag_fx_organize='fx_organize_datasets_in'):
        fx_method = self.fx_methods[tag_fx_organize]
        fx_attrs = deepcopy(self.fx_attrs)
        fx_datasets_dst = deepcopy(fx_method(fx_datasets_src, fx_attrs=fx_attrs))
        return fx_datasets_dst
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to organize fx parameters
    def organize_fx_parameters(self, tag_fx_organize='fx_organize_parameters'):
        fx_method = self.fx_methods[tag_fx_organize]
        fx_attrs = fx_method(self.fx_attrs)
        return fx_attrs
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to fill fx args included in fx signature
    @staticmethod
    def fill_fx_args(fx_signature, fx_data, fx_sep=','):

        fx_parameters_list_no_def, fx_parameters_list_no_value = [], []
        for fx_parameter in fx_signature.parameters.values():
            fx_parameter_name = fx_parameter.name
            fx_parameter_default = fx_parameter.default

            if fx_parameter_name not in list(fx_data.keys()):
                if fx_parameter_default is not inspect._empty:
                    fx_data[fx_parameter_name] = fx_parameter_default

                    fx_parameters_list_no_def.append(fx_parameter_name)
                else:
                    fx_data[fx_parameter_name] = None
                    fx_parameters_list_no_value.append(fx_parameter_name)

        if fx_parameters_list_no_def.__len__() > 0:
            fx_parameters_str_no_def = fx_sep.join(fx_parameters_list_no_def)
            log_stream.warning(' ===> Fx parameters "' + fx_parameters_str_no_def +
                               '" not defined; fx will use a default value')

        if fx_parameters_list_no_value.__len__() > 0:
            fx_parameters_str_no_value = fx_sep.join(fx_parameters_list_no_value)
            log_stream.warning(' ===> Fx parameters "' + fx_parameters_str_no_value +
                               '" not defined; fx will use a null value')

        return fx_data

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to remove fx args not included in fx signature
    @staticmethod
    def pop_fx_args(fx_signature, fx_data):

        fx_data_tmp = deepcopy(fx_data)
        for fx_key_tmp in fx_data_tmp.keys():
            if fx_key_tmp not in list(fx_signature.parameters.keys()):
                fx_data.pop(fx_key_tmp, None)

        return fx_data
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to inspect fx
    @staticmethod
    def get_fx_signature(fx_method):
        fx_signature = inspect.signature(fx_method)
        return fx_signature
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to exec fx
    def exec_fx(self, fx_datasets, fx_attrs, tag_fx_exec='fx_exec'):

        # Get method and signature
        fx_method = self.fx_methods[tag_fx_exec]
        fx_signature = self.get_fx_signature(fx_method)

        # Fill and pop fx data
        fx_data_collections = self.fill_fx_args(fx_signature, fx_data={**fx_datasets, **fx_attrs})
        fx_data_collections = self.pop_fx_args(fx_signature, fx_data=fx_data_collections)

        # Execute fx method
        fx_outcome_collections = fx_method(**fx_data_collections)

        return fx_outcome_collections
    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
