"""
Library Features:

Name:          lib_utils_data_analysis
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute data metrics
def compute_data_metrics(da_var, column_name=None, metrics=None):
    if column_name is None:
        column_name = ['data_time_series']
    if not isinstance(column_name, list):
        column_name = [column_name]

    if metrics is None:
        metrics = ['avg', 'max']

    # Supported metrics
    supported_metrics = {'avg', 'max', 'min', 'first', 'last'}
    results = {}

    for metric in metrics:
        if metric not in supported_metrics:
            log_stream.warning(f" ===> Metric '{metric}' is not supported and will be skipped.", UserWarning)
            continue

        if metric == 'avg':
            results['avg'] = float(da_var[column_name].mean())
        elif metric == 'max':
            results['max'] = float(da_var[column_name].max())
        elif metric == 'min':
            results['min'] = float(da_var[column_name].min())
        elif metric == 'first':
            time_first = da_var.index[0]
            results['first'] = float(da_var.loc[time_first].values[0])
        elif metric == 'last':
            time_last = da_var.index[-1]
            results['last'] = float(da_var.loc[time_last].values[0])

    return results

# ----------------------------------------------------------------------------------------------------------------------