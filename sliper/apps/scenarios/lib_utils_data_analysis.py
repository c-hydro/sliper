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
import pandas as pd

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
            mean_value = da_var[column_name].mean()
            results['avg'] = float(mean_value.iloc[0]) if isinstance(mean_value, pd.Series) else float(mean_value)

        elif metric == 'max':
            max_value = da_var[column_name].max()
            results['max'] = float(max_value.iloc[0]) if isinstance(max_value, pd.Series) else float(max_value)

        elif metric == 'min':
            min_value = da_var[column_name].min()
            results['min'] = float(min_value.iloc[0]) if isinstance(min_value, pd.Series) else float(min_value)

        elif metric == 'first':
            time_first = da_var.index[0]
            first_value = da_var.loc[time_first]
            results['first'] = float(first_value[column_name]) if isinstance(first_value, pd.Series) else float(first_value)

        elif metric == 'last':
            time_last = da_var.index[-1]
            last_value = da_var.loc[time_last]
            results['last'] = float(last_value[column_name]) if isinstance(last_value, pd.Series) else float(last_value)

    return results

# ----------------------------------------------------------------------------------------------------------------------