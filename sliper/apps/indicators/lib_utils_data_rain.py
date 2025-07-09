"""
Library Features:

Name:          lib_utils_data_rain
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import numpy as np
import pandas as pd

from lib_utils_time import split_time_window

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to compute rain accumulated maps over time period
def compute_rain_maps_accumulated(var_da_source,  coord_name_time='time', time_window=None, time_direction=None):

    if time_window is None:
        time_window = '3H'

    if time_direction is None:
        time_direction = 'right'

    if coord_name_time in list(var_da_source.coords):
        time_coords = var_da_source[coord_name_time]
    else:
        log_stream.error(' ===> Time coord "' + coord_name_time + '" must be defined in the source DataArray object')
        raise RuntimeError('Check your source DataArray object and include the time coord "' + coord_name_time + '"')

    time_period, time_frequency = split_time_window(time_window)
    time_stamp_start, time_stamp_end = pd.Timestamp(time_coords.values[0]), pd.Timestamp(time_coords.values[-1])

    if isinstance(time_period, str):
        time_period = int(time_period)

    if time_direction == 'left':
        var_da_sorted = var_da_source.sortby(time_coords, ascending=False)
        var_da_accumulated = var_da_sorted.rolling(time=time_period, center=False).sum()
    elif time_direction == 'right':
        var_da_sorted = var_da_source.sortby(time_coords, ascending=True)
        var_da_accumulated = var_da_sorted.rolling(time=time_period, center=False).sum()
    else:
        log_stream.error(' ===> Time direction "' + time_direction + '" flag is not allowed')
        raise IOError('Available flags for temporal direction are: "right" and "left"')

    var_da_accumulated.attrs = {'time_from': time_stamp_start, 'time_to': time_stamp_end,
                                'time_window': time_window, 'time_direction': time_direction}

    # Debug
    # plt.figure()
    # plt.imshow(var_da_resampled.values[:,:,0])
    # plt.colorbar()
    # plt.show()

    return var_da_accumulated

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to compute rain averaged time-series over time period
def compute_rain_ts_averaged(dframe_var, column_name=None,
                             time_window='3H', time_direction='right', time_inverse=True):
    if column_name is None:
        column_name = 'data_time_series'
    if isinstance(column_name, list):
        column_name = column_name[0]

    time_period, time_frequency = split_time_window(time_window)

    if isinstance(time_period, str):
        time_period = int(time_period)

    if time_inverse:
        # series_var = dframe_var[column_name].resample(time_window, label=time_direction).mean()[:-1]
        series_var = dframe_var[column_name].rolling(time_period, center=False).mean()[::-1]
    else:
        # series_var = dframe_var[column_name].resample(time_window, label=time_direction).mean()
        series_var = dframe_var[column_name].rolling(time_period, center=False).mean()

    series_var = series_var.dropna(how='all')

    return series_var
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to compute rain accumulated time-series over time period
def compute_rain_ts_accumulated(dframe_var, column_name=None,
                                time_window='3H', time_direction='right', time_inverse=True):
    if column_name is None:
        column_name = 'data_time_series'
    if isinstance(column_name, list):
        column_name = column_name[0]

    time_period, time_frequency = split_time_window(time_window)

    if isinstance(time_period, str):
        time_period = int(time_period)

    if time_inverse:
        # series_var = dframe_var[column_name].resample(time_window, label=time_direction).sum()[:-1]
        series_var = dframe_var[column_name].rolling(time_period, center=False).sum()[::-1]
    else:
        # series_var = dframe_var[column_name].resample(time_window, label=time_direction).sum()
        series_var = dframe_var[column_name].rolling(time_period, center=False).sum()

    series_var = series_var.dropna(how='all')

    return series_var
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to compute rain statistics
def compute_data_statistics(da_var, column_name=None, metrics=None):
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
            warnings.warn(f"Metric '{metric}' is not supported and will be skipped.", UserWarning)
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

# ----------------------------------------------------------------------------------------------------------------------
# Method to compute rain peaks
def compute_rain_peaks(var_da, var_point_collections, var_analysis='max'):

    var_time = var_da['time'].values
    var_data_3d = var_da.values

    obj_point_time_stamp, obj_point_collections = [], {}
    for time_id, date_time_step in enumerate(var_time):
        var_data_2d = var_data_3d[:, :, time_id]

        time_stamp_step = pd.Timestamp(date_time_step)

        for point_key, point_idxs in var_point_collections.items():
            var_data_1d = var_data_2d[point_idxs]

            if var_data_1d.size == 0 or np.all(np.isnan(var_data_1d)):
                value_max = value_avg = np.nan  # or some fallback value
            else:
                value_max, value_avg = np.nanmax(var_data_1d), np.nanmean(var_data_1d)

            if point_key not in list(obj_point_collections.keys()):
                obj_point_collections[point_key] = [value_max]
            else:
                value_tmp = obj_point_collections[point_key]
                value_tmp.append(value_max)
                obj_point_collections[point_key] = value_tmp

        obj_point_time_stamp.append(time_stamp_step)

    peaks_dframe = pd.DataFrame(index=obj_point_time_stamp, data=obj_point_collections)
    peaks_dframe = peaks_dframe.dropna(how='all')

    peaks_max = peaks_dframe.to_numpy().max()
    peaks_mean = peaks_dframe.to_numpy().mean()

    peaks_metrics = {'max': peaks_max, 'avg': peaks_mean}

    return peaks_dframe, peaks_metrics

# ----------------------------------------------------------------------------------------------------------------------
