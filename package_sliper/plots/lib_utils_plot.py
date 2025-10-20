"""
Library Features:

Name:          lib_utils_plot
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20240109'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import os

import numpy as np
import pandas as pd

import matplotlib as mpl
import matplotlib.pylab as plt
import matplotlib.patches as mpatches

from copy import deepcopy

from lib_info_args import logger_name

# logging
warnings.filterwarnings('ignore')
logging.getLogger("matplotlib").setLevel(logging.WARNING)
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to configure time series data
def configure_time_series_info(ts_data_in, fields=None):
    ts_data_list = []
    if fields is not None:
        for ts_name_in, ts_name_out in fields.items():
            if ts_name_in != 'time':
                if ts_name_in in ts_data_in.columns:
                    ts_data_list.append(ts_name_in)
                else:
                    log_stream.warning(' ===> Field "' + ts_name_in +
                                       '" not in the data dataframe object. Field keep the standard name')

    ts_data_list = sorted(ts_data_list)
    ts_data_out = deepcopy(ts_data_in[ts_data_list])

    return ts_data_out
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to configure time-series axes
def configure_time_series_axes(dframe_data, time_format='%m-%d %H'):

    tick_time_period = list(dframe_data.index)
    tick_time_idx = dframe_data.index
    tick_time_labels = [tick_label.strftime(time_format) for tick_label in dframe_data.index]

    return tick_time_period, tick_time_idx, tick_time_labels
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to configure time-series lut
def configure_time_series_lut(ts_name_in, ts_fields=None):

    ts_name_out = None
    if ts_fields is not None:
        for ts_field_key_in, ts_field_key_out in ts_fields.items():
            if ts_name_in == ts_field_key_in:
                ts_name_out = ts_field_key_out
                break
    else:
        log_stream.error(' ===> Time-series fields must be defined in the configuration file')
        raise RuntimeError('Time-series fields not defined')

    if ts_name_out is None:
        log_stream.error(' ===> Time-series name "' + ts_name_in + '" is not defined in the time-series fields')
        raise RuntimeError('Time-series name not defined')

    return ts_name_out
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to configure time-series style
def configure_time_series_style(ts_name, obj_configuration=None):
        ts_configuration = None
        if obj_configuration is not None:
            if ts_name in list(obj_configuration.keys()):
                ts_configuration = obj_configuration[ts_name]
        return ts_configuration
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to configure time-series for heatmaps
def configure_time_series_heatmap(point_ts, point_legend=None):

    point_label_list, point_name_list, point_arr = [], [], None
    for point_name, point_data in point_ts.items():
        if point_arr is None:
            point_arr = point_data.values
        else:
            point_arr = np.vstack([point_arr, point_data.values])

        if point_legend is not None:
            point_label = configure_time_series_lut(point_name, point_legend)
        else:
            point_label = deepcopy(point_name)

        point_label_list.append(point_label)
        point_name_list.append(point_name)

    return point_arr, point_name_list, point_label_list
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to view time series
def view_time_series(file_name, ts_data, ts_registry, ts_name='',
                     min_ts_sm=0, max_ts_sm=1, min_ts_rain=0, max_ts_rain=None,
                     min_ts_level=0, max_ts_level=4,
                     fig_title='figure',
                     fig_label_axis_sm_x='time', fig_label_axis_sm_y='soil moisture [%]',
                     fig_label_axis_rain_x='time', fig_label_axis_rain_y='rain [mm]',
                     fig_label_axis_level_x='time', fig_label_axis_level_y='warning level [-]',
                     fig_label_axis_event_x='time', fig_label_axis_event_y='event [n]',
                     fig_legend=None, fig_style=None,
                     fig_cbar_sm='coolwarm', fig_cbar_rain='Blues', fig_cbar_level=None,
                     fig_cbar_kw={}, fig_dpi=120):

    # ------------------------------------------------------------------------------------------------------------------
    # figure registry
    registry_name, registry_idx, registry_basin = ts_registry['name'], ts_registry['index'], ts_registry['basin']
    registry_warn_thr, registry_warn_idx = ts_registry['warning_threshold'], ts_registry['warning_index']
    # get level colors and indexes
    registry_warn_colors, registry_warn_level = list(registry_warn_idx.keys()), list(registry_warn_idx.values())

    registry_warn_bounds = []
    registry_warn_bound_equal, registry_warn_bounds_limits = 'n = {:}','{:} <= n <= {:}'
    registry_warn_bounds_upper, registry_warn_bounds_lower = 'n >= {:}', 'n <= {:}'
    registry_warn_bound_idx = []
    for warn_thr_key, warn_thr_value in registry_warn_thr.items():
        warn_thr_value_min, warn_thr_value_max = warn_thr_value[0], warn_thr_value[1]

        registry_warn_bound_idx.append(warn_thr_value_min)

        registry_warn_str = ''
        if warn_thr_value_min is None or warn_thr_value_min == 9999:
            registry_warn_str = registry_warn_bounds_lower.format(str(warn_thr_value_max))
        if warn_thr_value_max is None or warn_thr_value_max == 9999:
            registry_warn_str = registry_warn_bounds_upper.format(str(warn_thr_value_min))
        if warn_thr_value_min is not None and warn_thr_value_max is not None:
            if warn_thr_value_min == warn_thr_value_max:
                #if warn_thr_value_min > 0:
                registry_warn_str = registry_warn_bound_equal.format(
                    str(warn_thr_value_min))
            else:
                registry_warn_str = registry_warn_bounds_limits.format(
                    str(warn_thr_value_min), str(warn_thr_value_max))

        registry_warn_bounds.append(registry_warn_str)

    # figure fields
    if 'time' in ts_data.index.name:
        time_period = ts_data.index
        time_stamp_start, time_stamp_end = time_period[0], time_period[-1]
        time_str_start, time_str_end = time_stamp_start.strftime('%Y-%m-%d'), time_stamp_end.strftime('%Y-%m-%d')
    else:
        log_stream.error(' ===> Time field not in the dataframe object. Time field must be included in dataframe')
        raise RuntimeError('Time field not in the dataframe object')

    # configure time-series axes
    [tick_time_period, tick_time_idx, tick_time_labels] = configure_time_series_axes(ts_data)

    # configure time-series data for rain accumulated
    ts_rain_acc, ts_rain_peak = ts_data[['rain_accumulated']], ts_data[['rain_peak']]
    arr_rain_acc, name_rain_acc, label_rain_acc = configure_time_series_heatmap(ts_rain_acc, fig_legend)
    style_rain_acc = configure_time_series_style('rain_accumulated', fig_style)
    label_rain_acc = configure_time_series_lut('rain_accumulated', fig_legend)
    if min_ts_rain is not None:
        arr_rain_acc[arr_rain_acc < min_ts_rain] = np.nan
    if max_ts_rain is not None:
        arr_rain_acc[arr_rain_acc > max_ts_rain] = np.nan

    # configure time-series data for rain peak
    arr_rain_peak, name_rain_peak, label_rain_peak = configure_time_series_heatmap(ts_rain_peak, fig_legend)
    style_rain_peak = configure_time_series_style('rain_peak', fig_style)
    label_rain_peak = configure_time_series_lut('rain_peak', fig_legend)
    if min_ts_rain is not None:
        arr_rain_peak[arr_rain_peak < min_ts_rain] = np.nan
    if max_ts_rain is not None:
        arr_rain_peak[arr_rain_peak > max_ts_rain] = np.nan

    ts_rain = ts_data[['rain_accumulated', 'rain_peak']]
    arr_rain, name_rain, label_rain = configure_time_series_heatmap(ts_rain, fig_legend)

    # configure time-series sm
    ts_sm = ts_data[['soil_moisture']]
    arr_sm, name_sm, label_sm = configure_time_series_heatmap(ts_sm, fig_legend)
    style_sm = configure_time_series_style('soil_moisture', fig_style)
    label_sm = configure_time_series_lut('soil_moisture', fig_legend)
    if min_ts_sm is not None:
        arr_sm[arr_sm < min_ts_sm] = np.nan
    if max_ts_sm is not None:
        arr_sm[arr_sm > max_ts_sm] = np.nan

    # configure time-series event(s)
    ts_event = ts_data[['soil_slips_observed_events', 'soil_slips_predicted_events']]
    ts_event[ts_event < 0] = -1
    arr_event, name_event, label_event = configure_time_series_heatmap(ts_event, fig_legend)
    style_event_obs = configure_time_series_style('soil_slips_observed_events', fig_style)
    style_event_pred = configure_time_series_style('soil_slips_predicted_events', fig_style)
    arr_event[arr_event < 0] = -1

    # configure time-series alert
    ts_level = ts_data[['soil_slips_observed_alert_level', 'soil_slips_predicted_alert_level']]
    ts_level[ts_level < 0] = -1
    arr_level, name_level, label_level = configure_time_series_heatmap(ts_level, fig_legend)
    style_level_obs = configure_time_series_style('soil_slips_observed_alert_level', fig_style)
    style_level_pred = configure_time_series_style('soil_slips_predicted_alert_level', fig_style)
    arr_level[arr_level < 0] = -1

    # compute rain min and max
    min_ts_rain, max_ts_rain = 0, 100
    tmp_rain_max = np.nanmax(arr_rain_acc)
    if tmp_rain_max > 100:
        max_ts_rain = tmp_rain_max

    # open figure
    fig = plt.figure(figsize=(14, 10))
    fig.autofmt_xdate()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # subplot 1 (rain series)
    ax1 = plt.subplot(4, 1, 1)
    ax1.set_xticklabels([])

    p1_1 = ax1.bar(np.arange(len(tick_time_labels)), list(ts_rain_acc.values[:, 0]),
                   color='#33A1C9', alpha=0.7, width=.35, linewidth=0.5,
                   align='center', label=label_rain_acc)

    p1_2 = ax1.plot(np.arange(len(tick_time_labels)), list(ts_rain_peak.values[:, 0]),
                    label=label_rain_peak, **style_rain_peak)

    ax1.set_xlim(0, len(tick_time_labels))
    ax1.set_xticks(np.arange(len(tick_time_labels)))
    ax1.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)
    ax1.set_ylim(min_ts_rain, max_ts_rain)
    ax1.set_ylabel(fig_label_axis_rain_y, color='#000000')

    ax1.set_xticks(np.arange(arr_rain.shape[1] + 1) - .5, minor=True)

    ax1.grid(b=True)

    ax8 = ax1.twinx()
    p8_1 = ax8.plot(np.arange(len(tick_time_labels)), list(ts_sm.values[:, 0]), label=label_sm, **style_sm)

    ax8.set_xticks(np.arange(len(tick_time_labels)))
    ax8.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)
    ax8.set_ylabel(fig_label_axis_sm_y, rotation=-90, va="bottom", color='#000000')
    ax8.set_ylim(min_ts_sm, max_ts_sm)

    ax8.set_xticks(np.arange(arr_rain.shape[1] + 1) - .5, minor=True)

    leg1 = ax1.legend((p1_1[0], p1_2[0], p8_1[0]), (label_rain_acc, label_rain_peak, label_sm), frameon=False, loc=2)
    ax1.add_artist(leg1)

    # set title
    fig_title = fig_title.format(alert_area_name=ts_name, time_start=time_str_start, time_end=time_str_end)
    fig_figure = ' == Rain and Soil Moisture Datasets == '
    fig_title = fig_title + '\n' + fig_figure
    ax1.set_title(fig_title, size=14, color='black', weight='bold')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # subplot 2 (rain heatmap)
    ax2 = plt.subplot(4, 1, 2)
    ax2.set_xticklabels([])

    # create heatmap
    image_norm = mpl.colors.Normalize(vmin=min_ts_rain, vmax=max_ts_rain)
    image_renderer = ax2.imshow(arr_rain, cmap=fig_cbar_rain, norm=image_norm)

    # show all ticks and label them with the respective list entries
    ax2.set_xlim(0, len(tick_time_labels))
    ax2.set_xticks(np.arange(len(tick_time_labels)))
    ax2.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)
    ax2.set_yticks(np.arange(len(label_rain)))
    ax2.set_yticklabels(label_rain)

    # create grid
    ax2.set_xticks(np.arange(arr_rain.shape[1] + 1) - .5, minor=True)
    ax2.set_yticks(np.arange(arr_rain.shape[0] + 1) - .5, minor=True)
    ax2.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax2.tick_params(which="minor", bottom=False, left=False)

    # create annotations
    for i in range(len(label_rain)):
        for j in range(len(tick_time_period)):
            text = ax2.text(
                j, i, arr_rain[i, j].round(1),
                ha="center", va="center", color="k", fontsize=6, fontweight='bold')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # subplot 3 (rain series)
    ax3 = plt.subplot(4, 1, 3)
    ax3.set_xticklabels([])

    p3_1 = ax3.bar(np.arange(len(tick_time_labels)), list(ts_event.values[:, 0]),
                   color='#008000', alpha=0.7, width=.35, edgecolor='#003666', linewidth=2,
                   align='center', fill=False, label=label_event[0])
    p3_2 = ax3.bar(np.arange(len(tick_time_labels)), list(ts_event.values[:, 1]),
                   color='#15B01A', alpha=0.7, width=.35, edgecolor='#0000FF', linewidth=2,
                   align='center', fill=False, label=label_event[1])

    ax3.set_xlim(0, len(tick_time_labels))
    ax3.set_xticks(np.arange(len(tick_time_labels)))
    ax3.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)
    ax3.set_ylim(0, 18)
    ax3.set_yticks(np.arange(0, 18, 2))
    ax3.set_ylabel(fig_label_axis_event_y, color='#000000')

    p3_vh = []
    for warn_color, warn_idx in zip(registry_warn_colors, registry_warn_bound_idx):
        p3_id = ax3.axhline(warn_idx, color=warn_color, linestyle='--', linewidth=2)
        p3_vh.append(p3_id)

    ax3.set_xticks(np.arange(arr_event.shape[1] + 1) - .5, minor=True)

    ax3.grid(b=True)

    '''
    ax9 = ax3.twinx()
    p9_1 = ax9.plot(np.arange(len(tick_time_labels)), list(ts_level.values[:, 0]),
                    label=label_level[0], **style_level_obs)
    p9_2 = ax9.plot(np.arange(len(tick_time_labels)), list(ts_level.values[:, 1]),
                    label=label_level[1], **style_level_pred)

    ax9.set_xticks(np.arange(len(tick_time_labels)))
    ax9.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)
    ax9.set_ylabel(fig_label_axis_level_y, rotation=-90, va="bottom", color='#000000')
    ax9.set_ylim(min_ts_level, max_ts_level)

    ax9.set_xticks(np.arange(arr_event.shape[1] + 1) - .5, minor=True)
    
    leg3 = ax3.legend((p3_1[0], p3_2[0], p9_1[0], p9_2[0]),
                      (label_event[0], label_event[1], label_level[0], label_level[1]), frameon=False, loc=2)
    '''
    leg3 = ax3.legend((p3_1[0], p3_2[0]),
                      (label_event[0], label_event[1]), frameon=False, loc=2)
    ax3.add_artist(leg3)

    # set title
    fig_figure = ' == Events and Warning Levels == '
    ax3.set_title(fig_figure, size=14, color='black', weight='bold')
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # subplot 4 (level heatmap)
    ax4 = plt.subplot(4, 1, 4)
    ax4.set_xticklabels([])

    registry_warn_location = np.arange(0, max(registry_warn_level),
                                       max(registry_warn_level) / float(len(registry_warn_colors)))
    fig_cbar_level = mpl.colors.ListedColormap(registry_warn_colors)

    # create heatmap
    image_norm = mpl.colors.Normalize(vmin=min_ts_level, vmax=max_ts_level)
    image_renderer = ax4.imshow(arr_level, cmap=fig_cbar_level, norm=image_norm)

    # show all ticks and label them with the respective list entries
    ax4.set_xlim(0, len(tick_time_labels))
    ax4.set_xticks(np.arange(len(tick_time_labels)))
    ax4.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)

    ax4.set_yticks(np.arange(len(label_level)))
    ax4.set_yticklabels(label_level)

    # create grid
    ax4.set_xticks(np.arange(arr_level.shape[1] + 1) - .5, minor=True)
    ax4.set_yticks(np.arange(arr_level.shape[0] + 1) - .5, minor=True)
    ax4.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax4.tick_params(which="minor", bottom=False, left=False)

    # create annotations
    for i in range(len(label_level)):
        for j in range(len(tick_time_period)):

            value = arr_level[i, j].round(0)
            if not np.isnan(value):
                value = int(value)

            text = ax4.text(
                j, i, value,
                ha="center", va="center", color="k", fontsize=6, fontweight='bold')

    # Create a legend with a color box
    patches_list = []
    for registry_color, registry_bound in zip(registry_warn_colors, registry_warn_bounds):
        patches_list.append(mpatches.Patch(
            facecolor=registry_color, label=registry_bound, alpha=1,
            linewidth=0.5, linestyle='solid', edgecolor='k'))
    patches_n = patches_list.__len__()

    plt.legend(handles=patches_list, framealpha=0.8,
               frameon=True, ncol=patches_n, loc=9, bbox_to_anchor=(0.5, -0.6))

    fig.tight_layout()

    # save figure
    file_path, file_folder = os.path.split(file_name)
    os.makedirs(file_path, exist_ok=True)
    fig.savefig(file_name, dpi=fig_dpi)

    # close figure
    # plt.show()
    plt.close()

# ----------------------------------------------------------------------------------------------------------------------
