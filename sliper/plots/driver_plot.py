"""
Class Features

Name:          driver_plot
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250731'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import os
import numpy as np

from copy import deepcopy

import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.patches as mpatches

from lib_utils_plot import (configure_time_series_axes, configure_time_series_heatmap,
                            configure_time_series_style, configure_time_series_lut)

from lib_info_args import logger_name

# logging
warnings.filterwarnings('ignore')
logging.getLogger("matplotlib").setLevel(logging.WARNING)
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# class DriverPlot
class DriverPlot:
    def __init__(self, time_run, config_dict, dpi=120, fields=None):

        self.time_run = time_run

        self.title_template = config_dict.get('title', '')
        self.label_axis_x_soil_moisture = config_dict.get('label_axis_x_soil_moisture', '')
        self.label_axis_y_soil_moisture = config_dict.get('label_axis_y_soil_moisture', '')
        self.label_axis_x_rain = config_dict.get('label_axis_x_rain', '')
        self.label_axis_y_rain = config_dict.get('label_axis_y_rain', '')

        self.legend = config_dict.get('legend', {})
        self.style = config_dict.get('style', {})

        self.dpi = dpi

        self.fields = fields

        self.def_warn_thr = {
            'white_range': [0, 0], 'green_range': [1, 2],
            'yellow_range': [3, 5], 'orange_range': [6, 13], 'red_range': [14, None]}

        self.def_warn_id = {
            'white_id': 0, 'green_id': 1,
            'yellow_id': 2, 'orange_id': 3, 'red_id': 4}

        self.def_warn_rgb = {
            'white_rgb': {'rgb': (255, 255, 255), 'opacity': 0.5}, 'green_rgb': {'rgb': (0, 128, 0), 'opacity': 0.1},
            'yellow_rgb': {'rgb': (255, 255, 0), 'opacity': 0.1}, 'orange_rgb': {'rgb': (255, 165, 0), 'opacity': 0.1},
            'red_rgb': {'rgb': (255, 0, 0), 'opacity': 0.1}}

        # Default warning colors (string names)
        self.def_warn_color = {
            'white_color': 'w','green_color': 'g',
            'yellow_color': 'y', 'orange_color': 'orange','red_color': 'r'
        }

    def get_title(self, alert_area_name, time_start, time_end):
        return self.title_template.format(alert_area_name=alert_area_name,
                                          time_start=time_start,
                                          time_end=time_end)

    def get_axis_labels(self):
        return {
            'x_soil_moisture': self.label_axis_x_soil_moisture,
            'y_soil_moisture': self.label_axis_y_soil_moisture,
            'x_rain': self.label_axis_x_rain,
            'y_rain': self.label_axis_y_rain
        }

    def get_legends(self):
        return self.legend

    def get_styles(self):
        return self.style

    def use_default_warn_thr(self):
        return self.def_warn_thr
    def use_default_warn_id(self):
        return self.def_warn_id
    def use_default_warn_rgb(self):
        return self.def_warn_rgb
    def use_default_warn_colors(self):
        return self.def_warn_color

    @staticmethod
    def map_ts(ts_data_in, fields=None):

        ts_data_list = []
        if fields is not None:
            for ts_name_in, ts_name_out in fields.items():
                if ts_name_in != 'time':
                    if ts_name_in in ts_data_in.columns:
                        ts_data_list.append(ts_name_in)
                    else:
                        log_stream.warning(
                            f' ===> Field "{ts_name_in}" not in dataframe. Keeping standard name.')
        ts_data_list = sorted(ts_data_list)
        return deepcopy(ts_data_in[ts_data_list])

    def _prepare_ts(self, ts_data):
        """Internal helper: remap fields if configured"""
        if self.fields is not None:
            return self.map_ts(ts_data, self.fields)
        return ts_data

    # method to plot time series data
    def view_ts(self,
                file_name, ts_data,
                ts_name='NA', ts_index=0, ts_catchment='NA',
                warning_thresholds=None, warning_index=None, warning_rgb=None, warning_colors=None,
                min_ts_sm=0, max_ts_sm=1, min_ts_rain=0, max_ts_rain=None,
                min_ts_level=0, max_ts_level=4,
                fig_cbar_sm='coolwarm', fig_cbar_rain='Blues', fig_cbar_level=None,
                fig_cbar_kw={}):
        """
        Generate and save a multi-panel figure of rainfall, soil moisture, events, and alert levels.
        Uses the configuration from this DriverPlot instance.
        """

        if warning_thresholds is None:
            warning_thresholds = self.use_default_warn_thr()
        if warning_index is None:
            warning_index = self.use_default_warn_id()
        if warning_rgb is None:
            warning_rgb = self.use_default_warn_rgb()
        if warning_colors is None:
            warning_colors = self.use_default_warn_colors()

        # Apply remapping once at the start
        ts_data = self._prepare_ts(ts_data)

        # registry
        #registry_name, registry_idx, registry_basin = ts_registry['name'], ts_registry['index'], ts_registry[
        #    'catchment']
        ##registry_warn_thr, registry_warn_idx = ts_registry['warning_threshold'], ts_registry['warning_index']
        warn_color_key, warn_color_str = list(warning_colors.keys()), list(warning_colors.values())

        registry_warn_bounds = []
        registry_warn_bound_equal, registry_warn_bounds_limits = 'n = {:}', '{:} <= n <= {:}'
        registry_warn_bounds_upper, registry_warn_bounds_lower = 'n >= {:}', 'n <= {:}'
        registry_warn_bound_idx = []
        for warn_thr_key, warn_thr_value in warning_thresholds.items():
            warn_thr_value_min, warn_thr_value_max = warn_thr_value[0], warn_thr_value[1]

            registry_warn_bound_idx.append(warn_thr_value_min)

            registry_warn_str = ''
            if warn_thr_value_min is None or warn_thr_value_min == 9999:
                registry_warn_str = registry_warn_bounds_lower.format(str(warn_thr_value_max))
            if warn_thr_value_max is None or warn_thr_value_max == 9999:
                registry_warn_str = registry_warn_bounds_upper.format(str(warn_thr_value_min))
            if warn_thr_value_min is not None and warn_thr_value_max is not None:
                if warn_thr_value_min == warn_thr_value_max:
                    registry_warn_str = registry_warn_bound_equal.format(
                        str(warn_thr_value_min))
                else:
                    registry_warn_str = registry_warn_bounds_limits.format(
                        str(warn_thr_value_min), str(warn_thr_value_max))

            registry_warn_bounds.append(registry_warn_str)

        # time field check
        if 'time' in ts_data.index.name:
            time_period = ts_data.index
            time_stamp_start, time_stamp_end = time_period[0], time_period[-1]
            time_str_start, time_str_end = time_stamp_start.strftime('%Y-%m-%d'), time_stamp_end.strftime(
                '%Y-%m-%d')
        else:
            raise RuntimeError('Time field not in the dataframe object')

        # configure time-series axes
        [tick_time_period, tick_time_idx, tick_time_labels] = configure_time_series_axes(ts_data)

        # use labels and style from class config
        fig_label_axis_rain_y = self.label_axis_y_rain
        fig_label_axis_sm_y = self.label_axis_y_soil_moisture
        fig_legend = self.legend
        fig_style = self.style
        fig_title = self.get_title(ts_name, time_str_start, time_str_end)

        # prepare rain accumulated
        ts_rain_acc, ts_rain_peak = ts_data[['rain_accumulated']], ts_data[['rain_peak']]
        arr_rain_acc, _, label_rain_acc = configure_time_series_heatmap(ts_rain_acc, fig_legend)
        style_rain_acc = configure_time_series_style('rain_accumulated', fig_style)
        label_rain_acc = configure_time_series_lut('rain_accumulated', fig_legend)
        if min_ts_rain is not None:
            arr_rain_acc[arr_rain_acc < min_ts_rain] = np.nan
        if max_ts_rain is not None:
            arr_rain_acc[arr_rain_acc > max_ts_rain] = np.nan

        # rain peak
        arr_rain_peak, _, label_rain_peak = configure_time_series_heatmap(ts_rain_peak, fig_legend)
        style_rain_peak = configure_time_series_style('rain_peak', fig_style)
        label_rain_peak = configure_time_series_lut('rain_peak', fig_legend)
        if min_ts_rain is not None:
            arr_rain_peak[arr_rain_peak < min_ts_rain] = np.nan
        if max_ts_rain is not None:
            arr_rain_peak[arr_rain_peak > max_ts_rain] = np.nan

        ts_rain = ts_data[['rain_accumulated', 'rain_peak']]
        arr_rain, _, label_rain = configure_time_series_heatmap(ts_rain, fig_legend)

        # soil moisture
        ts_sm = ts_data[['sm']]
        arr_sm, _, label_sm = configure_time_series_heatmap(ts_sm, fig_legend)
        style_sm = configure_time_series_style('sm', fig_style)
        label_sm = configure_time_series_lut('sm', fig_legend)
        if min_ts_sm is not None:
            arr_sm[arr_sm < min_ts_sm] = np.nan
        if max_ts_sm is not None:
            arr_sm[arr_sm > max_ts_sm] = np.nan

        # events
        ts_event = ts_data[['slips_obs_events', 'slips_pred_events']]
        ts_event[ts_event < 0] = np.nan
        arr_event, _, label_event = configure_time_series_heatmap(ts_event, fig_legend)
        arr_event[arr_event < 0] = np.nan

        # alert levels
        ts_level = ts_data[['slips_obs_alert_level', 'slips_pred_alert_level']]
        ts_level[ts_level < 0] = np.nan
        arr_level, _, label_level = configure_time_series_heatmap(ts_level, fig_legend)
        arr_level[arr_level < 0] = np.nan

        # compute rain min and max
        min_ts_rain, max_ts_rain = 0, 100
        tmp_rain_max = np.nanmax(arr_rain_acc)
        if tmp_rain_max > 100:
            max_ts_rain = tmp_rain_max

        # create figure
        fig = plt.figure(figsize=(14, 10))
        fig.autofmt_xdate()

        # subplot 1: rain and soil moisture
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
        ax1.grid(True, which='major', linestyle='-', linewidth=0.5)

        ax8 = ax1.twinx()
        p8_1 = ax8.plot(np.arange(len(tick_time_labels)), list(ts_sm.values[:, 0]), label=label_sm, **style_sm)
        ax8.set_ylim(min_ts_sm, max_ts_sm)
        ax8.set_ylabel(fig_label_axis_sm_y, rotation=-90, va="bottom", color='#000000')
        ax8.set_xticks(np.arange(arr_rain.shape[1] + 1) - .5, minor=True)

        leg1 = ax1.legend((p1_1[0], p1_2[0], p8_1[0]),
                          (label_rain_acc, label_rain_peak, label_sm),
                          frameon=False, loc=2)
        ax1.add_artist(leg1)

        fig_figure = ' == Rain and Soil Moisture Datasets == '
        fig_title = fig_title + '\n' + fig_figure
        ax1.set_title(fig_title, size=14, color='black', weight='bold')

        # subplot 2: rain heatmap
        ax2 = plt.subplot(4, 1, 2)
        ax2.set_xticklabels([])
        image_norm = mpl.colors.Normalize(vmin=min_ts_rain, vmax=max_ts_rain)
        ax2.imshow(arr_rain, cmap=fig_cbar_rain, norm=image_norm)
        ax2.set_xlim(0, len(tick_time_labels))
        ax2.set_xticks(np.arange(len(tick_time_labels)))
        ax2.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)
        ax2.set_yticks(np.arange(len(label_rain)))
        ax2.set_yticklabels(label_rain)
        ax2.set_xticks(np.arange(arr_rain.shape[1] + 1) - .5, minor=True)
        ax2.set_yticks(np.arange(arr_rain.shape[0] + 1) - .5, minor=True)
        ax2.grid(which="minor", color="w", linestyle='-', linewidth=3)
        ax2.tick_params(which="minor", bottom=False, left=False)
        for i in range(len(label_rain)):
            for j in range(len(tick_time_period)):
                val = arr_rain[i, j]
                if not np.isnan(val):
                    ax2.text(j, i, round(val, 1),
                             ha="center", va="center", color="k", fontsize=6, fontweight='bold')

        # subplot 3: events
        ax3 = plt.subplot(4, 1, 3)
        ax3.set_xticklabels([])
        ax3.bar(np.arange(len(tick_time_labels)), list(ts_event.values[:, 0]),
                color='#008000', alpha=0.7, width=.35, edgecolor='#003666', linewidth=2,
                align='center', fill=False, label=label_event[0])
        ax3.bar(np.arange(len(tick_time_labels)), list(ts_event.values[:, 1]),
                color='#15B01A', alpha=0.7, width=.35, edgecolor='#0000FF', linewidth=2,
                align='center', fill=False, label=label_event[1])
        ax3.set_ylim(0, 18)
        ax3.set_ylabel('event [n]', color='#000000')
        for warn_color, warn_idx in zip(warn_color_str, registry_warn_bound_idx):
            ax3.axhline(warn_idx, color=warn_color, linestyle='--', linewidth=2)

        ax3.set_xlim(0, len(tick_time_labels))
        ax3.set_xticks(np.arange(len(tick_time_labels)))
        ax3.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)

        ax3.legend(frameon=False, loc=2)

        ax3.set_xticks(np.arange(arr_rain.shape[1] + 1) - .5, minor=True)
        ax3.grid(True, which='major', linestyle='-', linewidth=0.5)
        ax3.set_title(' == Events and Warning Levels == ', size=14, color='black', weight='bold')

        # subplot 4: alert level heatmap
        ax4 = plt.subplot(4, 1, 4)
        ax4.set_xticklabels([])
        fig_cbar_level = mpl.colors.ListedColormap(warn_color_str)
        image_norm = mpl.colors.Normalize(vmin=min_ts_level, vmax=max_ts_level)
        ax4.imshow(arr_level, cmap=fig_cbar_level, norm=image_norm)
        ax4.set_xlim(0, len(tick_time_labels))
        ax4.set_xticks(np.arange(len(tick_time_labels)))
        ax4.set_xticklabels(tick_time_labels, rotation=90, fontsize=6)
        ax4.set_yticks(np.arange(len(label_level)))
        ax4.set_yticklabels(label_level)
        ax4.set_xticks(np.arange(arr_level.shape[1] + 1) - .5, minor=True)
        ax4.set_yticks(np.arange(arr_level.shape[0] + 1) - .5, minor=True)
        ax4.grid(which="minor", color="w", linestyle='-', linewidth=3)
        ax4.tick_params(which="minor", bottom=False, left=False)
        for i in range(len(label_level)):
            for j in range(len(tick_time_period)):
                val = arr_level[i, j]
                if not np.isnan(val):
                    ax4.text(j, i, int(val),
                             ha="center", va="center", color="k", fontsize=6, fontweight='bold')

        patches_list = [mpatches.Patch(facecolor=c, label=b, alpha=1, edgecolor='k')
                        for c, b in zip(warn_color_str, registry_warn_bounds)]
        plt.legend(handles=patches_list, framealpha=0.8, frameon=True,
                   ncol=len(patches_list), loc=9, bbox_to_anchor=(0.5, -0.6))

        fig.tight_layout()
        file_path, _ = os.path.split(file_name)
        os.makedirs(file_path, exist_ok=True)
        fig.savefig(file_name, dpi=self.dpi)
        plt.close()

# ----------------------------------------------------------------------------------------------------------------------
