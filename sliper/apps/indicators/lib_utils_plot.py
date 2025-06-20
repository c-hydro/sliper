"""
Library Features:

Name:          lib_utils_plot
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

#######################################################################################
# Libraries
import logging
import numpy as np
import pandas as pd

from lib_info_args import logger_name_scenarios as logger_name

import matplotlib.pylab as plt
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to plot rain against soil moisture values
def plot_scenarios_rain2sm(file_data, file_path,
                           var_x_name='soil_moisture', var_y_name='rain',
                           var_z_name='event_index', var_time_name='time',
                           var_x_limits=None, var_y_limits=None, var_z_limits=None,
                           event_n_min=0, event_n_max=None, event_label=True, season_label='NA',
                           figure_dpi=120, extra_args=None,
                           axes_x_template='soil moisture {:} [-]',
                           axes_y_template='rain accumulated {:} [mm]'):

    # Default argument(s)
    if var_x_limits is None:
        var_x_limits = [0, 1]
    if var_z_limits is None:
        var_z_limits = [0, 4]

    # Get datasets
    if var_time_name == file_data.index.name:
        var_time = file_data.index
    else:
        var_time = list(file_data[var_time_name].values)
    var_data_x = file_data[var_x_name].values
    var_data_z = file_data[var_z_name].values

    if var_time.__len__() > 0:

        var_time_from_ref = pd.Timestamp(var_time[-1]).strftime('%Y-%m-%d')
        var_time_to_ref = pd.Timestamp(var_time[0]).strftime('%Y-%m-%d')

        var_p95_x = np.percentile(var_data_x, 95)
        var_p99_x = np.percentile(var_data_x, 99)

        var_p95_str = '{0:.2f}'.format(var_p95_x)
        var_p99_str = '{0:.2f}'.format(var_p99_x)

        if 'rain_type' in list(extra_args.keys()):
            rain_type = extra_args['rain_type']
        else:
            log_stream.error(' ===> Rain Type is not defined in settings')
            raise IOError('Variable is not correctly defined')

        if 'soil_moisture_type' in list(extra_args.keys()):
            sm_type = extra_args['soil_moisture_type']
        else:
            log_stream.error(' ===> SoilMoisture Type is not defined in settings')
            raise IOError('Variable is not correctly defined')

        for rain_type_step in rain_type:

            if ('var_rain' in file_path) and ('var_sm' not in file_path):
                file_path_step = file_path.replace('var_rain', ':').format(rain_type_step)
            elif ('var_rain' in file_path) and ('var_sm' in file_path):
                file_path_step = file_path.replace('var_rain', ':')
                file_path_step = file_path_step.replace('var_sm', ':')
                file_path_step = file_path_step.format(rain_type_step, sm_type)
            else:
                log_stream.error(' ===> File path filling failed')
                raise NotImplementedError('Case not implemented yet')

            var_y_step = var_y_name.format(rain_type_step)
            var_data_y = file_data[var_y_step].values

            axis_y_step = axes_y_template.format(rain_type_step)
            axis_x_step = axes_x_template.format(sm_type)

            # Open figure
            fig = plt.figure(figsize=(17, 11))
            fig.autofmt_xdate()

            axes = plt.axes()
            axes.autoscale(True)

            p95 = axes.axvline(var_p95_x, color='#FFA500', linestyle='-', lw=2, label='95%')
            plt.text(var_p95_x, -0.02, var_p95_str, transform=axes.get_xaxis_transform(), ha='center', va='center')
            p99 = axes.axvline(var_p99_x, color='#FF0000', linestyle='-', lw=2, label='99%')
            plt.text(var_p99_x, -0.02, var_p99_str, transform=axes.get_xaxis_transform(), ha='center', va='center')

            colors = {0: 'grey', 1: 'green', 2: 'yellow', 3: 'orange', 4: 'red'}
            for t, x, y, z in zip(var_time, var_data_x, var_data_y, var_data_z):

                t = pd.Timestamp(t)

                if y >= 0:
                    if (np.isfinite(z)) and (z >= 0):
                        color = colors[z]
                    else:
                        color = 'k'
                    p1 = axes.scatter(x, y, alpha=1, color=color, s=20)

                    if event_label:
                        if z > event_n_min:
                            label = t.strftime('%Y-%m-%d')
                            plt.annotate(label,  # this is the text
                                         (x, y),  # this is the point to label
                                         textcoords="offset points",  # how to position the text
                                         xytext=(0, 5),  # distance from text to points (x,y)
                                         ha='center')  # horizontal alignment can be left, right or center
                else:
                    log_stream.warning(' ===> Value of y is negative (' + str(y) + ') at time ' + str(t))

            axes.set_xlabel(axis_x_step, color='#000000', fontsize=14, fontdict=dict(weight='medium'))
            axes.set_xlim(var_x_limits[0], var_x_limits[1] )
            axes.set_ylabel(axis_y_step, color='#000000', fontsize=14, fontdict=dict(weight='medium'))
            if var_y_limits is not None:
                axes.set_ylim(var_y_limits[0], var_y_limits[1])

            xticks_list = axes.get_xticks().tolist()
            xticks_list.insert(0, -0.01)
            xticks_list.insert(len(xticks_list), 1.01)
            axes.set_xticks(xticks_list)
            axes.set_xticklabels(['', '0.0', '0.2', '0.4', '0.6', '0.8', '1.0', ''], fontsize=12)

            legend = axes.legend((p95, p99), ('95%', '99%'), frameon=True, ncol=3, loc=9)
            axes.add_artist(legend)

            axes.grid(b=False, color='grey', linestyle='-', linewidth=0.5, alpha=1)

            axes.set_title(' #### Scenarios - Rain and Soil Moisture #### \n ' +
                           'TimePeriod :: ' + var_time_from_ref + ' - ' + var_time_to_ref + ' Season:: ' + season_label,
                           fontdict=dict(fontsize=16, fontweight='bold'))

            # plt.show()

            fig.savefig(file_path_step, dpi=figure_dpi)
            plt.close()

    else:
        log_stream.warning(' ===> Events are undefined for season ' + season_label)

# -------------------------------------------------------------------------------------
