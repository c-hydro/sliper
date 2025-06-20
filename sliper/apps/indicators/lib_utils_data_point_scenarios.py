
"""
Library Features:

Name:          lib_utils_data_point_scenarios
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

# -------------------------------------------------------------------------------------
# Library
import logging
import pandas as pd
from copy import deepcopy
from lib_info_args import logger_name_predictors as logger_name_predictors
from lib_info_args import logger_name_scenarios as logger_name_scenarios

# Logging
log_stream_predictors = logging.getLogger(logger_name_predictors)
log_stream_scenarios = logging.getLogger(logger_name_scenarios)

# Debug
import matplotlib.pylab as plt
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to filter scenarios dataframe
def filter_scenarios_dataframe(df_scenarios,
                               tag_column_rain='rain_accumulated_3H', tag_time='time', filter_rain=True,
                               value_min_rain=0, value_max_rain=None,
                               tag_column_sm='sm_max', filter_sm=True,
                               value_min_sm=0, value_max_sm=1,
                               tag_column_event='event_n', filter_event=True,
                               value_min_event=1, value_max_event=None,
                               tag_column_season='seasons', filter_season=True,
                               season_lut=None, season_name='ALL'):

    dframe_scenarios = deepcopy(df_scenarios)

    if not isinstance(tag_column_rain, list):
        tag_column_rain = [tag_column_rain]
    if not isinstance(tag_column_sm, list):
        tag_column_sm = [tag_column_sm]

    if filter_season:
        if season_lut is not None:

            if tag_time == dframe_scenarios.index.name:
                dframe_time_index = dframe_scenarios.index
            else:
                # dframe_scenarios[tag_time].values
                log_stream_scenarios.error(' ===> Time index "' + tag_time +
                                           '" is not defined in the scenario dataframe')
                raise RuntimeError('Time index must be defined to get the seasonal events')

            grp_season = [season_lut.get(pd.Timestamp(t_stamp).month) for t_stamp in dframe_time_index]
            dframe_scenarios[tag_column_season] = grp_season
        else:
            dframe_scenarios[tag_column_season] = 'ALL'
    else:
        dframe_scenarios[tag_column_season] = 'ALL'

    # Filter by rain not valid values
    if filter_rain:
        for tag_column_step in tag_column_rain:
            logging.info(' -------> Filter variable ' + tag_column_step + ' ... ')
            if tag_column_step in list(dframe_scenarios.columns):
                if value_min_rain is not None:
                    dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_step] < value_min_rain].index)
                if value_max_rain is not None:
                    dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_step] > value_max_rain].index)
                log_stream_scenarios.info(' -------> Filter variable ' + tag_column_step + ' ... DONE')
            else:
                log_stream_scenarios.info(' -------> Filter variable ' + tag_column_step + ' ... FAILED')
                log_stream_scenarios.warning(
                    ' ===> Filter rain datasets failed. Variable ' + tag_column_step +
                    ' is not in the selected dataframe')

    # Filter by soil moisture not valid values
    if filter_sm:
        for tag_column_step in tag_column_sm:
            log_stream_scenarios.info(' -------> Filter variable ' + tag_column_step + ' ... ')
            if tag_column_step in list(dframe_scenarios.columns):
                if value_min_sm is not None:
                    dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_step] < value_min_sm].index)
                if value_max_sm is not None:
                    dframe_scenarios = dframe_scenarios.drop(dframe_scenarios[dframe_scenarios[tag_column_step] > value_max_sm].index)
                log_stream_scenarios.info(' -------> Filter variable ' + tag_column_step + ' ... DONE')
            else:
                log_stream_scenarios.info(' -------> Filter variable ' + tag_column_step + ' ... FAILED')
                log_stream_scenarios.warning(
                    ' ===> Filter soil moisture datasets failed. Variable ' + tag_column_step +
                    ' is not in the selected dataframe')

    if dframe_scenarios.empty:
        log_stream_scenarios.warning(' ===> Scenarios dataframe is empty.')

    # Filter by event n
    if not dframe_scenarios.empty:
        if filter_event:
            if value_min_event is not None:
                dframe_scenarios = dframe_scenarios.drop(
                    dframe_scenarios[
                        (dframe_scenarios[tag_column_event] < value_min_event) &
                        (dframe_scenarios[tag_column_event] != -9999)].index)
            if value_max_event is not None:
                dframe_scenarios = dframe_scenarios.drop(
                    dframe_scenarios[
                        (dframe_scenarios[tag_column_event] > value_max_event) &
                        (dframe_scenarios[tag_column_event] != -9999)].index)
            if dframe_scenarios.empty:
                log_stream_scenarios.warning(
                    ' ===> Scenarios dataframe filtered by the events limits is returned empty.')

    # Filter by season name
    if not dframe_scenarios.empty:
        if filter_season:
            dframe_scenarios = dframe_scenarios.loc[dframe_scenarios[tag_column_season] == season_name]
            if dframe_scenarios.empty:
                log_stream_scenarios.warning(
                    ' ===> Scenarios dataframe filtered by the seasonal period is returned empty.')

    return dframe_scenarios
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to filter dataframe time range
def filter_dataframe_time_range(file_dframe, file_time_range=None):
    if file_time_range is not None:
        file_time_start, file_time_end = file_time_range[0], file_time_range[-1]
        file_dframe_selected = file_dframe[(file_dframe.index >= file_time_start.strftime('%Y-%m-%d')) &
                                           (file_dframe.index <= file_time_end.strftime('%Y-%m-%d'))]
    else:
        file_dframe_selected = deepcopy(file_dframe)
    return file_dframe_selected
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read scenarios file
def read_file_scenarios(file_name, file_sep=',', file_header=None, file_skiprows=0,
                        file_time_range=None, file_column_index='time'):

    file_data = pd.read_table(file_name, sep=file_sep, names=file_header, skiprows=file_skiprows)

    if file_column_index in list(file_data.columns):
        file_data.set_index(file_column_index, inplace=True)
    else:
        log_stream_predictors.error(' ===> Column "' + file_column_index + '" is not available in the DataFrame')
        raise RuntimeError('Column "' + file_column_index + '" must be available for avoiding error in the algorithm')

    file_data = filter_dataframe_time_range(file_data, file_time_range=file_time_range)

    return file_data

# -------------------------------------------------------------------------------------
