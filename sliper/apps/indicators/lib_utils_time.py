"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

# -------------------------------------------------------------------------------------
# Libraries
import logging
import numpy as np
import re
import pandas as pd

from copy import deepcopy
from datetime import date

from lib_info_args import logger_name_scenarios
from lib_info_args import logger_name_predictors
from lib_utils_logging import LogDecorator

from lib_utils_system import get_dict_nested_value

# Logging
log_stream = logging.getLogger()
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to decorate the logger_name of the time fx
@LogDecorator(logger_name=logger_name_scenarios)
def set_time_scenarios(**kwargs):
    return set_time(**kwargs)


@LogDecorator(logger_name=logger_name_predictors)
def set_time_predictors(**kwargs):
    return set_time(**kwargs)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to set time run
def set_time(time_run_args=None, time_run_file=None, time_format='%Y-%m-%d %H:$M',
             time_run_file_start=None, time_run_file_end=None,
             time_period=1, time_frequency='H', time_rounding='H', time_reverse=True):

    log_stream.info(' ----> Set time period ... ')
    if (time_run_file_start is None) and (time_run_file_end is None):

        log_stream.info(' -----> Time info defined by "time_run" argument ... ')

        if time_run_args is not None:
            time_tmp = time_run_args
            log_stream.info(' ------> Time ' + time_tmp + ' set by argument')
        elif (time_run_args is None) and (time_run_file is not None):
            time_tmp = time_run_file
            logging.info(' ------> Time ' + time_tmp + ' set by user')
        elif (time_run_args is None) and (time_run_file is None):
            time_now = date.today()
            time_tmp = time_now.strftime(time_format)
            log_stream.info(' ------> Time ' + time_tmp + ' set by system')
        else:
            log_stream.info(' ----> Set time period ... FAILED')
            log_stream.error(' ===> Argument "time_run" is not correctly set')
            raise IOError('Time type or format is wrong')

        time_run = pd.Timestamp(time_tmp)
        time_run = time_run.floor(time_rounding)

        if time_period > 0:
            time_range = pd.date_range(end=time_run, periods=time_period, freq=time_frequency)
        else:
            log_stream.warning(' ===> TimePeriod must be greater then 0. TimePeriod is set automatically to 1')
            time_range = pd.DatetimeIndex([time_now], freq=time_frequency)

        logging.info(' -----> Time info defined by "time_run" argument ... DONE')

    elif (time_run_file_start is not None) and (time_run_file_end is not None):

        log_stream.info(' -----> Time info defined by "time_start" and "time_end" arguments ... ')

        time_run_file_start = pd.Timestamp(time_run_file_start)
        time_run_file_start = time_run_file_start.floor(time_rounding)
        time_run_file_end = pd.Timestamp(time_run_file_end)
        time_run_file_end = time_run_file_end.floor(time_rounding)

        if time_run_file_start > time_run_file_end:
            log_stream.error(' ===> Variable "time_start" is greater than "time_end". Check your settings file.')
            raise RuntimeError('Time_Range is not correctly defined.')

        if time_run_args is not None:
            time_tmp = deepcopy(time_run_args)
            log_stream.info(' ------> Time ' + time_tmp + ' set by argument')
        elif (time_run_args is None) and (time_run_file is not None):
            time_tmp = deepcopy(time_run_file)
            logging.info(' ------> Time ' + time_tmp + ' set by user')
        elif (time_run_args is None) and (time_run_file is None):
            time_now = date.today()
            time_tmp = time_now.strftime(time_format)
            log_stream.info(' ------> Time ' + time_tmp + ' set by system')
        else:
            log_stream.info(' ----> Set time period ... FAILED')
            log_stream.error(' ===> Argument "time_run" is not correctly set')
            raise IOError('Time type or format is wrong')

        time_run = pd.Timestamp(time_tmp)
        time_run = time_run.floor(time_rounding)
        time_range = pd.date_range(start=time_run_file_start, end=time_run_file_end, freq=time_frequency)

        log_stream.info(' -----> Time info defined by "time_start" and "time_end" arguments ... DONE')

    else:
        log_stream.info(' ----> Set time period ... FAILED')
        log_stream.error(' ===> Arguments "time_start" and/or "time_end" is/are not correctly set')
        raise IOError('Time type or format is wrong')

    if time_reverse:
        time_range = time_range[::-1]

    log_stream.info(' ----> Set time period ... DONE')

    return [time_run, time_range]

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to verify time window
def verify_time_window(time_reference, time_period, time_step=1):

    if not isinstance(time_period, pd.DatetimeIndex):
        time_period = pd.DatetimeIndex(time_period)

    if time_reference in time_period:
        idx_start = time_period.get_loc(time_reference)
    else:
        idx_start = None

    idx_end = None
    if idx_start is not None:
        idx_end = idx_start + time_step - 1

    flag_temporal_window = False
    if (idx_start is not None) and (idx_end is not None):
        if time_period.shape[0] > idx_end:
            flag_temporal_window = True

    if not flag_temporal_window:
        log_stream.warning(' ===> Temporal window do not include all the needed step for computing variable')
        log_stream.warning(' ===> Due to this issue of the time period, the variable will be not plotted.')

    return flag_temporal_window

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to split time window
def split_time_window(time_window):

    if not isinstance(time_window, list):
        time_window = [time_window]

    time_period, time_frequency = [], []
    for tmp_window in time_window:
        tmp_period = re.findall(r'\d+', tmp_window)
        if tmp_period.__len__() > 0:
            tmp_period = int(tmp_period[0])
        else:
            tmp_period = 1
        tmp_frequency = re.findall("[a-zA-Z]+", tmp_window)[0]

        time_period.append(tmp_period)
        time_frequency.append(tmp_frequency)

    if len(time_period) == 1:
        time_period = time_period[0]
        time_frequency = time_frequency[0]

    return time_period, time_frequency
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to find time maximum delta
def find_time_maximum_delta(time_delta_list):
    delta_string_max = None
    delta_seconds_max = 0
    for delta_string_step in time_delta_list:
        delta_seconds_step = pd.to_timedelta(delta_string_step).total_seconds()
        if delta_seconds_step > delta_seconds_max:
            delta_seconds_max = delta_seconds_step
            delta_string_max = delta_string_step

    return delta_string_max
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to search time features
def search_time_features(data_structure, data_key='rain_datasets', data_search_type='max'):

    search_period_selected, search_frequency_selected, search_type_selected = None, None, None
    if (data_key is not None) and (isinstance(data_key, str)):

        type_check_steps, type_length_steps = [], []
        period_check_steps = []
        for group_name, group_fields in data_structure.items():
            type_tmp = get_dict_nested_value(group_fields, [data_key, "search_type"])
            period_tmp = get_dict_nested_value(group_fields, [data_key, "search_period"])
            type_length_steps.append(len(type_tmp))
            type_check_steps.append(type_tmp)
            period_check_steps.append(period_tmp)
        type_length_arr = np.asarray(type_length_steps)
        max_length_idx = np.argmax(type_length_arr)
        type_length_max = type_length_arr[max_length_idx]

        # case 1: ['12H', '3H', '24H', '6H'], ['left']
        if type_length_max == 1:

            search_period_list, search_type_list = [], []
            for group_name, group_fields in data_structure.items():
                period_tmp = get_dict_nested_value(group_fields, [data_key, "search_period"])
                type_tmp = get_dict_nested_value(group_fields, [data_key, "search_type"])
                search_period_list.extend(period_tmp)
                search_type_list.extend(type_tmp)
            search_period_list = list(set(search_period_list))
            search_type_list = list(set(search_type_list))

        # case 2: ['12H', '3H'], ['left', 'right'] --> check if all elements are equal defined by the same objects
        elif type_length_max == 2:

            if not all(type_length_arr) == 2:
                log_stream.warning(' ===> Obj "search_type_list" is not defined by all elements equal to 2.')

            type_length_idx = np.argwhere(np.asarray(type_length_steps) == 2)[:, 0]
            type_select_steps = [type_check_steps[tmp_idx] for tmp_idx in type_length_idx]
            period_select_steps = [period_check_steps[tmp_idx] for tmp_idx in type_length_idx]

            period_left_step, period_right_step = [], []
            type_left_step, type_right_step = [], []
            for type_select_tmp, period_select_tmp in zip(type_select_steps, period_select_steps):

                if type_select_tmp[0] == 'left':
                    type_left_step.append(type_select_tmp[0])
                    period_left_step.append(period_select_tmp[0])
                elif type_select_tmp[0] == 'right':
                    log_stream.warning(
                        ' ===> Obj "type" right position is in a wrong position. '
                        'Check the settings file, By default the left position will be swapped for type and period')
                    type_left_step.append(type_select_tmp[1])
                    period_left_step.append(period_select_tmp[1])
                if type_select_tmp[1] == 'right':
                    type_right_step.append(type_select_tmp[1])
                    period_right_step.append(period_select_tmp[1])
                elif type_select_tmp[1] == 'left':
                    log_stream.warning(
                        ' ===> Obj "type" left position is in a wrong position. '
                        'Check the settings file, By default the right position will be swapped for type and period')
                    type_right_step.append(type_select_tmp[0])
                    period_right_step.append(period_select_tmp[0])

            period_left_max, period_right_max = None, None
            for period_left_tmp, period_right_tmp in zip(period_left_step, period_right_step):
                if period_left_max is None:
                    period_left_max = period_left_tmp
                else:
                    if pd.to_timedelta(period_left_tmp).total_seconds() > pd.to_timedelta(period_left_max).total_seconds():
                        period_left_max = period_left_tmp
                if period_right_max is None:
                    period_right_max = period_right_tmp
                else:
                    if pd.to_timedelta(period_right_tmp).total_seconds() > pd.to_timedelta(period_right_max).total_seconds():
                        period_right_max = period_right_tmp

            period_select_defined = [period_left_max, period_right_max]
            for period_id_steps, period_tmp_steps in enumerate(period_check_steps):
                if period_tmp_steps != period_select_defined:
                    log_stream.warning(' ===> Obj "period" should be have the same elements. Check the settings file')
                    log_stream.warning(' Select element is "' + str(period_select_defined) +
                                       '" and not "' + str(period_tmp_steps) +
                                       '". Update the settings file using the same period. '
                                       'The selected period will be used as default')

                    period_check_steps[period_id_steps] = period_select_defined

            type_select_defined = [type_left_step[0], type_right_step[0]]
            for type_id_steps, type_tmp_steps in enumerate(type_check_steps):
                if type_tmp_steps != type_select_defined:
                    log_stream.warning(' ===> Obj "type" should be have the same elements. Check the settings file')
                    log_stream.warning(' Select element is "' + str(type_select_defined) +
                                       '" and not "' + str(type_tmp_steps) +
                                       '". Update the settings file using the same type. '
                                       'The selected type will be used as default')

                    type_check_steps[type_id_steps] = type_select_defined

            search_type_list = type_check_steps[0]
            search_period_list = period_check_steps[0]
        else:
            log_stream.error(' ===> Obj "search_type_list" is not defined by 1 or 2 elements.')
            raise NotImplementedError('Case not implemented yet')

        if data_search_type == 'max':
            if len(search_type_list) == 1:  # 24, H, left
                search_delta_selected = find_time_maximum_delta(search_period_list)
                search_period_selected, search_frequency_selected = split_time_window(search_delta_selected)
                search_type_selected = search_type_list[0]
            elif len(search_type_list) == 2:
                search_delta_selected = deepcopy(search_period_list)
                search_period_selected, search_frequency_selected = split_time_window(search_delta_selected)
                search_type_selected = deepcopy(search_type_list)
            else:
                log_stream.error(' ===> Obj "search_type_list" is not defined by 1 or 2 elements.')
                raise NotImplementedError('Case not implemented yet')

        else:
            log_stream.error(' ===> Search type "' + data_search_type + '" is not supported')
            raise NotImplementedError('Case not implemented yet')
    else:
        log_stream.warning(' ===> Search time features are defined by NoneType ')

    return search_period_selected, search_frequency_selected, search_type_selected
# -------------------------------------------------------------------------------------
