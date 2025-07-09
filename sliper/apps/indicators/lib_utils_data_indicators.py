"""
Library Features:

Name:          lib_utils_data_indicators
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from typing import Dict, List, Union, Any

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert a period string (e.g., '3M', '2H', '1D') to seconds
def convert_to_seconds(period_str):
    unit_multipliers = {'M': 60, 'H': 3600, 'D': 86400}
    number = int(''.join(filter(str.isdigit, period_str)))
    unit = ''.join(filter(str.isalpha, period_str)).upper()

    if not number:
        log_stream.error(f' ===> Invalid period string: {period_str}. No number found.')
        raise ValueError(f'Invalid period string: {period_str}. No number found.')

    if not unit:
        log_stream.error(f' ===> Invalid period string: {period_str}. No unit found.')
        raise ValueError(f'Invalid period string: {period_str}. No unit found.')

    return number * unit_multipliers.get(unit, 0)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to define window by type
def search_window_by_type(
        group_search_type: Dict[Any, List[str]],
        group_search_period: Dict[Any, Union[List[Any], List[List[Any]]]]):

    type_periods = {"left": [], "right": []}
    type_window = {"left": [], "right": []}

    for (type_id, type_info), (period_id, period_info) in zip(group_search_type.items(), group_search_period.items()):
        types = list(map(str.lower, type_info))
        periods_group = period_info

        # Support positional mapping if both are lists
        for idx, t in enumerate(types):
            if t in type_periods:
                for period_step in periods_group:
                    if not isinstance(period_step, list):
                        period_step = [period_step]
                    if period_step not in type_periods[t]:
                        type_periods[t].extend(period_step)

    for type_name, type_values in type_periods.items():
        type_unique = list(set(type_values))  # Remove duplicates
        type_unique = sorted(type_unique, key=lambda x: int(x.rstrip('H')))
        type_window[type_name] = type_unique

    return type_window
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to define period by type
def search_period_by_type(
    group_search_type: Dict[Any, List[str]],
    group_search_period: Dict[Any, Union[List[Any], List[List[Any]]]]
) -> Dict[str, Union[Any, None]]:

    type_periods = {"left": [], "right": []}

    for (type_id, type_info), (period_id, period_info) in zip(group_search_type.items(), group_search_period.items()):

        types = list(map(str.lower, type_info))
        periods_group = period_info

        # Support positional mapping if both are lists
        for idx, t in enumerate(types):
            if t in type_periods:
                for period_step in periods_group:
                    if not isinstance(period_step, list):
                        period_step = [period_step]
                    if period_step not in type_periods[t]:
                        type_periods[t].extend(period_step)

        # Handle 'both' (applies all periods to both types)
        if "both" in types:
            flat = [p for sublist in periods_group for p in sublist] if isinstance(periods_group[0], list) else periods_group
            type_periods["left"].extend(flat)
            type_periods["right"].extend(flat)

    max_left = max(set(type_periods["left"]), key=convert_to_seconds) if type_periods["left"] else None
    max_right = max(set(type_periods["right"]), key=convert_to_seconds) if type_periods["right"] else None

    return {
        "max_search_period_left": max_left,
        "max_search_period_right": max_right
    }

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to define a time range around a reference time using maximum search periods and frequency
def search_period_by_time(
    time_run: pd.Timestamp,
    max_search_periods: Dict[str, Union[Any, None]],
    time_frequency: str = "1h"
) -> pd.DatetimeIndex:
    """
    Define a time range around a reference time using maximum search periods and frequency.

    Parameters:
        max_search_periods: Dictionary with 'max_search_period_left' and 'max_search_period_right'.
        time_run: Reference timestamp (usually current or event time).
        time_frequency: Frequency for time range (default: "1h", e.g., '30min', '1d').

    Returns:
        A pandas DatetimeIndex covering the range with the specified frequency.
    """
    # Convert to seconds, defaulting to 0 if missing
    max_left_period = max_search_periods.get("max_search_period_left", None)
    if max_left_period is not None:
        left_seconds = convert_to_seconds(max_left_period)
    else:
        left_seconds = 0
    max_right_period = max_search_periods.get("max_search_period_right", None)
    if max_right_period is not None:
        right_seconds = convert_to_seconds(max_right_period)
    else:
        right_seconds = 0

    start_time = time_run - pd.to_timedelta(left_seconds, unit='s')
    end_time = time_run + pd.to_timedelta(right_seconds, unit='s')

    return pd.date_range(start=start_time, end=end_time, freq=time_frequency)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to convert a period string (e.g., '3M', '2H', '1D') to seconds
def normalize_to_seconds(period_str):
    unit_multipliers = {'M': 60, 'H': 3600, 'D': 86400}
    number = int(''.join(filter(str.isdigit, period_str)))
    unit = ''.join(filter(str.isalpha, period_str)).upper()
    return number * unit_multipliers.get(unit, 0)
# ----------------------------------------------------------------------------------------------------------------------
