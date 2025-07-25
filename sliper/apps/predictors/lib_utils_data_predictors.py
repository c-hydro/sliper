"""
Library Features:

Name:          lib_utils_data_predictors
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from typing import Optional

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to define analysis period
def define_analysis_period(
    time_start: Optional[pd.Timestamp] = None,
    time_end: Optional[pd.Timestamp] = None,
    time_period: Optional[int] = None,
    time_frequency: str = 'D',
    time_rounding: str = 'D',
    time_reference: Optional[pd.Timestamp] = None, **kwargs
) -> (pd.DatetimeIndex, None):
    """
    Define a period for analysis as a DatetimeIndex.

    Priority:
    1. If time_period and time_reference are provided:
       - Generate range ending at time_reference.
    2. Else if time_start and time_end are provided:
       - Generate range between start and end.
    3. If only one of time_start or time_end is given, raise an error.
    4. If no valid configuration, raise ValueError.

    Parameters
    ----------
    time_start : pd.Timestamp, optional
        Start of period.
    time_end : pd.Timestamp, optional
        End of period.
    time_period : int, optional
        Number of periods to generate.
    time_frequency : str, default 'D'
        Frequency string (pandas offset alias).
    time_rounding : str, default 'D'
        Rounding unit for time_reference.
    time_reference : pd.Timestamp, optional
        Reference time (used with time_period).

    Returns
    -------
    pd.DatetimeIndex
        Generated DatetimeIndex.

    Raises
    ------
    ValueError
        If no valid combination of inputs is provided.
    """

    # Round reference time if provided
    if time_reference is not None:
        time_reference = time_reference.round(time_rounding)

    # 1. Use time_reference and time_period
    if time_period is not None and time_reference is not None:
        return pd.date_range(end=time_reference, periods=time_period, freq=time_frequency)

    # 2. Use time_start and time_end (both required)
    if time_start is not None and time_end is not None:
        return pd.date_range(start=time_start, end=time_end, freq=time_frequency)

    # 3. Partial ranges with period
    if time_start is not None and time_period is not None:
        return pd.date_range(start=time_start, periods=time_period, freq=time_frequency)

    if time_end is not None and time_period is not None:
        return pd.date_range(end=time_end, periods=time_period, freq=time_frequency)

    if time_reference is not None and time_start is None and time_end is None:
        return None

# ----------------------------------------------------------------------------------------------------------------------