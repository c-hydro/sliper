"""
Library Features:

Name:          lib_utils_data_predictors
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250730'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from collections import Counter, defaultdict
from typing import Optional

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to rename keys in a dictionary
def remap_data(data: dict, rename_map: dict) -> pd.DataFrame:
    """
    Rename keys in a dictionary based on rename_map.
    - If mapping value is None -> remove the key
    - Warn if old_key not found in data
    - If new_key duplicates occur, append index to make them unique
    """

    result = {}

    # 1. Warn for missing keys
    for old_key in rename_map:
        if old_key not in data:
            log_stream.warning(f" ===> Warning: Key '{old_key}' not found in data.")

    # 2. Detect duplicates in new_key values (ignoring None)
    new_keys = [v for v in rename_map.values() if v is not None]
    counts = Counter(new_keys)

    # Prepare a tracker to rename duplicates
    duplicate_count = defaultdict(int)

    def unique_new_key(base_key):
        """Generate a unique key by appending index if needed."""
        if counts[base_key] <= 1:
            return base_key
        # If multiple duplicates
        duplicate_count[base_key] += 1
        if duplicate_count[base_key] == 1:
            # First occurrence uses base_key
            return base_key
        else:
            # Next occurrences get suffix
            return f"{base_key}_{duplicate_count[base_key]-1}"

    # 3. Build the new dictionary
    for key, value in data.items():
        if key in rename_map:
            new_key = rename_map[key]
            if new_key is not None:
                final_key = unique_new_key(new_key)
                # Warn if there were duplicates
                if counts[new_key] > 1:
                    log_stream.warning(f" ===> Warning: Duplicate target name '{new_key}', "
                          f"renamed to '{final_key}'")
                result[final_key] = value
            # else skip (removal)
        else:
            result[key] = value

    # Convert to DataFrame
    data = pd.DataFrame(result)

    return data
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