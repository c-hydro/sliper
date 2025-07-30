"""
Library Features:

Name:          lib_utils_data
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

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to merge two dataframes with column checks
def merge_data(df1, df2):
    """
    Merges two dataframes along rows (vertically), checking if either of the dataframes
    is None and ensuring that columns match before merging.

    Parameters:
    - df1 (pd.DataFrame or None): First dataframe to merge
    - df2 (pd.DataFrame or None): Second dataframe to merge

    Returns:
    - pd.DataFrame: Merged dataframe, or the non-None dataframe if one is None.
    """
    # Handle cases where one of the dataframes is None
    if df1 is None and df2 is None:
        print("Both dataframes are None. Returning None.")
        return None
    elif df1 is None:
        print("df1 is None. Returning df2.")
        return df2
    elif df2 is None:
        print("df2 is None. Returning df1.")
        return df1

    # Check if both dataframes have the same columns
    if not all(df1.columns == df2.columns):
        print(f"Warning: The columns of df1 and df2 do not match.")
        print(f"df1 columns: {df1.columns}")
        print(f"df2 columns: {df2.columns}")
        return None  # Or choose to handle it differently, e.g., raise an exception.

    # If columns match, concatenate the dataframes along rows
    merged_df = pd.concat([df1, df2], axis=0, ignore_index=True)

    return merged_df
# ----------------------------------------------------------------------------------------------------------------------
