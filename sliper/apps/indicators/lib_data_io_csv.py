"""
Library Features:

Name:          lib_data_io_csv
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250618'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd

from datetime import datetime

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to write file csv
def write_file_csv(
        data,
        extra_fields=None,
        filename='output.csv',
        csv_sep=';',
        time_format='%Y-%m-%d %H:%M:%S',
        float_format='%.2f',
        orientation='rows'  # 'columns' or 'rows'
):
    """
    Accept dict or DataFrame, optionally flatten, add fields, and save to CSV.

    Args:
        data (dict or pd.DataFrame): Source data.
        extra_fields (dict): Extra columns or rows to add.
        filename (str): Output CSV file path.
        sep (str): Delimiter for flattening nested dict keys.
        csv_sep (str): CSV delimiter.
        time_format (str): Format string for datetime fields.
        float_format (str): Format string for floats.
        orientation (str): 'columns' (default) or 'rows'.
    """
    # Prepare DataFrame
    if isinstance(data, dict):
        df = pd.DataFrame([data])
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        raise ValueError("data must be a dict or pandas DataFrame")

    # Add extra fields
    if extra_fields:
        for key, value in extra_fields.items():
            if isinstance(value, (datetime, pd.Timestamp)):
                value = value.strftime(time_format)
            df[key] = value

    # Handle row orientation
    if orientation == 'rows':
        df = df.T  # transpose to write each key as a row

        # Apply float formatting manually
        def format_val(v):
            if isinstance(v, float):
                return float_format % v
            return v

        df = df.applymap(format_val)

        df.to_csv(filename, header=False, float_format=float_format, sep=csv_sep)
    else:
        df.to_csv(filename, index=False, float_format=float_format, sep=csv_sep)

# ----------------------------------------------------------------------------------------------------------------------
