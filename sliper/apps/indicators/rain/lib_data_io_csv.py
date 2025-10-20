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
# method to read file csv
def read_file_csv(file_path: str, key_column: str, delimiter: str = ',', main_key: str = None) -> dict:
    """
    Reads a CSV file and returns a dictionary with nested or flat structure.

    Parameters:
        file_path (str): Path to the CSV file.
        key_column (str): Column to use as the reference (top-level) key.
        delimiter (str): Delimiter used in the CSV file.
        main_key (str, optional): If provided, all subkeys will be grouped under this key.

    Returns:
        dict: A dictionary representation of the CSV with either flat or nested subkeys.
    """
    df = pd.read_csv(file_path, delimiter=delimiter)

    if key_column not in df.columns:
        raise ValueError(f"Key column '{key_column}' not found in the CSV headers.")

    result = {}
    for _, row in df.iterrows():
        key = row[key_column]
        row_dict = row.to_dict()  # Keep key_column in the dict
        result[key] = {main_key: row_dict} if main_key else row_dict

    return result
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

        # suppose format_val is your function for formatting or transforming values
        def format_val(x):
            return f"{x:.2f}" if isinstance(x, (int, float)) else x

        # Apply element-wise using map on each column
        df = df.apply(lambda col: col.map(format_val))

        df.to_csv(filename, header=False, float_format=float_format, sep=csv_sep)
    else:
        df.to_csv(filename, index=True, float_format=float_format, sep=csv_sep)

# ----------------------------------------------------------------------------------------------------------------------
