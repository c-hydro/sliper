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

from typing import Optional, Dict, Union
from copy import deepcopy
from datetime import datetime

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------
# method to read file csv
def read_file_csv(file_path: str,
                  fields: Optional[Dict[str, str]] = None,
                  key_column: Optional[str] = None,
                  encoding: Optional[str] = None,
                  delimiter: str = ',',
                  main_key: Optional[str] = None,
                  result_format: str = 'dictionary',
                  prefix_key: Optional[str] = 'rain',
                  prefix_delimiter: Optional[str] = '_',
                  extra_fields: dict = None,
                  time_col: str = 'time') -> Union[dict, pd.DataFrame, None]:
    """
    Reads a CSV file and returns a dictionary with nested or flat structure.

    Parameters:
        file_path (str): Path to the CSV file.
        fields (dict, optional): Mapping of output keys to CSV column names. Use ':' or '{:}' to auto-generate with prefix.
        key_column (str, optional): Column to use as dictionary key.
        encoding (str, optional): File encoding.
        delimiter (str): CSV delimiter.
        main_key (str, optional): If set, wraps row data under this key.
        result_format (str): 'dictionary' or 'dataframe'.
        prefix_key (str): Prefix to use for auto-generated keys.
        prefix_delimiter (str): Delimiter used after the prefix.
        extra_fields (dict): Extra fields to be added to the source dataframe
        time_col (str): Mandatory logical name for time column.

    Returns:
        dict or pd.DataFrame: Parsed CSV content.
    """

    # Read CSV
    df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding or 'utf-8')

    if key_column and key_column not in df.columns:
        raise ValueError(f"Key column '{key_column}' not found in the CSV headers.")

    if df.empty:
        return None

    # add fields (if needed and are not available in the source)
    if extra_fields is not None:
        for name, value in extra_fields.items():
            if name not in list(df.columns):
                df[name] = value

    # Fields processing
    if fields is not None and fields:
        updated_fields = {}
        has_time = False

        for out_key, in_col in fields.items():
            # Case 1: Placeholder colon key (e.g., ":": "amount")
            if out_key == ":":
                out_key = f"{prefix_key}{prefix_delimiter}{in_col}" if prefix_delimiter else f"{prefix_key}{in_col}"

            # Case 2: Pattern like "{:}_suffix"
            elif out_key.startswith("{:}"):
                suffix = out_key[4:]
                out_key = f"{prefix_key}{prefix_delimiter}{suffix}" if prefix_delimiter else f"{prefix_key}{suffix}"

            # Time field exemption
            if out_key == time_col:
                has_time = True
            elif not out_key.startswith(prefix_key) and out_key != time_col:
                raise ValueError(f"Invalid key '{out_key}'. Must start with prefix '{prefix_key}' (except '{time_col}').")

            updated_fields[out_key] = in_col

        if not has_time:
            raise ValueError(f"The mandatory time key '{time_col}' is missing in fields definition.")

        # Apply filtering and renaming
        map_filter = deepcopy(updated_fields)
        valid_input_cols = [v for v in map_filter.values() if v in df.columns]

        if not valid_input_cols:
            raise ValueError("None of the specified columns were found in the CSV.")

        tmp_df = df[valid_input_cols]
        map_rename = {v: k for k, v in updated_fields.items()}
        df = tmp_df.rename(columns=map_rename)

    # Format output
    if key_column is not None:
        tmp = {}
        for _, row in df.iterrows():
            key = row[key_column]
            row_dict = row.to_dict()
            tmp[key] = {main_key: row_dict} if main_key else row_dict

        if result_format == 'dataframe':
            result = pd.DataFrame(tmp)
        elif result_format == 'dictionary':
            result = deepcopy(tmp)
        else:
            raise ValueError("The variable result_format must be either 'dataframe' or 'dictionary'")
    else:
        result = df if result_format == 'dataframe' else df.to_dict()

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
