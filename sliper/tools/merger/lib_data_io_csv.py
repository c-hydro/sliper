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
import warnings
import pandas as pd

from typing import Optional, Dict, Union, List
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
                  prefix_delimiter: Optional[str] = '_', prefix_mandatory: bool = True,
                  domain_col: Optional[str] = 'domain',
                  time_col: str = 'time', time_index: bool = False,
                  allowed_prefix: Optional[List[str]] = None
                  ) -> Union[dict, pd.DataFrame]:
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
        prefix_key (str): Prefix to use for auto-generated keys (when allowed_prefix is None).
        prefix_delimiter (str): Delimiter used after the prefix.
        time_col (str): Mandatory logical name for time column.
        allowed_prefix (list[str]): If provided, disables auto-generation and checks that all keys
                                    (except time_col) start with one of these prefixes.

    Returns:
        dict or pd.DataFrame: Parsed CSV content.
    """
    # check allowed_prefix
    if allowed_prefix is not None and not isinstance(allowed_prefix, list):
        allowed_prefix = [allowed_prefix]

    # mode validation
    if prefix_key is None:
        if prefix_mandatory:
            if allowed_prefix is None:
                raise ValueError("Either prefix_key must be provided or allowed_prefix must be defined.")
    else:
        if allowed_prefix is not None:
            warnings.warn(
                "Both prefix_key and allowed_prefix are defined. "
                "allowed_prefix will be ignored and prefix_key mode will be used."
            )
            allowed_prefix = None

    # read csv
    df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding or 'utf-8')

    if key_column and key_column not in df.columns:
        raise ValueError(f"Key column '{key_column}' not found in the CSV headers.")

    # Fields processing
    if fields is not None and fields:
        updated_fields = {}
        has_time, has_domain = False, False

        for out_key, in_col in fields.items():
            if allowed_prefix:
                # Auto-generation not allowed when allowed_prefix is provided
                if out_key == ":" or out_key.startswith("{:}"):
                    raise ValueError(
                        "Auto-generated keys (':' or '{:}') are not allowed when allowed_prefix is set. "
                        "Specify explicit keys instead."
                    )
            else:
                # Handle auto-generation only if allowed_prefix is not set
                if out_key == ":":
                    out_key = f"{prefix_key}{prefix_delimiter}{in_col}" if prefix_delimiter else f"{prefix_key}{in_col}"
                elif out_key.startswith("{:}"):
                    suffix = out_key[4:]
                    out_key = f"{prefix_key}{prefix_delimiter}{suffix}" if prefix_delimiter else f"{prefix_key}{suffix}"

            # Validation
            if out_key == time_col:
                has_time = True
            elif out_key == domain_col:
                has_domain = True
            else:
                if allowed_prefix:
                    # Validate with allowed_prefix list
                    valid_prefix = any(out_key.startswith(pfx) for pfx in allowed_prefix)
                    if not valid_prefix:
                        raise ValueError(
                            f"Invalid key '{out_key}'. Must start with one of {allowed_prefix} (except '{time_col}')."
                        )
                else:
                    # Original validation
                    if not out_key.startswith(prefix_key):
                        raise ValueError(
                            f"Invalid key '{out_key}'. Must start with prefix '{prefix_key}' (except '{time_col}')."
                        )

            updated_fields[out_key] = in_col

        if not has_time:
            raise ValueError(f"The mandatory time key '{time_col}' is missing in fields definition.")
        if not has_domain:
            raise ValueError(f"The mandatory domain key '{domain_col}' is missing in fields definition.")

        # Apply filtering and renaming
        valid_input_cols = [v for v in updated_fields.values() if v in df.columns]
        if not valid_input_cols:
            raise ValueError("None of the specified columns were found in the CSV.")

        tmp_df = df[valid_input_cols]
        map_rename = {v: k for k, v in updated_fields.items()}
        df = tmp_df.rename(columns=map_rename)

    # set time index if required
    if time_index:
        if time_col not in df.columns:
            raise ValueError(f"The mandatory time column '{time_col}' is missing in the CSV.")

        # Convert time column to datetime and set as index
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
        if df[time_col].isnull().any():
            raise ValueError(f"Invalid datetime format in column '{time_col}'.")

        df.set_index(time_col, inplace=True)

    # Format output
    if key_column is not None:
        tmp = {}
        for _, row in df.iterrows():
            key = row[key_column]
            row_dict = row.to_dict()
            tmp[key] = {main_key: row_dict} if main_key else row_dict

        if result_format == 'dataframe':
            result = pd.DataFrame.from_dict(tmp, orient='index')
        elif result_format == 'dictionary':
            result = deepcopy(tmp)
        else:
            raise ValueError("The variable result_format must be either 'dataframe' or 'dictionary'")
    else:
        result = df if result_format == 'dataframe' else df.to_dict()

    return result

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to filter file csv by time
def filter_file_csv_by_time(
    df: pd.DataFrame,
    datetime_col: str,
    start: str = None,
    end: str = None,
    conditions: dict = None
) -> pd.DataFrame:
    """
    Load a CSV, parse datetime column, set it as index, and filter by time and conditions.

    Parameters:
    -----------
    file_path : str
        Path to the CSV file.
    datetime_col : str
        Column name that contains datetime values.
    start : str, optional
        Start date for filtering (inclusive).
    end : str, optional
        End date for filtering (inclusive).
    conditions : dict, optional
        Dictionary of column_name: value to filter other columns.

    Returns:
    --------
    pd.DataFrame
        Filtered DataFrame with DateTimeIndex.
    """
    # Load CSV and parse datetime
    df = df.set_index(datetime_col)

    # check if the index is already a datetime type
    if isinstance(df.index[0], str):
        # Convert index to datetime if it's not already
        df.index = pd.to_datetime(df.index, errors='coerce')

    # Time filtering
    if start or end:
        mask = pd.Series(True, index=df.index)
        if start:
            mask &= df.index >= pd.to_datetime(start)
        if end:
            mask &= df.index <= pd.to_datetime(end)
        df = df.loc[mask]

    # Apply other column conditions
    if conditions:
        for col, val in conditions.items():
            if isinstance(val, (list, tuple, set)):
                df = df[df[col].isin(val)]
            else:
                df = df[df[col] == val]

    return df
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
