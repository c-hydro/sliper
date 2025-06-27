"""
Library Features:

Name:          lib_data_io_csv_predictors
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

# -------------------------------------------------------------------------------------
# Library
import logging
import pandas as pd

from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------

import pandas as pd
from datetime import datetime

def save_dataframe_with_extras(
    filename='output.csv',
    df,
    extra_fields=None,
    csv_sep=',',
    time_format='%Y-%m-%d %H:%M',
    float_format='%.2f'
):
    """
    Add extra fields to a DataFrame, format timestamps and floats, and save as CSV.

    Args:
        df (pd.DataFrame): The original DataFrame.
        extra_fields (dict): Extra columns to add to each row.
        filename (str): Output CSV file path.
        csv_sep (str): CSV delimiter.
        time_format (str): Format string for datetime fields.
        float_format (str): Format string for float values (e.g., '%.2f').
    """
    if extra_fields:
        for key, value in extra_fields.items():
            if isinstance(value, (datetime, pd.Timestamp)):
                value = value.strftime(time_format)
            df[key] = value

    df.to_csv(filename, index=False, sep=csv_sep, float_format=float_format)



# -------------------------------------------------------------------------------------
# Method to write file csv
def write_file_csv(file_name, file_data, file_sep=',', file_header=True, file_index=True, file_format='%.3f',
                   file_tag_columns=None, file_tag_index='time'):

    if file_tag_columns is not None:

        if not isinstance(file_tag_columns, list):
            file_tag_columns = list(file_tag_columns)

        if file_tag_index in file_tag_columns:
            file_tag_columns.remove(file_tag_index)

        if file_data.columns.__len__() == file_tag_columns.__len__():
            file_data.columns = file_tag_columns
            file_data.index.name = file_tag_index
        else:
            log_stream.error(' ===> Columns name to write csv file is not equal to the DataFrame columns')
            raise RuntimeError('Columns name defined by the user must be equal to the columns of the DataFrame')

    if isinstance(file_data, pd.DataFrame):
        file_data.to_csv(file_name, sep=file_sep, header=file_header, index=file_index, float_format=file_format)
    else:
        log_stream.error(' ===> Variable type for writing csv file not supported')
        raise NotImplementedError('Case not implemented yet')
# -------------------------------------------------------------------------------------
