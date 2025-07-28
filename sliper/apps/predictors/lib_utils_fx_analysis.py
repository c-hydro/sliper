"""
Library Features:

Name:          lib_utils_fx_analysis
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250728'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to compute slip alert information
def compute_alert_info(df,
                       column_data='slips_pred_n',
                       column_id='slip_alert_id',
                       column_color='slip_alert_color', column_rgba='slip_alert_rgba',
                       color_ranges: dict = None, color_ids: dict = None, color_rgbas: dict = None):
    """
    Adds slip alert classification columns (id, color name, RGBA)
    based on the number of predicted slips.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with a column of slip predictions (counts).
    column_data : str
        Name of the column with slip prediction counts.
    column_id : str
        Name of the new column for slip alert IDs.
    column_color : str
        Name of the new column for slip alert colors.
    column_rgba : str
        Name of the new column for slip alert RGBA values.
    color_ranges : dict, optional
        Dictionary defining ranges for each alert color.
    color_ids : dict, optional
        Dictionary mapping color names to unique IDs.
    color_rgbas : dict, optional
        Dictionary mapping color names to RGBA values.

    Returns
    -------
    pd.DataFrame
        DataFrame with three new columns:
        - slip_alert_id
        - slip_alert_color
        - slip_alert_rgba
    """

    # --- configuration ---
    if color_ranges is None:
        color_ranges = {
            'white_range': [0, 0],
            'green_range': [1, 2],
            'yellow_range': [3, 5],
            'orange_range': [6, 13],
            'red_range': [14, None]
        }
    if color_ids is None:

        color_ids = {
            'white_id': 0,
            'green_id': 1,
            'yellow_id': 2,
            'orange_id': 3,
            'red_id': 4
        }

    if color_rgbas is None:
        # Default RGBA values with low alpha for transparency
        color_rgbas = {
            'white': [255, 255, 255, 0.1],
            'green': [0, 128, 0, 0.1],
            'yellow': [255, 255, 0, 0.1],
            'orange': [255, 165, 0, 0.1],
            'red': [255, 0, 0, 0.1],
        }

    # --- classification helper ---
    def classify_slip(count):
        if color_ranges['white_range'][0] <= count <= color_ranges['white_range'][1]:
            return color_ids['white_id']
        elif color_ranges['green_range'][0] <= count <= color_ranges['green_range'][1]:
            return color_ids['green_id']
        elif color_ranges['yellow_range'][0] <= count <= color_ranges['yellow_range'][1]:
            return color_ids['yellow_id']
        elif color_ranges['orange_range'][0] <= count <= color_ranges['orange_range'][1]:
            return color_ids['orange_id']
        elif count >= color_ranges['red_range'][0]:
            return color_ids['red_id']
        else:
            return np.nan

    # --- apply classification ---
    df[column_id] = df[column_data].apply(classify_slip)
    id_to_color = {v: k.replace('_id', '') for k, v in color_ids.items()}
    df[column_color] = df[column_id].map(id_to_color)

    # Convert RGBA list to a simple comma-separated string: "r,g,b,a"
    def rgba_to_string(color_name):
        rgba = color_rgbas.get(color_name, [0, 0, 0, 0.1])
        return f"{rgba[0]},{rgba[1]},{rgba[2]},{rgba[3]}"

    df[column_rgba] = df[column_color].map(rgba_to_string)

    return df
# ----------------------------------------------------------------------------------------------------------------------
