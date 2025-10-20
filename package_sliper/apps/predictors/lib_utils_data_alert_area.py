"""
Library Features:

Name:          lib_utils_data_alert_area
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# merge geo objects by keys
def merge_geo_objects(geo_objects_ref: dict, geo_objects_other: dict,
                      name_object_ref: str = 'info', name_object_other: str = 'datasets',
                      name_object_sort: bool = True) -> dict:

    merged = {}
    all_keys = set(geo_objects_ref.keys()) | set(geo_objects_other.keys())

    for key in all_keys:
        if key in geo_objects_ref and key in geo_objects_other:
            # Merge the two entries
            merged[key] = {
                name_object_ref: geo_objects_ref[key],
                name_object_other: geo_objects_other[key]
            }
        else:
            log_stream.warning(' ===> Key "' + key + '" not found in both dictionaries')

    # Sort by keys
    if name_object_sort:
        merged = dict(sorted(merged.items()))

    return merged

# ----------------------------------------------------------------------------------------------------------------------