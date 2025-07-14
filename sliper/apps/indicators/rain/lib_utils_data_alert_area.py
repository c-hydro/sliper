"""
Library Features:

Name:          lib_utils_data_sm
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re

from copy import deepcopy

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------------------
# method to translate geo object data
def translate_geo_object(geo_object):

    def parse_range(value):
        if '+' in value:
            return [int(value.replace('+', '')), None]
        start_end = re.split(r'[-–]', value)
        return [int(start_end[0]), int(start_end[1])]

    geo_result = {}
    for key, area in geo_object.items():
        transformed = {
            'name': area['name'],
            'alert_area': area['alert_area'],
            'index': area['index'],
            'catchment': [c.strip() for c in area['catchment'].split(',')],
            'white_range': parse_range(area['white_range']),
            'green_range': parse_range(area['green_range']),
            'yellow_range': parse_range(area['yellow_range']),
            'orange_range': parse_range(area['orange_range']),
            'red_range': parse_range(area['red_range']),
        }
        geo_result[key] = transformed
    return geo_result

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