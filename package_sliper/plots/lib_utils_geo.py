"""
Library Features:

Name:          lib_utils_data_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220420'
Version:       '1.5.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re

from pyproj import CRS

from lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to normalize EPSG code
def normalize_crs(epsg_code: (int, str) = 4326):
    """
    Convert EPSG input (like 4326 or 'epsg=4326') to standardized 'EPSG:4326' string.
    """
    if isinstance(epsg_code, str) and "epsg=" in epsg_code.lower():
        epsg_int = int(epsg_code.lower().replace("epsg=", ""))
    elif isinstance(epsg_code, int):
        epsg_int = epsg_code
    else:
        log_stream.error(' ===> Invalid EPSG input: %s', epsg_code)
        raise ValueError("The EPSG input must be an integer or a string in the format 'epsg=XXXX'.")

    return CRS.from_epsg(epsg_int).to_string()
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to translate geo object data
def translate_geo_object(geo_object, id_range=None, rgb_range=None):

    def parse_range(value):
        if '+' in value:
            return [int(value.replace('+', '')), None]
        start_end = re.split(r'[-–]', value)
        return [int(start_end[0]), int(start_end[1])]

    if id_range is None:
        id_range = {
            'white_id': 0,
            'green_id': 1,
            'yellow_id': 2,
            'orange_id': 3,
            'red_id': 4
        }
    if rgb_range is None:
        rgb_range = {
            'white_rgb': {'rgb': (255, 255, 255), 'opacity': 0.5},
            'green_rgb': {'rgb': (0, 128, 0), 'opacity': 0.1},
            'yellow_rgb': {'rgb': (255, 255, 0), 'opacity': 0.1},
            'orange_rgb': {'rgb': (255, 165, 0), 'opacity': 0.1},
            'red_rgb': {'rgb': (255, 0, 0), 'opacity': 0.1}
        }

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

        if id_range is not None:
            transformed['white_id'] = id_range.get('white_id', 0)
            transformed['green_id'] = id_range.get('green_id', 1)
            transformed['yellow_id'] = id_range.get('yellow_id', 2)
            transformed['orange_id'] = id_range.get('orange_id', 3)
            transformed['red_id'] = id_range.get('red_id', 4)

        if rgb_range is not None:
            transformed['white_rgb'] = rgb_range.get('white_rgb', {'rgb': (255, 255, 255), 'opacity': 1.0})
            transformed['green_rgb'] = rgb_range.get('green_rgb', {'rgb': (0, 128, 0), 'opacity': 0.9})
            transformed['yellow_rgb'] = rgb_range.get('yellow_rgb', {'rgb': (255, 255, 0), 'opacity': 0.8})
            transformed['orange_rgb'] = rgb_range.get('orange_rgb', {'rgb': (255, 165, 0), 'opacity': 0.7})
            transformed['red_rgb'] = rgb_range.get('red_rgb', {'rgb': (255, 0, 0), 'opacity': 0.6})

        geo_result[key] = transformed

    return geo_result

# ----------------------------------------------------------------------------------------------------------------------
