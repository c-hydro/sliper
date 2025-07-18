#!/usr/bin/python3
"""
SLIPER APP - SOIL SLIPS DATA PROCESSING - Soil Landslide Information and Prediction & Early Response

__date__ = '20250714'
__version__ = '1.0.0'
__author__ =
        'Fabio Delogu (fabio.delogu@cimafoundation.org',
        'Francesco Silvestro (francesco.silvestro@cimafoundation.org)'

__library__ = 'sliper'

General command line:
python app_data_processing_soil_slips_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20250714 (1.0.0) --> Beta release for sliper package based on previous package(s) and version(s)
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import time
import os

from driver_geo import DriverGeo
from driver_data import DriverData

from argparse import ArgumentParser

from lib_data_io_json import read_file_settings
from lib_utils_time import set_time
from lib_utils_logging import set_logging_file
from lib_info_args import logger_name, time_format_algorithm

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Algorithm information
project_name = 'sliper'
alg_name = 'SLIPER APP - SOIL SLIPS DATA PROCESSING'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2025-07-14'
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Script Main
def main():

    # ------------------------------------------------------------------------------------------------------------------
    # Get algorithm settings
    alg_settings, alg_time = get_args()

    # Set algorithm settings
    data_settings = read_file_settings(alg_settings)

    # Set algorithm logging
    set_logging_file(
        logger_name=logger_name,
        logger_file=os.path.join(data_settings['log']['folder_name'], data_settings['log']['file_name']))
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Info algorithm
    log_stream.info(' ============================================================================ ')
    log_stream.info('[' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
                    ' - Release ' + alg_release + ')]')
    log_stream.info(' ==> START ... ')
    log_stream.info(' ')

    # Time algorithm information
    alg_time_start = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Organize time run
    time_run, time_range = set_time(
        time_run_args=alg_time,
        time_run_file=data_settings['time']['time_now'],
        time_run_file_start=data_settings['time']['time_start'],
        time_run_file_end=data_settings['time']['time_end'],
        time_format=time_format_algorithm,
        time_period=data_settings['time']['time_period'],
        time_frequency=data_settings['time']['time_frequency'],
        time_rounding=data_settings['time']['time_rounding']
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Geographical datasets
    driver_geo = DriverGeo(
        src_dict=data_settings['data']['static']['source'],
        dst_dict=data_settings['data']['static']['destination'],
        tmp_dict=data_settings['tmp'],
        tags_dict=data_settings['algorithm']['template'],
        flag_update=data_settings['algorithm']['flags']['update_static'])
    # organize geo collections
    geo_info, geo_data, geo_areas = driver_geo.organize_data()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # method to call driver data
    driver_data = DriverData(
        time_run=time_run,
        src_dict=data_settings['data']['dynamic']['source'],
        anc_dict=data_settings['data']['dynamic']['ancillary'],
        dst_dict=data_settings['data']['dynamic']['destination'],
        tags_dict=data_settings['algorithm']['template'],
        tmp_dict=data_settings['tmp'],
        collections_data_geo_info=geo_info,
        collections_data_geo_ref=geo_data,
        collections_data_geo_areas=geo_areas,
        flag_update=data_settings['algorithm']['flags']['update_dynamic'])

    # organize data collections
    data_info = driver_data.organize_data()
    # dump data collections
    driver_data.dump_data(data_info)

    # --------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Info algorithm
    alg_time_elapsed = round(time.time() - alg_time_start, 1)

    log_stream.info(' ')
    log_stream.info('[' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
                    ' - Release ' + alg_release + ')]')
    log_stream.info(' ==> TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    log_stream.info(' ==> ... END')
    log_stream.info(' ==> Bye, Bye')
    log_stream.info(' ============================================================================ ')
    # ------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to get script argument(s)
def get_args():
    parser_handle = ArgumentParser()
    parser_handle.add_argument('-settings_file', action="store", dest="alg_settings")
    parser_handle.add_argument('-time', action="store", dest="alg_time")
    parser_values = parser_handle.parse_args()

    alg_settings, alg_time = 'configuration.json', None
    if parser_values.alg_settings:
        alg_settings = parser_values.alg_settings
    if parser_values.alg_time:
        alg_time = parser_values.alg_time

    return alg_settings, alg_time

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Call script from external library
if __name__ == '__main__':
    main()
# ----------------------------------------------------------------------------------------------------------------------
