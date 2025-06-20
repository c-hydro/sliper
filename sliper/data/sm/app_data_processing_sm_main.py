#!/usr/bin/python3
"""
SLIPER APP - SOIL-MOISTURE DATA PROCESSING - Soil Landslide Information and Prediction & Early Response

__date__ = '20250618'
__version__ = '1.0.0'
__author__ =
        'Fabio Delogu (fabio.delogu@cimafoundation.org',
        'Francesco Silvestro (francesco.silvestro@cimafoundation.org)'

__library__ = 'sliper'

General command line:
python app_data_processing_sm_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20250618 (1.0.0) --> Beta release for sliper package based on previous package(s) and version(s)
"""

# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# libraries
import logging
import argparse
import time
import os

from lib_utils_logging import set_logging_file
from lib_data_io_json import read_file_settings
from lib_utils_time import set_time
from lib_info_args import logger_name, time_format_algorithm

from driver_geo import DriverGeo
from driver_data import DriverData

# logger
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# algorithm information
project_name = 'sliper'
alg_name = 'SLIPER APP - RAIN DATA PROCESSING'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2025-06-18'
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# script main
def main():

    # -------------------------------------------------------------------------------------
    # Get algorithm settings
    alg_settings, alg_time = get_args()

    # Set algorithm settings
    data_settings = read_file_settings(alg_settings)

    # Set algorithm logging
    set_logging_file(
        logger_name=logger_name,
        logger_file=os.path.join(data_settings['log']['folder_name'], data_settings['log']['file_name']))
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    log_stream.info(' ============================================================================ ')
    log_stream.info('[' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
                    ' - Release ' + alg_release + ')]')
    log_stream.info(' ==> START ... ')
    log_stream.info(' ')

    # Time algorithm information
    alg_time_start = time.time()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Organize time run
    time_run, time_range, time_chunks = set_time(
        time_run_args=alg_time,
        time_run_file=data_settings['time']['time_run'],
        time_run_file_start=data_settings['time']['time_start'],
        time_run_file_end=data_settings['time']['time_end'],
        time_format=time_format_algorithm,
        time_period=data_settings['time']['time_period'],
        time_frequency=data_settings['time']['time_frequency'],
        time_rounding=data_settings['time']['time_rounding'],
        time_reverse=data_settings['time']['time_reverse']
    )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Driver and method of static datasets
    driver_geo = DriverGeo(
        src_dict=data_settings['data']['static']['source'],
        anc_dict=data_settings['data']['static']['ancillary'],
        dst_dict=data_settings['data']['static']['destination'],
        alg_ancillary=data_settings['algorithm']['ancillary'],
        alg_template=data_settings['algorithm']['template'],
        flag_dset_cleaning=data_settings['algorithm']['flags']['cleaning_static']
    )
    geo_collection = driver_geo.organize_data()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Iterate over time chunks
    for time_reference, time_period in time_chunks.items():

        # -------------------------------------------------------------------------------------
        # Driver and method of dynamic datasets
        driver_data = DriverData(
            time_reference, time_period=time_period,
            static_data_collection=geo_collection,
            src_dict=data_settings['data']['dynamic']['source'],
            anc_dict=data_settings['data']['dynamic']['ancillary'],
            dst_dict=data_settings['data']['dynamic']['destination'],
            alg_ancillary=data_settings['algorithm']['ancillary'],
            alg_template_tags=data_settings['algorithm']['template'],
            alg_tmp=data_settings['tmp'],
            flag_cleaning_dynamic_data=data_settings['algorithm']['flags']['cleaning_dynamic_data'],
            flag_cleaning_dynamic_ancillary=data_settings['algorithm']['flags']['cleaning_dynamic_ancillary'],
            flag_cleaning_dynamic_tmp=data_settings['algorithm']['flags']['cleaning_dynamic_tmp'])

        driver_data.organize_data()
        driver_data.dump_data()
        driver_data.clean_dynamic_tmp()
        # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    alg_time_elapsed = round(time.time() - alg_time_start, 1)

    log_stream.info(' ')
    log_stream.info('[' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
                    ' - Release ' + alg_release + ')]')
    log_stream.info(' ==> TIME ELAPSED: ' + str(alg_time_elapsed) + ' seconds')
    log_stream.info(' ==> ... END')
    log_stream.info(' ==> Bye, Bye')
    log_stream.info(' ============================================================================ ')
    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get script argument(s)
def get_args():
    parser_handle = argparse.ArgumentParser()
    parser_handle.add_argument('-settings_file', action="store", dest="alg_settings")
    parser_handle.add_argument('-time', action="store", dest="alg_time")
    parser_values = parser_handle.parse_args()

    if parser_values.alg_settings:
        alg_settings = parser_values.alg_settings
    else:
        alg_settings = 'configuration.json'

    if parser_values.alg_time:
        alg_time = parser_values.alg_time
    else:
        alg_time = None

    return alg_settings, alg_time

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to set logging information
def set_logging(logger_file='log.txt', logger_format=None):
    if logger_format is None:
        logger_format = '%(asctime)s %(name)-12s %(levelname)-8s ' \
                        '%(filename)s:[%(lineno)-6s - %(funcName)20s()] %(message)s'

    # Remove old logging file
    if os.path.exists(logger_file):
        os.remove(logger_file)

    # Set level of root debugger
    logging.root.setLevel(logging.DEBUG)

    # Open logging basic configuration
    logging.basicConfig(level=logging.DEBUG, format=logger_format, filename=logger_file, filemode='w')

    # Set logger handle
    logger_handle_1 = logging.FileHandler(logger_file, 'w')
    logger_handle_2 = logging.StreamHandler()
    # Set logger level
    logger_handle_1.setLevel(logging.DEBUG)
    logger_handle_2.setLevel(logging.DEBUG)
    # Set logger formatter
    logger_formatter = logging.Formatter(logger_format)
    logger_handle_1.setFormatter(logger_formatter)
    logger_handle_2.setFormatter(logger_formatter)

    # Add handle to logging
    logging.getLogger('').addHandler(logger_handle_1)
    logging.getLogger('').addHandler(logger_handle_2)

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Call script from external library
if __name__ == "__main__":
    main()
# -------------------------------------------------------------------------------------
