#!/usr/bin/python3
"""
ARPAL Viewer Tool - SOIL SLIPS PREDICTORS TIME-SERIES VIEWER

__date__ = '20240112'
__version__ = '1.0.0'
__author__ = 'Fabio Delogu (fabio.delogu@cimafoundation.org'
__library__ = 'sliper'

General command line:
python sliper_app_viewer_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20250731 (1.5.0) --> Refactor to the sliper package
20240117 (1.0.0) --> Beta release
"""

# -------------------------------------------------------------------------------------
# libraries
import logging
import time
import os

from driver_data_static import DriverData as DriverData_Static
from driver_data_dynamic import DriverData as DriverData_Dynamic

from argparse import ArgumentParser

from lib_data_io_json import read_file_json
from lib_utils_time import set_time
from lib_utils_logging import set_logging_file
from lib_info_args import logger_name, time_format_algorithm

# logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# algorithm information
project_name = 'sliper'
alg_name = 'SOIL SLIPS VIEWER TOOL'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2024-01-12'
# algorithm parameter(s)
time_format = '%Y-%m-%d %H:%M'
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# main
def main():

    # -------------------------------------------------------------------------------------
    # get algorithm settings
    alg_settings, alg_time = get_args()

    # set algorithm settings
    data_settings = read_file_json(alg_settings)

    # set algorithm logging
    set_logging_file(
        logger_name=logger_name,
        logger_file=os.path.join(data_settings['log']['folder_name'], data_settings['log']['file_name']))
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # info algorithm start message
    log_stream.info(' ============================================================================ ')
    log_stream.info(' ==> ' + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    log_stream.info(' ==> START ... ')
    log_stream.info(' ')

    # info algorithm time
    start_time = time.time()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # method to organize time
    time_run, time_range = set_time(
        time_ref_args=alg_time,
        time_ref_file=data_settings['time']['time_reference'],
        time_ref_file_start=data_settings['time']['time_start'],
        time_ref_file_end=data_settings['time']['time_end'],
        time_format=time_format_algorithm,
        time_period=data_settings['time']['time_period'],
        time_frequency=data_settings['time']['time_frequency'],
        time_rounding=data_settings['time']['time_rounding']
    )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # geographical datasets
    driver_data_static = DriverData_Static(
        src_dict=data_settings['data']['static']['source'],
        dst_dict=data_settings['data']['static']['destination'],
        tmp_dict=data_settings['tmp'],
        template_dict=data_settings['algorithm']['template'],
        info_dict=data_settings['algorithm']['info'],
        flags_dict=data_settings['algorithm']['flags'])
    # method to organize data
    data_static_collection = driver_data_static.organize_data()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # iterate over time steps
    for time_step in time_range:

        # -------------------------------------------------------------------------------------
        # dynamic datasets
        driver_data_dynamic = DriverData_Dynamic(
            time_run=time_step,
            static_obj=data_static_collection,
            source_dict=data_settings['data']['dynamic']['source'],
            ancillary_dict=data_settings['data']['dynamic']['ancillary'],
            destination_dict=data_settings['data']['dynamic']['destination'],
            template_dict=data_settings['algorithm']['template'],
            info_dict=data_settings['algorithm']['info'],
            flags_dict=data_settings['algorithm']['flags'],
            tmp_dict=data_settings['tmp'])
        # method to organize data
        data_source_collection = driver_data_dynamic.organize_data()
        # method to view data
        driver_data_dynamic.view_data(data_source_collection)
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # # info algorithm end message
    time_elapsed = round(time.time() - start_time, 1)

    log_stream.info(' ')
    log_stream.info(' ==> ' + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    log_stream.info(' ==> TIME ELAPSED: ' + str(time_elapsed) + ' seconds')
    log_stream.info(' ==> ... END')
    log_stream.info(' ==> Bye, Bye')
    log_stream.info(' ============================================================================ ')
    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
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

# -------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------
# call main
if __name__ == '__main__':
    main()
# ----------------------------------------------------------------------------
