#!/usr/bin/python3
"""
SLIPER APP - REPORT RUN - Soil Landslide Information and Prediction & Early Response

__date__ = '20261021'
__version__ = '1.0.0'
__author__ =
        'Fabio Delogu (fabio.delogu@cimafoundation.org',
        'Francesco Silvestro (francesco.silvestro@cimafoundation.org)',
        'Stefania Magri (stefania.magri@arpal.liguria.it)',
        'Monica Solimano (monica.solimano@arpal.liguria.it)'

__library__ = 'sliper'

General command line:
python sliper_app_report_run_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20261021 (1.0.0) --> Beta release
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import argparse
import json
import logging
import os
import time

from argparse import ArgumentParser
from datetime import date, datetime, timedelta
from pathlib import Path

from lib_analysis import monitor
from lib_data_io_json import read_file_settings, read_file_report, save_file_report
from lib_utils_io import resolve_file_path
from lib_utils_time import set_time
from lib_utils_logging import set_logging_file
from lib_info_args import logger_name, time_format_algorithm

# Logging
log_stream = logging.getLogger(logger_name)

# general settings
TAG = 'run'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Algorithm information
project_name = 'sliper'
alg_name = 'SLIPER APP - REPORT'
alg_type = 'Package'
alg_version = '1.0.0'
alg_release = '2026-10-21'
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# main
def main():

    # ------------------------------------------------------------------------------------------------------------------
    # get algorithm settings
    alg_settings, alg_mode, alg_time = get_args()

    # set algorithm settings
    data_settings = read_file_settings(alg_settings, tag=TAG)

    # set time information
    time_now, time_day, time_run = set_time(alg_time)

    # set logger file
    logger_file = resolve_file_path(data_settings["log"], time_run)
    # set logger handle
    set_logging_file(logger_name=logger_name,logger_file=logger_file)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # info algorithm
    log_stream.info(' ============================================================================ ')
    log_stream.info('[' + project_name + ' ' + alg_type + ' - ' + alg_name + ' (Version ' + alg_version +
                    ' - Release ' + alg_release + ')]')
    log_stream.info(' ==> START ... ')
    log_stream.info(' ')

    # time algorithm information
    alg_time_start = time.time()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # set file report
    file_path_report = resolve_file_path(data_settings[TAG]["destination"], time_run)
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # load the previous report (if available for the same day)
    log_stream.info(f" ---> Get previous report file ... ")
    log_stream.info(f" :::: Report File: {file_path_report}")
    file_data_report = read_file_report(file_path_report)
    log_stream.info(f" ---> Get previous report file ... DONE")
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # call monitor algorithm
    log_stream.info(f" ---> Execute analysis ... ")

    report = monitor(
        cfg_data=data_settings[TAG]['source'],
        cfg_parameters=data_settings['parameters'],
        time_day=time_run, previous=file_data_report, tag=TAG)

    log_stream.info(f" ---> Execute analysis ... DONE")
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # save the current report
    log_stream.info(f" ---> Dump current report file ... ")
    log_stream.info(f" :::: Report File: {file_path_report}")
    save_file_report(report, file_path_report)
    log_stream.info(f" ---> Dump current report file ... DONE")
    # ------------------------------------------------------------------------------------------------------------------

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
# Method to get script argument(s)
def get_args():

    parser_handle = ArgumentParser(description='Report .info execution status')

    parser_handle.add_argument(
        '-settings_file', '--config', action='store',
        dest='alg_settings',default='configuration.json', help='Configuration JSON file'
    )

    parser_handle.add_argument(
        '-mode', '--mode', choices=['realtime', 'history'],
        default='realtime', dest='alg_mode',help='Execution mode (default: realtime)'
    )

    parser_handle.add_argument(
        '-time', '--time', action='store',
        dest='alg_time', default=None, help='Reference time YYYY-MM-DD (required for history mode)'
    )

    parser_values = parser_handle.parse_args()

    # Get arguments
    alg_settings = parser_values.alg_settings
    alg_mode = parser_values.alg_mode
    alg_time = parser_values.alg_time

    # get current time
    time_now = datetime.now().replace(minute=0, second=0, microsecond=0)

    # Check algorithm settings
    if alg_mode == 'history' and not alg_time:
        parser_handle.error('Argument --time is required with --mode history')

    if alg_mode == 'realtime' and alg_time:
        # Convert string to datetime
        try:
            time_check = datetime.strptime(alg_time, "%Y-%m-%d %H:%M")
        except ValueError:
            parser_handle.error('Invalid --time format. Expected YYYY-MM-DD HH:MM')

        # Normalize to hourly resolution
        time_check = time_check.replace(minute=0, second=0, microsecond=0)
        # Check current day
        if time_check.date() != time_now.date():
            parser_handle.error(
                'Argument --time must match the current day in realtime mode'
            )

    return alg_settings, alg_mode, alg_time

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Call script from external library
if __name__ == '__main__':
    main()
# ----------------------------------------------------------------------------------------------------------------------
