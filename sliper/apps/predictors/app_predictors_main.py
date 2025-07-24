#!/usr/bin/python3
"""
ARPAL Processing Tool - SOIL SLIPS PREDICTORS

__date__ = '20221013'
__version__ = '1.3.0'
__author__ =
        'Stefania Magri (stefania.magri@arpal.liguria.it',
        'Mauro Quagliati (mauro.quagliati@arpal.liguria.it',
        'Monica Solimano (monica.solimano@arpal.liguria.it)',
        'Fabio Delogu (fabio.delogu@cimafoundation.org'

__library__ = 'ARPAL'

General command line:
python3 arpal_soil_slips_predictors_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20221013 (1.3.0) --> Bugs fix to the kernel fx
20220517 (1.2.0) --> Bugs fix and methods refactor
20220320 (1.1.0) --> Beta release for operational chain mode
20220105 (1.0.0) --> Beta release for Jupyter Notebook mode
"""

# -------------------------------------------------------------------------------------
# Complete library
import logging
import time
import os

from driver_data_io_fx_kernel import DriverTraining
from driver_analysis_predictors import DriverAnalysis as DriverAnalysisPredictors

from argparse import ArgumentParser

from lib_data_io_json import read_file_json
from lib_utils_time import set_time_predictors
from lib_utils_logging import set_logging_file

from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_version = '1.3.0'
alg_release = '2022-10-13'
alg_name = 'SOIL SLIPS PREDICTORS MAIN'
# Algorithm parameter(s)
time_format = '%Y-%m-%d %H:%M'
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Script Main
def main():

    # -------------------------------------------------------------------------------------
    # Get algorithm settings
    alg_settings, alg_time = get_args()

    # Set algorithm settings
    data_settings = read_file_json(alg_settings)

    # Set algorithm logging
    set_logging_file(
        logger_name=logger_name,
        logger_file=os.path.join(data_settings['log']['folder_name'], data_settings['log']['file_name']))
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
    log_stream.info(' ============================================================================ ')
    log_stream.info(' ==> ' + alg_name + ' (Version: ' + alg_version + ' Release_Date: ' + alg_release + ')')
    log_stream.info(' ==> START ... ')
    log_stream.info(' ')

    # Time algorithm information
    start_time = time.time()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Organize time run
    time_run, time_range = set_time_predictors(
        time_run_args=alg_time,
        time_run_file=data_settings['time']['time_now'],
        time_run_file_start=data_settings['time']['time_start'],
        time_run_file_end=data_settings['time']['time_end'],
        time_format=time_format,
        time_period=data_settings['time']['time_period'],
        time_frequency=data_settings['time']['time_frequency'],
        time_rounding=data_settings['time']['time_rounding']
    )
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Organize fx static datasets
    driver_fx_data = DriverTraining(
        src_dict=data_settings['data']['static']['source'],
        dst_dict=data_settings['data']['static']['destination'],
        flag_training_updating=data_settings['algorithm']['flags']['updating_static_data'])
    fx_data_collection = driver_fx_data.organize_data()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Iterate over time(s)
    for time_step in time_range:

        # Analysis datasets to define predictors
        driver_analysis_predictors = DriverAnalysisPredictors(
            time_step,
            info_dict=data_settings['data']['dynamic']['info'],
            src_dict=data_settings['data']['dynamic']['source'],
            anc_dict=data_settings['data']['dynamic']['ancillary'],
            dst_dict=data_settings['data']['dynamic']['destination'],
            template_dict=data_settings['algorithm']['template'],
            ancillary_dict=data_settings['algorithm']['ancillary'],
            fx_dict=fx_data_collection,
            flag_fx=data_settings['algorithm']['flags']['running_mode'],
            flag_anc_updating=data_settings['algorithm']['flags']['updating_dynamic_ancillary'],
            flag_dst_updating=data_settings['algorithm']['flags']['updating_dynamic_destination']
        )

        source_datasets = driver_analysis_predictors.organize_analysis()
        analysis_datasets = driver_analysis_predictors.compute_analysis(source_datasets)
        driver_analysis_predictors.dump_analysis(analysis_datasets)
        # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Info algorithm
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
# Method to get script argument(s)
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
# Call script from external library
if __name__ == '__main__':
    main()
# ----------------------------------------------------------------------------
