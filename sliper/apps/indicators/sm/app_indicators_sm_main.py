#!/usr/bin/python3
"""
SLIPER APP - SM INDICATORS PROCESSING - Soil Landslide Information and Prediction & Early Response

__date__ = '20250709'
__version__ = '2.5.0'
__author__ =
        'Fabio Delogu (fabio.delogu@cimafoundation.org',
        'Francesco Silvestro (francesco.silvestro@cimafoundation.org)',
        'Stefania Magri (stefania.magri@arpal.liguria.it)',
        'Monica Solimano (monica.solimano@arpal.liguria.it)'

__library__ = 'sliper'

General command line:
python app_indicators_sm_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

Version(s):
20250620 (2.5.0) --> Beta release for sliper package based on previous package(s) and version(s)
20250430 (2.0.5) --> Fix and review codes for operational release based on realtime requirements
20250310 (2.0.4) --> Fix and review codes for operational release based on realtime requirements
20241007 (2.0.3) --> Fix and review codes for operational release based on realtime requirements
20240710 (2.0.2) --> Fix bugs in regridding rain data, fix bugs in epsg code detection from source ascii file
20230118 (2.0.1) --> Add support to xls and xlsx weather stations file
20220413 (2.0.0) --> Pre-operational release
20210515 (1.4.0) --> Add rain maps limits checks, add unique scenarios .csv file, fix rain peaks computations
20210412 (1.3.0) --> Add dependencies management, add forcing point creation for saving rain peaks
20210319 (1.2.1) --> Fix bugs in creating output indicators workspace files and csv and png scenarios files
20210202 (1.2.0) --> Fix bugs in creating rain datasets; fix bugs in output csv scenarios files
20201125 (1.1.0) --> Update of reader and writer methods for rain and soil moisture variables
20200515 (1.0.0) --> Beta release
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import time
import os

from driver_geo_reference import DriverGeoReference
from driver_geo_alert_area import DriverGeoAlertArea
from driver_data import DriverData

from argparse import ArgumentParser

from lib_data_io_json import read_file_json
from lib_utils_time import set_time
from lib_utils_logging import set_logging_file
from lib_info_args import logger_name, time_format_algorithm

# Logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Algorithm information
project_name = 'sliper'
alg_name = 'SLIPER APP - SOIL MOISTURE INDICATORS PROCESSING'
alg_type = 'Package'
alg_version = '2.5.0'
alg_release = '2025-07-09'
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Script Main
def main():

    # ------------------------------------------------------------------------------------------------------------------
    # Get algorithm settings
    alg_settings, alg_time = get_args()

    # Set algorithm settings
    data_settings = read_file_json(alg_settings)

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
        time_period_observed=data_settings['time']['time_period']['observed'],
        time_period_forecast=data_settings['time']['time_period']['forecast'],
        time_frequency=data_settings['time']['time_frequency'],
        time_rounding=data_settings['time']['time_rounding']
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Geographical datasets
    driver_geo_reference = DriverGeoReference(
        src_dict=data_settings['data']['static']['reference']['source'],
        dst_dict=data_settings['data']['static']['reference']['destination'],
        tmp_dict=data_settings['tmp'],
        tags_dict=data_settings['algorithm']['template'],
        flag_update=data_settings['algorithm']['flags']['update_static'])
    # organize geo collections
    geo_data_collection_ref = driver_geo_reference.organize_data()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # Geographical datasets
    driver_geo_alert_area = DriverGeoAlertArea(
        src_dict=data_settings['data']['static']['alert_area']['source'],
        anc_dict=data_settings['data']['static']['alert_area']['ancillary'],
        dst_dict=data_settings['data']['static']['alert_area']['destination'],
        info_dict=data_settings['algorithm']['info'],
        tmp_dict=data_settings['tmp'],
        tags_dict=data_settings['algorithm']['template'],
        flag_update=data_settings['algorithm']['flags']['update_static'])
    # organize geo collections
    (geo_info_aa,
     geo_data_collections_aa, geo_point_collections_aa) = driver_geo_alert_area.organize_data()
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    # data driver to define indicators
    driver_data = DriverData(
        time_run, time_range,
        src_dict=data_settings['data']['dynamic']['source'],
        anc_dict=data_settings['data']['dynamic']['ancillary'],
        dst_dict=data_settings['data']['dynamic']['destination'],
        tags_dict=data_settings['algorithm']['template'],
        collections_data_geo_grid_ref=geo_data_collection_ref,
        collections_data_geo_info=geo_info_aa,
        collections_data_geo_grid_other=geo_data_collections_aa,
        collections_data_geo_pnt_other=geo_point_collections_aa,
        flag_update_anc_grid=data_settings['algorithm']['flags']['update_dynamic_ancillary_grid'],
        flag_update_anc_ts=data_settings['algorithm']['flags']['update_dynamic_ancillary_ts'],
        flag_update_dst=data_settings['algorithm']['flags']['update_dynamic_destination'])

    # method to organize data collections
    data_collections = driver_data.organize_data(time_run)
    # method to analyze data collections
    analysis_collections = driver_data.analyze_data(time_run, data_collections)
    # method to dump data collections
    driver_data.dump_data(time_run, analysis_collections)
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
