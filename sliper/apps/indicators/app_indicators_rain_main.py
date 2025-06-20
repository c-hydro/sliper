#!/usr/bin/python3
"""
SLIPER APP - RAIN INDICATORS PROCESSING - Soil Landslide Information and Prediction & Early Response

__date__ = '20250620'
__version__ = '2.0.5'
__author__ =
        'Fabio Delogu (fabio.delogu@cimafoundation.org',
        'Francesco Silvestro (francesco.silvestro@cimafoundation.org)',
        'Stefania Magri (stefania.magri@arpal.liguria.it)',
        'Monica Solimano (monica.solimano@arpal.liguria.it)'

__library__ = 'ARPAL'

General command line:
python app_indicators_rain_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"

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

# -------------------------------------------------------------------------------------
# Complete library
import logging
import time
import os

from driver_data_io_geo_point_soil_slips import DriverGeoPoint as DriverGeoPoint_SoilSlips
from driver_data_io_geo_point_weather_stations import DriverGeoPoint as DriverGeoPoint_WeatherStations
from driver_data_io_geo_grid import DriverGeoGrid
from driver_data_io_forcing_rain import DriverForcing as DriverForcingRain
from driver_data_io_forcing_sm import DriverForcing as DriverForcingSM
from driver_analysis_indicators import DriverAnalysis as DriverAnalysisIndicators
from driver_analysis_scenarios import DriverAnalysis as DriverAnalysisScenarios

from argparse import ArgumentParser

from lib_data_io_json import read_file_json
from lib_utils_time import set_time_scenarios
from lib_utils_logging import set_logging_file
from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Algorithm information
alg_version = '2.0.5'
alg_release = '2025-04-30'
alg_name = 'SOIL SLIPS SCENARIOS MAIN'
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

    # Set algorithm library dependencies
    set_deps(data_settings['algorithm']['dependencies'], env_extra=['PROJ_LIB', 'GDAL_DATA_SCRIPT', 'GDAL_DATA'])
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
    time_run, time_range = set_time_scenarios(
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
    # Geographical datasets
    driver_data_geo_grid = DriverGeoGrid(
        src_dict=data_settings['data']['static']['source'],
        dst_dict=data_settings['data']['static']['destination'],
        tmp_dict=data_settings['tmp'],
        collections_data_group=data_settings['algorithm']['ancillary']['group'],
        collections_options=data_settings['algorithm']['ancillary']['options'],
        alg_template_tags=data_settings['algorithm']['template'],
        flag_geo_updating=data_settings['algorithm']['flags']['updating_static_ancillary_grid'])
    geo_grid_collection = driver_data_geo_grid.organize_data()

    # Weather stations point datasets
    driver_data_geo_point_weather_stations = DriverGeoPoint_WeatherStations(
        src_dict=data_settings['data']['static']['source'],
        dst_dict=data_settings['data']['static']['destination'],
        tmp_dict=data_settings['tmp'],
        collections_data_geo=geo_grid_collection,
        collections_data_group=data_settings['algorithm']['ancillary']['group'],
        alg_template_tags=data_settings['algorithm']['template'],
        flag_geo_updating=data_settings['algorithm']['flags']['updating_static_ancillary_point_weather_stations'])
    geo_point_collection_weather_stations = driver_data_geo_point_weather_stations.organize_data()

    # Soil-slips point datasets
    driver_data_geo_point_soil_slips = DriverGeoPoint_SoilSlips(
        src_dict=data_settings['data']['static']['source'],
        dst_dict=data_settings['data']['static']['destination'],
        collections_data_geo=geo_grid_collection,
        collections_data_group=data_settings['algorithm']['ancillary']['group'],
        flag_geo_updating=data_settings['algorithm']['flags']['updating_static_ancillary_point_soil_slips'])
    geo_point_collection_soil_slips = driver_data_geo_point_soil_slips.organize_data()
    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Activate analyzer mode
    if activate_algorithm_step(['organizer', 'analyzer'], data_settings['algorithm']['flags']['running_mode']):

        # Iterate over time(s)
        for time_step in time_range:

            # Rain datasets
            driver_forcing_rain = DriverForcingRain(
                time_step,
                src_dict=data_settings['data']['dynamic']['source'],
                anc_dict=data_settings['data']['dynamic']['ancillary'],
                dst_dict=data_settings['data']['dynamic']['destination'],
                tmp_dict=data_settings['tmp'],
                time_data=data_settings['data']['dynamic']['time'],
                collections_data_geo_grid=geo_grid_collection,
                collections_data_geo_pnt=geo_point_collection_weather_stations,
                collections_data_group=data_settings['algorithm']['ancillary']['group'],
                alg_template_tags=data_settings['algorithm']['template'],
                flag_ancillary_updating=data_settings['algorithm']['flags']['updating_dynamic_ancillary_rain'])
            if activate_algorithm_step(['organizer'], data_settings['algorithm']['flags']['running_mode']):
                driver_forcing_rain.organize_forcing()

            # Soil moisture datasets
            driver_forcing_sm = DriverForcingSM(
                time_step,
                src_dict=data_settings['data']['dynamic']['source'],
                anc_dict=data_settings['data']['dynamic']['ancillary'],
                dst_dict=data_settings['data']['dynamic']['destination'],
                tmp_dict=data_settings['tmp'],
                time_data=data_settings['data']['dynamic']['time'],
                collections_data_geo=geo_grid_collection,
                collections_data_group=data_settings['algorithm']['ancillary']['group'],
                alg_template_tags=data_settings['algorithm']['template'],
                flag_ancillary_updating=data_settings['algorithm']['flags']['updating_dynamic_ancillary_sm'])
            if activate_algorithm_step(['organizer'], data_settings['algorithm']['flags']['running_mode']):
                driver_forcing_sm.organize_forcing()

            # Analysis datasets to define indicators
            driver_analysis_indicators = DriverAnalysisIndicators(
                time_step,
                file_list_rain=driver_forcing_rain.file_path_processed,
                file_list_sm=driver_forcing_sm.file_path_processed,
                anc_dict=data_settings['data']['dynamic']['ancillary'],
                dst_dict=data_settings['data']['dynamic']['destination'],
                time_data=data_settings['data']['dynamic']['time'],
                collections_data_geo_grid=geo_grid_collection,
                collections_data_geo_pnt=geo_point_collection_weather_stations,
                collections_data_group=data_settings['algorithm']['ancillary']['group'],
                alg_template_tags=data_settings['algorithm']['template'],
                flag_dst_updating=data_settings['algorithm']['flags']['updating_dynamic_destination_indicators'])

            if activate_algorithm_step(['analyzer'], data_settings['algorithm']['flags']['running_mode']):
                analysis_collection_rain = driver_analysis_indicators.organize_analysis_rain()
                analysis_collection_sm = driver_analysis_indicators.organize_analysis_sm()

                driver_analysis_indicators.save_analysis(analysis_collection_sm, analysis_collection_rain,
                                                         geo_point_collection_soil_slips)
        # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Activate publisher mode
    if activate_algorithm_step(['publisher'], data_settings['algorithm']['flags']['running_mode']):

        # -------------------------------------------------------------------------------------
        # Analysis datasets to define scenarios
        driver_analysis_scenarios = DriverAnalysisScenarios(
            time_run, time_range,
            anc_dict=data_settings['data']['dynamic']['ancillary'],
            dst_dict=data_settings['data']['dynamic']['destination'],
            graph_dict=data_settings['graph'],
            collections_data_geo=geo_grid_collection,
            collections_data_group=data_settings['algorithm']['ancillary']['group'],
            collections_data_graph=data_settings['algorithm']['ancillary']['graph'],
            alg_template_tags=data_settings['algorithm']['template'],
            flag_dst_updating=data_settings['algorithm']['flags']['updating_dynamic_destination_scenarios'])

        scenarios_collections = driver_analysis_scenarios.collect_scenarios()
        driver_analysis_scenarios.dump_scenarios(scenarios_collections)
        driver_analysis_scenarios.plot_scenarios(scenarios_collections)
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
# Method to activate algorithm part
def activate_algorithm_step(algorithm_mode_step, algorithm_mode_list, algorithm_mode_type='any'):
    if algorithm_mode_type == 'any':
        algorithm_mode_flag = any(item in algorithm_mode_step for item in algorithm_mode_list)
    elif algorithm_mode_type == 'all':
        algorithm_mode_flag = all(item in algorithm_mode_step for item in algorithm_mode_list)
    else:
        algorithm_mode_flag = any(item in algorithm_mode_step for item in algorithm_mode_list)
    return algorithm_mode_flag
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to set libraries dependencies
def set_deps(algorithm_deps, env_ld_library='LD_LIBRARY_PATH', env_path='PATH', env_extra=None):

    # ENV LD_LIBRARY_PATH
    deps_list = algorithm_deps[env_ld_library]
    if deps_list is not None:
        for deps_step in deps_list:
            if env_ld_library not in list(os.environ):
                os.environ[env_ld_library] = deps_step
            else:
                os.environ[env_ld_library] += os.pathsep + deps_step
    # ENV PATH
    deps_list = algorithm_deps[env_path]
    if deps_list is not None:
        for deps_step in deps_list:
            if env_path not in list(os.environ):
                os.environ[env_path] = deps_step
            else:
                os.environ[env_path] += os.pathsep + deps_step
    # ENV EXTRA (NOT PATH OR LD_LIBRARY_PATH)
    if env_extra is not None:
        for env_name in env_extra:
            env_value = algorithm_deps[env_name]
            if env_value is not None:
                os.environ[env_name] = env_value

    # check GDAL DATA (check to avoid errors in load a gldal_data in the conda environment)
    if 'GDAL_DATA' in os.environ:
        gdal_folder = os.environ['GDAL_DATA']
        log_stream.info(' ===> GDAL_DATA SET: "' + gdal_folder + '"')
    else:
        log_stream.info(' ===> GDAL_DATA NOT SET')
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
