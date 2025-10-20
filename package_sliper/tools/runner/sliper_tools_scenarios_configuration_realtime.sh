#!/bin/bash -e

#-----------------------------------------------------------------------------------------
# Script information
script_name='SLIPER TOOLS - SCENARIOS RUNNER - REALTIME'
script_version="1.3.0"
script_date='2023/01/19'

# Script flag(s)
flag_execution=true

# Script Info
script_folder='/home/admin/library/package_soilslips/'
script_file_main='/home/admin/library/package_soilslips/sliper_app_indicators_rain_main.py'
script_file_settings_type_domain='/home/admin/soilslips-exec/arpal_soil_slips_scenarios_configuration_realtime_domain.json'
script_file_settings_type_alert_area='/home/admin/soilslips-exec/arpal_soil_slips_scenarios_configuration_realtime_alert_area.json'

# VirtualEnv Info
virtualenv_folder='/home/admin/library/env_conda'
virtualenv_name='env_conda_python3_soil_slips_libraries'

# Libraries Info
libs_folder='/home/admin/library/env_system_library'
libs_file_settings='system_library_generic'
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Get information (-u to get gmt time)
time_run=$(date -u +"%Y-%m-%d %H:00")
#time_run="2022-10-10 07:35" # DEBUG ANALYSIS CASE "2019-12-30 00:00"
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Activate python miniconda virtualenv
export PATH=$virtualenv_folder/bin:$PATH
source activate $virtualenv_name
# Add path to pythonpath
export PYTHONPATH="${PYTHONPATH}:$script_folder"
# Add LD_LIBRARY_PATH and PATH system library
source ${libs_folder}/${libs_file_settings}
#-----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Info script start
echo " ==================================================================================="
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> START ..."

# Section to execute the soilslip procedure
echo " ====> COMPUTE SOIL SLIPS SCENARIOS ... "
if $flag_execution; then

    # Run python command line for domain type
    echo " =====> EXECUTE TYPE DOMAIN: " python $script_file_main -settings_file $script_file_settings_type_domain -time $time_run
    python $script_file_main -settings_file $script_file_settings_type_domain -time "$time_run"
    
    # Run python command line for alert area type
    echo " =====> EXECUTE TYPE ALERT AREA: " python $script_file_main -settings_file $script_file_settings_type_alert_area -time $time_run
    python $script_file_main -settings_file $script_file_settings_type_alert_area -time "$time_run"

    echo " ====> COMPUTE SOIL SLIPS SCENARIOS ... DONE"

else
    echo " ====> COMPUTE SOIL SLIPS SCENARIOS ... SKIPPED. FLAG NOT ACTIVATED"
fi

# Info script end
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------

