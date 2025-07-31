#!/bin/bash -e

#-----------------------------------------------------------------------------------------
# Script information
script_name='ARPAL - SOIL SLIPS PREDICTORS - RUNNER - REALTIME'
script_version="1.2.0"
script_date='2022/10/13'

# Script flag(s)
flag_execution=true

# Script Info
script_folder='/home/admin/library/package_soilslips/'
script_file_main='/home/admin/library/package_soilslips/arpal_soil_slips_predictors_main.py'
script_file_settings='/home/admin/soilslips-exec/arpal_soil_slips_predictors_configuration_realtime_domain.json'

# VirtualEnv Info
virtualenv_folder='/home/admin/library/env_conda'
virtualenv_name='env_conda_python3_soil_slips_libraries'
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Get information (-u to get gmt time)
time_run=$(date -u +"%Y-%m-%d %H:00")
#time_run="2022-10-13 09:35" # DEBUG ANALYSIS CASE "2019-12-30 00:00"
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Activate python miniconda virtualenv
export PATH=$virtualenv_folder/bin:$PATH
source activate $virtualenv_name
# Add path to pythonpath
export PYTHONPATH="${PYTHONPATH}:$script_folder"
#-----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Info script start
echo " ==================================================================================="
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> START ..."

# Section to execute the soilslip procedure
echo " ====> COMPUTE SOIL SLIPS PREDICTORS ... "
if $flag_execution; then

    # Run python command line
    echo " =====> EXECUTE: " python $script_file_main -settings_file $script_file_settings -time $time_run
    python $script_file_main -settings_file $script_file_settings -time "$time_run"

    echo " ====> COMPUTE SOIL SLIPS PREDICTORS ... DONE"

else
    echo " ====> COMPUTE SOIL SLIPS PREDICTORS ... SKIPPED. FLAG NOT ACTIVATED"
fi

# Info script end
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------

