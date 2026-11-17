# clean datasets not using the same date of the folder where they are (TODAY-2)
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_other_days.sh --catch-file sliper_tools_cleaner_datasets_catchments.txt --when today-2

# clean datasets not using the same date of the folder where they are (today-1 --n-days 4 --dry-run) 
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_other_days.sh --catch-file sliper_tools_cleaner_datasets_catchments.txt --when today-1 --n-days 4 --dry-run --log-file /home/admin/log/manager_system/cleaner_datasets_other_days_$(date +%Y%m%d_%H%M).log

# clean datasets not using the same date of the folder where they are (today-1 --n-days 4) 
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_other_days.sh --catch-file sliper_tools_cleaner_datasets_catchments.txt --when today-1 --n-days 4 --log-file /home/admin/log/manager_system/cleaner_datasets_other_days_$(date +%Y%m%d_%H%M).log

# clean tmp datasets (sm) 
# realtime sm --dry-run
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_tmp.sh --mode realtime --n-days 3 --root /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/{catchment_name}/{Y}/{m}/{d} --dry-run --verbose

# history sm --dry-run
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_tmp.sh   --mode history   --start 2025-10-01   --end 2025-10-25   --root /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/{catchment_name}/{Y}/{m}/{d}   --dry-run --verbose

# history sm (DELETE active)
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_tmp.sh   --mode history   --start 2022-01-01   --end 2025-10-25   --root /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/{catchment_name}/{Y}/{m}/{d} --dry-run --verbose

# FILE NC (NOT USED FROM CONTINUUM IN THE STORAGE (FROM 2022-09-01 TO NOW)
# select file (no remove)
find /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/ -type f -name "*.nc"
# select file (REMOVE)
find /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/ -type f -name "*.nc" -delete

# FILE BIN (NOT USED FROM CONTINUUM IN THE STORAGE (FROM 2013-10-01 2022-08-31)
# select file (no remove)
find /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/ -type f -name "*.bin"
# select file (REMOVE)
find /home/admin/soilslips-ws/storage/data/source/soil_moisture/obs/ -type f -name "*.bin" -delete
