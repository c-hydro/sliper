# clean datasets not using the same date of the folder where they are (TODAY-2)
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_other_days.sh --catch-file sliper_tools_cleaner_datasets_catchments.txt --when today-2

# clean datasets not using the same date of the folder where they are (today-1 --n-days 4 --dry-run) 
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_other_days.sh --catch-file sliper_tools_cleaner_datasets_catchments.txt --when today-1 --n-days 4 --dry-run --log-file /home/admin/log/manager_system/cleaner_datasets_other_days_$(date +%Y%m%d_%H%M).log

# clean datasets not using the same date of the folder where they are (today-1 --n-days 4) 
/home/admin/soilslips-system/manager_system/sliper_tools_cleaner_datasets_other_days.sh --catch-file sliper_tools_cleaner_datasets_catchments.txt --when today-1 --n-days 4 --log-file /home/admin/log/manager_system/cleaner_datasets_other_days_$(date +%Y%m%d_%H%M).log
