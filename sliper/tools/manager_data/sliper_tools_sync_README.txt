### -------------------------------------------------------------------------------
### README OF RUN AND DATASETS (RAIN AND SM) ORGANIZER
### -------------------------------------------------------------------------------

### -------------------------------------------------------------------------------
## 1) select run time (based on rain forecast)
# Realtime: 
	./sliper_tools_select_run.sh --config sliper_tools_select_run.json --mode now
# History: 
	./sliper_tools_select_run.sh --mode history --date 2025-09-23 --time 14:00 --config sliper_tools_select_run.json
### -------------------------------------------------------------------------------

### -------------------------------------------------------------------------------
## 2a) sync rain data
# Realtime FRC: 
	./sliper_tools_sync_data.sh --mode now --config sliper_tools_sync_data_rain_frc.json --verbose --lockfile sliper_tools_sync_data_rain_frc.lock --debug-gate

# Realtime OBS:
	./sliper_tools_sync_data.sh   --config sliper_tools_sync_data_rain_obs.json   --mode now   --freq-minutes 60   --n-days 2   --start-hour "00:00"   --filename-date-regex 'Rain_(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})(?P<HH>\d{2})(?P<mm>\d{2})\.tif$'

# Realtime OBS4FRC:	
	./sliper_tools_sync_data.sh --mode now --config sliper_tools_sync_data_rain_obs4frc.json --verbose --n-days 2 --start-hour "00:00" --freq-minutes 60 --lockfile sliper_tools_sync_data_rain_obs4frc.lock --filename-date-regex 'Rain_(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})(?P<HH>\d{2})(?P<mm>\d{2})\.tif$' --debug-gate --end-at-date-obs

# History EXAMPLE: 
	./sliper_tools_sync_data.sh --mode history --date 2025-09-29 --time 12:00 --config /home/admin/soilslips-system/manager_data_rain/sliper_tools_sync_data_obs.json --n-days 2 --start-hour 00:00 --freq-minutes 60 --filename-date-regex 'Rain_(?P<ts>[0-9]{12})\.tif'
### -------------------------------------------------------------------------------

### -------------------------------------------------------------------------------
## 3) sync soil moisture data
# Realtime FRC:
	./sliper_tools_sync_data.sh --mode now --config sliper_tools_sync_data_sm_frc.json --verbose --lockfile sliper_tools_sync_data_sm_frc.lock --debug-gate

# Realtime OBS:
	./sliper_tools_sync_data.sh --mode now --config sliper_tools_sync_data_sm_obs.json --verbose --lockfile sliper_tools_sync_data_sm_obs.lock --n-days 1 --start-hour 00:00  --end-at-date-obs --filename-date-regex 'hmc\.output-grid\.(?P<ts>[0-9]{12})\.nc\.gz'  --debug-gate

# Realtime OBS4FRC:
	./sliper_tools_sync_data.sh   --mode now   --config sliper_tools_sync_data_sm_obs4frc.json   --verbose   --lockfile sliper_tools_sync_data_sm_obs4frc.lock --n-days 1 --start-hour 00:00  --end-at-date-obs   --filename-date-regex 'hmc\.output-grid\.(?P<ts>[0-9]{12})\.nc\.gz' --debug-gate
### -------------------------------------------------------------------------------

