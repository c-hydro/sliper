# Operational status report and dashboard

The report application reads the forecast workflow's `.info` files and writes a daily JSON snapshot. The real-time configuration covers forecast-cycle selection, rainfall and soil moisture synchronization and processing, indicators, scenarios, predictors, and time-series analysis.

## Generate a status report

Copy `sliper/tools/binaries/report/sliper_app_report_run_realtime.json` to `report.json`. Set the `run.source` paths and filename patterns to match the metadata written by your wrappers. Set `run.destination` and `log` to writable locations.

From the repository root, run:

```bash
python sliper/report/sliper_app_report_run_main.py -settings_file report.json
```

Real-time mode defaults to the current time rounded down to the hour. Use `--mode history --time "2026-11-14 15:00"` to check a past run. The bundled configuration names the resulting daily file `soil_slips_status_YYYYMMDD.json`. Repeated runs read and update that day's report.

## Generate the HTML dashboard

Pass the report directory, an output directory, and the HTML template to the dashboard script:

```bash
bash sliper/tools/binaries/report/sliper_app_dashboard_run.sh \
  /path/to/report/summary \
  /path/to/report/dashboard \
  sliper/tools/binaries/report/sliper_app_dashboard_template.html
```

The script uses the most recently checked valid report. It writes the current dashboard to `latest/index.html` and an hourly snapshot to `by_time/YYYY/MM/DD/YYYYMMDD_HH.html`.
