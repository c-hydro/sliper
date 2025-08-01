# SLIPER – Soil Moisture Indicators Processing

**Version:** 2.5.0  
**Release Date:** 2025-07-09  
**Authors:**  
- Fabio Delogu – fabio.delogu@cimafoundation.org  
- Francesco Silvestro – francesco.silvestro@cimafoundation.org  
- Stefania Magri – stefania.magri@arpal.liguria.it  
- Monica Solimano – monica.solimano@arpal.liguria.it  

---

## 📘 Description

This repository contains the script `sliper_app_indicators_sm_main.py`, part of the **SLIPER** package, used to compute **soil moisture-based indicators** supporting landslide early warning systems.

The application processes soil moisture gridded data and generates indicators for different alert areas.  
It supports operational forecasting systems for landslide risk assessment.

---

## 📥 Input Data

### 1. Configuration Files
JSON configuration files (e.g., `sliper_app_indicators_sm_configuration_obs.json` or `sliper_app_indicators_sm_configuration_frc.json`) that define:

- Paths for input/output data
- Time setup (observed and forecast periods)
- Thresholds and algorithm flags

### 2. Static Data
- **Reference Grid (GeoTIFF)**: e.g., `geo_liguria.tiff`
- **Alert Areas**:
  - Info file (CSV): `alert_area_info.csv`
  - Shapefile: `alert_area_epsg4326.shp`

### 3. Dynamic Data
- Soil moisture rasters: `sm.YYYYMMDDHHMM.tiff`
- Organized in folders by date

---

## 📤 Outputs

### 1. Indicators CSV
Generated for each alert area:

```
indicators_sm_YYYYMMDDHHMM_ALERTAREA.csv
```

### 2. Ancillary Files
Workspace files containing intermediate grid and time-series data:

- `indicators_sm_[run]_[start]_[end]_grid.workspace`
- `indicators_sm_[run]_[start]_[end]_ts.workspace`

### 3. Logs
Execution logs stored as defined in the configuration:

```
sliper_indicators_sm_log.txt
```

---

## ▶️ Running the Script

### Requirements
- Python 3.8+
- Packages: NumPy, GDAL, Rasterio, etc.
- SLIPER dependencies

### Command

```bash
python sliper_app_indicators_sm_main.py \
  -settings_file configuration.json \
  -time "YYYY-MM-DD HH:MM"
```

### Example

```bash
python sliper_app_indicators_sm_main.py \
  -settings_file sliper_app_indicators_sm_configuration_obs.json \
  -time "2025-07-09 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                                   | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| `update_static`                       | Force static data refresh                          |
| `update_dynamic_ancillary_grid`       | Update ancillary grid workspaces                   |
| `update_dynamic_ancillary_ts`         | Update ancillary time-series workspaces            |
| `update_dynamic_destination`          | Overwrite existing indicator CSVs                  |
| `search_period`                       | Periods for soil moisture accumulation (2H, 24H)   |
| `search_type`                         | Window type: `left` or `right`                     |
| `search_part`                         | Type of temporal aggregation: `unique`, `multiple` |

---

## 🧱 Suggested Folder Structure

```
project/
├── sliper_app_indicators_sm_main.py
├── sliper_app_indicators_sm_configuration_obs.json
├── sliper_app_indicators_sm_configuration_frc.json
├── /data_static/
│   ├── reference/
│   ├── alert_area/
├── /data_dynamic/
│   ├── source/
│   ├── destination/
│   ├── ancillary/
├── /tmp/
└── /log/
```

---

## 👞 Contact

For support or contributions:  
fabio.delogu@cimafoundation.org
