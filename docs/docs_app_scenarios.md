# SLIPER – Scenarios Processing

**Version:** 2.5.0  
**Release Date:** 2025-07-21  
**Authors:**  
- Fabio Delogu – fabio.delogu@cimafoundation.org  
- Francesco Silvestro – francesco.silvestro@cimafoundation.org  
- Stefania Magri – stefania.magri@arpal.liguria.it  
- Monica Solimano – monica.solimano@arpal.liguria.it  

---

## 📘 Description

This repository contains the script `sliper_app_scenarios_main.py`, part of the **SLIPER** package, used to generate **scenarios** combining rainfall, soil moisture indicators, and observed landslides.  

The application processes multi-source data to produce scenario CSV outputs for different alert areas.  
It supports operational landslide forecasting and early warning.

---

## 📥 Input Data

### 1. Configuration File
JSON configuration file (e.g., `sliper_app_scenarios_configuration.json`) that defines:

- Paths for input/output data
- Time setup
- Algorithm flags

### 2. Static Data
- Alert area information:
  - CSV file: `alert_area_info.csv`

### 3. Dynamic Data
The application uses these input datasets:
- **Rainfall indicators:**  
  `indicators_rain_YYYYMMDDHHMM_ALERTAREA.csv`
- **Soil moisture indicators:**  
  `indicators_sm_YYYYMMDDHHMM_ALERTAREA.csv`
- **Observed soil slips:**  
  `soil_slips_YYYYMMDDHHMM_ALERTAREA.csv`

---

## 📤 Outputs

### 1. Scenario CSV
Generated for each alert area:

```
scenarios_YYYYMMDDHHMM_ALERTAREA.csv
```

### 2. Ancillary Files
Workspace files containing intermediate datasets and analysis:

- `scenarios_datasets_[run]_[alert_area].workspace`
- `scenarios_analysis_[run]_[alert_area].workspace`

### 3. Logs
Execution logs stored as defined in the configuration:

```
sliper_scenarios_log.txt
```

---

## ▶️ Running the Script

### Requirements
- Python 3.8+
- Packages: NumPy, GDAL, Rasterio, etc.
- SLIPER dependencies

### Command

```bash
python sliper_app_scenarios_main.py \
  -settings_file configuration.json \
  -time "YYYY-MM-DD HH:MM"
```

### Example

```bash
python sliper_app_scenarios_main.py \
  -settings_file sliper_app_scenarios_configuration.json \
  -time "2025-07-21 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                                   | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| `update_static`                       | Force static data refresh                          |
| `update_dynamic_ancillary_datasets`   | Update intermediate scenario datasets              |
| `update_dynamic_ancillary_analysis`   | Update intermediate scenario analysis files        |
| `update_dynamic_destination`          | Overwrite existing scenario CSV files              |

---

## 🧱 Suggested Folder Structure

```
project/
├── sliper_app_scenarios_main.py
├── sliper_app_scenarios_configuration.json
├── /data_static/
│   └── alert_area/
├── /data_dynamic/
│   ├── destination/
│   │   ├── indicators_rain/
│   │   ├── indicators_sm/
│   │   ├── soil_slips/
│   │   └── scenarios/
│   ├── ancillary/
│   │   └── scenarios/
├── /tmp/
└── /log/
```

---

## 👞 Contact

For support or contributions:  
fabio.delogu@cimafoundation.org
