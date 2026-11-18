# SLIPER – Viewer Tool (Soil Slips Time-Series)

**Version:** 1.5.0  
**Release Date:** 2025-07-31  
**Authors:**  
- Fabio Delogu – fabio.delogu@cimafoundation.org  

---

## 📘 Description

This repository contains the script `sliper_app_viewer_main.py`, part of the **SLIPER** package.  
The Viewer tool visualizes **time-series plots** combining soil slips predictors, soil moisture, and rainfall data.

It generates **JPEG plots** for selected alert areas and time windows.

---

## 📥 Input Data

### 1. Configuration File
JSON configuration file (e.g., `sliper_app_viewer_configuration.json`) that defines:

- Paths for input/output data
- Visualization settings
- Time window and parameters

### 2. Static Data
- **Alert area info:**  
  - `alert_area_info.csv`  
  - `alert_area_epsg4326.shp`

### 3. Dynamic Data
- **Predictors CSV files** from the Predictors application:  
  `predictors_YYYYMMDD_REGISTRY.csv`

---

## 📤 Outputs

### 1. JPEG Plots
Plots generated for each alert area and time window:

```
predictors_YYYYMMDD_REGISTRY.jpeg
```

### 2. Ancillary Files
Workspace files storing intermediate visualization data:

- `predictors_[datetime]_[registry].workspace`

### 3. Logs
Execution logs stored as defined in the configuration:

```
sliper_viewer_log.txt
```

---

## ▶️ Running the Script

### Requirements
- Python 3.8+
- Packages: NumPy, Matplotlib, GDAL, Rasterio
- SLIPER dependencies

### Command

```bash
python sliper/analysis/sliper_app_viewer_ts_main.py \
  -settings_file configuration.json \
  -time "YYYY-MM-DD HH:MM"
```

### Example

```bash
python sliper/analysis/sliper_app_viewer_ts_main.py \
  -settings_file sliper/analysis/sliper_app_viewer_ts_configuration.json \
  -time "2025-07-31 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                                   | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| `update_static`                       | Refresh static datasets                             |
| `update_dynamic_source`               | Refresh input predictor data                        |
| `update_dynamic_destination`          | Overwrite existing plots                            |

---

## 🧱 Suggested Folder Structure

```
project/
├── sliper_app_viewer_main.py
├── sliper_app_viewer_configuration.json
├── /data_static/
│   ├── alert_area/
│   └── viewer/
├── /data_dynamic/
│   ├── destination/
│   │   ├── predictors/
│   │   └── plots/
│   ├── ancillary/
│   │   └── plots/
├── /tmp/
└── /log/
```

---

## 👞 Contact

For support or contributions:  
fabio.delogu@cimafoundation.org
