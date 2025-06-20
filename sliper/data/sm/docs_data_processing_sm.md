# SLIPER – Soil Moisture Data Processing Algorithm

**Version**: 1.0.0  
**Release Date**: 2025-06-18  
**Authors**:  
- Fabio Delogu (fabio.delogu@cimafoundation.org)  
- Francesco Silvestro (francesco.silvestro@cimafoundation.org)  

---

## 📘 Description

This script (`app_data_processing_sm_main.py`) is part of the **SLIPER** suite, focusing on the preprocessing of **gridded soil moisture data**. It performs:
- Time selection and chunking
- Static and dynamic data handling
- Geospatial referencing and masking
- Dataset export to GeoTIFF
- Logging and workspace management

This module is tailored for landslide monitoring and hydrological risk assessment.

---

## 📥 Input

### 1. **Configuration File**
A JSON file (e.g., `configuration.json`) containing definitions for input/output paths, time settings, flags, and processing parameters.

Example:
```json
{
  "time": {
    "time_run": null,
    "time_start": null,
    "time_end": null,
    "time_frequency": "H"
  },
  "data": {
    "static": {...},
    "dynamic": {...}
  },
  "log": {
    "folder_name": ".../log/",
    "file_name": "sliper_data_preprocessing_sm_log.txt"
  }
}
```

### 2. **Static Data**
- Digital Elevation Models (DEMs)
- Watermark masks
- Geo-reference grids for soil moisture data

### 3. **Dynamic Data**
- HMC NetCDF files (`hmc.output-grid.YYYYMMDDHHMM.nc.gz`) with soil moisture variables per domain and timestamp

---

## 📤 Output

### 1. **Processed Soil Moisture Files**
- Output as GeoTIFF: `sm.YYYYMMDDHHMM.tiff`  
- Saved in domain/time-structured folders

### 2. **Log File**
- Records of script execution and errors  
- Defined in config (`sliper_data_preprocessing_sm_log.txt`)

### 3. **Temporary Workspace**
- Intermediate files (e.g., `.workspace` Pickle files) to speed up repeated executions

---

## ▶️ How to Run

### Requirements
- Python 3.8+
- External libraries: NumPy, GDAL, xarray, etc.
- SLIPER dependencies in place

### Command
```bash
python app_data_processing_sm_main.py \
  -settings_file configuration.json \
  -time "YYYY-MM-DD HH:MM"
```

### Example
```bash
python app_data_processing_sm_main.py \
  -settings_file configuration.json \
  -time "2025-06-18 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                        | Description                                               |
|----------------------------|-----------------------------------------------------------|
| `cleaning_static`          | Reset cached static data (terrain, watermark)             |
| `cleaning_dynamic_data`    | Overwrite destination outputs                             |
| `cleaning_dynamic_ancillary`| Recompute ancillary workspace files                       |
| `domain_name`              | List of domains to process (e.g., Ligurian basins)        |
| `layer_name`               | Variables of interest, typically `SM`                     |
| `file_frequency`           | Data resolution (`H` = hourly)                            |

---

## 🧱 Folder Structure

```text
project/
├── app_data_processing_sm_main.py
├── configuration.json
├── /data_static/
│   ├── catchments/
│   ├── ancillary/
│   └── reference/
├── /data_dynamic/
│   ├── source/sm/
│   ├── destination/sm/
│   └── ancillary/
├── /tmp/
└── /log/
```

---

## 👞 Contact

For questions, collaborations, or bug reports:  
📧 fabio.delogu@cimafoundation.org
