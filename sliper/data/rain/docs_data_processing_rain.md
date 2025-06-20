# SLIPER – Rainfall Data Processing Algorithm

**Version**: 1.0.0  
**Release Date**: 2025-06-16  
**Authors**:  
- Fabio Delogu (fabio.delogu@cimafoundation.org)  
- Francesco Silvestro (francesco.silvestro@cimafoundation.org)  
- Stefania Magri (stefania.magri@arpal.liguria.it)  
- Monica Solimano (monica.solimano@arpal.liguria.it)  

---

## 📘 Description

This script (`app_data_processing_rain_main.py`) is part of the **SLIPER** package and is designed to preprocess **gridded rainfall data**. It provides:
- Flexible time window management
- Static and dynamic data handling
- Geospatial referencing and alignment
- Data export in GeoTIFF format
- Logging and intermediate file management

It supports hydrological risk and landslide early-warning systems.

---

## 📥 Input

### 1. **Configuration File**
A JSON file (e.g., `configuration.json`) specifying paths, time parameters, and algorithmic flags.

Example:
```json
{
  "time": {
    "time_now": "2023-01-15 23:45",
    "time_frequency": "H",
    "time_period": 24
  },
  "data": {
    "static": {...},
    "dynamic": {...}
  },
  "log": {
    "folder_name": ".../log/",
    "file_name": "sliper_data_preprocessing_rain_log.txt"
  }
}
```

### 2. **Static Data**
- GeoTIFF-based reference grid (e.g., `grid_rain.tiff`)
- Optional weather station database

### 3. **Dynamic Data**
- Hourly rainfall files: `Rain_YYYYMMDDHHMM.tif`  
- Organized in date-specific folders

---

## 📤 Output

### 1. **Processed Rainfall Files**
- Output as GeoTIFF: `rain_YYYYMMDDHHMM.tiff`  
- Saved in domain/time-structured folders

### 2. **Log File**
- Execution and error messages  
- Configurable in `configuration.json`

### 3. **Temporary Workspace**
- Used for caching data and intermediate computations

---

## ▶️ How to Run

### Requirements
- Python 3.8+
- Libraries: NumPy, GDAL, Rasterio, etc.
- SLIPER modules installed

### Command
```bash
python app_data_processing_rain_main.py \
  -settings_file configuration.json \
  -time "YYYY-MM-DD HH:MM"
```

### Example
```bash
python app_data_processing_rain_main.py \
  -settings_file configuration.json \
  -time "2025-06-18 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                  | Description                                 |
|----------------------|---------------------------------------------|
| `time_period`        | Number of hours/days to process             |
| `time_frequency`     | Step size for iteration (e.g., `H`, `D`)    |
| `updating_static`    | Flag to update static datasets              |
| `updating_dynamic`   | Flag to overwrite output                    |
| `var_min`, `var_max` | Range checks for rainfall data              |

---

## 🧱 Folder Structure

```text
project/
├── app_data_processing_rain_main.py
├── configuration.json
├── /data_static/
│   ├── datasets/
│   └── db_weather_stations/
├── /data_dynamic/
│   ├── source/
│   └── destination/
├── /tmp/
└── /log/
```

---

## 👞 Contact

For support or inquiries, please contact:  
📧 fabio.delogu@cimafoundation.org
