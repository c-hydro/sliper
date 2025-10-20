# SLIPER – Rainfall Indicators Processing

**Version**: 2.5.0  
**Release Date**: 2025-06-20  
**Authors**:  
- Fabio Delogu (fabio.delogu@cimafoundation.org)  
- Francesco Silvestro (francesco.silvestro@cimafoundation.org)  
- Stefania Magri (stefania.magri@arpal.liguria.it)  
- Monica Solimano (monica.solimano@arpal.liguria.it)  

---

## 📘 Description

This script (`app_indicators_rain_main.py`) is part of the **SLIPER** package and is designed to compute **rainfall-based indicators** for landslide early warning. It handles:
- Static reference and alert area data
- Dynamic rainfall datasets
- Multi-scale temporal analysis
- Threshold-based warning classification
- Output of time-series indicators in `.csv` format

It supports operational landslide prediction systems in Liguria (Italy).

---

## 📥 Input

### 1. **Configuration File**
A JSON file (e.g., `configuration.json`) specifying all paths, time setup, thresholds, and algorithmic flags.

Example snippet:
```json
"time": {
  "time_now": "2023-01-15 23:45",
  "time_frequency": "H",
  "time_period": {"observed": 24, "forecast": 48}
}
```

### 2. **Static Data**
- Reference grid (GeoTIFF): `grid_rain.tiff`
- Alert area shapefile: `alert_area_epsg4326.shp`

### 3. **Dynamic Data**
- Rainfall raster files in TIFF format: `rain_YYYYMMDDHHMM.tiff`
- Organized in daily folders

---

## 📤 Output

### 1. **Indicators CSV**
- Rainfall indicators by alert area  
- Saved as: `indicators_rain_YYYYMMDDHHMM_A.csv`  
  (where `A` is the alert area name)

### 2. **Temporary Workspaces**
- Grid and time-series `.workspace` files for caching

### 3. **Logs**
- Stored as defined in `configuration.json`, e.g.,  
  `sliper_indicators_rain_log.txt`

---

## ▶️ How to Run

### Requirements
- Python 3.8+
- Required packages: NumPy, Rasterio, GDAL, etc.
- SLIPER module dependencies

### Command
```bash
python sliper_app_indicators_rain_main.py \
  -settings_file configuration.json \
  -time "YYYY-MM-DD HH:MM"
```

### Example
```bash
python sliper_app_indicators_rain_main.py \
  -settings_file configuration.json \
  -time "2025-06-20 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                          | Description                                            |
|------------------------------|--------------------------------------------------------|
| `update_static`              | Force update of static data layers                    |
| `update_dynamic_destination` | Overwrite existing output indicators                  |
| `warning_threshold`          | Thresholds per alert area group for warning levels     |
| `search_period`              | Rainfall accumulation periods (e.g., 3H, 6H, 24H)      |
| `search_type`                | Direction of the search window: `left` or `right`     |

---

## 🧱 Folder Structure

```text
project/
├── app_indicators_rain_main.py
├── configuration.json
├── /data_static/
│   ├── datasets/
│   └── alert_area/
├── /data_dynamic/
│   ├── source/
│   └── destination/
├── /tmp/
└── /log/
```

---

## 👞 Contact

For support or collaboration, please reach out to:  
📧 fabio.delogu@cimafoundation.org
