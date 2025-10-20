# SLIPER – Predictors Processing

**Version:** 1.4.0  
**Release Date:** 2025-07-24  
**Authors:**  
- Stefania Magri – stefania.magri@arpal.liguria.it  
- Mauro Quagliati – mauro.quagliati@arpal.liguria.it  
- Monica Solimano – monica.solimano@arpal.liguria.it  
- Fabio Delogu – fabio.delogu@cimafoundation.org  

---

## 📘 Description

This repository contains the script `sliper_app_predictors_main.py`, part of the **SLIPER** package, used to compute **predictors** for landslide forecasting.  

The application processes scenario data (rainfall, soil moisture, soil slips) and uses statistical kernels and training datasets to compute predictors for each alert area.

---

## 📥 Input Data

### 1. Configuration File
JSON configuration file (e.g., `sliper_app_predictors_configuration.json`) that defines:

- Paths for input/output data
- Kernel parameters
- Time setup
- Algorithm flags

### 2. Static Data
- **Alert area info (CSV)**
- **Training datasets:**
  - `Xtr_centered.csv`, `Xtr_max.csv`, `Xtrn_mean.csv`
  - `C_coefficients.csv`

### 3. Dynamic Data
- **Scenario CSV files:**  
  `scenarios_YYYYMMDDHHMM_ALERTAREA.csv`

---

## 📤 Outputs

### 1. Predictors CSV
Generated for each alert area:

```
predictors_YYYYMMDDHHMM_ALERTAREA.csv
```

### 2. Ancillary Files
Workspace files containing intermediate results:

- `predictors_[datetime]_[alert_area].workspace`

### 3. Logs
Execution logs stored as defined in the configuration:

```
sliper_predictors_log.txt
```

---

## ▶️ Running the Script

### Requirements
- Python 3.8+
- Packages: NumPy, GDAL, Rasterio, etc.
- SLIPER dependencies

### Command

```bash
python sliper_app_predictors_main.py \
  -settings_file configuration.json \
  -time "YYYY-MM-DD HH:MM"
```

### Example

```bash
python sliper_app_predictors_main.py \
  -settings_file sliper_app_predictors_configuration.json \
  -time "2025-07-24 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                                   | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| `update_static`                       | Force static data refresh                          |
| `update_dynamic_ancillary`            | Update intermediate predictor workspaces           |
| `update_dynamic_destination`          | Overwrite existing predictor CSV files             |
| `fx_kernel`                           | Kernel method and parameters for predictors        |

---

## 🧱 Suggested Folder Structure

```
project/
├── sliper_app_predictors_main.py
├── sliper_app_predictors_configuration.json
├── /data_static/
│   ├── alert_area/
│   ├── training_predictors/
│   └── workspace/
├── /data_dynamic/
│   ├── destination/
│   │   ├── scenarios/
│   │   └── predictors/
│   ├── ancillary/
│   │   └── predictors/
├── /tmp/
└── /log/
```

---

## 👞 Contact

For support or contributions:  
fabio.delogu@cimafoundation.org
