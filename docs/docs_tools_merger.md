# SLIPER – Tools Merger (CSV Datasets)

**Version:** 1.0.0  
**Release Date:** 2025-07-30  
**Authors:**  
- Fabio Delogu – fabio.delogu@cimafoundation.org  

---

## 📘 Description

This repository contains the script `sliper_tools_merger_main.py`, part of the **SLIPER** package.  
The Tools Merger app merges **CSV predictor datasets** from different times or domains into unified CSV files.

It simplifies the consolidation of predictors for subsequent analysis or visualization.

---

## 📥 Input Data

### 1. Configuration File
JSON configuration file (e.g., `sliper_tools_merger_configuration.json`) that defines:

- Paths for input/output data
- Time setup
- Algorithm flags

### 2. Static Data
- **Alert area info (CSV):** `alert_area_info.csv`

### 3. Dynamic Data
- **Predictors CSV files:**  
  Files produced by the Predictors application.

---

## 📤 Outputs

### 1. Merged CSV
Unified CSV output for predictors:

```
predictors_YYYYMMDD_liguria.csv
```

### 2. Ancillary Files
Workspace file storing intermediate data:

```
predictors_[datetime]_liguria.workspace
```

### 3. Logs
Execution logs stored as defined in the configuration:

```
sliper_csv_merger_log.txt
```

---

## ▶️ Running the Script

### Requirements
- Python 3.8+
- Packages: NumPy, Pandas, GDAL
- SLIPER dependencies

### Command

```bash
python sliper_tools_merger_main.py \\
  -settings_file configuration.json \\
  -time "YYYY-MM-DD HH:MM"
```

### Example

```bash
python sliper_tools_merger_main.py \\
  -settings_file sliper_tools_merger_configuration.json \\
  -time "2025-07-30 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                                   | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| `update_static`                       | Refresh static datasets                             |
| `update_dynamic_ancillary`            | Update intermediate merged datasets                 |
| `update_dynamic_destination`          | Overwrite existing merged CSVs                      |

---

## 🧱 Suggested Folder Structure

```
project/
├── sliper_tools_merger_main.py
├── sliper_tools_merger_configuration.json
├── /data_static/
│   ├── alert_area/
├── /data_dynamic/
│   ├── destination/
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
