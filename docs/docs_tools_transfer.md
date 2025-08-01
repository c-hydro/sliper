# SLIPER – Tools Transfer (File Transfer)

**Version:** 1.0.0  
**Release Date:** 2021-11-18  
**Authors:**  
- Fabio Delogu – fabio.delogu@cimafoundation.org  

---

## 📘 Description

This repository contains the script `sliper_tools_transfer_datasets.py`, part of the **SLIPER** package.  
The Tools Transfer app automates **file transfer between source and destination locations** for SLIPER datasets.

It supports transferring files using different methods (e.g., rsync, ftp, local copy).

---

## 📥 Input Data

### 1. Configuration File
JSON configuration file (e.g., `configuration.json`) that defines:

- Source datasets and locations
- Destination datasets and locations
- Transfer methods and commands

### 2. Methods Supported
- **rsync**  
- **ftp**  
- Local copy (OS-specific commands)

---

## 📤 Outputs

Transferred files are copied into the defined destination paths.  
No analytical outputs are generated, only transferred files.

Logs of all transfers are recorded.

---

## ▶️ Running the Script

### Requirements
- Python 3.8+
- Packages: Pandas
- rsync/ftp tools available on the system if required

### Command

```bash
python sliper_tools_transfer_datasets.py \\
  -settings_file configuration.json \\
  -time "YYYY-MM-DD HH:MM"
```

### Example

```bash
python sliper_tools_transfer_datasets.py \\
  -settings_file configuration.json \\
  -time "2025-01-01 00:00"
```

---

## ⚙️ Configuration Parameters

| Key                                   | Description                                         |
|---------------------------------------|-----------------------------------------------------|
| `source`                              | Source datasets (folders and files)                |
| `destination`                         | Destination datasets (folders and files)           |
| `method`                              | Transfer method (e.g., rsync, ftp)                 |
| `time`                                | Time period configuration                          |

---

## 🧱 Suggested Folder Structure

```
project/
├── sliper_tools_transfer_datasets.py
├── configuration.json
├── /data_static/
├── /data_dynamic/
├── /tmp/
└── /log/
```

---

## 👞 Contact

For support or contributions:  
fabio.delogu@cimafoundation.org
