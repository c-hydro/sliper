# SLIPER – Tools Organizer (Soil Moisture file-to-folders)

**Version:** 1.0.0  
**Release Date:** 2025-08-01  
**Authors:**  
- Fabio Delogu – fabio.delogu@cimafoundation.org  

---

## 📘 Description

This repository contains the shell script `sliper_tools_organizer_sm_file2folders.sh`, part of the **SLIPER** tools suite.  
The **Organizer tool** restructures soil moisture dataset files into a **hierarchical folder structure by date** to facilitate subsequent processing steps.

---

## 📥 Input Data

### 1. Files
- Soil moisture dataset files (e.g., `sm.YYYYMMDDHHMM.tiff` or `sm.YYYYMMDDHHMM.nc`)

### 2. Folder Organization
The script organizes these files into subfolders based on their timestamp, creating a structured hierarchy.

---

## 📤 Outputs

- Moves/organizes files into subfolders by year/month/day.

Example structure after running:

```
data_dynamic/source/sm/
├── 2025/
│   ├── 08/
│   │   ├── 01/
│   │   │   ├── sm.202508010000.tiff
│   │   │   ├── sm.202508010100.tiff
```

---

## ▶️ Running the Script

### Requirements
- Linux/Unix environment with Bash
- Read/write permissions on target directories

### Command

```bash
bash sliper_tools_organizer_sm_file2folders.sh [SOURCE_FOLDER] [DESTINATION_FOLDER]
```

### Example

```bash
bash sliper_tools_organizer_sm_file2folders.sh ./data_dynamic/source/sm ./organized_data_dynamic/source/sm
```

---

## ⚙️ Notes

- The script reads filenames, extracts dates from them, and organizes them into year/month/day folders.
- It is recommended to **backup data before running**, as files are moved.

---

## 🧱 Suggested Folder Structure

```
project/
├── sliper_tools_organizer_sm_file2folders.sh
├── /data_dynamic/
│   ├── source/
│   │   └── sm/
│   └── organized_data_dynamic/
└── /log/
```

---

## 👞 Contact

For support or contributions:  
fabio.delogu@cimafoundation.org
