# SLIPER – Tools Realtime Configuration (Scenarios and Predictors)

**Version:** 1.0.0  
**Release Date:** 2025-08-01  
**Authors:**  
- Fabio Delogu – fabio.delogu@cimafoundation.org  

---

## 📘 Description

This repository contains two shell scripts:

1. **sliper_tools_scenarios_configuration_realtime.sh**  
2. **sliper_tools_predictors_configuration_realtime.sh**  

These scripts simplify the **setup of configuration files for real-time execution** of the SLIPER Scenarios and Predictors applications.

---

## 📥 Purpose

- **Scenarios configuration script**:  
  Generates or updates configuration files for real-time Scenarios processing.

- **Predictors configuration script**:  
  Generates or updates configuration files for real-time Predictors processing.

---

## 📤 Outputs

- Automatically created or updated JSON configuration files for the relevant application (Scenarios or Predictors).

---

## ▶️ Running the Scripts

### Requirements
- Linux/Unix environment with Bash
- Proper directory structure for SLIPER applications
- Necessary permissions for writing configuration files

### Commands

```bash
bash sliper_tools_scenarios_configuration_realtime.sh
bash sliper_tools_predictors_configuration_realtime.sh
```

These scripts can be customized inside to fit specific **real-time operational chains**.

---

## 🧱 Suggested Folder Structure

```
project/
├── sliper_tools_scenarios_configuration_realtime.sh
├── sliper_tools_predictors_configuration_realtime.sh
├── /data_static/
├── /data_dynamic/
└── /log/
```

---

## ⚙️ Notes

- Modify these scripts to reflect your environment paths and operational needs.
- They are intended for operational deployment to keep configuration files updated.

---

## 👞 Contact

For support or contributions:  
fabio.delogu@cimafoundation.org
