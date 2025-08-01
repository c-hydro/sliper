
# SLIPER APP - Soil Slips Data Processing

**SLIPER** (Soil Landslide Information and Prediction & Early Response) is a Python tool developed by ARPAL and CIMA Research Foundation for preprocessing soil slip datasets.

- **Version**: 1.0.0  
- **Release**: 2025-07-14  
- **Authors**: Fabio Delogu, Francesco Silvestro

---

## 🔧 How to Use

Run with:

```bash
python sliper_app_data_processing_slips_main.py -settings_file configuration.json -time "YYYY-MM-DD HH:MM"
```

- `-settings_file`: Path to the JSON config
- `-time`: Optional run time (format: YYYY-MM-DD HH:MM)

---

## 📂 Project Structure

- `app_data_processing_soil_slips_main.py`: Main script  
- `configuration.json`: Settings file  
- `data_static/`, `data_dynamic/`: Input/Output folders  
- `log/`: Log file directory  

---

## 📤 Output

Processed CSV files saved under `data_dynamic/destination/`, including time and alert area details. Logs go to `log/sliper_data_preprocessing_soil_slips_log.txt`.

---

## 📝 Notes

- Fully configurable via JSON
- Uses custom drivers for geographic and dynamic data handling

---

## 📬 Contact

For support:  
- fabio.delogu@cimafoundation.org  
- francesco.silvestro@cimafoundation.org
