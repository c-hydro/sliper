Saved as README.md

# SLIPER Toolkit

---

## ✨ Overview
SLIPER (Soil Landslide Information and Prediction & Early Response) is a Python framework for analyzing and validating soil moisture, rainfall, and related datasets. It provides workflows for data ingestion, transformation, indicators, predictors, scenario building, and visualization.

---

## 🔍 Features
- **Data Processing:** Tools for rain, soil moisture, and landslide datasets
- **Indicators:** Generate rainfall and soil moisture indicators
- **Predictors:** Prepare and train predictors
- **Scenarios:** Construct and analyze simulation scenarios
- **Visualization:** Plotting and reporting utilities
- **Utilities:** Merge, organize, transfer data, and execute workflows

All components are modular and configurable using YAML/JSON.

---

## 📦 Main Components

### Applications (`sliper/apps/`)
- **indicators/** – Rain and soil moisture indicators
- **predictors/** – Predictor creation and training
- **scenarios/** – Scenario configuration and execution

### Data Modules (`sliper/data/`)
- **rain/** – Rainfall datasets
- **slips/** – Landslide/slip data
- **sm/** – Soil moisture data

### Visualization (`sliper/plots/`)
- Plotting, geospatial visualization, and summary graphics

### Utilities (`sliper/utils/`)
- **merger/** – Merge datasets
- **organizer/** – Organize soil moisture files
- **runner/** – Real-time configuration scripts
- **transfer/** – Transfer datasets (FTP, rsync)

---

## 📂 Structure
```
.
├── sliper/
│   ├── apps/
│   ├── data/
│   ├── plots/
│   └── utils/
├── conda/
│   └── sliper_runner_data_settings/
├── ws/
│   ├── data_dynamic/
│   ├── data_static/
│   ├── log/
│   └── tmp/
├── README.md
├── CHANGELOG.md
├── LICENSE.md
├── AUTHORS.md
├── CODEOWNERS.md
└── setup_sliper_system_conda_runner_data.sh
```

---

## 🚀 Quick Start

### 1. Clone
```bash
git clone https://github.com/c-hydro/sliper.git
cd sliper
```

### 2. Setup Environment
```bash
conda create -n sliper_env python=3.10
conda activate sliper_env
bash setup_sliper_system_conda_runner_data.sh
```

### 3. Configure
Edit YAML configuration files in:
```
conda/sliper_runner_data_settings/
```

### 4. Run
```bash
python sliper/apps/sliper_runner.py -settings_file conda/sliper_runner_data_settings/your_config.yml
```
Outputs (logs, results, intermediate files) will be generated in the `ws/` directory.

---

## ▶ Runner Script Examples
The `sliper/utils/runner/` folder includes ready-made scripts:

**Predictors:**
```bash
bash sliper/utils/runner/sliper_tools_predictors_configuration_realtime.sh
```
**Scenarios:**
```bash
bash sliper/utils/runner/sliper_tools_scenarios_configuration_realtime.sh
```
Scripts can be customized to adjust paths, configuration, and runtime options.

---

## 📚 Documentation and References
- [CHANGELOG.md](CHANGELOG.md): Updates and version history
- [AUTHORS.md](AUTHORS.md): Contributors and maintainers
- [LICENSE.md](LICENSE.md): License details
- [CODEOWNERS.md](CODEOWNERS.md): Code ownership and responsibilities

---

SLIPER is designed for modular scientific workflows with reproducibility and extensibility in mind.

