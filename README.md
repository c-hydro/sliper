# SLIPER Toolkit

**SLIPER** (Soil Moisture and Land Surface Interactions for Prediction and Evaluation of Rainfall) is a Python-based framework for analyzing and evaluating hydrometeorological datasets, with a focus on rainfall estimation, soil moisture interactions, and remote sensing product validation.

---

## 📦 Version

**Current version: 3.0.0**  
This release integrates and improves upon previous algorithms, with a focus on unified structure, optimized performance, and enhanced modularity.

Key updates in 3.0.0:
- Consolidated legacy components from 2.x releases
- Enhanced configuration management with dynamic YAML handling
- Refactored runner logic for improved maintainability
- Structured logging and result tracking
- Smoother conda-based deployment with a self-contained environment

---

## 📁 Project Structure

```plaintext
.
├── sliper/                   # Main source code
│   ├── apps/                # Runner scripts and applications
│   ├── data/                # Data handling modules
│   └── utils/               # Utility functions and tools
├── conda/                   # Self-contained conda environment
│   └── sliper_runner_data_settings/  # Workflow configuration files
├── ws/                      # Runtime workspace
│   ├── data_dynamic/        # Dynamic data (e.g. rainfall, model output)
│   ├── data_static/         # Static data (e.g. terrain, masks)
│   ├── log/                 # Logging directory
│   └── tmp/                 # Temporary processing files
├── docs/                    # Supplementary documentation
├── example/                 # (To be populated) example config/data
├── test/                    # Unit and integration tests
├── old/                     # Legacy code (e.g., soilslips-dev)
├── scripts                  # Setup scripts (e.g. miniconda.sh)
├── README.md                # Project readme (this file)
├── LICENSE.md               # License information
└── *.sh                     # Environment setup scripts
```

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/c-hydro/sliper.git
cd sliper
```

### 2. Set Up Environment

Run the provided setup script (which uses the embedded Miniconda environment):

```bash
bash setup_sliper_system_conda_runner_data.sh
```

> 📝 Alternatively, use `miniconda.sh` to install Miniconda manually if needed.

### 3. Configure Settings

Edit a YAML file from:

```
conda/sliper_runner_data_settings/
```

These files define the workflow settings (e.g., input/output paths, models, time ranges).

### 4. Run an Application

```bash
python sliper/apps/sliper_runner.py -settings_file conda/sliper_runner_data_settings/<your_config>.yml
```

Output will be written to the `ws/` directory.

---

## ⚙️ Core Features

- **Flexible configuration** via YAML files
- **Support for multiple data formats** (NetCDF, GeoTIFF, HDF5)
- **Satellite precipitation product evaluation**
- **Statistical metrics:** RMSE, correlation, bias, etc.
- **Modular structure** for easy customization
- **Full backward compatibility** with legacy algorithms (v2.x series)

---

## 🧪 Testing

Tests are located in the `test/` directory and can be run using `pytest`:

```bash
pytest test/
```

---

## 📄 Documentation

Basic structure and usage notes are in `docs/`. A future update will include full documentation with function references and tutorials.

---

## 👤 Authors & Contributors

See [`AUTHORS.md`](./AUTHORS.md) for full credits.

---

## 📜 License

Distributed under the terms of the [MIT License](./LICENSE.md).

---

## 🔄 Changelog

Refer to [`CHANGELOG.md`](./CHANGELOG.md) for a history of changes and releases.

---

## 🚲 Acknowledgements

Part of the **C-Hydro** initiative for hydrological modeling and satellite data integration.

