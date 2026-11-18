# Changelog

All notable changes to this project are documented in this file.

---

## [3.3.1] - 2026-11-19

### Added
- Sphinx documentation and a Read the Docs build configuration.
- A guide to operational status reports and the HTML dashboard.

### Changed
- Updated the README and workflow guides for the current application layout, Conda setup, and operational wrappers.

## [3.3.0] – 2026-11-14

### Added
- Soil-slip ingestion from PostgreSQL alongside the existing CSV input, with a database-enabled Conda setup script.
- Observed dataset build wrappers for rainfall and soil moisture processing, indicators, scenarios, and predictors.
- Observed soil moisture build configurations for NetCDF and two HMC binary input layouts.
- Real-time forecast wrappers covering data processing, indicators, scenarios, predictors, and time-series analysis.
- Daily JSON reports for forecast-stage execution status and a static HTML dashboard with latest and hourly views.

### Changed
- Grouped operational wrappers under `sliper/tools/binaries/` by execution stage, analysis, reporting, and system tasks.

### Fixed
- Corrected predictor ordering when merging results into scenarios.
- Updated predictor and analysis time handling when prior simulations are missing.

## [3.2.0] – 2026-09-08
### Added
- Operational script configurations for managing and using datasets 
### Fixed
- Bug fixes in analysis rain and datasets in multi-year analysis

## [3.1.3] – 2026-08-23
### Added
- Script to debug and plot data array
### Fixed
- Bug fixes and improvements across all modules (manage observed and forecast datasets);
- Bug fixes for soil moisture and rain indicators

## [3.1.2] – 2026-06-28
### Fixed
- Bug fixes in indicators, scenarios and predictors applications;

## [3.1.1] – 2025-10-27
### Added
- Script to manage and organize run
- Script to sync forecast/observed datasets to realtime modes
### Fixed
- Bug fixes and improvements across all modules (manage observed and forecast datasets)

## [3.1.0] – 2025-10-27
### Added
- Operational release (forecast and observed mode)
- Applications to collect and manage datasets (rain, soil moisture and soil slips)
- Applications to define indicators (rain and soil moisture)
- Applications to define scenarios
- Applications to define predictors
- Applications to analyze indicators, scenarios and predictors daily time-series

## [3.0.0] – 2025-09-04
### Added
- Unified SLIPER runner integrating previous tools under a single modular framework
- Workspace-based I/O structure (data_static, data_dynamic, tmp, log)
- Automated conda-based environment provisioning
- Structured logging and error tracking mechanisms
- Add applications to process rain, soil moisture and soil slips datasets
- Add applications to define rain and soil moisture indicators
- Add applications to define scenarios
- Add applications to define predictors
- Add applications to view scenarios/predictors results
- Add utils and tools to manage datasets (organizer and checker for sm and rain datasets)

### Changed
- Refactored runner logic for maintainability and consistency
- Enhanced compatibility with legacy soil slips predictors and scenario tools
- Streamlined setup via `setup_sliper_system_conda_runner_data.sh`

---

## [2.6.0] – 2025-05-13
### Fixed
- Time selection issues in soil slips scenarios
- Indicators computation (soil moisture)
- Database format change from shapefile to CSV

---

## [2.5.0] – 2024-10-10
### Fixed
- Time selection bugs for rain and soil moisture
- Indicator computation inconsistencies
- Added support for left/right temporal selection

---

## [2.4.0] – 2024-07-10
### Fixed
- Regridding rain maps using `pyresample`
- ASCII grid EPSG errors
- Added `GDAL_DATA` dependency

---

## [2.3.0] – 2024-01-16
### Added
- New viewer for soil slips scenario analysis

---

## [2.2.0] – 2023-01-18
### Added
- Support for XLS/XLSX station input
- Auto-detection of soil slips alert area

---

## [2.1.2] – 2022-10-13
### Fixed
- Kernel function errors in predictors module

---

## [2.1.1] – 2022-04-13
### Fixed
- Multiple bugs across predictors and scenarios tools
- Refactored internal methods

---

## [2.1.0] – 2022-03-20
### Notes
- Pre-operational release

---

## [2.0.0] – 2022-04-13
### Notes
- Pre-operational release with scenarios and predictors apps

---

## [1.4.1] – 2022-01-05
### Added
- Jupyter analysis for soil slips time series

---

## [1.4.0] – 2021-05-15
### Added
- Rain limits check
- Unified `.csv` file output for scenarios
- Rain peaks computation fix

---

## [1.3.0] – 2021-04-12
### Added
- Dependency management improvements
- Rain peaks forcing creation

---

## [1.2.1] – 2021-03-19
### Fixed
- Output indicators and scenario generation bugs

---

## [1.2.0] – 2021-02-02
### Fixed
- Output CSV formatting and rain dataset generation

---

## [1.1.0] – 2020-11-25
### Changed
- Updated readers/writers for rain and soil moisture variables

---

## [1.0.0] – 2020-05-15
### Notes
- Initial beta release of `scenarios_main.py`

---

## [0.0.1] – 2020-05-10
### Notes
- First commit and initialization

