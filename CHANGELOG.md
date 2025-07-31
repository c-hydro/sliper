# Changelog

All notable changes to this project are documented in this file.

---

## [3.0.0] – 2025-07-30
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
- Add utils and tools to manage datasets

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

