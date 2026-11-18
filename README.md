# SLIPER

SLIPER (Soil Landslide Information and Prediction & Early Response) is an open-source toolkit for rainfall-triggered landslide forecasting and early warning, developed by CIMA Research Foundation and ARPAL Liguria. It combines rainfall, soil moisture and observed soil slips by alert area to produce predictors for nowcasting and forecasting.

## Applications

| Stage | What it does | Guide |
| --- | --- | --- |
| Data processing | Prepares rainfall and soil moisture grids and normalizes soil-slip records from CSV or a configured database. | [Rainfall](docs/docs_app_data_rain.md) · [Soil moisture](docs/docs_app_data_sm.md) · [Soil slips](docs/docs_app_data_slips.md) |
| Indicators | Computes rainfall and soil moisture indicators by alert area and time window. | [Rainfall indicators](docs/docs_app_indicators_rain.md) · [Soil moisture indicators](docs/docs_app_indicators_sm.md) |
| Scenarios | Joins indicators and soil-slip observations into scenario tables. | [Scenarios](docs/docs_app_scenarios.md) |
| Predictors | Uses scenarios and training data to estimate soil-slip occurrence. | [Predictors](docs/docs_app_predictors.md) |
| Analysis | Plots scenario and predictor time series for observed and forecast runs. | [Time-series viewer](docs/docs_app_viewer.md) |
| Reporting | Summarizes execution status from run metadata and renders a static HTML dashboard. | [Status report and dashboard](docs/docs_app_report.md) |

The operational wrappers in [`sliper/tools/binaries/`](sliper/tools/binaries/) include real-time observed and forecast chains, historical processing, and configurations for building observed datasets. The [workflow guide](docs/docs_sliper_workflows.md) describes run modes and where to find the wrappers and settings.

## Set up and run

Clone the repository and use the supplied Linux x86-64 Conda setup script to create a Python 3.11 environment. Pass a dedicated install directory for the new Conda installation.

```bash
git clone https://github.com/c-hydro/sliper.git
cd sliper
bash setup_sliper_system_conda_data.sh sliper_data "$HOME/.local/sliper-data"
source "$HOME/.local/sliper-data/sliper_data_settings"
```

Use [`setup_sliper_system_conda_db.sh`](setup_sliper_system_conda_db.sh) for the soil-slip ingestion application, which imports the PostgreSQL client for both source modes. Database runs also require access to the source database and its credentials.

Copy the relevant JSON configuration and set the input and output paths, alert-area data, and time windows for your deployment. For example, to run the forecast rainfall indicator application from the repository root:

```bash
cp sliper/apps/indicators/rain/sliper_app_indicators_rain_configuration_frc.json rainfall-forecast.json
# Edit rainfall-forecast.json for your data and workspace.
python sliper/apps/indicators/rain/sliper_app_indicators_rain_main.py \
  -settings_file rainfall-forecast.json \
  -time "2026-11-14 00:00"
```

For scheduled operation, use the matching shell wrapper and JSON template under [`sliper/tools/binaries/`](sliper/tools/binaries/). Review the environment, paths, and scheduling settings in the wrapper before running it.

## Documentation and releases

- [Documentation index](docs/index.md)
- [Changelog](CHANGELOG.md)
- [Authors](AUTHORS.md), [maintainers](CODEOWNERS.md), and [license](LICENSE.md)

To preview the documentation locally, run the following commands with Python 3.11 in a virtual environment:

```bash
python -m pip install -r docs/requirements.txt
python -m sphinx -W -b html docs /tmp/sliper-docs-html
```

Open `/tmp/sliper-docs-html/index.html` to view the generated site.
