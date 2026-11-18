# Forecast run selection

The scripts in `sliper/tools/binaries/system/run/` select the forecast cycle used by the forecast wrappers. The selector reads the configured forecast model metadata and writes a run-information JSON file. The real-time wrapper also records an `.info` file for the status report.

Set the input and output paths in `sliper_tools_select_run.json` and review the environment and workspace settings in the shell scripts. From the repository root, list the wrapper options with:

```bash
bash sliper/tools/binaries/system/run/sliper_tools_wrap_run.sh --help
```

In a configured deployment, pass `--run-time HH:MM` to select a specific cycle for the current day. For historical run selection, use `sliper_tools_select_run.sh --mode history --date YYYY-MM-DD --time HH:MM --config /path/to/config.json`.

Once the run is selected, [synchronize the input datasets](docs_tools_organizer.md) and run the [application workflow](docs_sliper_workflows.md).
