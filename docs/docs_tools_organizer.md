# Dataset synchronization

The scripts in `sliper/tools/binaries/system/data/` synchronize rainfall and soil moisture files into the SLIPER workspace. Choose the JSON configuration for the input stream.

| Configuration suffix | Input stream |
| --- | --- |
| `obs` | Observations for the observed workflow. |
| `frc` | Forecasts for the selected model cycle. |
| `obs4frc` | Observations preceding a forecast cycle, for the forecast workflow. |
| `obs4storage` | Observations for storage. |

Set source and destination paths in the JSON template. Review the matching wrapper for its expected filenames, time windows, and environment settings.

The shared synchronization script accepts real-time and historical timestamps. To see its options, run:

```bash
bash sliper/tools/binaries/system/data/sliper_tools_sync_data.sh --help
```

The repository's `sliper/tools/binaries/system/data/sliper_tools_sync_README.txt` provides example invocations for observed and forecast data. See [forecast run selection](docs_tools_runner.md) for selecting the cycle used by forecast sync.
