"""
Library Features:

Name:          lib_utils_debug
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20260824'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import numpy as np
import matplotlib.pyplot as plt

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to analyze data array
def analyze_data_array(data_da, analysis_variable='rain', analysis_mode="default",
                       separetor_prefix="---->"):
    """
    Print diagnostics for an xarray.DataArray.

    Parameters
    ----------
    data_da : xr.DataArray
        DataArray to analyze.

    analysis_mode : str
        - "default"   : min, max, average, time start/end
        - "extensive" : full diagnostics and per-time-step statistics

    separator_length : int
        Number of "-" characters used for separators.
    """

    # check analysis mode
    if analysis_mode not in ["default", "extensive"]:
        raise ValueError(
            f'Analysis mode "{analysis_mode}" is not supported. '
            f'Use "default" or "extensive".'
        )

    # info start
    log_stream.info(f" {separetor_prefix} Analyze data array for variable {analysis_variable} ... ")

    # get values
    values = data_da.values
    mask_finite = np.isfinite(values)

    # default analysis
    if np.any(mask_finite):
        log_stream.info(f" -{separetor_prefix} Min        : {np.nanmin(values)}")
        log_stream.info(f" -{separetor_prefix} Max        : {np.nanmax(values)}")
        log_stream.info(f" -{separetor_prefix} Average    : {np.nanmean(values)}")
    else:
        log_stream.info(f" -{separetor_prefix} Min        : NA")
        log_stream.info(f" -{separetor_prefix} Max        : NA")
        log_stream.info(f" -{separetor_prefix} Average    : NA")

    # time information
    if "time" in data_da.coords and data_da.sizes.get("time", 0) > 0:
        log_stream.info(f" -{separetor_prefix} Time start : {data_da['time'].values[0]}")
        log_stream.info(f" -{separetor_prefix} Time end   : {data_da['time'].values[-1]}")

    # extensive analysis
    if analysis_mode == "extensive":

        log_stream.info(f" -{separetor_prefix} Object")

        log_stream.info(f" -{separetor_prefix} Type       : {type(data_da)}")
        log_stream.info(f" -{separetor_prefix} Name       : {data_da.name}")
        log_stream.info(f" -{separetor_prefix} Shape      : {data_da.shape}")
        log_stream.info(f" -{separetor_prefix} Dims       : {data_da.dims}")
        log_stream.info(f" -{separetor_prefix} Sizes      : {dict(data_da.sizes)}")
        log_stream.info(f" -{separetor_prefix} Dtype      : {data_da.dtype}")

        log_stream.info(f" -{separetor_prefix} Coordinates")

        for coord_name, coord_data in data_da.coords.items():
            log_stream.info(f" -{separetor_prefix} {coord_name:15s} shape={coord_data.shape}, dtype={coord_data.dtype}")

        log_stream.info(f" -{separetor_prefix} Values")

        log_stream.info(f" -{separetor_prefix} Total      : {values.size}")
        log_stream.info(f" -{separetor_prefix} Finite     : {np.count_nonzero(mask_finite)}")
        log_stream.info(f" -{separetor_prefix} NaN        : {np.count_nonzero(np.isnan(values))}")

        if np.any(mask_finite):
            log_stream.info(f" -{separetor_prefix} Std        : {np.nanstd(values)}")

        # statistics by time step
        if "time" in data_da.dims:

            log_stream.info(f" -{separetor_prefix} Time-step statistics")

            for time_id in range(data_da.sizes["time"]):

                values_step = data_da.isel(time=time_id).values
                mask_step = np.isfinite(values_step)
                time_step = data_da["time"].values[time_id]

                if np.any(mask_step):
                    log_stream.info(
                        f" -{separetor_prefix} [{time_id:03d}] "
                        f"time={time_step} | "
                        f"finite={np.count_nonzero(mask_step):8d} | "
                        f"min={np.nanmin(values_step):10.4f} | "
                        f"max={np.nanmax(values_step):10.4f} | "
                        f"avg={np.nanmean(values_step):10.4f}"
                    )
                else:
                    log_stream.info(f" -{separetor_prefix} [{time_id:03d}] time={time_step} | ALL NaN")
    # info end
    log_stream.info(f" {separetor_prefix} Analyze data array for variable {analysis_variable} ... DONE")
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to plot data 2d (debug mode active)
def plot_data_2d(
        data,
        debug=False,
        title=None, cmap=None, colorbar=True, block=True,
        vmin=None, vmax=None,
        prefix="---->", variable='rain'):

    if not debug:
        return

    # info start
    log_stream.info(f" {prefix} Plot data 2d for variable {variable} ... ")

    if data is None:
        log_stream.warning(" ===> Debug plot skipped: data is None")
        log_stream.info(f" {prefix} Plot data 2d for variable {variable} ... SKIPPED")
        return

    data = np.asarray(data)

    if data.ndim != 2:
        log_stream.warning(f" ===> Debug plot skipped: expected 2D data, found shape={data.shape}")
        log_stream.info(f" {prefix} Plot data 2d for variable {variable} ... SKIPPED")
        return

    plt.figure()
    plt.imshow(data, cmap=cmap, vmin=vmin, vmax=vmax)

    if title is not None: plt.title(title)
    if colorbar: plt.colorbar()

    plt.tight_layout()
    plt.show(block=block)

    # info end
    log_stream.info(f" {prefix} Plot data 2d for variable {variable} ... DONE")
# ----------------------------------------------------------------------------------------------------------------------
