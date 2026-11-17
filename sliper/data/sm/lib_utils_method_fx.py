"""
Library Features:

Name:          lib_utils_method_fx
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261020'
Version:       '1.1.0'
"""
# -------------------------------------------------------------------------------------
# Libraries
import logging
import numpy as np

from lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Method to convert volume to soil moisture
def convert_volume2sm(
        values_vtot: np.ndarray, values_vmax: np.ndarray = None, values_geo: np.ndarray = None,
        values_min: float = 0.0, values_max: float = 100.0, debug: bool = False) -> np.ndarray:

    # Check input
    if values_vmax is None:
        return None

    # Convert input to numpy arrays
    values_vtot = np.asarray(values_vtot, dtype=np.float32)
    values_vmax = np.asarray(values_vmax, dtype=np.float32)

    # Check volume dimensions
    if values_vtot.ndim not in (2, 3):
        raise ValueError('Volume total must be a 2D or 3D array.')

    # Adjust maximum volume dimensions
    if values_vtot.ndim == 3 and values_vmax.ndim == 2:
        values_vmax = values_vmax[:, :, np.newaxis]

    # Check spatial dimensions and broadcasting compatibility
    try:
        np.broadcast_shapes(values_vtot.shape, values_vmax.shape)
    except ValueError:
        raise ValueError(f'Volume dimensions are not compatible: vtot={values_vtot.shape}, vmax={values_vmax.shape}')

    # Compute soil moisture
    with np.errstate(divide='ignore', invalid='ignore'):
        values_sm = np.divide(values_vtot, values_vmax,
            out=np.full(values_vtot.shape, np.nan, dtype=np.float32), where=np.isfinite(values_vmax) & (values_vmax > 0))
    values_sm *= 100.0

    # Clip soil moisture values
    values_sm = np.clip(values_sm, values_min, values_max)

    # Apply geographical mask
    if values_geo is not None:
        values_geo = np.asarray(values_geo)

        # Adjust geographical dimensions
        if values_vtot.ndim == 3 and values_geo.ndim == 2:
            values_geo = values_geo[:, :, np.newaxis]

        # Check dimensions
        try:
            np.broadcast_shapes(values_sm.shape, values_geo.shape)
        except ValueError:
            raise ValueError(
                f'Geographical dimensions are not compatible: vtot={values_vtot.shape}, geo={values_geo.shape}')

        # Apply terrain mask
        values_sm = np.where(values_geo < 0, np.nan, values_sm)

    # Plot debug
    if debug:

        def to_2d(data):
            if data is None:
                return None
            return data[:, :, 0] if data.ndim == 3 else data

        fig, axes = plt.subplots(2, 2, figsize=(12, 8))

        for ax, (title, data) in zip(
                axes.flat, [('Volume Total', values_vtot), ('Volume Max', values_vmax),
                            ('Terrain', values_geo), ('Soil Moisture', values_sm)]):

            if data is not None:
                im = ax.imshow(to_2d(data))
                ax.set_title(title)
                plt.colorbar(im, ax=ax)

        plt.tight_layout()
        plt.show()

    return values_sm
# -------------------------------------------------------------------------------------
