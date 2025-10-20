"""
Library Features:

Name:          lib_utils_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220420'
Version:       '1.5.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging

import numpy as np
import xarray as xr
from pyresample import geometry
from pyresample.kd_tree import get_neighbour_info, resample_nearest, resample_custom

from lib_info_args import logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to resample data from source to target grid using pyresample
def resample_data(
        source_da,
        target_da,
        method='nearest',
        radius_of_influence=50000,
        neighbours=1,
        index_array=None,
        weight_array=None,
        valid_output_index=None
):
    """
    Regrid source_da to target_da using either precomputed indices or direct pyresample.

    Parameters:
        source_da (xarray.DataArray): Source data with lat/lon coordinates.
        target_da (xarray.DataArray): Target grid with lat/lon coordinates.
        method (str): 'nearest' or 'weighted'.
        radius_of_influence (float): Search radius for neighbors (meters).
        neighbours (int): Number of neighbors to use in interpolation.
        index_array (np.ndarray): Precomputed source indices for target points.
        weight_array (np.ndarray): Precomputed interpolation weights.
        valid_output_index (np.ndarray): Valid output target indices.

    Returns:
        xarray.DataArray: Regridded output with target shape.
    """

    # Ensure source and target coordinates are 2D
    lons_src, lats_src = np.meshgrid(source_da.longitude.values, source_da.latitude.values)
    lons_tgt, lats_tgt = np.meshgrid(target_da.longitude.values, target_da.latitude.values)

    source_def = geometry.SwathDefinition(lons=lons_src, lats=lats_src)
    target_def = geometry.SwathDefinition(lons=lons_tgt, lats=lats_tgt)

    source_data = source_da.values
    source_flat = source_data.ravel()

    n_target = target_da.size
    target_flat = np.full(n_target, np.nan)

    if index_array is not None and valid_output_index is not None:
        if method == 'nearest':
            nearest_idx = index_array[:, 0]
            target_flat[valid_output_index] = source_flat[nearest_idx]

        elif method == 'weighted' and weight_array is not None:
            for i, (src_idxs, weights) in enumerate(zip(index_array, weight_array)):
                wsum = np.sum(weights)
                if wsum > 0:
                    target_flat[valid_output_index[i]] = np.sum(source_flat[src_idxs] * weights) / wsum
        else:
            raise ValueError("For 'weighted' method, weight_array must be provided.")
    else:
        # Fallback to direct method from pyresample
        if method == 'nearest':
            result = resample_nearest(
                source_geo_def=source_def,
                data=source_data,
                target_geo_def=target_def,
                radius_of_influence=radius_of_influence,
                fill_value=np.nan
            )
            target_flat = result.ravel()

        elif method == 'weighted':
            result = resample_custom(
                source_geo_def=source_def,
                data=source_data,
                target_geo_def=target_def,
                radius_of_influence=radius_of_influence,
                neighbours=neighbours,
                weight_funcs=None,
                fill_value=np.nan
            )
            target_flat = result.ravel()

        else:
            raise ValueError("Unsupported method. Use 'nearest' or 'weighted'.")

    # Reshape to match the target grid
    reshaped = target_flat.reshape(target_da.shape)

    return xr.DataArray(
        data=reshaped,
        coords=target_da.coords,
        dims=target_da.dims,
        name=(source_da.name + '_regridded') if source_da.name else None,
        attrs=source_da.attrs
    )
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to resample the index and weights
def resample_index(source_da, target_da, radius_of_influence=25000, neighbours=1, epsilon=1e-6):
    """
    Compute the index and weights to regrid source_da over target_da using pyresample.

    Parameters:
        source_da (xarray.DataArray): Source data with lat/lon coordinates.
        target_da (xarray.DataArray): Target grid with lat/lon coordinates.
        radius_of_influence (float): Radius in meters to search for neighbours.
        neighbours (int): Number of neighbours to use in interpolation.
        epsilon (float): A small number to avoid zero distance issues.

    Returns:
        tuple: (valid_input_index, valid_output_index, index_array, weight_array)
    """

    # Extract lat/lon for source and target
    lons_src = source_da.longitude.values
    lats_src = source_da.latitude.values
    lons_tgt = target_da.longitude.values
    lats_tgt = target_da.latitude.values

    # Handle 1D vs 2D coordinate grids
    if lons_src.ndim == 1:
        lons_src, lats_src = np.meshgrid(lons_src, lats_src)
    if lons_tgt.ndim == 1:
        lons_tgt, lats_tgt = np.meshgrid(lons_tgt, lats_tgt)

    # Define pyresample SwathDefinition objects
    source_def = geometry.SwathDefinition(lons=lons_src, lats=lats_src)
    target_def = geometry.SwathDefinition(lons=lons_tgt, lats=lats_tgt)

    # Compute the lookup index and weights
    valid_input_index, valid_output_index, index_array, weight_array = get_neighbour_info(
        source_geo_def=source_def,
        target_geo_def=target_def,
        radius_of_influence=radius_of_influence,
        neighbours=neighbours,
        epsilon=epsilon,
        nprocs=1  # Parallelism if desired
    )

    return valid_input_index, valid_output_index, index_array, weight_array
# ----------------------------------------------------------------------------------------------------------------------
