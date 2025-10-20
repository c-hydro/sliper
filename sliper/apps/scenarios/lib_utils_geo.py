"""
Library Features:

Name:          lib_utils_geo
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250618'
Version:       '1.5.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import warnings

import numpy as np
import xarray as xr
import pandas as pd

from functools import wraps
from typing import Union, List

import pyproj
from scipy.interpolate import griddata
from copy import deepcopy

from pyresample import geometry
from pyresample.kd_tree import get_neighbour_info, resample_nearest, resample_custom

from functools import partial
from shapely.ops import transform
from shapely.geometry import Point
from shapely.vectorized import contains

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
warnings.filterwarnings("ignore", category=FutureWarning, module="shapely")

# debugging
# import matplotlib.pylab as plt
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to join circle and its grid
def find_points_with_buffer(points_values, points_x_values_2d, points_y_values_2d,
                            mask_values, mask_x_values_2d, mask_y_values_2d,
                            point_buffer=10, tag_point_name='point_{:}'):
    points_collections = {}
    points_summary = np.zeros(shape=(mask_values.shape[0], mask_values.shape[1]))
    points_summary[:, :] = np.nan
    for point_id, (point_mask, point_x, point_y) in enumerate(
            zip(points_values.ravel(), points_x_values_2d.ravel(), points_y_values_2d.ravel())):

        points_map = np.ones(shape=(mask_values.shape[0], mask_values.shape[1]))
        if point_mask == 1:
            mask_polygon, mask_coords = compute_geodesic_point_buffer(point_x, point_y, point_buffer)
            points_mask = contains(mask_polygon, mask_x_values_2d, mask_y_values_2d)
            points_masked = np.ma.masked_array(points_map, points_mask)

            points_data = points_masked.data
            points_data[~points_mask] = np.nan
            points_data[mask_values == 0] = np.nan

            points_idxs = np.nonzero(points_data == 1)

            points_collections[tag_point_name.format(point_id)] = points_idxs
            points_summary[points_idxs] = point_id

    # plt.figure()
    # plt.imshow(mask_values)
    # plt.colorbar()
    # plt.figure()
    # plt.imshow(points_summary)
    # plt.colorbar()
    # plt.show()

    return points_collections

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Method to compute grid from bounds
def compute_grid_from_bounds(geo_mask_2d, geo_x_2d, geo_y_2d, km=10):

    # Get geo boundary
    geo_x_min, geo_x_max = np.min(geo_x_2d), np.max(geo_x_2d)
    geo_y_min, geo_y_max = np.min(geo_y_2d), np.max(geo_y_2d)

    # Set up transformers, EPSG:3857 is metric, same as EPSG:900913
    to_proxy_transformer = pyproj.Transformer.from_crs('epsg:4326', 'epsg:3857')
    to_original_transformer = pyproj.Transformer.from_crs('epsg:3857', 'epsg:4326')

    # Create corners of rectangle to be transformed to a grid
    geo_sw = Point((geo_x_min, geo_y_min))
    geo_ne = Point((geo_x_max, geo_y_max))
    # Define step size
    step_size = km * 1000  # km grid step size

    # Project corners to target projection
    transformed_sw = to_proxy_transformer.transform(geo_sw.x, geo_sw.y)  # Transform NW point to 3857
    transformed_ne = to_proxy_transformer.transform(geo_ne.x, geo_ne.y)  # .. same for SE

    # Iterate over 2D area
    collection_point_obj, collection_points_x, collection_points_y = [], [], []
    x = transformed_sw[0]
    while x < transformed_ne[0]:
        y = transformed_sw[1]
        while y < transformed_ne[1]:
            p = Point(to_original_transformer.transform(x, y))
            collection_point_obj.append(p)
            collection_points_x.append(p.x)
            collection_points_y.append(p.y)
            y += step_size
        x += step_size

    point_x_1d = np.unique(np.array(collection_points_x).astype(float))
    point_y_1d = np.unique(np.array(collection_points_y).astype(float))

    point_x_2d, point_y_2d = np.meshgrid(point_x_1d, point_y_1d)
    point_y_upper, point_y_lower = point_y_2d[0, 0], point_y_2d[-1, 0]
    if point_y_lower > point_y_upper:
        point_y_2d = np.flipud(point_y_2d)

    point_mask_2d = griddata((geo_x_2d.ravel(), geo_y_2d.ravel()), geo_mask_2d.ravel(),
                             (point_x_2d, point_y_2d), method='nearest', fill_value=-9999.0)

    return point_mask_2d, point_x_2d, point_y_2d

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to define a buffer around a geographical point
def compute_geodesic_point_buffer(lon, lat, km=10):

    # Azimuthal equidistant projection
    local_azimuthal_projection = '+proj=aeqd +R=6371000 +units=m +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'

    wgs84_to_aeqd = partial(
        pyproj.transform,
        pyproj.Proj('+proj=longlat +datum=WGS84 +no_defs'),
        pyproj.Proj(local_azimuthal_projection.format(lat=lat, lon=lon)),
    )

    aeqd_to_wgs84 = partial(
        pyproj.transform,
        pyproj.Proj(local_azimuthal_projection.format(lat=lat, lon=lon)),
        pyproj.Proj('+proj=longlat +datum=WGS84 +no_defs'),
    )

    point_obj = Point(lon, lat)

    point_aeqd = transform(wgs84_to_aeqd, point_obj)
    buffer = point_aeqd.buffer(km * 1000)  # distance in metres
    points_wgs84 = transform(aeqd_to_wgs84, buffer)

    point_polygon = deepcopy(points_wgs84)
    points_coords = points_wgs84.exterior.coords[:]

    return point_polygon, points_coords
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# decorator to apply a function to each spatial slice of a DataArray
def apply_to_spatial_slices(func):
    @wraps(func)
    def wrapper(source_da, target_da, *args, **kwargs):

        # check if source_da is 2D
        if source_da.ndim == 2:
            return func(source_da, target_da, *args, **kwargs)

        # define spatial and non-spatial dimensions
        spatial_dims = ['latitude', 'longitude']
        non_spatial_dim = ['time']

        non_spatial_idx = None
        if non_spatial_dim in list(source_da.dims):
            non_spatial_idx = list(source_da.dims).index(non_spatial_dim)

        # check expected and data dimensions
        all_dims = []
        all_dims.extend(spatial_dims)
        all_dims.extend(non_spatial_dim)

        if all_dims.__len__() != source_da.ndim:
            raise ValueError("Source DataArray must have 2D or ND with last two dims as spatial.")

        # apply function to each 2D slice
        regridded_list, time_list = [], []
        for idx in np.ndindex(*[source_da.sizes[dim] for dim in non_spatial_dim]):

            time_step = pd.Timestamp(source_da[non_spatial_dim[0]].values[idx])  # Get the current index for non-spatial dims

            slice_time = {non_spatial_dim[0]: pd.DatetimeIndex([time_step])}
            slice_2d = source_da.sel(slice_time)

            regridded_slice = func(slice_2d, target_da, *args, **kwargs)
            regridded_slice.expand_dims(slice_time)

            time_list.append(time_step)
            regridded_list.append(regridded_slice)

        # combine the regridded slices into a single DataArray
        combined_tyx = xr.combine_nested(regridded_list, concat_dim=non_spatial_dim)
        # reorder dimensions to match target grid
        combined_yxt = combined_tyx.transpose('latitude', 'longitude', 'time')
        combined_yxt['time'] = pd.DatetimeIndex(time_list)

        return combined_yxt
    return wrapper
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to resample data from source to target grid using pyresample
@apply_to_spatial_slices
def resample_data(
        source_da,
        target_da,
        name_da=None,
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
        nsme_da (str): Name for the output DataArray. If None, uses source_da name with '_regridded' suffix.
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
            if index_array.ndim == 1:
                # If index_array is 1D, it means each target point has a single nearest source point
                nearest_idx = index_array
            elif index_array.ndim == 2:
                # If index_array is 2D, take the first column as nearest indices
                nearest_idx = index_array[:, 0]
            else:
                raise ValueError("index_array must be 1D or 2D.")
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

    if name_da is None:
       name_da = source_da.name + '_regridded' if source_da.name else None

    return xr.DataArray(
        data=reshaped,
        coords=target_da.coords,
        dims=target_da.dims,
        name=name_da,
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


# ----------------------------------------------------------------------------------------------------------------------
# method to mask data with a 2D mask
def mask_data(data_array: xr.DataArray, mask_array: xr.DataArray,
              mask_value: int = 1, mask_along_dim: str = 'time') -> xr.DataArray:
    """
    Apply a 2D mask to a 2D or 3D xarray DataArray.

    Parameters:
    - data_array: xr.DataArray
        The data to be masked. Can be 2D (lat, lon) or 3D (time, lat, lon).
    - mask_array: xr.DataArray
        A 2D (lat, lon) mask. Values equal to mask_value are retained.
    - mask_value: int, default=1
        The value in the mask to retain in the data_array.
    - mask_along_dim: str, default='time'
        The dimension along which to broadcast the mask if data_array is 3D.
        This should match one of the dimensions in data_array.

    Returns:
    - xr.DataArray
        The masked data with NaNs where the mask condition is not met.
    """
    # Ensure mask is boolean
    mask_bool = mask_array == mask_value

    # If data_array is 3D, broadcast the mask across the time dimension
    if data_array.ndim == 3 and mask_along_dim in data_array.dims:
        # Align the dimensions by expanding the mask
        mask_bool = mask_bool.expand_dims({mask_along_dim: data_array.sizes[mask_along_dim]}, axis=0)
        mask_bool = mask_bool.transpose(*data_array.dims)

    # Check shape compatibility
    if data_array.shape != mask_bool.shape:
        raise ValueError("Mask shape does not align with data_array shape after broadcasting.")

    return data_array.where(mask_bool)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to transform data 2D or 3D to time series
def transform_data2ts(
    da_var: xr.DataArray,
    column_name: Union[str, List[str], None] = None,
    dim_name_time: str = 'time',
    dim_name_geo_x: str = 'longitude',
    dim_name_geo_y: str = 'latitude'
) -> pd.DataFrame:
    """
    Convert a 2D or 3D spatial-temporal rainfall DataArray into a time-series DataFrame.

    Parameters:
        da_var (xr.DataArray): Input DataArray with time and spatial dimensions.
        column_name (str | list[str] | None): Name(s) of the output column(s).
        dim_name_time (str): Name of the time dimension.
        dim_name_geo_x (str): Name of the x spatial dimension.
        dim_name_geo_y (str): Name of the y spatial dimension.

    Returns:
        pd.DataFrame: Time-indexed DataFrame with spatially averaged values.
    """
    # Input validation
    if not isinstance(da_var, xr.DataArray):
        raise TypeError("da_var must be an xarray.DataArray.")
    if dim_name_time not in da_var.dims:
        raise ValueError(f"Dimension '{dim_name_time}' not found in DataArray.")
    if da_var.ndim not in [2, 3]:
        raise ValueError("da_var must be either 2D or 3D including the time dimension.")

    # Normalize column name
    if column_name is None:
        column_name = ['data_time_series']
    elif isinstance(column_name, str):
        column_name = [column_name]
    elif not isinstance(column_name, list):
        raise TypeError("column_name must be a string, list of strings, or None.")

    # Handle 3D (time + spatial) case
    if dim_name_geo_x in da_var.dims and dim_name_geo_y in da_var.dims:
        da_avg = da_var.mean(dim=[dim_name_geo_y, dim_name_geo_x])
    # Handle 2D (time + one spatial dimension) case
    elif dim_name_geo_x in da_var.dims:
        da_avg = da_var.mean(dim=dim_name_geo_x)
    elif dim_name_geo_y in da_var.dims:
        da_avg = da_var.mean(dim=dim_name_geo_y)
    else:
        raise ValueError("At least one spatial dimension must be present for averaging.")

    # Ensure computation is performed (especially for lazy-loaded data)
    values_period_avg = da_avg.compute().values
    times_period_index = pd.DatetimeIndex(da_var[dim_name_time].values)

    # Create DataFrame
    dframe_var = pd.DataFrame(index=times_period_index, data=values_period_avg,
                              columns=column_name).fillna(value=pd.NA)

    return dframe_var
# ----------------------------------------------------------------------------------------------------------------------
