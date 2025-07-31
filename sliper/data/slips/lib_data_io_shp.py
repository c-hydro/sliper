"""
Library Features:

Name:          lib_data_io_shp
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250709'
Version:       '1.0.0'
"""


# -------------------------------------------------------------------------------------
# libraries
import logging
import warnings
import os

from copy import deepcopy
from osgeo import osr, ogr, gdal

import geopandas as gpd

from lib_data_io_geo import read_file_grid

from lib_utils_data_geo import normalize_crs
from lib_utils_generic import create_filename_tmp

from lib_info_args import proj_wkt as proj_default_wkt
from lib_info_args import logger_name

from shapely.errors import ShapelyDeprecationWarning

# logging
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)
logging.getLogger("fiona").setLevel(logging.WARNING)
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to read shape file
def read_file_shp(file_name, file_fields=None, file_epsg=4326):

    # read the shapefile using geopandas
    shape_dframe = gpd.read_file(file_name)

    # apply fields filtering and renaming if provided
    if file_fields is not None and file_fields:
        # define filter map
        map_filter = deepcopy(file_fields)
        # filter the DataFrame to keep only the specified fields
        tmp = shape_dframe[[v for v in map_filter.values() if v in shape_dframe.columns]]

        # define rename map
        map_rename = {v: k for k, v in file_fields.items()}
        # rename the columns
        shape_dframe = tmp.rename(columns=map_rename)

    # ensure the EPSG code is normalized
    file_epsg = normalize_crs(file_epsg)
    # ensure the shapefile has the correct CRS
    shape_dframe = shape_dframe.to_crs(file_epsg)

    # check if the geometry column is present
    shape_geoms = ((feature['geometry'], 1) for feature in shape_dframe.iterfeatures())
    # convert geometries to Shapely objects
    shape_datasets = list(shape_dframe.values)

    # create a dictionary to hold the polygons
    shape_polygons = {}
    for shape_group in shape_datasets:
        shape_id, shape_coords = shape_group[0], shape_group[1]
        shape_polygons[shape_id] = shape_coords

    return shape_dframe, shape_polygons, shape_geoms
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Convert polygons to shape file
def convert_polygons_2_shp(shape_polygon, shape_file):

    driver = ogr.GetDriverByName('Esri Shapefile')
    ds = driver.CreateDataSource(shape_file)

    spatial_reference = osr.SpatialReference()
    spatial_reference.ImportFromWkt(proj_default_wkt)

    if shape_polygon.type == 'MultiPolygon':
        layer = ds.CreateLayer('', spatial_reference, ogr.wkbMultiPolygon)
    elif shape_polygon.type == 'Polygon':
        layer = ds.CreateLayer('', spatial_reference, ogr.wkbPolygon)
    else:
        raise IOError('Shape type not implemented yet')
    defn = layer.GetLayerDefn()
    feat = ogr.Feature(defn)

    # Make a geometry, from Shapely object
    geom = ogr.CreateGeometryFromWkb(shape_polygon.wkb)
    feat.SetGeometry(geom)

    layer.CreateFeature(feat)
    feat = geom = None  # destroy these

    # Save and close everything
    ds = layer = feat = geom = None
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# Method to transform shape file to tiff
def convert_shp_2_tiff(shape_file, tiff_file=None, tiff_remove=True,
                       pixel_size=0.1, burn_value=1, epsg=4326, folder_tmp=None):

    # read the shape file
    input_shp = ogr.Open(shape_file)
    shp_layer = input_shp.GetLayer()

    # create a temporary folder if not specified
    if folder_tmp is None:
        # create a temporary folder in the same directory of the shape file
        folder_tmp = os.path.dirname(shape_file)

    if tiff_file is not None:
        grid_file = tiff_file
    else:
        grid_file = create_filename_tmp(folder=folder_tmp)

    # convert shapefile to raster
    folder_grid, file_grid = os.path.split(grid_file)
    os.makedirs(folder_grid, exist_ok=True)

    # get extent values to set size of output raster.
    x_min, x_max, y_min, y_max = shp_layer.GetExtent()
    # get spatial reference
    shp_crs = shp_layer.GetSpatialRef()
    if shp_crs is None:
        log_stream.warning(' ===> Spatial reference of "' + shape_file + '" is not defined.')

    # calculate size/resolution of the raster.
    x_res = int((x_max - x_min) / pixel_size)
    y_res = int((y_max - y_min) / pixel_size)

    # get GeoTiff driver by
    image_type = 'GTiff'
    driver = gdal.GetDriverByName(image_type)

    # passing the filename, x and y direction resolution, no. of bands, new raster.
    raster_handle = driver.Create(grid_file, x_res, y_res, 1, gdal.GDT_Byte)

    # transforms between pixel raster space to projection coordinate space.
    raster_handle.SetGeoTransform((x_min, pixel_size, 0, y_min, 0, pixel_size))

    # get required raster band.
    band = raster_handle.GetRasterBand(1)

    # assign no data value to empty cells.
    no_data_value = -9999
    band.SetNoDataValue(no_data_value)
    band.FlushCache()

    # adding a spatial reference
    raster_srs = osr.SpatialReference()
    raster_srs.ImportFromEPSG(epsg)
    raster_handle.SetProjection(raster_srs.ExportToWkt())

    # main conversion method
    gdal.Rasterize(raster_handle, shape_file, burnValues=[burn_value])
    raster_handle = None

    # reload the raster file to return as a DataArray
    da = read_file_grid(grid_file)
    # set attributes for the DataArray
    da.attrs = {
        'shape_file': shape_file,
        'pixel_size': pixel_size,
        'burn_value': burn_value,
        'epsg': epsg}

    # remove the temporary file
    if os.path.exists(grid_file):
        if tiff_remove:
            os.remove(grid_file)

    return da

# ----------------------------------------------------------------------------------------------------------------------
