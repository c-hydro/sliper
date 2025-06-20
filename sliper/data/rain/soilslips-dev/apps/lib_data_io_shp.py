"""
Library Features:

Name:          lib_data_io_shp
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220105'
Version:       '1.0.0'
"""


# -------------------------------------------------------------------------------------
# Libraries
import logging
import rasterio
import ogr
from osgeo import osr, ogr, gdal

import geopandas as gpd

from lib_info_args import proj_wkt as proj_default_wkt
from lib_info_args import logger_name_predictors as logger_name

# Logging
logging.getLogger("fiona").setLevel(logging.WARNING)
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to read shape file
def read_file_point(file_name):

    shape_dframe = gpd.read_file(file_name)
    shape_geoms = ((feature['geometry'], 1) for feature in shape_dframe.iterfeatures())

    shape_collections = list(shape_dframe.values)

    return shape_dframe, shape_collections, shape_geoms
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Convert polygons to shape file
def convert_polygons_2_shp(shape_polygon, shape_file, template_file_name=None):

    template_handle = rasterio.open(template_file_name)
    metadata = template_handle.meta.copy()
    metadata.update(compress='lzw')

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
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to transform shape file to tiff
def convert_shp_2_tiff(shape_file, raster_file, pixel_size=0.1, burn_value=1, epsg=4326):

    input_shp = ogr.Open(shape_file)
    shp_layer = input_shp.GetLayer()

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
    raster_handle = driver.Create(raster_file, x_res, y_res, 1, gdal.GDT_Byte)

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
    ds = gdal.Rasterize(raster_handle, shape_file, burnValues=[burn_value])
    ds = None
# -------------------------------------------------------------------------------------
