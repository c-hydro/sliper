"""
Library Features:

Name:          lib_utils_io_tiff
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220420'
Version:       '1.5.0'
"""
#######################################################################################
# Libraries
import logging
import os
import numpy as np
import rasterio

from copy import deepcopy
from rasterio.crs import CRS
from osgeo import ogr

from lib_data_io_tiff_OLD import write_file_tiff
from lib_data_io_shp import convert_shp_2_tiff
from lib_utils_io_obj import create_darray_2d

from lib_info_args import proj_wkt as proj_default_wkt
from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Convert data to tiff
def convert_polygons_2_tiff(shape_polygon, shape_file, raster_file, template_file=None,
                            pixel_size=0.001, burn_value=1, epsg=4326):

    template_handle = rasterio.open(template_file)
    metadata = template_handle.meta.copy()
    metadata.update(compress='lzw')

    driver = ogr.GetDriverByName('Esri Shapefile')
    ds = driver.CreateDataSource(shape_file)

    if shape_polygon.type == 'MultiPolygon':
        layer = ds.CreateLayer('', None, ogr.wkbMultiPolygon)
    elif shape_polygon.type == 'Polygon':
        layer = ds.CreateLayer('', None, ogr.wkbPolygon)
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

    convert_shp_2_tiff(shape_file, raster_file,
                       pixel_size=pixel_size, burn_value=burn_value, epsg=epsg)

# -------------------------------------------------------------------------------------



