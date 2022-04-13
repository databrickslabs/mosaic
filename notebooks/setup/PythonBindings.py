# Databricks notebook source
MosaicContextClass = getattr(sc._jvm.com.databricks.labs.mosaic.functions, "MosaicContext")
MosaicPatchClass = getattr(sc._jvm.com.databricks.labs.mosaic.patch, "MosaicPatch")
mosaicPackageRef = getattr(sc._jvm.com.databricks.labs.mosaic, "package$")
mosaicPackageObject = getattr(mosaicPackageRef, "MODULE$")
H3 = getattr(mosaicPackageObject, "H3")
OGC = getattr(mosaicPackageObject, "OGC")

mosaicContext = MosaicContextClass.apply(H3(), OGC())
mosaicPatch = MosaicPatchClass.apply(H3(), OGC())

# COMMAND ----------

from pyspark.sql.column import Column as MosaicColumn
from typing import Any
from pyspark import SparkContext

def _mosaic_invoke_function(name: str, mosaic_context: "MosaicContext", *args: Any) -> MosaicColumn:
  assert SparkContext._active_spark_context is not None
  func = getattr(mosaic_context.functions(), name)
  return MosaicColumn(func(*args))

# COMMAND ----------

import pyspark.sql.functions as F 
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

#################################
# Bindings of mosaic functions  #
#################################
def as_hex(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("as_hex", mosaicContext, pyspark_to_java_column(inGeom))

def as_json(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("as_hex", mosaicContext, pyspark_to_java_column(inGeom))

def as_json(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("as_hex", mosaicContext, pyspark_to_java_column(inGeom))

def flatten_polygons(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("flatten_polygons", mosaicContext, pyspark_to_java_column(inGeom))

def st_xmax(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_xmax", mosaicContext, pyspark_to_java_column(inGeom))

def st_xmin(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_xmin", mosaicContext, pyspark_to_java_column(inGeom))

def st_ymax(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_ymax", mosaicContext, pyspark_to_java_column(inGeom))

def st_ymin(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_ymin", mosaicContext, pyspark_to_java_column(inGeom))

def st_zmax(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_zmax", mosaicContext, pyspark_to_java_column(inGeom))

def st_zmin(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_zmin", mosaicContext, pyspark_to_java_column(inGeom))

def st_isvalid(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_isvalid", mosaicContext, pyspark_to_java_column(inGeom))

def st_geometrytype(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_geometrytype", mosaicContext, pyspark_to_java_column(inGeom))

def st_area(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_area", mosaicContext, pyspark_to_java_column(inGeom))

def st_centroid2D(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_centroid2D", mosaicContext, pyspark_to_java_column(inGeom))

def st_centroid3D(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_centroid3D", mosaicContext, pyspark_to_java_column(inGeom))

def convert_to(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("convert_to", mosaicContext, pyspark_to_java_column(inGeom))

def st_geomfromwkt(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_geomfromwkt", mosaicContext, pyspark_to_java_column(inGeom))

def st_geomfromwkb(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_geomfromwkb", mosaicContext, pyspark_to_java_column(inGeom))

def st_geomfromgeojson(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_geomfromgeojson", mosaicContext, pyspark_to_java_column(inGeom))

def st_aswkt(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_aswkt", mosaicContext, pyspark_to_java_column(inGeom))

def st_astext(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_astext", mosaicContext, pyspark_to_java_column(inGeom))

def st_aswkb(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_aswkb", mosaicContext, pyspark_to_java_column(inGeom))

def st_asbinary(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_asbinary", mosaicContext, pyspark_to_java_column(inGeom))

def st_asgeojson(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_asgeojson", mosaicContext, pyspark_to_java_column(inGeom))

def st_length(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_length", mosaicContext, pyspark_to_java_column(inGeom))

def st_perimeter(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("st_perimeter", mosaicContext, pyspark_to_java_column(inGeom))

def st_distance(geom1: "ColumnOrName", geom2: "ColumnOrName"):
  return _mosaic_invoke_function(
    "st_distance", 
    mosaicContext, 
    pyspark_to_java_column(geom1), 
    pyspark_to_java_column(geom2)
  )

def mosaic_fill(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
  return _mosaic_invoke_function(
    "mosaic_fill",
    mosaicContext, 
    pyspark_to_java_column(inGeom), 
    pyspark_to_java_column(resolution)
  )

def point_index(lat: "ColumnOrName", lng: "ColumnOrName", resolution: "ColumnOrName"):
  return _mosaic_invoke_function(
    "point_index", 
    mosaicContext, 
    pyspark_to_java_column(lat), 
    pyspark_to_java_column(lng), 
    pyspark_to_java_column(resolution)
  )

def mosaic_polyfill(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
  return _mosaic_invoke_function(
    "mosaic_polyfill",
    mosaicContext, 
    pyspark_to_java_column(inGeom), 
    pyspark_to_java_column(resolution)
  )

#################################
# Patched functions definitions #
# These have to happen after    #
# original bindings             #
#################################
def flatten_polygons(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("flatten_polygons", mosaicPatch, pyspark_to_java_column(inGeom))

def mosaic_explode(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
  return _mosaic_invoke_function(
    "mosaic_explode", 
    mosaicPatch, 
    pyspark_to_java_column(inGeom), 
    pyspark_to_java_column(resolution)
  )
