from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from .mosaic import mosaicContext, _mosaic_invoke_function


#################################
# Bindings of mosaic functions  #
#################################
def as_hex(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("as_hex", mosaicContext, pyspark_to_java_column(inGeom))

def as_json(inGeom: "ColumnOrName"):
  return _mosaic_invoke_function("as_json", mosaicContext, pyspark_to_java_column(inGeom))

def st_point(xVal: "ColumnOrName", yVal: "ColumnOrName"):
  return _mosaic_invoke_function("st_point", mosaicContext, pyspark_to_java_column(xVal), pyspark_to_java_column(yVal))

def st_makeline(points: "ColumnOrName"):
  return _mosaic_invoke_function("st_makeline", mosaicContext, pyspark_to_java_column(points))

def st_makepolygon(boundaryRing: "ColumnOrName"):
  return _mosaic_invoke_function("st_makepolygon", mosaicContext, pyspark_to_java_column(boundaryRing))

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

def mosaicfill(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
  return _mosaic_invoke_function(
    "mosaicfill", 
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

def polyfill(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
  return _mosaic_invoke_function(
    "polyfill", 
    mosaicContext, 
    pyspark_to_java_column(inGeom), 
    pyspark_to_java_column(resolution)
  )
