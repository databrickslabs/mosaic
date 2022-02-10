from pyspark.sql import SparkSession
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from .library_handler import MosaicLibraryHandler
from .mosaic_context import MosaicContext

mosaicContext = None


def enable_mosaic(spark: SparkSession):
    global mosaicContext
    handler = MosaicLibraryHandler(spark)
    mosaicContext = MosaicContext(spark)


#################################
# Bindings of mosaic functions  #
#################################
def as_hex(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("as_hex", pyspark_to_java_column(inGeom))


def as_json(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("as_json", pyspark_to_java_column(inGeom))


def st_point(xVal: "ColumnOrName", yVal: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_point",
        pyspark_to_java_column(xVal),
        pyspark_to_java_column(yVal),
    )


def st_makeline(points: "ColumnOrName"):
    return mosaicContext.invoke_function("st_makeline", pyspark_to_java_column(points))


def st_makepolygon(boundaryRing: "ColumnOrName", holeRingArray: "ColumnOrName" = None):
    if holeRingArray:
        return mosaicContext.invoke_function(
            "st_makepolygon",
            pyspark_to_java_column(boundaryRing),
            pyspark_to_java_column(holeRingArray),
        )
    return mosaicContext.invoke_function(
        "st_makepolygon", pyspark_to_java_column(boundaryRing)
    )


def flatten_polygons(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "flatten_polygons", pyspark_to_java_column(inGeom)
    )


def st_xmax(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_xmax", pyspark_to_java_column(inGeom))


def st_xmin(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_xmin", pyspark_to_java_column(inGeom))


def st_ymax(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_ymax", pyspark_to_java_column(inGeom))


def st_ymin(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_ymin", pyspark_to_java_column(inGeom))


def st_zmax(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_zmax", pyspark_to_java_column(inGeom))


def st_zmin(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_zmin", pyspark_to_java_column(inGeom))


def st_isvalid(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_isvalid", pyspark_to_java_column(inGeom))


def st_geometrytype(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_geometrytype", pyspark_to_java_column(inGeom)
    )


def st_area(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_area", pyspark_to_java_column(inGeom))


def st_centroid2D(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_centroid2D", pyspark_to_java_column(inGeom)
    )


def st_centroid3D(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_centroid3D", pyspark_to_java_column(inGeom)
    )


def convert_to(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("convert_to", pyspark_to_java_column(inGeom))


def st_geomfromwkt(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_geomfromwkt", pyspark_to_java_column(inGeom)
    )


def st_geomfromwkb(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_geomfromwkb", pyspark_to_java_column(inGeom)
    )


def st_geomfromgeojson(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_geomfromgeojson", pyspark_to_java_column(inGeom)
    )


def st_aswkt(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_aswkt", pyspark_to_java_column(inGeom))


def st_astext(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_astext", pyspark_to_java_column(inGeom))


def st_aswkb(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_aswkb", pyspark_to_java_column(inGeom))


def st_asbinary(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_asbinary", pyspark_to_java_column(inGeom))


def st_asgeojson(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_asgeojson", pyspark_to_java_column(inGeom))


def st_length(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_length", pyspark_to_java_column(inGeom))


def st_perimeter(inGeom: "ColumnOrName"):
    return mosaicContext.invoke_function("st_perimeter", pyspark_to_java_column(inGeom))


def st_distance(geom1: "ColumnOrName", geom2: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_distance",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )


def st_contains(geom1: "ColumnOrName", geom2: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "st_contains",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )


def mosaicfill(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "mosaicfill",
        pyspark_to_java_column(inGeom),
        pyspark_to_java_column(resolution),
    )


def point_index(lat: "ColumnOrName", lng: "ColumnOrName", resolution: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "point_index",
        pyspark_to_java_column(lat),
        pyspark_to_java_column(lng),
        pyspark_to_java_column(resolution),
    )


def polyfill(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "polyfill",
        pyspark_to_java_column(inGeom),
        pyspark_to_java_column(resolution),
    )


def mosaic_explode(inGeom: "ColumnOrName", resolution: "ColumnOrName"):
    return mosaicContext.invoke_function(
        "mosaic_explode",
        pyspark_to_java_column(inGeom),
        pyspark_to_java_column(resolution),
    )
