from typing import Union

from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from .library_handler import MosaicLibraryHandler
from .mosaic_context import MosaicContext

mosaic_context: MosaicContext


def enable_mosaic(spark: SparkSession):
    global mosaic_context
    handler = MosaicLibraryHandler(spark)
    mosaic_context = MosaicContext(spark)


ColumnOrName = Union[Column, str]

#################################
# Bindings of mosaic functions  #
#################################


def as_hex(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("as_hex", pyspark_to_java_column(geom))


def as_json(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("as_json", pyspark_to_java_column(geom))


def st_point(x_val: ColumnOrName, y_val: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_point",
        pyspark_to_java_column(x_val),
        pyspark_to_java_column(y_val),
    )


def st_makeline(points: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_makeline", pyspark_to_java_column(points))


def st_makepolygon(
    boundary_ring: ColumnOrName, hole_ring_array: ColumnOrName = None
) -> Column:
    if hole_ring_array:
        return mosaic_context.invoke_function(
            "st_makepolygon",
            pyspark_to_java_column(boundary_ring),
            pyspark_to_java_column(hole_ring_array),
        )
    return mosaic_context.invoke_function(
        "st_makepolygon", pyspark_to_java_column(boundary_ring)
    )


def flatten_polygons(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "flatten_polygons", pyspark_to_java_column(geom)
    )


def st_xmax(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_xmax", pyspark_to_java_column(geom))


def st_xmin(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_xmin", pyspark_to_java_column(geom))


def st_ymax(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_ymax", pyspark_to_java_column(geom))


def st_ymin(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_ymin", pyspark_to_java_column(geom))


def st_zmax(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_zmax", pyspark_to_java_column(geom))


def st_zmin(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_zmin", pyspark_to_java_column(geom))


def st_isvalid(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_isvalid", pyspark_to_java_column(geom))


def st_geometrytype(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_geometrytype", pyspark_to_java_column(geom)
    )


def st_area(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_area", pyspark_to_java_column(geom))


def st_centroid2D(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_centroid2D", pyspark_to_java_column(geom)
    )


def st_centroid3D(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_centroid3D", pyspark_to_java_column(geom)
    )


def convert_to(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("convert_to", pyspark_to_java_column(geom))


def st_geomfromwkt(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_geomfromwkt", pyspark_to_java_column(geom)
    )


def st_geomfromwkb(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_geomfromwkb", pyspark_to_java_column(geom)
    )


def st_geomfromgeojson(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_geomfromgeojson", pyspark_to_java_column(geom)
    )


def st_aswkt(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_aswkt", pyspark_to_java_column(geom))


def st_astext(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_astext", pyspark_to_java_column(geom))


def st_aswkb(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_aswkb", pyspark_to_java_column(geom))


def st_asbinary(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_asbinary", pyspark_to_java_column(geom)
    )


def st_asgeojson(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_asgeojson", pyspark_to_java_column(geom)
    )


def st_length(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_length", pyspark_to_java_column(geom))


def st_perimeter(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_perimeter", pyspark_to_java_column(geom)
    )


def st_distance(geom1: ColumnOrName, geom2: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_distance",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )


def st_contains(geom1: ColumnOrName, geom2: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_contains",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )


def mosaicfill(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "mosaicfill",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


def point_index(
    lat: ColumnOrName, lng: ColumnOrName, resolution: ColumnOrName
) -> Column:
    return mosaic_context.invoke_function(
        "point_index",
        pyspark_to_java_column(lat),
        pyspark_to_java_column(lng),
        pyspark_to_java_column(resolution),
    )


def polyfill(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "polyfill",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


def mosaic_explode(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "mosaic_explode",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )
