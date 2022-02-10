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


def as_hex(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("as_hex", pyspark_to_java_column(in_geom))


def as_json(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("as_json", pyspark_to_java_column(in_geom))


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


def flatten_polygons(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "flatten_polygons", pyspark_to_java_column(in_geom)
    )


def st_xmax(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_xmax", pyspark_to_java_column(in_geom))


def st_xmin(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_xmin", pyspark_to_java_column(in_geom))


def st_ymax(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_ymax", pyspark_to_java_column(in_geom))


def st_ymin(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_ymin", pyspark_to_java_column(in_geom))


def st_zmax(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_zmax", pyspark_to_java_column(in_geom))


def st_zmin(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_zmin", pyspark_to_java_column(in_geom))


def st_isvalid(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_isvalid", pyspark_to_java_column(in_geom))


def st_geometrytype(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_geometrytype", pyspark_to_java_column(in_geom)
    )


def st_area(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_area", pyspark_to_java_column(in_geom))


def st_centroid2D(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_centroid2D", pyspark_to_java_column(in_geom)
    )


def st_centroid3D(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_centroid3D", pyspark_to_java_column(in_geom)
    )


def convert_to(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("convert_to", pyspark_to_java_column(in_geom))


def st_geomfromwkt(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_geomfromwkt", pyspark_to_java_column(in_geom)
    )


def st_geomfromwkb(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_geomfromwkb", pyspark_to_java_column(in_geom)
    )


def st_geomfromgeojson(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_geomfromgeojson", pyspark_to_java_column(in_geom)
    )


def st_aswkt(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_aswkt", pyspark_to_java_column(in_geom))


def st_astext(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_astext", pyspark_to_java_column(in_geom))


def st_aswkb(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_aswkb", pyspark_to_java_column(in_geom))


def st_asbinary(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_asbinary", pyspark_to_java_column(in_geom)
    )


def st_asgeojson(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_asgeojson", pyspark_to_java_column(in_geom)
    )


def st_length(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("st_length", pyspark_to_java_column(in_geom))


def st_perimeter(in_geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "st_perimeter", pyspark_to_java_column(in_geom)
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


def mosaicfill(in_geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "mosaicfill",
        pyspark_to_java_column(in_geom),
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


def polyfill(in_geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "polyfill",
        pyspark_to_java_column(in_geom),
        pyspark_to_java_column(resolution),
    )


def mosaic_explode(in_geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    return mosaic_context.invoke_function(
        "mosaic_explode",
        pyspark_to_java_column(in_geom),
        pyspark_to_java_column(resolution),
    )
