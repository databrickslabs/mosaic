from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

#########################
# Geometry constructors #
#########################

__all__ = [
    "st_point",
    "st_makeline",
    "st_makepolygon",
    "st_geomfromwkt",
    "st_geomfromwkb",
    "st_geomfromgeojson",
]


def st_point(x: ColumnOrName, y: ColumnOrName) -> Column:
    """
    Create a new Mosaic Point geometry from two DoubleType values.

    Parameters
    ----------
    x : Column (DoubleType)
        `x` co-ordinate of the point
    y : Column (DoubleType)
        `y` co-ordinate of the point

    Returns
    -------
    Column (InternalGeometryType)
        A point geometry.

    """
    return config.mosaic_context.invoke_function(
        "st_point",
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def st_makeline(points: ColumnOrName) -> Column:
    """
    Create a new Mosaic LineString geometry from an Array of Mosaic Points

    Parameters
    ----------
    points : Column (ArrayType[InternalGeometryType])
        Point array

    Returns
    -------
    Column (InternalGeometryType)
        A linestring geometry.

    """
    return config.mosaic_context.invoke_function(
        "st_makeline", pyspark_to_java_column(points)
    )


def st_makepolygon(
    boundary_ring: ColumnOrName, hole_ring_array: ColumnOrName = None
) -> Column:
    """
    Create a new Mosaic Polygon geometry from a closed LineString.

    Parameters
    ----------
    boundary_ring : Column (InternalGeometryType)
    hole_ring_array : Column (InternalGeometryType), optional

    Returns
    -------
    Column (InternalGeometryType)
        A polygon geometry.

    """
    if hole_ring_array:
        return config.mosaic_context.invoke_function(
            "st_makepolygon",
            pyspark_to_java_column(boundary_ring),
            pyspark_to_java_column(hole_ring_array),
        )
    return config.mosaic_context.invoke_function(
        "st_makepolygon", pyspark_to_java_column(boundary_ring)
    )


def st_geomfromwkt(geom: ColumnOrName) -> Column:
    """
    Create a new Mosaic geometry from Well-known Text.

    Parameters
    ----------
    geom : Column (StringType)
        Well-known Text Geometry

    Returns
    -------
    Column (InternalGeometryType)
        A Mosaic geometry.

    """
    return config.mosaic_context.invoke_function(
        "st_geomfromwkt", pyspark_to_java_column(geom)
    )


def st_geomfromwkb(geom: ColumnOrName) -> Column:
    """
    Create a new Mosaic geometry from Well-known Binary.

    Parameters
    ----------
    geom : Column (BinaryType)
        Well-known Binary Geometry

    Returns
    -------
    Column (InternalGeometryType)
        A Mosaic geometry

    """
    return config.mosaic_context.invoke_function(
        "st_geomfromwkb", pyspark_to_java_column(geom)
    )


def st_geomfromgeojson(geom: ColumnOrName) -> Column:
    """
    Create a new Mosaic geometry from GeoJSON.

    Parameters
    ----------
    geom : Column (StringType)
        GeoJSON Geometry

    Returns
    -------
    Column (InternalGeometryType)
        A Mosaic geometry

    """
    return config.mosaic_context.invoke_function(
        "st_geomfromgeojson", pyspark_to_java_column(geom)
    )
