from typing import overload

from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

#####################
# Spatial functions #
#####################


def st_area(geom: ColumnOrName) -> Column:
    """
    Compute the area of a geometry.

    Parameters
    ----------
    geom : Column
        The input geometry

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_area", pyspark_to_java_column(geom)
    )


def st_length(geom: ColumnOrName) -> Column:
    """
    Compute the length of a geometry.

    Parameters
    ----------
    geom : Column
        The input geometry

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_length", pyspark_to_java_column(geom)
    )


def st_perimeter(geom: ColumnOrName) -> Column:
    """
    Compute the perimeter length of a geometry.

    Parameters
    ----------
    geom : Column
        The input geometry

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_perimeter", pyspark_to_java_column(geom)
    )


def st_convexhull(geom: ColumnOrName) -> Column:
    """
    Compute the convex hull of a geometry or multi-geometry object.

    Parameters
    ----------
    geom : Column
        The input geometry

    Returns
    -------
    Column
        A polygon

    """
    return config.mosaic_context.invoke_function(
        "st_convexhull", pyspark_to_java_column(geom)
    )


def st_dump(geom: ColumnOrName) -> Column:
    """
    Explodes a multi-geometry into one row per constituent geometry.

    Parameters
    ----------
    geom : Column
        The input multi-geometry

    Returns
    -------
    Column
        A geometry

    """
    return config.mosaic_context.invoke_function(
        "st_dump", pyspark_to_java_column(geom)
    )


def st_translate(geom: ColumnOrName, xd: ColumnOrName, yd: ColumnOrName) -> Column:
    """
    Translates `geom` to a new location using the distance parameters `xd` and `yd`

    Parameters
    ----------
    geom : Column
        The input geometry
    xd : Column (any numeric type)
        Offset in the x-direction
    yd : Column (any numeric type)
        Offset in the y-direction

    Returns
    -------
    Column
        The translated geometry.

    """
    return config.mosaic_context.invoke_function(
        "st_translate",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(xd),
        pyspark_to_java_column(yd),
    )


def st_scale(geom: ColumnOrName, xd: ColumnOrName, yd: ColumnOrName) -> Column:
    """
    Scales `geom` using the scaling factors `xd` and `yd`

    Parameters
    ----------
    geom : Column
        The input geometry
    xd : Column (any numeric type)
        Scale factor in the x-direction
    yd : Column (any numeric type)
        Scale factor in the y-direction

    Returns
    -------
    Column
        The scaled geometry.

    """
    return config.mosaic_context.invoke_function(
        "st_scale",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(xd),
        pyspark_to_java_column(yd),
    )


def st_rotate(geom: ColumnOrName, td: ColumnOrName) -> Column:
    """
    Rotates `geom` using the rotational factor `td`

    Parameters
    ----------
    geom : Column
        The input geometry
    td : Column (any numeric type)
        Rotation (in radians)

    Returns
    -------
    Column
        The rotated geometry.

    """
    return config.mosaic_context.invoke_function(
        "st_rotate", pyspark_to_java_column(geom), pyspark_to_java_column(td)
    )


def st_centroid2D(geom: ColumnOrName) -> Column:
    """
    Returns the x and y coordinates representing the centroid of `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (StructType[x: DoubleType, y: DoubleType])
        Coordinates of the centroid.

    """
    return config.mosaic_context.invoke_function(
        "st_centroid2D", pyspark_to_java_column(geom)
    )


def st_centroid3D(geom: ColumnOrName) -> Column:
    """
    Returns the x, y and z coordinates representing the centroid of the geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (StructType[x: DoubleType, y: DoubleType, z: DoubleType])
        Coordinates of the centroid.

    """
    return config.mosaic_context.invoke_function(
        "st_centroid3D", pyspark_to_java_column(geom)
    )


def st_isvalid(geom: ColumnOrName) -> Column:
    """
    Returns true if the geometry `geom` is valid.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (BooleanType)

    Notes
    -----
    Validity assertions will be dependent on the chosen geometry API.
    The assertions used in the ESRI geometry API (the default) follow the definitions in
    the “Simple feature access - Part 1” document (OGC 06-103r4) for each geometry type.

    """
    return config.mosaic_context.invoke_function(
        "st_isvalid", pyspark_to_java_column(geom)
    )


def st_intersects(left_geom: ColumnOrName, right_geom: ColumnOrName) -> Column:
    """
    Returns true if the geometry `left_geom` intersects `right_geom`.

    Parameters
    ----------
    left_geom : Column
    right_geom : Column

    Returns
    -------
    Column (BooleanType)

    Notes
    -----
    Intersection logic will be dependent on the chosen geometry API (ESRI or JTS).

    """
    return config.mosaic_context.invoke_function(
        "st_intersects", pyspark_to_java_column(left_geom), pyspark_to_java_column(right_geom)
    )

def st_intersection(left_geom: ColumnOrName, right_geom: ColumnOrName) -> Column:
    """
    Returns the geometry result of the intersection between `left_geom` and `right_geom`.

    Parameters
    ----------
    left_geom : Column
    right_geom : Column

    Returns
    -------
    Column
        The intersection geometry.

    Notes
    -----
    The resulting geometry could give different results depending on the chosen 
    geometry API (ESRI or JTS), especially for polygons that are invalid based on
    the choosen geometry API.

    """
    return config.mosaic_context.invoke_function(
        "st_intersection", pyspark_to_java_column(left_geom), pyspark_to_java_column(right_geom)
    )


def st_geometrytype(geom: ColumnOrName) -> Column:
    """
    Returns the type of the input geometry `geom` (“POINT”, “LINESTRING”, “POLYGON” etc.).

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (StringType)

    """
    return config.mosaic_context.invoke_function(
        "st_geometrytype", pyspark_to_java_column(geom)
    )


def st_xmin(geom: ColumnOrName) -> Column:
    """
    Returns the smallest x coordinate in the input geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_xmin", pyspark_to_java_column(geom)
    )


def st_xmax(geom: ColumnOrName) -> Column:
    """
    Returns the largest x coordinate in the input geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_xmax", pyspark_to_java_column(geom)
    )


def st_ymin(geom: ColumnOrName) -> Column:
    """
    Returns the smallest y coordinate in the input geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_ymin", pyspark_to_java_column(geom)
    )


def st_ymax(geom: ColumnOrName) -> Column:
    """
    Returns the largest y coordinate in the input geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_ymax", pyspark_to_java_column(geom)
    )


def st_zmin(geom: ColumnOrName) -> Column:
    """
    Returns the smallest z coordinate in the input geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_zmin", pyspark_to_java_column(geom)
    )


def st_zmax(geom: ColumnOrName) -> Column:
    """
    Returns the largest z coordinate in the input geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_zmax", pyspark_to_java_column(geom)
    )


def flatten_polygons(geom: ColumnOrName) -> Column:
    """
    Explodes a multi-geometry into one row per constituent geometry.

    Parameters
    ----------
    geom : Column
        The input multi-geometry

    Returns
    -------
    Column
        A geometry

    """
    return config.mosaic_context.invoke_function(
        "flatten_polygons", pyspark_to_java_column(geom)
    )


@overload
def point_index(
    lng: ColumnOrName, lat: ColumnOrName, resolution: ColumnOrName
) -> Column:
    """
    Returns the `resolution` grid index associated with the input `lat` and `lng` coordinates.

    Parameters
    ----------
    lng : Column (DoubleType)
    lat : Column (DoubleType)
    resolution : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "point_index_lonlat",
        pyspark_to_java_column(lng),
        pyspark_to_java_column(lat),
        pyspark_to_java_column(resolution),
    )


def point_index(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    Returns the `resolution` grid index associated with the input `lat` and `lng` coordinates.

    Parameters
    ----------
    lng : Column (DoubleType)
    lat : Column (DoubleType)
    resolution : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "point_index",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


def index_geometry(index_id: ColumnOrName) -> Column:
    return config.mosaic_context.invoke_function(
        "index_geometry", pyspark_to_java_column(index_id)
    )


def polyfill(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    Returns the set of grid indices covering the input geometry `geom` at resolution `resolution`.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)

    Returns
    -------
    Column (ArrayType[LongType])

    """
    return config.mosaic_context.invoke_function(
        "polyfill",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


def mosaic_explode(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    Generates:
    - a set of core indices that are fully contained by `geom`; and
    - a set of border indices and sub-polygons that are partially contained by the input.

    Outputs a row per index.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)

    Returns
    -------
    Column (StructType[is_core: BooleanType, h3: LongType, wkb: BinaryType])
        `wkb` in this struct represents a border chip geometry and is null for all 'core' chips.

    """
    return config.mosaic_context.invoke_function(
        "mosaic_explode",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


def mosaicfill(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    Generates:
    - a set of core indices that are fully contained by `geom`; and
    - a set of border indices and sub-polygons that are partially contained by the input.

    Outputs an array of chip structs for each input row.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)

    Returns
    -------
    Column (ArrayType[StructType[is_core: BooleanType, h3: LongType, wkb: BinaryType]])
        `wkb` in this struct represents a border chip geometry and is null for all 'core' chips.

    """
    return config.mosaic_context.invoke_function(
        "mosaicfill",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )
