from typing import Any

from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column
from pyspark.sql.functions import lit

from mosaic.config import config
from mosaic.utils.types import ColumnOrName, as_typed_col

#####################
# Spatial functions #
#####################

__all__ = [
    "st_area",
    "st_length",
    "st_perimeter",
    "st_convexhull",
    "st_buffer",
    "st_bufferloop",
    "st_dump",
    "st_envelope",
    "st_srid",
    "st_setsrid",
    "st_transform",
    "st_hasvalidcoordinates",
    "st_translate",
    "st_scale",
    "st_rotate",
    "st_centroid",
    "st_centroid2D",
    "st_numpoints",
    "st_isvalid",
    "st_distance",
    "st_haversine",
    "st_intersection",
    "st_difference",
    "st_simplify",
    "st_union",
    "st_unaryunion",
    "st_updatesrid",
    "st_geometrytype",
    "st_xmin",
    "st_xmax",
    "st_ymin",
    "st_ymax",
    "st_zmin",
    "st_zmax",
    "st_x",
    "st_y",

    "rst_bandmetadata",
    "rst_metadata",
    "rst_subdatasets",

    "flatten_polygons",

    "grid_boundaryaswkb",
    "grid_boundary",
    "grid_longlatascellid",
    "grid_pointascellid",
    "grid_polyfill",
    "grid_tessellate",
    "grid_tessellateexplode",
    "grid_cellarea",
    "grid_cell_intersection",
    "grid_cell_union",
    "grid_cellkring",
    "grid_cellkloop",
    "grid_cellkringexplode",
    "grid_cellkloopexplode",
    "grid_geometrykring",
    "grid_geometrykloop",
    "grid_geometrykringexplode",
    "grid_geometrykloopexplode",

    "point_index_geom",
    "point_index_lonlat",
    "index_geometry",
    "polyfill",
    "mosaic_explode",
    "mosaicfill",

]


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


def st_buffer(geom: ColumnOrName, radius: ColumnOrName) -> Column:
    """
    Compute the buffered geometry based on geom and radius.

    Parameters
    ----------
    geom : Column
        The input geometry
    radius : Column
        The radius of buffering

    Returns
    -------
    Column
        A geometry

    """
    return config.mosaic_context.invoke_function(
        "st_buffer", pyspark_to_java_column(geom), pyspark_to_java_column(radius)
    )


def st_bufferloop(geom: ColumnOrName, inner_radius: ColumnOrName, outer_radius: ColumnOrName) -> Column:
    """
    Compute the buffered geometry loop (hollow ring) based on geom and provided radius-es.
    The result geometry is a polygon/multipolygon with a hole in the center.
    The hole covers the area of st_buffer(geom, inner_radius).
    The result geometry covers the area of st_difference(st_buffer(geom, outer_radius), st_buffer(geom, innerRadius)).

    Parameters
    ----------
    geom : Column
        The input geometry
    inner_radius : Column
        The inner radius of buffering
    outer_radius : Column
        The outer radius of buffering

    Returns
    -------
    Column
        A geometry

    """
    return config.mosaic_context.invoke_function(
        "st_bufferloop",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(inner_radius),
        pyspark_to_java_column(outer_radius)
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


def st_srid(geom: ColumnOrName) -> Column:
    """
    Looks up the Coordinate Reference System well-known identifier for `geom`.

    Parameters
    ----------
    geom : Column
        The input geometry

    Returns
    -------
    Column (IntegerType)
        The SRID of the provided geometry.

    """
    return config.mosaic_context.invoke_function(
        "st_srid", pyspark_to_java_column(geom)
    )


def st_setsrid(geom: ColumnOrName, srid: ColumnOrName) -> Column:
    """
    Sets the Coordinate Reference System well-known identifier for `geom`.

    Parameters
    ----------
    geom : Column
        The input geometry
    srid : Column (IntegerType)
        The spatial reference identifier of `geom`, expressed as an integer,
        e.g. 4326 for EPSG:4326 / WGS84

    Returns
    -------
    Column
        The input geometry with a new SRID set.

    Notes
    -----
    ST_SetSRID does not transform the coordinates of `geom`,
    rather it tells Mosaic the SRID in which the current coordinates are expressed.

    """
    return config.mosaic_context.invoke_function(
        "st_setsrid", pyspark_to_java_column(geom), pyspark_to_java_column(srid)
    )


def st_transform(geom: ColumnOrName, srid: ColumnOrName) -> Column:
    """
    Transforms the horizontal (XY) coordinates of `geom` from the current reference system to that described by `srid`.

    Parameters
    ----------
    geom : Column
        The input geometry
    srid : Column (IntegerType)
        The target spatial reference system for `geom`, expressed as an integer,
        e.g. 3857 for EPSG:3857 / Pseudo-Mercator

    Returns
    -------
    Column
        The transformed geometry.

    Notes
    -----
    If `geom` does not have an associated SRID, use ST_SetSRID to set this before calling ST_Transform.

    """
    return config.mosaic_context.invoke_function(
        "st_transform", pyspark_to_java_column(geom), pyspark_to_java_column(srid)
    )


def st_hasvalidcoordinates(
        geom: ColumnOrName, crs: ColumnOrName, which: ColumnOrName
) -> Column:
    """
    Checks if all points in geometry are valid with respect to crs bounds.
    CRS bounds can be provided either as bounds or as reprojected_bounds.

    Parameters
    ----------
    geom : Column
        The input geometry
    crs : Column (StringType)
        The spatial reference system for `geom`, expressed as a string,
        e.g. EPSG:3857
    which : Column (StringType)
        Either 'bounds' or 'reprojected_bounds' - controls how the check
        is executed at runtime.

    Returns
    -------
    Column
        BooleanType - true if all points in geometry are within provided bounds.
    """
    return config.mosaic_context.invoke_function(
        "st_hasvalidcoordinates",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(crs),
        pyspark_to_java_column(which),
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


def st_centroid(geom: ColumnOrName) -> Column:
    """
    Returns the POINT geometry representing the centroid of `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (WKT/WKB)
        Coordinates of the centroid.

    """
    return config.mosaic_context.invoke_function(
        "st_centroid", pyspark_to_java_column(geom)
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


def st_numpoints(geom: ColumnOrName) -> Column:
    """
    Returns the number of points in `geom`.

    Parameters
    ----------
    geom : Column
        The input geometry

    Returns
    -------
    Column (IntegerType)

    """
    return config.mosaic_context.invoke_function(
        "st_numpoints", pyspark_to_java_column(geom)
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


def st_distance(geom1: ColumnOrName, geom2: ColumnOrName) -> Column:
    """
    Compute the distance between `geom1` and `geom2`.

    Parameters
    ----------
    geom1 : Column
    geom2 : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_distance",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )

def st_haversine(lat1: ColumnOrName, lng1: ColumnOrName, lat2: ColumnOrName, lng2: ColumnOrName) -> Column:
    """
    Compute the haversine distance in kilometers between two latitude/longitude pairs.

    Parameters
    ----------
    lat1 : Column
    lng1 : Column
    lat2 : Column
    lng2 : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_haversine",
        pyspark_to_java_column(lat1),
        pyspark_to_java_column(lng1),
        pyspark_to_java_column(lat2),
        pyspark_to_java_column(lng2),
    )


def st_difference(geom1: ColumnOrName, geom2: ColumnOrName) -> Column:
    """
    Compute the difference between `geom1` and `geom2`.

    Parameters
    ----------
    geom1 : Column
    geom2 : Column

    Returns
    -------
    Column
        The difference geometry.
    """
    return config.mosaic_context.invoke_function(
        "st_difference",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2)
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
    the chosen geometry API.

    """
    return config.mosaic_context.invoke_function(
        "st_intersection",
        pyspark_to_java_column(left_geom),
        pyspark_to_java_column(right_geom),
    )


def st_envelope(geom: ColumnOrName) -> Column:
    """
    Returns the minimum bounding box for the supplied geomtery. This bounding box is defined by the rectangular polygon
    with corner points `(x_min, y_min)`, `(x_max, y_min)`, `(x_min, y_max)`, `(x_max, y_max)`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column
        The minimum bounding box as geometry.
    """
    return config.mosaic_context.invoke_function(
        "st_envelope", pyspark_to_java_column(geom)
    )


def st_simplify(geom: ColumnOrName, tolerance: ColumnOrName) -> Column:
    """
    Simplifies the geometry.

    Parameters
    ----------
    geom: Column
    tolerance: Column

    Returns
    -------
    Column
        The simplified geometry.

    Notes
    -----
    The tolerance will be ignored by the ESRI geometry API.
    """
    return config.mosaic_context.invoke_function(
        "st_simplify", pyspark_to_java_column(geom), pyspark_to_java_column(tolerance)
    )


def st_union(left_geom: ColumnOrName, right_geom: ColumnOrName) -> Column:
    """
    Returns the union of the input geometries.

    Parameters
    ----------
    left_geom: Column
    right_geom: Column

    Returns
    -------
    Column
        The union geometry.
    """
    return config.mosaic_context.invoke_function(
        "st_union", pyspark_to_java_column(left_geom), pyspark_to_java_column(right_geom)
    )


def st_unaryunion(geom: ColumnOrName) -> Column:
    """
    Unions a geometry (which may be a geometry collection) together.

    Parameters
    ----------
    geom: Column

    Returns
    -------
    Column
        The union geometry.
    """
    return config.mosaic_context.invoke_function(
        "st_unaryunion", pyspark_to_java_column(geom)
    )


def st_updatesrid(geom: ColumnOrName, srcSRID: ColumnOrName, destSRID: ColumnOrName) -> Column:
    """
    Updates the SRID of the input geometry `geom` from `srcSRID` to `destSRID`.

    Parameters
    ----------
    geom: Column
        Geometry to update the SRID

    srcSRID: Column
        Original SRID

    destSRID: Column
        New SRID

    Returns
    -------
    Column
        Geometry with updated SRID
    """
    return config.mosaic_context.invoke_function(
        "st_updatesrid", pyspark_to_java_column(geom), pyspark_to_java_column(srcSRID), pyspark_to_java_column(destSRID)
    )


def st_x(geom: ColumnOrName) -> Column:
    """
    Returns the x coordinate of the input geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_x", pyspark_to_java_column(geom)
    )


def st_y(geom: ColumnOrName) -> Column:
    """
    Returns the y coordinate of the input geometry `geom`.

    Parameters
    ----------
    geom : Column

    Returns
    -------
    Column (DoubleType)

    """
    return config.mosaic_context.invoke_function(
        "st_y", pyspark_to_java_column(geom)
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


def rst_metadata(raster: ColumnOrName, path: Any = "") -> Column:
    """
    Extracts metadata from a raster row.

    Parameters
    ----------
    raster : ColumnOrName
        The input raster column.
    path : ColumnOrName
        The path of the metadata within the raster row.

    Returns
    -------
    Column
        A map column containing the metadata.

    """
    if type(path) == str:
        path = lit(path)
    return config.mosaic_context.invoke_function(
        "rst_metadata",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(path)
    )


def rst_subdatasets(raster: ColumnOrName, path: Any = "") -> Column:
    """
    Extracts subdatasets from a raster row.

    Parameters
    ----------
    raster : ColumnOrName
        The input raster column.
    path : ColumnOrName
        The path of subdatasets within the raster row.

    Returns
    -------
    Column
        A map column containing the subdatasets.

    """
    if type(path) == str:
        path = lit(path)
    return config.mosaic_context.invoke_function(
        "rst_subdatasets",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(path)
    )


def rst_bandmetadata(raster: ColumnOrName, band: ColumnOrName, path: Any = "") -> Column:
    """
    Extracts band metadata from a raster row.

    Parameters
    ----------
    raster : ColumnOrName
        The input raster column.
    band : ColumnOrName
        The band index.
    path : ColumnOrName
        The path of the metadata within the raster row and the band.

    Returns
    -------
    Column
        A map column containing the metadata.

    """
    if type(path) == str:
        path = lit(path)
    return config.mosaic_context.invoke_function(
        "rst_bandmetadata",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(band),
        pyspark_to_java_column(path)
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


def grid_boundaryaswkb(index_id: ColumnOrName) -> Column:
    """
    Returns a WKB representing the grid cell boundary

    Parameters
    ----------
    index_id : Column
        The grid cell ID

    Returns
    -------
    Column
        A geometry in WKB format
    """
    return config.mosaic_context.invoke_function(
        "grid_boundaryaswkb", pyspark_to_java_column(index_id)
    )

def grid_cellarea(index_id: ColumnOrName) -> Column:
    """
    Returns the area of the grid cell in km^2.

    Parameters
    ----------
    index_id : Column
        The grid cell ID

    Returns
    -------
    Column
        The area of the grid cell in km^2
    """
    return config.mosaic_context.invoke_function(
        "grid_cellarea",
        pyspark_to_java_column(index_id)
    )


def grid_boundary(index_id: ColumnOrName, format_name: ColumnOrName) -> Column:
    """
    Returns a geometry representing the grid cell boundary using specified format.

    Parameters
    ----------
    index_id : Column
        The grid cell ID
    format_name : Column
        The format of the geometry to return. One of "wkb", "wkt", "geojson"

    Returns
    -------
    Column
        A geometry
    """
    return config.mosaic_context.invoke_function(
        "grid_boundary",
        pyspark_to_java_column(index_id),
        pyspark_to_java_column(format_name)
    )


def grid_longlatascellid(
        lon: ColumnOrName, lat: ColumnOrName, resolution: ColumnOrName
) -> Column:
    """
    Returns the grid's cell ID associated with the input `lng` and `lat` coordinates at a given grid `resolution`.

    Parameters
    ----------
    lon : Column (DoubleType) Longitude
    lat : Column (DoubleType) Latitude
    resolution : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "grid_longlatascellid",
        pyspark_to_java_column(as_typed_col(lon, "double")),
        pyspark_to_java_column(as_typed_col(lat, "double")),
        pyspark_to_java_column(resolution),
    )


def grid_pointascellid(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    Returns the grid's cell ID associated with the input point geometry `geom` at a given grid `resolution`.

    Parameters
    ----------
    geom: Column (Geometry)
    resolution : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "grid_pointascellid",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


def grid_polyfill(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    Returns the set of grid cell IDs whose centroid is contained in the input geometry `geom` at
    resolution `resolution`.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)

    Returns
    -------
    Column (ArrayType[LongType])

    """
    return config.mosaic_context.invoke_function(
        "grid_polyfill",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


def grid_tessellate(
        geom: ColumnOrName, resolution: ColumnOrName, keep_core_geometries: Any = True
) -> Column:
    """
    Generates:
    - a set of core indices that are fully contained by `geom`; and
    - a set of border indices and sub-polygons that are partially contained by the input.

    Outputs an array of chip structs for each input row.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)
    keep_core_geometries : Column (BooleanType) | bool

    Returns
    -------
    Column (ArrayType[StructType[is_core: BooleanType, h3: LongType, wkb: BinaryType]])
        `wkb` in this struct represents a border chip geometry and is null for all 'core' chips
        if keep_core_geometries is set to False.

    """

    if type(keep_core_geometries) == bool:
        keep_core_geometries = lit(keep_core_geometries)

    return config.mosaic_context.invoke_function(
        "grid_tessellate",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
        pyspark_to_java_column(keep_core_geometries),
    )


def grid_tessellateexplode(
        geom: ColumnOrName, resolution: ColumnOrName, keep_core_geometries: Any = True
) -> Column:
    """
    Generates:
    - a set of core grid cells that are fully contained by `geom`; and
    - a set of border grid cells and sub-polygons that are partially contained by the input.

    Outputs a row per grid cell.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)
    keep_core_geometries : Column (BooleanType) | bool

    Returns
    -------
    Column (StructType[is_core: BooleanType, h3: LongType, wkb: BinaryType])
        `wkb` in this struct represents a border chip geometry and is null for all 'core' chips
        if keep_core_geometries is set to False.

    """
    if type(keep_core_geometries) == bool:
        keep_core_geometries = lit(keep_core_geometries)

    return config.mosaic_context.invoke_function(
        "grid_tessellateexplode",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
        pyspark_to_java_column(keep_core_geometries),
    )

def grid_cell_intersection(
        left_chip: ColumnOrName, right_chip: ColumnOrName
) -> Column:
    """
    Returns the chip representing the intersection of two chips based on the same grid cell.

    Parameters
    ----------
    left_chip : Column (ChipType(LongType))
    right_chip : Column (ChipType(LongType))

    Returns
    -------
    Column (ChipType(LongType))

    """
    return config.mosaic_context.invoke_function(
        "grid_cell_intersection",
        pyspark_to_java_column(left_chip),
        pyspark_to_java_column(right_chip),
    )

def grid_cell_union(
        left_chip: ColumnOrName, right_chip: ColumnOrName
) -> Column:
    """
    Returns the chip representing the union of two chips based on the same grid cell.

    Parameters
    ----------
    left_chip : Column (ChipType(LongType))
    right_chip : Column (ChipType(LongType))

    Returns
    -------
    Column (ChipType(LongType))

    """
    return config.mosaic_context.invoke_function(
        "grid_cell_union",
        pyspark_to_java_column(left_chip),
        pyspark_to_java_column(right_chip),
    )


def grid_cellkring(
        cellid: ColumnOrName, k: ColumnOrName
) -> Column:
    """
    Returns the k-ring of cells around the input cell ID.

    Parameters
    ----------
    cellid : Column (LongType)
    k : Column (IntegerType)

    Returns
    -------
    Column (ArrayType[LongType])

    """
    return config.mosaic_context.invoke_function(
        "grid_cellkring",
        pyspark_to_java_column(cellid),
        pyspark_to_java_column(k),
    )


def grid_cellkloop(
        cellid: ColumnOrName, k: ColumnOrName
) -> Column:
    """
    Returns the k loop (hollow ring) of cells around the input cell ID.

    Parameters
    ----------
    cellid : Column (LongType)
    k : Column (IntegerType)

    Returns
    -------
    Column (ArrayType[LongType])

    """
    return config.mosaic_context.invoke_function(
        "grid_cellkloop",
        pyspark_to_java_column(cellid),
        pyspark_to_java_column(k),
    )


def grid_cellkringexplode(
        cellid: ColumnOrName, k: ColumnOrName
) -> Column:
    """
    Returns the exploded k-ring of cells around the input cell ID.

    Parameters
    ----------
    cellid : Column (LongType)
    k : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "grid_cellkringexplode",
        pyspark_to_java_column(cellid),
        pyspark_to_java_column(k),
    )


def grid_cellkloopexplode(
        cellid: ColumnOrName, k: ColumnOrName
) -> Column:
    """
    Returns the exploded k loop (hollow ring) of cells around the input cell ID.

    Parameters
    ----------
    cellid : Column (LongType)
    k : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "grid_cellkloopexplode",
        pyspark_to_java_column(cellid),
        pyspark_to_java_column(k),
    )


def grid_geometrykring(
        geom: ColumnOrName, resolution: ColumnOrName, k: ColumnOrName
) -> Column:
    """
    Returns the k-ring of cells around the input geometry.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)
    k : Column (IntegerType)

    Returns
    -------
    Column (ArrayType[LongType])

    """
    return config.mosaic_context.invoke_function(
        "grid_geometrykring",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
        pyspark_to_java_column(k),
    )


def grid_geometrykloop(
        geom: ColumnOrName, resolution: ColumnOrName, k: ColumnOrName
) -> Column:
    """
    Returns the k loop (hollow ring) of cells around the input geometry.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)
    k : Column (IntegerType)

    Returns
    -------
    Column (ArrayType[LongType])

    """
    return config.mosaic_context.invoke_function(
        "grid_geometrykloop",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
        pyspark_to_java_column(k),
    )


def grid_geometrykringexplode(
        geom: ColumnOrName, resolution: ColumnOrName, k: ColumnOrName
) -> Column:
    """
    Returns the exploded k-ring of cells around the input geometry.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)
    k : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "grid_geometrykringexplode",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
        pyspark_to_java_column(k),
    )


def grid_geometrykloopexplode(
        geom: ColumnOrName, resolution: ColumnOrName, k: ColumnOrName
) -> Column:
    """
    Returns the exploded k loop (hollow ring) of cells around the input geometry.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)
    k : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "grid_geometrykloopexplode",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
        pyspark_to_java_column(k),
    )


def point_index_geom(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    [Deprecated] alias for `grid_pointascellid`
    Returns the `resolution` grid index associated with the input geometry `geom`.

    Parameters
    ----------
    geom: Column (Geometry)
    resolution : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "point_index_geom",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


def point_index_lonlat(
        lon: ColumnOrName, lat: ColumnOrName, resolution: ColumnOrName
) -> Column:
    """
    [Deprecated] alias for `grid_longlatascellid`
    Returns the `resolution` grid index associated with the input `lng` and `lat` coordinates.

    Parameters
    ----------
    lon : Column (DoubleType) Longitude
    lat : Column (DoubleType) Latitude
    resolution : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return config.mosaic_context.invoke_function(
        "point_index_lonlat",
        pyspark_to_java_column(as_typed_col(lon, "double")),
        pyspark_to_java_column(as_typed_col(lat, "double")),
        pyspark_to_java_column(resolution),
    )


def index_geometry(index_id: ColumnOrName) -> Column:
    """
    [Deprecated] alias for `grid_boundaryaswkb`
    """
    return config.mosaic_context.invoke_function(
        "index_geometry", pyspark_to_java_column(index_id)
    )


def polyfill(geom: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    [Deprecated] alias for `grid_polyfill`
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


def mosaic_explode(
        geom: ColumnOrName, resolution: ColumnOrName, keep_core_geometries: Any = True
) -> Column:
    """
    [Deprecated] alias for `grid_tessellateexplode`
    Generates:
    - a set of core indices that are fully contained by `geom`; and
    - a set of border indices and sub-polygons that are partially contained by the input.

    Outputs a row per index.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)
    keep_core_geometries : Column (BooleanType) | bool

    Returns
    -------
    Column (StructType[is_core: BooleanType, h3: LongType, wkb: BinaryType])
        `wkb` in this struct represents a border chip geometry and is null for all 'core' chips
        if keep_core_geometries is set to False.

    """
    if type(keep_core_geometries) == bool:
        keep_core_geometries = lit(keep_core_geometries)

    return config.mosaic_context.invoke_function(
        "mosaic_explode",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
        pyspark_to_java_column(keep_core_geometries),
    )


def mosaicfill(
        geom: ColumnOrName, resolution: ColumnOrName, keep_core_geometries: Any = True
) -> Column:
    """
    [Deprecated] alias for `grid_tessellate`
    Generates:
    - a set of core indices that are fully contained by `geom`; and
    - a set of border indices and sub-polygons that are partially contained by the input.

    Outputs an array of chip structs for each input row.

    Parameters
    ----------
    geom : Column
    resolution : Column (IntegerType)
    keep_core_geometries : Column (BooleanType) | bool

    Returns
    -------
    Column (ArrayType[StructType[is_core: BooleanType, h3: LongType, wkb: BinaryType]])
        `wkb` in this struct represents a border chip geometry and is null for all 'core' chips
        if keep_core_geometries is set to False.

    """

    if type(keep_core_geometries) == bool:
        keep_core_geometries = lit(keep_core_geometries)

    return config.mosaic_context.invoke_function(
        "mosaicfill",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
        pyspark_to_java_column(keep_core_geometries),
    )
