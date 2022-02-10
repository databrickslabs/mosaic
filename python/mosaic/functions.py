from typing import Union

from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from .library_handler import MosaicLibraryHandler
from .mosaic_context import MosaicContext

mosaic_context: MosaicContext
ColumnOrName = Union[Column, str]


def enable_mosaic(spark: SparkSession) -> None:
    """
    Enable Mosaic functions.

    Use this function at the start of your workflow to ensure all of the required dependencies are installed and
    Mosaic is configured according to your needs.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
            The active SparkSession.

    Returns
    -------

    Notes
    -----
    Users can control various aspects of Mosaic's operation with the following Spark confs:

    - `spark.databricks.mosaic.jar.autoattach`: 'true' (default) or 'false'
       Automatically attach the Mosaic JAR to the Databricks cluster? (Optional)
    - `spark.databricks.mosaic.jar.location`
       Explicitly specify the path to the Mosaic JAR.
       (Optional and not required at all in a standard Databricks environment).
    - `spark.databricks.mosaic.geometry.api`: 'OGC' (default) or 'JTS'
       Explicitly specify the underlying geometry library to use for spatial operations. (Optional)
    - `spark.databricks.mosaic.index.system`: 'H3' (default)
       Explicitly specify the index system to use for optimized spatial joins. (Optional)

    """
    global mosaic_context
    handler = MosaicLibraryHandler(spark)
    mosaic_context = MosaicContext(spark)


#########################
# Geometry constructors #
#########################


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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function("st_makeline", pyspark_to_java_column(points))


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
        return mosaic_context.invoke_function(
            "st_makepolygon",
            pyspark_to_java_column(boundary_ring),
            pyspark_to_java_column(hole_ring_array),
        )
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function(
        "st_geomfromgeojson", pyspark_to_java_column(geom)
    )


#######################
# Geometry accessors  #
#######################


def st_aswkt(geom: ColumnOrName) -> Column:
    """
    Translate a geometry into its Well-known Text (WKT) representation.

    Parameters
    ----------
    geom : Column (BinaryType, HexType, JSONType or InternalGeometryType)
        Geometry column

    Returns
    -------
    Column (StringType)
        A WKT geometry

    """
    return mosaic_context.invoke_function("st_aswkt", pyspark_to_java_column(geom))


def st_astext(geom: ColumnOrName) -> Column:
    """
    Translate a geometry into its Well-known Text (WKT) representation.

    Parameters
    ----------
    geom : Column (BinaryType, HexType, JSONType or InternalGeometryType)
        Geometry column

    Returns
    -------
    Column (StringType)
        A WKT geometry

    """
    return mosaic_context.invoke_function("st_astext", pyspark_to_java_column(geom))


def st_aswkb(geom: ColumnOrName) -> Column:
    """
    Translate a geometry into its Well-known Binary (WKB) representation.

    Parameters
    ----------
    geom : Column (StringType, HexType, JSONType or InternalGeometryType)

    Returns
    -------
    Column (BinaryType)
        A WKB geometry

    """
    return mosaic_context.invoke_function("st_aswkb", pyspark_to_java_column(geom))


def st_asbinary(geom: ColumnOrName) -> Column:
    """
    Translate a geometry into its Well-known Binary (WKB) representation.

    Parameters
    ----------
    geom : Column (StringType, HexType, JSONType or InternalGeometryType)

    Returns
    -------
    Column (BinaryType)
        A WKB geometry

    """
    return mosaic_context.invoke_function("st_asbinary", pyspark_to_java_column(geom))


def st_asgeojson(geom: ColumnOrName) -> Column:
    """
    Translate a geometry into its GeoJSON representation.

    Parameters
    ----------
    geom : Column (BinaryType, StringType, HexType or InternalGeometryType)

    Returns
    -------
    Column (JSONType)
        A GeoJSON geometry

    """
    return mosaic_context.invoke_function("st_asgeojson", pyspark_to_java_column(geom))


def as_hex(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("as_hex", pyspark_to_java_column(geom))


def as_json(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("as_json", pyspark_to_java_column(geom))


def convert_to(geom: ColumnOrName) -> Column:
    return mosaic_context.invoke_function("convert_to", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_area", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_length", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_perimeter", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_convexhull", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_dump", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function("st_centroid2D", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_centroid3D", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_isvalid", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function("st_xmin", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_xmax", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_ymin", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_ymax", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_zmin", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function("st_zmax", pyspark_to_java_column(geom))


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
    return mosaic_context.invoke_function(
        "flatten_polygons", pyspark_to_java_column(geom)
    )


def point_index(
    lat: ColumnOrName, lng: ColumnOrName, resolution: ColumnOrName
) -> Column:
    """
    Returns the `resolution` grid index associated with the input `lat` and `lng` coordinates.

    Parameters
    ----------
    lat : Column (DoubleType)
    lng : Column (DoubleType)
    resolution : Column (IntegerType)

    Returns
    -------
    Column (LongType)

    """
    return mosaic_context.invoke_function(
        "point_index",
        pyspark_to_java_column(lat),
        pyspark_to_java_column(lng),
        pyspark_to_java_column(resolution),
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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function(
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
    return mosaic_context.invoke_function(
        "mosaicfill",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(resolution),
    )


######################
# Spatial predicates #
######################


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
    return mosaic_context.invoke_function(
        "st_distance",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )


def st_contains(geom1: ColumnOrName, geom2: ColumnOrName) -> Column:
    """
    Returns `true` if geom1 'spatially' contains geom2.

    Parameters
    ----------
    geom1 : Column
    geom2 : Column

    Returns
    -------
    Column (BooleanType)

    """
    return mosaic_context.invoke_function(
        "st_contains",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )
