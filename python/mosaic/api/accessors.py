from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

#######################
# Geometry accessors  #
#######################

__all__ = [
    "st_aswkt",
    "st_astext",
    "st_aswkb",
    "st_asbinary",
    "st_asgeojson",
    "as_hex",
    "as_json",
    "convert_to",
    "st_asewkt",
]


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
    return config.mosaic_context.invoke_function(
        "st_aswkt", pyspark_to_java_column(geom)
    )


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
    return config.mosaic_context.invoke_function(
        "st_astext", pyspark_to_java_column(geom)
    )


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
    return config.mosaic_context.invoke_function(
        "st_aswkb", pyspark_to_java_column(geom)
    )


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
    return config.mosaic_context.invoke_function(
        "st_asbinary", pyspark_to_java_column(geom)
    )


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
    return config.mosaic_context.invoke_function(
        "st_asgeojson", pyspark_to_java_column(geom)
    )


def as_hex(geom: ColumnOrName) -> Column:
    return config.mosaic_context.invoke_function("as_hex", pyspark_to_java_column(geom))


def as_json(geom: ColumnOrName) -> Column:
    return config.mosaic_context.invoke_function(
        "as_json", pyspark_to_java_column(geom)
    )


def convert_to(geom: ColumnOrName) -> Column:
    return config.mosaic_context.invoke_function(
        "convert_to", pyspark_to_java_column(geom)
    )

def st_asewkt(geom: ColumnOrName) -> Column:
    """
    Translate a geometry into its Extended Well-known Text (EWKT) representation.

    Parameters
    ----------
    geom : Column (BinaryType, HexType, JSONType or InternalGeometryType)
        Geometry column

    Returns
    -------
    Column (StringType)
        An EWKT geometry

    """
    return config.mosaic_context.invoke_function(
        "st_asewkt", pyspark_to_java_column(geom)
    )