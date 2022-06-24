from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

######################
# Spatial predicates #
######################


__all__ = ["st_intersects", "st_contains"]


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
        "st_intersects",
        pyspark_to_java_column(left_geom),
        pyspark_to_java_column(right_geom),
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
    return config.mosaic_context.invoke_function(
        "st_contains",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )
