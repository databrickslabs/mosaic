from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

#######################
# Spatial aggregators #
#######################

__all__ = ["st_intersection_aggregate", "st_intersects_aggregate"]


def st_intersection_aggregate(
    leftIndex: ColumnOrName, rightIndex: ColumnOrName
) -> Column:
    """
    Computes the intersection of all `leftIndex` : `rightIndex` pairs
    and unions these to produce a single geometry.

    Parameters
    ----------
    leftIndex : Column
        The index field of the left-hand geometry
    rightIndex : Column
        The index field of the right-hand geometry

    Returns
    -------
    Column
        The aggregated intersection geometry.

    """
    return config.mosaic_context.invoke_function(
        "st_intersection_aggregate",
        pyspark_to_java_column(leftIndex),
        pyspark_to_java_column(rightIndex),
    )


def st_intersects_aggregate(
    leftIndex: ColumnOrName, rightIndex: ColumnOrName
) -> Column:
    """
    Tests if any `leftIndex` : `rightIndex` pairs intersect.

    Parameters
    ----------
    leftIndex : Column
        The index field of the left-hand geometry
    rightIndex : Column
        The index field of the right-hand geometry

    Returns
    -------
    Column (BooleanType)

    """
    return config.mosaic_context.invoke_function(
        "st_intersects_aggregate",
        pyspark_to_java_column(leftIndex),
        pyspark_to_java_column(rightIndex),
    )
