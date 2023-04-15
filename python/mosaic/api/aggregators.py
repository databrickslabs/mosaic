from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

#######################
# Spatial aggregators #
#######################

__all__ = ["st_intersection_aggregate", "st_intersects_aggregate", "st_union_agg", "grid_cell_union_agg", "grid_cell_intersection_agg"]


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

def st_union_agg(geom: ColumnOrName) -> Column:
    """
    Returns the point set union of the aggregated geometries.

    Parameters
    ----------
    geom: Column

    Returns
    -------
    Column
        The union geometry.
    """
    return config.mosaic_context.invoke_function(
        "st_union_agg", pyspark_to_java_column(geom)
    )


def grid_cell_intersection_agg(chips: ColumnOrName) -> Column:
    """
    Returns the chip representing the aggregated intersection of chips on some grid cell.

    Parameters
    ----------
    chips: Column

    Returns
    -------
    Column
        The intersection chip.
    """
    return config.mosaic_context.invoke_function(
        "grid_cell_intersection_agg", pyspark_to_java_column(chips)
    )

def grid_cell_union_agg(chips: ColumnOrName) -> Column:
    """
    Returns the chip representing the aggregated union of chips on some grid cell.

    Parameters
    ----------
    chips: Column

    Returns
    -------
    Column
        The union chip.
    """
    return config.mosaic_context.invoke_function(
        "grid_cell_union_agg", pyspark_to_java_column(chips)
    )