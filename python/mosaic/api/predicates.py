from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

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
    return config.mosaic_context.invoke_function(
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
    return config.mosaic_context.invoke_function(
        "st_contains",
        pyspark_to_java_column(geom1),
        pyspark_to_java_column(geom2),
    )
