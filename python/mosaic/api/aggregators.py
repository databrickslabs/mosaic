from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

#######################
# Spatial aggregators #
#######################

__all__ = [
    "st_asgeojsontile_agg",
    "st_asmvttile_agg",
    "st_union_agg",
    "grid_cell_union_agg",
    "grid_cell_intersection_agg",
    "rst_merge_agg",
    "rst_combineavg_agg",
    "rst_derivedband_agg",
    "st_intersection_agg",
    "st_intersects_agg",
]


def st_intersection_agg(leftIndex: ColumnOrName, rightIndex: ColumnOrName) -> Column:
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
        "st_intersection_agg",
        pyspark_to_java_column(leftIndex),
        pyspark_to_java_column(rightIndex),
    )


def st_asgeojsontile_agg(geom: ColumnOrName, attributes: ColumnOrName) -> Column:
    """
    Returns the aggregated GeoJSON tile.

    Parameters
    ----------
    geom : Column
        The geometry column to aggregate.
    attributes : Column
        The attributes column to aggregate.

    Returns
    -------
    Column
        The aggregated GeoJSON tile.
    """
    return config.mosaic_context.invoke_function(
        "st_asgeojsontile_agg",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(attributes),
    )


def st_asmvttile_agg(
    geom: ColumnOrName, attributes: ColumnOrName, zxyID: ColumnOrName
) -> Column:
    """
    Returns the aggregated MVT tile.

    Parameters
    ----------
    geom : Column
        The geometry column to aggregate.
    attributes : Column
        The attributes column to aggregate.
    zxyID : Column
        The zxyID column to aggregate.

    Returns
    -------
    Column
        The aggregated MVT tile.
    """
    return config.mosaic_context.invoke_function(
        "st_asmvttile_agg",
        pyspark_to_java_column(geom),
        pyspark_to_java_column(attributes),
        pyspark_to_java_column(zxyID),
    )


def st_intersects_agg(leftIndex: ColumnOrName, rightIndex: ColumnOrName) -> Column:
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
        "st_intersects_agg",
        pyspark_to_java_column(leftIndex),
        pyspark_to_java_column(rightIndex),
    )


def st_intersects_agg(leftIndex: ColumnOrName, rightIndex: ColumnOrName) -> Column:
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
        "st_intersects_agg",
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


def rst_merge_agg(raster_tile: ColumnOrName) -> Column:
    """
    Merges (unions) the aggregated raster tiles into a single tile.
    Returns the raster tile representing the aggregated union of rasters on some grid cell.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Aggregate Raster tile column to merge.

    Returns
    -------
     Column (RasterTileType)
        Raster tile struct of the union raster.
    """
    return config.mosaic_context.invoke_function(
        "rst_merge_agg", pyspark_to_java_column(raster_tile)
    )


def rst_combineavg_agg(raster_tile: ColumnOrName) -> Column:
    """
    Returns the raster tile representing the aggregated average of rasters.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Aggregate raster tile col to combine.

    Returns
    -------
    Column (RasterTileType)
        The combined raster tile.
    """
    return config.mosaic_context.invoke_function(
        "rst_combineavg_agg", pyspark_to_java_column(raster_tile)
    )


def rst_derivedband_agg(
    raster_tile: ColumnOrName, python_func: ColumnOrName, func_name: ColumnOrName
) -> Column:
    """
    Returns the raster tile representing the aggregation of rasters using provided python function.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Aggregate raster tile col to derive from.
    python_func : Column (StringType)
        The python function to apply to the bands.
    func_name : Column (StringType)
        The name of the function.

    Returns
    -------
    Column (RasterTileType)
        Creates a new band by applying the given python function to the input rasters.
        The result is a raster tile.

    """
    return config.mosaic_context.invoke_function(
        "rst_derivedband_agg",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(python_func),
        pyspark_to_java_column(func_name),
    )
