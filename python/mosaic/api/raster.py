import numpy as np

from typing import Any

from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column
from pyspark.sql.functions import lit

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

#######################
# Raster functions    #
#######################

__all__ = [
    "rst_asformat",
    "rst_avg",
    "rst_bandmetadata",
    "rst_boundingbox",
    "rst_clip",
    "rst_combineavg",
    "rst_convolve",
    "rst_derivedband",
    "rst_dtmfromgeoms",
    "rst_filter",
    "rst_format",
    "rst_frombands",
    "rst_fromcontent",
    "rst_fromfile",
    "rst_georeference",
    "rst_getnodata",
    "rst_getsubdataset",
    "rst_height",
    "rst_initnodata",
    "rst_isempty",
    "rst_maketiles",
    "rst_mapalgebra",
    "rst_max",
    "rst_median",
    "rst_memsize",
    "rst_merge",
    "rst_metadata",
    "rst_min",
    "rst_ndvi",
    "rst_numbands",
    "rst_pixelcount",
    "rst_pixelheight",
    "rst_pixelwidth",
    "rst_rastertogridavg",
    "rst_rastertogridcount",
    "rst_rastertogridmax",
    "rst_rastertogridmedian",
    "rst_rastertogridmin",
    "rst_rastertoworldcoord",
    "rst_rastertoworldcoordx",
    "rst_rastertoworldcoordy",
    "rst_retile",
    "rst_rotation",
    "rst_type",
    "rst_scalex",
    "rst_scaley",
    "rst_separatebands",
    "rst_setnodata",
    "rst_setsrid",
    "rst_skewx",
    "rst_skewy",
    "rst_srid",
    "rst_subdatasets",
    "rst_subdivide",
    "rst_summary",
    "rst_tessellate",
    "rst_transform",
    "rst_tooverlappingtiles",
    "rst_to_overlapping_tiles",  # <- deprecated
    "rst_type",
    "rst_to_overlapping_tiles",
    "rst_tryopen",
    "rst_updatetype",
    "rst_upperleftx",
    "rst_upperlefty",
    "rst_width",
    "rst_worldtorastercoord",
    "rst_worldtorastercoordx",
    "rst_worldtorastercoordy",
    "rst_write",
]


def rst_asformat(raster_tile: ColumnOrName, driver: ColumnOrName) -> Column:
    """
    Translates the raster to the specified format.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    driver : Column (StringType)
        The format driver to use.

    Returns
    -------
    Column (RasterTileType)
        The updated raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_asformat",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(driver),
    )


def rst_avg(raster_tile: ColumnOrName) -> Column:
    """
    Returns an array containing mean value for each band.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column ArrayType(DoubleType)
        mean value per band.

    """
    return config.mosaic_context.invoke_function(
        "rst_avg", pyspark_to_java_column(raster_tile)
    )


def rst_bandmetadata(raster_tile: ColumnOrName, band: ColumnOrName) -> Column:
    """
    Returns the metadata for the band as a map type, (key->value) pairs.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    band : Column (IntegerType)
        Band index, starts from 1.

    Returns
    -------
    Column (MapType(StringType, StringType)
        A map of metadata key-value pairs.

    """
    return config.mosaic_context.invoke_function(
        "rst_bandmetadata",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(band),
    )


def rst_boundingbox(raster_tile: ColumnOrName) -> Column:
    """
    Returns the bounding box of the raster as a WKT polygon.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (StringType)
        A WKT polygon representing the bounding box of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_boundingbox", pyspark_to_java_column(raster_tile)
    )


def rst_clip(
    raster_tile: ColumnOrName, geometry: ColumnOrName, cutline_all_touched: Any = True
) -> Column:
    """
    Clips `raster_tile` to the given supported `geometry` (WKT, WKB, GeoJSON).
    The result is a Mosaic raster tile representing the clipped raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    geometry : Column (StringType)
        The geometry to clip the tile to.
    cutline_all_touched : Column (BooleanType)
        optional override to specify whether any pixels touching
        cutline should be included vs half-in only, default is true

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.

    """
    if type(cutline_all_touched) == bool:
        cutline_all_touched = lit(cutline_all_touched)

    return config.mosaic_context.invoke_function(
        "rst_clip",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(geometry),
        pyspark_to_java_column(cutline_all_touched),
    )


def rst_combineavg(raster_tiles: ColumnOrName) -> Column:
    """
    Combines the rasters into a single raster.

    Parameters
    ----------
    raster_tiles : Column (ArrayType(RasterTileType))
        Raster tiles to combine.

    Returns
    -------
    Column (RasterTileType)
        The combined raster tile.

    """
    return config.mosaic_context.invoke_function(
        "rst_combineavg", pyspark_to_java_column(raster_tiles)
    )


def rst_convolve(raster_tile: ColumnOrName, kernel: ColumnOrName) -> Column:
    """
    Applies a convolution filter to the raster.
    The result is Mosaic raster tile struct column to the filtered raster.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    kernel : Column (ArrayType(ArrayType(DoubleType)))
        The kernel to apply to the raster.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.

    """
    return config.mosaic_context.invoke_function(
        "rst_convolve",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(kernel),
    )


def rst_derivedband(
    raster_tile: ColumnOrName, python_func: ColumnOrName, func_name: ColumnOrName
) -> Column:
    """
    Creates a new band by applying the given python function to the input rasters.
    The result is a raster tile.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    python_func : Column (StringType)
        The python function to apply to the bands.
    func_name : Column (StringType)
        The name of the function.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.

    """
    return config.mosaic_context.invoke_function(
        "rst_derivedband",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(python_func),
        pyspark_to_java_column(func_name),
    )


def rst_dtmfromgeoms(
    points_array: ColumnOrName,
    lines_array: ColumnOrName,
    merge_tolerance: ColumnOrName,
    snap_tolerance: ColumnOrName,
    origin: ColumnOrName,
    x_width: ColumnOrName,
    y_width: ColumnOrName,
    x_size: ColumnOrName,
    y_size: ColumnOrName,
    split_point_finder: str = "NONENCROACHING",
    no_data_value: float = np.nan,
) -> Column:
    """
    Generate a raster with interpolated elevations across a grid of points described by
    `origin`, `x_width`, `y_width`, `x_size`, and `y_size`.

    The underlying algorithm first creates a surface mesh by triangulating `points_array`
    (including `lines_array` as a set of constraint lines) then determines where each point
    in the grid would lie on the surface mesh. Finally, it interpolates the
    elevation of that point based on the surrounding triangle's vertices.

    Notes:
    - Uses (x, y) _not_ (i, j) order to generate the grid (i.e. `origin` is assumed to be the bottom-left corner).
    To generate a grid from a top-left `origin`, use a negative value for `y_size`.

    Parameters
    ----------
    points_array : Column
        An array of mass points including Z-values.
    lines_array : Column
        An array of lines that are used as constraints during the triangulation process.
    merge_tolerance : Column
        A tolerance used to coalesce points in close proximity to each other before performing triangulation.
    snap_tolerance : Column
        A snapping tolerance used to relate created points to their corresponding lines for elevation interpolation.
    origin : Column
        The bottom-left corner of the grid. Use a negative value for `y_size` if you wish to supply a top-left origin.
    x_width : Column
        The number of points on the grid's x-axis
    y_width : Column
        The number of points on the grid's y-axis
    x_size : Column
        The spacing between each point on the grid's x-axis
        (in meters or degrees depending on the projection of `points_array`)
    y_size : Column
        The spacing between each point on the grid's y-axis
        (in meters or degrees depending on the projection of `points_array`)
    split_point_finder : String
        (Optional) The split point finding algorithm used to incorporate `lines_array` into the triangulation.
        Default is "NONENCROACHING", alternative is "MIDPOINT".
    no_data_value : Float
        (Optional) The nodata value to assign to the output raster. Must be a Double Type value. Default is NaN.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.
    """
    return config.mosaic_context.invoke_function(
        "rst_dtmfromgeoms",
        pyspark_to_java_column(points_array),
        pyspark_to_java_column(lines_array),
        pyspark_to_java_column(merge_tolerance),
        pyspark_to_java_column(snap_tolerance),
        pyspark_to_java_column(lit(split_point_finder)),
        pyspark_to_java_column(origin),
        pyspark_to_java_column(x_width),
        pyspark_to_java_column(y_width),
        pyspark_to_java_column(x_size),
        pyspark_to_java_column(y_size),
        pyspark_to_java_column(lit(no_data_value)),
    )


def rst_filter(raster_tile: ColumnOrName, kernel_size: Any, operation: Any) -> Column:
    """
    Applies a filter to the raster.
    :param raster_tile: Mosaic raster tile struct column.
    :param kernel_size: The size of the kernel. Has to be odd.
    :param operation: The operation to apply to the kernel.
    :return: A new raster tile with the filter applied.
    """
    if type(kernel_size) == int:
        kernel_size = lit(kernel_size)

    if type(operation) == str:
        operation = lit(operation)

    return config.mosaic_context.invoke_function(
        "rst_filter",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(kernel_size),
        pyspark_to_java_column(operation),
    )


def rst_format(raster_tile: ColumnOrName) -> Column:
    """
    Returns the format of the raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (StringType)
        The format of the raster (driver required for reading).

    """
    return config.mosaic_context.invoke_function(
        "rst_format", pyspark_to_java_column(raster_tile)
    )


def rst_frombands(bands: ColumnOrName) -> Column:
    """
    Stack an array of bands into a raster tile.
    The result is Mosaic raster tile struct.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    bands : Column (ArrayType(RasterTileType))
        Raster tiles of the bands to merge.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct of the band stacking.

    """
    return config.mosaic_context.invoke_function(
        "rst_frombands", pyspark_to_java_column(bands)
    )


def rst_fromcontent(
    raster_bin: ColumnOrName, driver: ColumnOrName, size_in_mb: Any = -1
) -> Column:
    """
    Tiles the raster binary into tiles of the given size.
    :param raster_bin:
    :param driver:
    :param size_in_mb:
    :return:
    """
    if type(size_in_mb) == int:
        size_in_mb = lit(size_in_mb)

    return config.mosaic_context.invoke_function(
        "rst_fromcontent",
        pyspark_to_java_column(raster_bin),
        pyspark_to_java_column(driver),
        pyspark_to_java_column(size_in_mb),
    )


def rst_fromfile(raster_path: ColumnOrName, size_in_mb: Any = -1) -> Column:
    """
    Tiles the raster into tiles of the given size.
    :param raster_path:
    :param sizeInMB:
    :return:
    """
    if type(size_in_mb) == int:
        size_in_mb = lit(size_in_mb)

    return config.mosaic_context.invoke_function(
        "rst_fromfile",
        pyspark_to_java_column(raster_path),
        pyspark_to_java_column(size_in_mb),
    )


def rst_georeference(raster_tile: ColumnOrName) -> Column:
    """
    Returns GeoTransform of the raster as a GT array of doubles.
    GT(0) x-coordinate of the upper-left corner of the upper-left pixel.
    GT(1) w-e pixel resolution / pixel width.
    GT(2) row rotation (typically zero).
    GT(3) y-coordinate of the upper-left corner of the upper-left pixel.
    GT(4) column rotation (typically zero).
    GT(5) n-s pixel resolution / pixel height (negative value for a north-up image).


    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (ArrayType(DoubleType))
        A map of metadata key-value pairs.

    """
    return config.mosaic_context.invoke_function(
        "rst_georeference", pyspark_to_java_column(raster_tile)
    )


def rst_getnodata(raster_tile: ColumnOrName) -> Column:
    """
    Returns the nodata value of the band.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    band : Column (IntegerType)
        Band index, starts from 1.

    Returns
    -------
    Column (DoubleType)
        The nodata value of the band.

    """
    return config.mosaic_context.invoke_function(
        "rst_getnodata", pyspark_to_java_column(raster_tile)
    )


def rst_getsubdataset(raster_tile: ColumnOrName, subdataset: ColumnOrName) -> Column:
    """
    Returns the subdataset of the raster.
    The subdataset is the Mosaic raster tile struct of the subdataset of the raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    subdataset : Column (IntegerType)
        The index of the subdataset to get.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct of the subdataset.

    """
    return config.mosaic_context.invoke_function(
        "rst_getsubdataset",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(subdataset),
    )


def rst_height(raster_tile: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
        The height of the raster in pixels.

    """
    return config.mosaic_context.invoke_function(
        "rst_height", pyspark_to_java_column(raster_tile)
    )


def rst_initnodata(raster_tile: ColumnOrName) -> Column:
    """
    Initializes the nodata value of the band.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.

    """
    return config.mosaic_context.invoke_function(
        "rst_initnodata", pyspark_to_java_column(raster_tile)
    )


def rst_isempty(raster_tile: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (BooleanType)
        The flag indicating if the raster is empty.

    """
    return config.mosaic_context.invoke_function(
        "rst_isempty", pyspark_to_java_column(raster_tile)
    )


def rst_maketiles(
    input: ColumnOrName,
    driver: Any = "no_driver",
    size_in_mb: Any = -1,
    with_checkpoint: Any = False,
) -> Column:
    """
    Tiles the raster into tiles of the given size.
    :param input: If the raster is stored on disc, the path
        to the raster is provided. If the raster is stored in memory, the bytes of
        the raster are provided.
    :param driver: The driver to use for reading the raster. If not specified, the driver is
        inferred from the file extension. If the input is a byte array, the driver
        has to be specified.
    :param size_in_mb: The size of the tiles in MB. If set to -1, the file is loaded and returned
        as a single tile. If set to 0, the file is loaded and subdivided into
        tiles of size 64MB. If set to a positive value, the file is loaded and
        subdivided into tiles of the specified size. If the file is too big to fit
        in memory, it is subdivided into tiles of size 64MB.
    :param with_checkpoint: If set to true, the tiles are written to the checkpoint directory. If set
        to false, the tiles are returned as a in-memory byte arrays.
    :return: A collection of tiles of the raster.
    """
    if type(size_in_mb) == int:
        size_in_mb = lit(size_in_mb)

    if type(with_checkpoint) == bool:
        with_checkpoint = lit(with_checkpoint)

    if type(driver) == str:
        driver = lit(driver)

    return config.mosaic_context.invoke_function(
        "rst_maketiles",
        pyspark_to_java_column(input),
        pyspark_to_java_column(driver),
        pyspark_to_java_column(size_in_mb),
        pyspark_to_java_column(with_checkpoint),
    )


def rst_mapalgebra(raster_tile: ColumnOrName, json_spec: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    json_spec : Column (StringType)

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.

    """
    return config.mosaic_context.invoke_function(
        "rst_mapalgebra",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(json_spec),
    )


def rst_max(raster_tile: ColumnOrName) -> Column:
    """
    Returns an array containing max value for each band.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column ArrayType(DoubleType)
        max value per band.

    """
    return config.mosaic_context.invoke_function(
        "rst_max", pyspark_to_java_column(raster_tile)
    )


def rst_median(raster_tile: ColumnOrName) -> Column:
    """
    Returns an array containing median value for each band.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column ArrayType(DoubleType)
        median value per band.

    """
    return config.mosaic_context.invoke_function(
        "rst_median", pyspark_to_java_column(raster_tile)
    )


def rst_memsize(raster_tile: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
        The size of the raster in bytes.

    """
    return config.mosaic_context.invoke_function(
        "rst_memsize", pyspark_to_java_column(raster_tile)
    )


def rst_merge(raster_tiles: ColumnOrName) -> Column:
    """
    Merges (mosaics) the rasters into a single raster.
    The result is Mosaic raster tile struct of the merged raster.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    raster_tiles : Column (ArrayType(RasterTileType))
        Raster tiles to merge.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct of the merged raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_merge", pyspark_to_java_column(raster_tiles)
    )


def rst_metadata(raster_tile: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (MapType<StringType, StringType>)
        The metadata of the raster as a map type, (key->value) pairs.

    """
    return config.mosaic_context.invoke_function(
        "rst_metadata", pyspark_to_java_column(raster_tile)
    )


def rst_min(raster_tile: ColumnOrName) -> Column:
    """
    Returns an array containing min value for each band.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column ArrayType(DoubleType)
        min value per band.

    """
    return config.mosaic_context.invoke_function(
        "rst_min", pyspark_to_java_column(raster_tile)
    )


def rst_ndvi(
    raster_tile: ColumnOrName, band1: ColumnOrName, band2: ColumnOrName
) -> Column:
    """
    Computes the NDVI of the raster.
    The result is Mosaic raster tile struct of the NDVI raster.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    band1 : Column (IntegerType)
        The first band index.
    band2 : Column (IntegerType)
        The second band index.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile structs of the NDVI raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_ndvi",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(band1),
        pyspark_to_java_column(band2),
    )


def rst_numbands(raster_tile: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
        The number of bands in the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_numbands", pyspark_to_java_column(raster_tile)
    )


def rst_pixelcount(
    raster_tile: ColumnOrName, count_nodata: Any = False, count_all: Any = False
) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    count_nodata : Column(BooleanType)
        If false do not include noData pixels in count (default is false).
    count_all : Column(BooleanType)
        If true, simply return bandX * bandY (default is false).
    Returns
    -------
    Column (ArrayType(LongType))
        Array containing valid pixel count values for each band.

    """

    if type(count_nodata) == bool:
        count_nodata = lit(count_nodata)

    if type(count_all) == bool:
        count_all = lit(count_all)

    return config.mosaic_context.invoke_function(
        "rst_pixelcount",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(count_nodata),
        pyspark_to_java_column(count_all),
    )


def rst_pixelheight(raster_tile: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
        The height of the pixel in the raster derived via GeoTransform.

    """
    return config.mosaic_context.invoke_function(
        "rst_pixelheight", pyspark_to_java_column(raster_tile)
    )


def rst_pixelwidth(raster_tile: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
        The width of the pixel in the raster derived via GeoTransform.

    """
    return config.mosaic_context.invoke_function(
        "rst_pixelwidth", pyspark_to_java_column(raster_tile)
    )


def rst_rastertogridavg(raster_tile: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the average of the pixel values in the cell.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (ArrayType(ArrayType(StructType(LongType|StringType, DoubleType))))
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridavg",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(resolution),
    )


def rst_rastertogridcount(
    raster_tile: ColumnOrName, resolution: ColumnOrName
) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the number of pixels in the cell.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (ArrayType(ArrayType(StructType(LongType|StringType, DoubleType))))
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridcount",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(resolution),
    )


def rst_rastertogridmax(raster_tile: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the maximum pixel value.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (ArrayType(ArrayType(StructType(LongType|StringType, DoubleType))))
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridmax",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(resolution),
    )


def rst_rastertogridmedian(
    raster_tile: ColumnOrName, resolution: ColumnOrName
) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the median pixel value.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (ArrayType<ArrayType<StructType<LongType|StringType, DoubleType>>>)
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridmedian",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(resolution),
    )


def rst_rastertogridmin(raster_tile: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the minimum pixel value.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (ArrayType(ArrayType(StructType(LongType|StringType, DoubleType))))
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridmin",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(resolution),
    )


def rst_rastertoworldcoord(
    raster_tile: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is a WKT point geometry.
    The coordinates are computed using the GeoTransform of the raster to respect the projection.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (StringType)
        A point geometry in WKT format.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertoworldcoord",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_rastertoworldcoordx(
    raster_tile: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is the X coordinate of the point after applying the GeoTransform of the raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
        The X coordinate of the point after applying the GeoTransform of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertoworldcoordx",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_rastertoworldcoordy(
    raster_tile: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is the Y coordinate of the point after applying the GeoTransform of the raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
        The Y coordinate of the point after applying the GeoTransform of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertoworldcoordy",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_retile(
    raster_tile: ColumnOrName, tile_width: ColumnOrName, tile_height: ColumnOrName
) -> Column:
    """
    Retiles the raster to the given tile size. The result is a collection of new raster files.
    The new rasters are stored in the checkpoint directory.
    The results are Mosaic raster tile struct of the new rasters.
    The result set is automatically exploded.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile structs from the exploded retile.

    """
    return config.mosaic_context.invoke_function(
        "rst_retile",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(tile_width),
        pyspark_to_java_column(tile_height),
    )


def rst_rotation(raster_tile: ColumnOrName) -> Column:
    """
    Computes the rotation of the raster in degrees.
    The rotation is the angle between the X axis and the North axis.
    The rotation is computed using the GeoTransform of the raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
        The rotation of the raster in degrees.

    """
    return config.mosaic_context.invoke_function(
        "rst_rotation", pyspark_to_java_column(raster_tile)
    )


def rst_scalex(raster_tile: ColumnOrName) -> Column:
    """
    Computes the scale of the raster in the X direction.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
        The scale of the raster in the X direction.

    """
    return config.mosaic_context.invoke_function(
        "rst_scalex", pyspark_to_java_column(raster_tile)
    )


def rst_scaley(raster_tile: ColumnOrName) -> Column:
    """
    Computes the scale of the raster in the Y direction.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
        The scale of the raster in the Y direction.

    """
    return config.mosaic_context.invoke_function(
        "rst_scaley", pyspark_to_java_column(raster_tile)
    )


def rst_separatebands(raster_tile: ColumnOrName) -> Column:
    """
    Returns a set of new single-band rasters, one for each band in the input raster.
    Result set is automatically exploded.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (MosaicTile)
        The single-band raster tiles, exploded.

    """
    return config.mosaic_context.invoke_function(
        "rst_separatebands",
        pyspark_to_java_column(raster_tile),
    )


def rst_setnodata(raster_tile: ColumnOrName, nodata: ColumnOrName) -> Column:
    """
    Sets the nodata value of the band.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    nodata : Column (DoubleType)
        The nodata value to set.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.

    """
    return config.mosaic_context.invoke_function(
        "rst_setnodata",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(nodata),
    )


def rst_setsrid(raster_tile: ColumnOrName, srid: ColumnOrName) -> Column:
    """
    Sets the SRID of the raster.
    The SRID is the EPSG code of the raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    srid : Column (IntegerType)
        EPSG authority code for the file's projection.
    Returns
    -------
    Column (MosaicRasterTile)
        The updated raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_setsrid", pyspark_to_java_column(raster_tile), pyspark_to_java_column(srid)
    )


def rst_skewx(raster_tile: ColumnOrName) -> Column:
    """
    Computes the skew of the raster in the X direction.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
        The skew of the raster in the X direction.

    """
    return config.mosaic_context.invoke_function(
        "rst_skewx", pyspark_to_java_column(raster_tile)
    )


def rst_skewy(raster_tile: ColumnOrName) -> Column:
    """
    Computes the skew of the raster in the Y direction.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
        The skew of the raster in the Y direction.

    """
    return config.mosaic_context.invoke_function(
        "rst_skewy", pyspark_to_java_column(raster_tile)
    )


def rst_srid(raster_tile: ColumnOrName) -> Column:
    """
    Computes the SRID of the raster.
    The SRID is the EPSG code of the raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
        The SRID of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_srid", pyspark_to_java_column(raster_tile)
    )


def rst_subdatasets(raster_tile: ColumnOrName) -> Column:
    """
    Computes the subdatasets of the raster.
    The input is Mosaic raster tile struct.
    The result is a map of the subdataset path to the subdatasets and the description of the subdatasets.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (MapType(StringType, StringType))
        The SRID of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_subdatasets", pyspark_to_java_column(raster_tile)
    )


def rst_subdivide(raster_tile: ColumnOrName, size_in_mb: ColumnOrName) -> Column:
    """
    Subdivides the raster into tiles that have to be smaller than the given size in MB.
    All the tiles have the same aspect ratio as the original raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    size_in_mb : Column (IntegerType)
        The size of the tiles in MB.

    Returns
    -------
    Column (RasterTileType)
        A collection of tiles of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_subdivide",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(size_in_mb),
    )


def rst_summary(raster_tile: ColumnOrName) -> Column:
    """
    Computes the summary of the raster.
    The summary is a map of the statistics of the raster.
    The logic is produced by gdalinfo procedure.
    The result is stored as JSON.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (StringType)
        A JSON string containing the summary of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_summary", pyspark_to_java_column(raster_tile)
    )


def rst_updatetype(raster_tile: ColumnOrName, data_type: ColumnOrName) -> Column:
    """
    Updates the data type of the raster.
    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    data_type : Column (StringType)
        The data type for the updated raster.
    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.
    """
    return config.mosaic_context.invoke_function(
        "rst_updatetype",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(data_type),
    )


def rst_tessellate(raster_tile: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    Clip the raster into raster tiles where each tile is a grid tile for the given resolution.
    The tile set union forms the original raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    resolution : Column (IntegerType)
        The resolution of the tiles.

    Returns
    -------
    Column (RasterTileType)
        A struct containing the tiles of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_tessellate",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(resolution),
    )


def rst_tooverlappingtiles(
    raster_tile: ColumnOrName,
    width: ColumnOrName,
    height: ColumnOrName,
    overlap: ColumnOrName,
) -> Column:
    """
    Tiles the raster into tiles of the given size.
    :param raster_tile:
    :param sizeInMB:
    :return:
    """

    return config.mosaic_context.invoke_function(
        "rst_tooverlappingtiles",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(width),
        pyspark_to_java_column(height),
        pyspark_to_java_column(overlap),
    )


def rst_to_overlapping_tiles(
    raster_tile: ColumnOrName,
    width: ColumnOrName,
    height: ColumnOrName,
    overlap: ColumnOrName,
) -> Column:
    return rst_tooverlappingtiles(raster_tile, width, height, overlap)


def rst_transform(raster_tile: ColumnOrName, srid: ColumnOrName) -> Column:
    """
    Transforms the raster to the given SRID.
    The result is a Mosaic raster tile struct of the transformed raster.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    srid : Column (IntegerType)
        EPSG authority code for the file's projection.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.

    """
    return config.mosaic_context.invoke_function(
        "rst_transform",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(srid),
    )


def rst_tryopen(raster_tile: ColumnOrName) -> Column:
    """
    Tries to open the raster and returns a flag indicating if the raster can be opened.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (BooleanType)
        Whether the raster can be opened.

    """
    return config.mosaic_context.invoke_function(
        "rst_tryopen", pyspark_to_java_column(raster_tile)
    )


def rst_type(raster_tile: ColumnOrName) -> Column:
    """
    Returns the data type of `raster_tile`.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (StringType)
        A string representing the data type.

    """
    return config.mosaic_context.invoke_function(
        "rst_type", pyspark_to_java_column(raster_tile)
    )


def rst_updatetype(raster_tile: ColumnOrName, data_type: ColumnOrName) -> Column:
    """
    Updates the data type of the raster.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    data_type : Column (StringType)
        The data type for the updated raster.

    Returns
    -------
    Column (RasterTileType)
        Mosaic raster tile struct column.

    """
    return config.mosaic_context.invoke_function(
        "rst_updatetype",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(data_type),
    )


def rst_upperleftx(raster_tile: ColumnOrName) -> Column:
    """
    Computes the upper left X coordinate of the raster.
    The value is computed based on GeoTransform.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
        The upper left X coordinate of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_upperleftx", pyspark_to_java_column(raster_tile)
    )


def rst_upperlefty(raster_tile: ColumnOrName) -> Column:
    """
    Computes the upper left Y coordinate of the raster.
    The value is computed based on GeoTransform.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
       Mosaic raster tile struct column.

    Returns
    -------
    Column (DoubleType)
       The upper left Y coordinate of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_upperlefty", pyspark_to_java_column(raster_tile)
    )


def rst_width(raster_tile: ColumnOrName) -> Column:
    """
    Computes the width of the raster in pixels.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
       Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
         The width of the raster in pixels.

    """
    return config.mosaic_context.invoke_function(
        "rst_width", pyspark_to_java_column(raster_tile)
    )


def rst_worldtorastercoord(
    raster_tile: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
       Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
            The pixel coordinates.

    """
    return config.mosaic_context.invoke_function(
        "rst_worldtorastercoord",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_worldtorastercoordx(
    raster_tile: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.
    This method returns the X coordinate.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
       Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
            The X pixel coordinate.

    """
    return config.mosaic_context.invoke_function(
        "rst_worldtorastercoordx",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_worldtorastercoordy(
    raster_tile: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.
    This method returns the Y coordinate.

    Parameters
    ----------
    raster_tile : Column (RasterTileType)
       Mosaic raster tile struct column.

    Returns
    -------
    Column (IntegerType)
            The Y pixel coordinate.

    """
    return config.mosaic_context.invoke_function(
        "rst_worldtorastercoordy",
        pyspark_to_java_column(raster_tile),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_write(raster_tile: ColumnOrName, path: ColumnOrName) -> Column:
    """
     Write rasters to the given director `path`.

     Parameters
     ----------
     raster_tile : Column (RasterTileType)
        Mosaic raster tile struct column.
    path : Column (StringType)
        The path to write the raster.

     Returns
     -------
     Column (RasterTileType)
             The raster with an updated location at `path`.

    """
    return config.mosaic_context.invoke_function(
        "rst_write", pyspark_to_java_column(raster_tile), pyspark_to_java_column(path)
    )
