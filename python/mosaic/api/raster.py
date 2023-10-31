from pyspark.sql import Column
from pyspark.sql.functions import _to_java_column as pyspark_to_java_column

from mosaic.config import config
from mosaic.utils.types import ColumnOrName

#######################
# Raster functions    #
#######################

__all__ = [
    "rst_bandmetadata",
    "rst_boundingbox",
    "rst_clip",
    "rst_combineavg",
    "rst_fromfile",
    "rst_frombands",
    "rst_georeference",
    "ret_getnodata",
    "rst_getsubdataset",
    "rst_height",
    "rst_isempty",
    "rst_initnodata",
    "rst_memsize",
    "rst_metadata",
    "rst_merge",
    "rst_numbands",
    "rst_ndvi",
    "rst_pixelheight",
    "rst_pixelwidth",
    "rst_rastertogridavg",
    "rst_rastertogridcount",
    "rst_rastertogridmax",
    "rst_rastertogridmin",
    "rst_rastertogridmedian",
    "rst_rastertoworldcoord",
    "rst_rastertoworldcoordx",
    "rst_rastertoworldcoordy",
    "rst_retile",
    "rst_rotation",
    "rst_scalex",
    "rst_scaley",
    "rst_setnodata",
    "rst_skewx",
    "rst_skewy",
    "rst_srid",
    "rst_subdatasets",
    "rst_summary",
    "rst_subdivide",
    "rst_tessellate",
    "rst_to_overlapping_tiles",
    "rst_tryopen",
    "rst_upperleftx",
    "rst_upperlefty",
    "rst_width",
    "rst_worldtorastercoord",
    "rst_worldtorastercoordx",
    "rst_worldtorastercoordy",
]


def rst_bandmetadata(raster: ColumnOrName, band: ColumnOrName) -> Column:
    """
    Returns the metadata for the band as a map type, (key->value) pairs.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.
    band : Column (IntegerType)
        Band index, starts from 1.

    Returns
    -------
    Column (MapType(StringType, StringType)
        A map of metadata key-value pairs.

    """
    return config.mosaic_context.invoke_function(
        "rst_bandmetadata", pyspark_to_java_column(raster), pyspark_to_java_column(band)
    )


def rst_boundingbox(raster: ColumnOrName) -> Column:
    """
    Returns the bounding box of the raster as a WKT polygon.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (StringType)
        A WKT polygon representing the bounding box of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_boundingbox", pyspark_to_java_column(raster)
    )


def rst_clip(raster: ColumnOrName, geometry: ColumnOrName) -> Column:
    """
    Clips the raster to the given geometry.
    The result is the path to the clipped raster.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.
    geometry : Column (StringType)
        The geometry to clip the raster to.

    Returns
    -------
    Column (StringType)
        The path to the clipped raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_clip", pyspark_to_java_column(raster), pyspark_to_java_column(geometry)
    )


def rst_combineavg(rasters: ColumnOrName) -> Column:
    """
    Combines the rasters into a single raster.

    Parameters
    ----------
    rasters : Column (ArrayType(StringType))
        Raster tiles to combine.

    Returns
    -------
    Column (RasterTile)
        The combined raster tile.

    """
    return config.mosaic_context.invoke_function(
        "rst_combineavg", pyspark_to_java_column(rasters)
    )


def rst_georeference(raster: ColumnOrName) -> Column:
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
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (ArrayType(DoubleType))
        A map of metadata key-value pairs.

    """
    return config.mosaic_context.invoke_function(
        "rst_georeference", pyspark_to_java_column(raster)
    )


def ret_getnodata(raster: ColumnOrName) -> Column:
    """
    Returns the nodata value of the band.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.
    band : Column (IntegerType)
        Band index, starts from 1.

    Returns
    -------
    Column (DoubleType)
        The nodata value of the band.

    """
    return config.mosaic_context.invoke_function(
        "ret_getnodata", pyspark_to_java_column(raster)
    )


def rst_getsubdataset(raster: ColumnOrName, subdataset: ColumnOrName) -> Column:
    """
    Returns the subdataset of the raster.
    The subdataset is the path to the subdataset of the raster.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.
    subdataset : Column (IntegerType)
        The index of the subdataset to get.

    Returns
    -------
    Column (StringType)
        The path to the subdataset.

    """
    return config.mosaic_context.invoke_function(
        "rst_getsubdataset",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(subdataset),
    )


def rst_height(raster: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (IntegerType)
        The height of the raster in pixels.

    """
    return config.mosaic_context.invoke_function(
        "rst_height", pyspark_to_java_column(raster)
    )


def rst_initnodata(raster: ColumnOrName) -> Column:
    """
    Initializes the nodata value of the band.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (StringType)
        The path to the raster file.

    """
    return config.mosaic_context.invoke_function(
        "rst_initnodata",
        pyspark_to_java_column(raster)
    )


def rst_isempty(raster: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (BooleanType)
        The flag indicating if the raster is empty.

    """
    return config.mosaic_context.invoke_function(
        "rst_isempty", pyspark_to_java_column(raster)
    )


def rst_memsize(raster: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (IntegerType)
        The size of the raster in bytes.

    """
    return config.mosaic_context.invoke_function(
        "rst_memsize", pyspark_to_java_column(raster)
    )


def rst_metadata(raster: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (MapType<StringType, StringType>)
        The metadata of the raster as a map type, (key->value) pairs.

    """
    return config.mosaic_context.invoke_function(
        "rst_metadata", pyspark_to_java_column(raster)
    )


def rst_merge(rasters: ColumnOrName) -> Column:
    """
    Merges the rasters into a single raster.
    The result is the path to the merged raster.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    rasters : Column (ArrayType(StringType))
        Paths to the rasters to merge.

    Returns
    -------
    Column (StringType)
        The path to the merged raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_merge", pyspark_to_java_column(rasters)
    )


def rst_frombands(bands: ColumnOrName) -> Column:
    """
    Merges the bands into a single raster.
    The result is the path to the merged raster.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    bands : Column (ArrayType(StringType))
        Paths to the bands to merge.

    Returns
    -------
    Column (StringType)
        The path to the merged raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_frombands", pyspark_to_java_column(bands)
    )


def rst_numbands(raster: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (IntegerType)
        The number of bands in the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_numbands", pyspark_to_java_column(raster)
    )


def rst_ndvi(raster: ColumnOrName, band1: ColumnOrName, band2: ColumnOrName) -> Column:
    """
    Computes the NDVI of the raster.
    The result is the path to the NDVI raster.
    The result is stored in the checkpoint directory.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.
    band1 : Column (IntegerType)
        The first band index.
    band2 : Column (IntegerType)
        The second band index.

    Returns
    -------
    Column (StringType)
        The path to the NDVI raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_ndvi",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(band1),
        pyspark_to_java_column(band2),
    )


def rst_pixelheight(raster: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (IntegerType)
        The height of the pixel in the raster derived via GeoTransform.

    """
    return config.mosaic_context.invoke_function(
        "rst_pixelheight", pyspark_to_java_column(raster)
    )


def rst_pixelwidth(raster: ColumnOrName) -> Column:
    """
    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (IntegerType)
        The width of the pixel in the raster derived via GeoTransform.

    """
    return config.mosaic_context.invoke_function(
        "rst_pixelwidth", pyspark_to_java_column(raster)
    )


def rst_rastertogridavg(raster: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the average of the pixel values in the cell.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (ArrayType(ArrayType(StructType(LongType|StringType, DoubleType))))
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridavg",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(resolution),
    )


def rst_rastertogridcount(raster: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the number of pixels in the cell.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (ArrayType(ArrayType(StructType(LongType|StringType, DoubleType))))
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridcount",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(resolution),
    )


def rst_rastertogridmax(raster: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the maximum pixel value.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (ArrayType(ArrayType(StructType(LongType|StringType, DoubleType))))
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridmax",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(resolution),
    )


def rst_rastertogridmedian(raster: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the median pixel value.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (ArrayType<ArrayType<StructType<LongType|StringType, DoubleType>>>)
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridmedian",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(resolution),
    )


def rst_rastertogridmin(raster: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the minimum pixel value.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (ArrayType(ArrayType(StructType(LongType|StringType, DoubleType))))
        A collection (cellID->value) pairs for each band of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertogridmin",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(resolution),
    )


def rst_rastertoworldcoord(
    raster: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is a WKT point geometry.
    The coordinates are computed using the GeoTransform of the raster to respect the projection.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (StringType)
        A point geometry in WKT format.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertoworldcoord",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_rastertoworldcoordx(
    raster: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is the X coordinate of the point after applying the GeoTransform of the raster.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (StringType)
        The X coordinate of the point after applying the GeoTransform of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertoworldcoordx",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_rastertoworldcoordy(
    raster: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the world coordinates of the raster pixel at the given x and y coordinates.
    The result is the Y coordinate of the point after applying the GeoTransform of the raster.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (StringType)
        The X coordinate of the point after applying the GeoTransform of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_rastertoworldcoordy",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(x),
        pyspark_to_java_column(y),
    )


def rst_retile(
    raster: ColumnOrName, tileWidth: ColumnOrName, tileHeight: ColumnOrName
) -> Column:
    """
    Retiles the raster to the given tile size. The result is a collection of new raster files.
    The new rasters are stored in the checkpoint directory.
    The results are the paths to the new rasters.
    The result set is automatically exploded.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (StringType)
        The path to the raster tiles exploded.

    """
    return config.mosaic_context.invoke_function(
        "rst_retile",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(tileWidth),
        pyspark_to_java_column(tileHeight),
    )


def rst_rotation(raster: ColumnOrName) -> Column:
    """
    Computes the rotation of the raster in degrees.
    The rotation is the angle between the X axis and the North axis.
    The rotation is computed using the GeoTransform of the raster.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (DoubleType)
        The rotation of the raster in degrees.

    """
    return config.mosaic_context.invoke_function(
        "rst_rotation", pyspark_to_java_column(raster)
    )


def rst_scalex(raster: ColumnOrName) -> Column:
    """
    Computes the scale of the raster in the X direction.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (DoubleType)
        The scale of the raster in the X direction.

    """
    return config.mosaic_context.invoke_function(
        "rst_scalex", pyspark_to_java_column(raster)
    )


def rst_scaley(raster: ColumnOrName) -> Column:
    """
    Computes the scale of the raster in the Y direction.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (DoubleType)
        The scale of the raster in the Y direction.

    """
    return config.mosaic_context.invoke_function(
        "rst_scaley", pyspark_to_java_column(raster)
    )


def rst_setnodata(raster: ColumnOrName, nodata: ColumnOrName) -> Column:
    """
    Sets the nodata value of the band.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.
    nodata : Column (DoubleType)
        The nodata value to set.

    Returns
    -------
    Column (StringType)
        The path to the raster file.

    """
    return config.mosaic_context.invoke_function(
        "rst_setnodata",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(nodata),
    )


def rst_skewx(raster: ColumnOrName) -> Column:
    """
    Computes the skew of the raster in the X direction.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (DoubleType)
        The skew of the raster in the X direction.

    """
    return config.mosaic_context.invoke_function(
        "rst_skewx", pyspark_to_java_column(raster)
    )


def rst_skewy(raster: ColumnOrName) -> Column:
    """
    Computes the skew of the raster in the Y direction.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (DoubleType)
        The skew of the raster in the Y direction.

    """
    return config.mosaic_context.invoke_function(
        "rst_skewy", pyspark_to_java_column(raster)
    )


def rst_srid(raster: ColumnOrName) -> Column:
    """
    Computes the SRID of the raster.
    The SRID is the EPSG code of the raster.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (IntegerType)
        The SRID of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_srid", pyspark_to_java_column(raster)
    )


def rst_subdatasets(raster: ColumnOrName) -> Column:
    """
    Computes the subdatasets of the raster.
    The subdatasets are the paths to the subdatasets of the raster.
    The result is a map of the subdataset path to the subdatasets and the description of the subdatasets.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (MapType(StringType, StringType))
        The SRID of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_subdatasets", pyspark_to_java_column(raster)
    )


def rst_summary(raster: ColumnOrName) -> Column:
    """
    Computes the summary of the raster.
    The summary is a map of the statistics of the raster.
    The logic is produced by gdalinfo procedure.
    The result is stored as JSON.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (StringType)
        A JSON string containing the summary of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_summary", pyspark_to_java_column(raster)
    )


def rst_tessellate(raster: ColumnOrName, resolution: ColumnOrName) -> Column:
    """
    Clip the raster into raster tiles where each tile is a grid tile for the given resolution.
    The tile set union forms the original raster.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.
    resolution : Column (IntegerType)
        The resolution of the tiles.

    Returns
    -------
    Column (RasterTiles)
        A struct containing the tiles of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_tessellate",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(resolution),
    )


def rst_fromfile(raster: ColumnOrName, sizeInMB: ColumnOrName) -> Column:
    """
    Tiles the raster into tiles of the given size.
    :param raster:
    :param sizeInMB:
    :return:
    """

    return config.mosaic_context.invoke_function(
        "rst_fromfile",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(sizeInMB)
    )


def rst_to_overlapping_tiles(raster: ColumnOrName, width: ColumnOrName, height: ColumnOrName, overlap: ColumnOrName) -> Column:
    """
    Tiles the raster into tiles of the given size.
    :param raster:
    :param sizeInMB:
    :return:
    """

    return config.mosaic_context.invoke_function(
        "rst_to_overlapping_tiles",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(width),
        pyspark_to_java_column(height),
        pyspark_to_java_column(overlap)
    )


def rst_tryopen(raster: ColumnOrName) -> Column:
    """
    Tries to open the raster and returns a flag indicating if the raster can be opened.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (BooleanType)
        Whether the raster can be opened.

    """
    return config.mosaic_context.invoke_function(
        "rst_tryopen", pyspark_to_java_column(raster)
    )


def rst_subdivide(raster: ColumnOrName, size_in_mb: ColumnOrName) -> Column:
    """
    Subdivides the raster into tiles that have to be smaller than the given size in MB.
    All the tiles have the same aspect ratio as the original raster.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.
    size_in_mb : Column (IntegerType)
        The size of the tiles in MB.

    Returns
    -------
    Column (RasterTiles)
        A collection of tiles of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_subdivide",
        pyspark_to_java_column(raster),
        pyspark_to_java_column(size_in_mb),
    )


def rst_upperleftx(raster: ColumnOrName) -> Column:
    """
    Computes the upper left X coordinate of the raster.
    The value is computed based on GeoTransform.

    Parameters
    ----------
    raster : Column (StringType)
        Path to the raster file.

    Returns
    -------
    Column (DoubleType)
        The upper left X coordinate of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_upperleftx", pyspark_to_java_column(raster)
    )


def rst_upperlefty(raster: ColumnOrName) -> Column:
    """
    Computes the upper left Y coordinate of the raster.
    The value is computed based on GeoTransform.

    Parameters
    ----------
    raster : Column (StringType)
       Path to the raster file.

    Returns
    -------
    Column (DoubleType)
       The upper left Y coordinate of the raster.

    """
    return config.mosaic_context.invoke_function(
        "rst_upperlefty", pyspark_to_java_column(raster)
    )


def rst_width(raster: ColumnOrName) -> Column:
    """
    Computes the width of the raster in pixels.

    Parameters
    ----------
    raster : Column (StringType)
       Path to the raster file.

    Returns
    -------
    Column (IntegerType)
         The width of the raster in pixels.

    """
    return config.mosaic_context.invoke_function(
        "rst_width", pyspark_to_java_column(raster)
    )


def rst_worldtorastercoord(
    raster: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.

    Parameters
    ----------
    raster : Column (StringType)
       Path to the raster file.

    Returns
    -------
    Column (IntegerType)
            The pixel coordinates.

    """
    return config.mosaic_context.invoke_function(
        "rst_worldtorastercoord", pyspark_to_java_column(raster)
    )


def rst_worldtorastercoordx(
    raster: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.
    This method returns the X coordinate.

    Parameters
    ----------
    raster : Column (StringType)
       Path to the raster file.

    Returns
    -------
    Column (IntegerType)
            The X pixel coordinate.

    """
    return config.mosaic_context.invoke_function(
        "rst_worldtorastercoordx", pyspark_to_java_column(raster)
    )


def rst_worldtorastercoordy(
    raster: ColumnOrName, x: ColumnOrName, y: ColumnOrName
) -> Column:
    """
    Computes the raster coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.
    This method returns the Y coordinate.

    Parameters
    ----------
    raster : Column (StringType)
       Path to the raster file.

    Returns
    -------
    Column (IntegerType)
            The Y pixel coordinate.

    """
    return config.mosaic_context.invoke_function(
        "rst_worldtorastercoordy", pyspark_to_java_column(raster)
    )
