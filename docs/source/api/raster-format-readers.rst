=====================
Raster Format Readers
=====================


Intro
#####
Mosaic provides spark readers for tile files supported by GDAL OGR drivers.
Only the drivers that are built by default are supported.
Here are some common useful file formats:

    * `GTiff <https://gdal.org/drivers/tile/gtiff.html>`__ (GeoTiff) using .tif file extension
    * `COG <https://gdal.org/drivers/tile/cog.html>`__ (Cloud Optimized GeoTiff) using .tif file extension
    * `HDF4 <https://gdal.org/drivers/tile/hdf4.html>`__ using .hdf file extension
    * `HDF5 <https://gdal.org/drivers/tile/hdf5.html>`__ using .h5 file extension
    * `NetCDF <https://gdal.org/drivers/tile/netcdf.html>`__ using .nc file extension
    * `JP2ECW <https://gdal.org/drivers/tile/jp2ecw.html>`__ using .jp2 file extension
    * `JP2KAK <https://gdal.org/drivers/tile/jp2kak.html>`__ using .jp2 file extension
    * `JP2OpenJPEG <https://gdal.org/drivers/tile/jp2openjpeg.html>`__ using .jp2 file extension
    * `PDF <https://gdal.org/drivers/tile/pdf.html>`__ using .pdf file extension
    * `PNG <https://gdal.org/drivers/tile/png.html>`__ using .png file extension
    * `VRT <https://gdal.org/drivers/tile/vrt.html>`__ using .vrt file extension
    * `XPM <https://gdal.org/drivers/tile/xpm.html>`__ using .xpm file extension
    * `GRIB <https://gdal.org/drivers/tile/grib.html>`__ using .grb file extension
    * `Zarr <https://gdal.org/drivers/tile/zarr.html>`__ using .zarr file extension

For more information please refer to gdal `tile driver <https://gdal.org/drivers/tile/index.html>`__ documentation.

Mosaic provides two flavors of the readers:

    * :code:`spark.read.format("gdal")` for reading 1 file per spark task
    * :code:`mos.read().format("raster_to_grid")` reader that automatically converts tile to grid.


spark.read.format("gdal")
*************************
A base Spark SQL data source for reading GDAL tile data sources.
It reads metadata of the tile and exposes the direct paths for the tile files.
The output of the reader is a DataFrame with the following columns (provided in order):

    * :code:`rawPath` - rawPath read (StringType)
    * :code:`modificationTime` - last modification of the tile (TimestampType)
    * :code:`length` -  size of the tile, e.g. memory size (LongType)
    * :code:`uuid` -  unique identifier for the tile (LongType)
    * :code:`x_Size` - width of the tile in pixels (IntegerType)
    * :code:`y_size` - height of the tile in pixels (IntegerType)
    * :code:`bandCount` - number of bands in the tile (IntegerType)
    * :code:`metadata` - tile metadata (MapType(StringType, StringType))
    * :code:`subdatasets` - tile subdatasets (MapType(StringType, StringType))
    * :code:`srid` - tile spatial reference system identifier (IntegerType)
    * :code:`tile` - loaded tile tile (StructType - RasterTileType)

.. figure:: ../images/gdal-reader.png
   :figclass: doc-figure

.. function:: format("gdal")

    Loads a GDAL tile file and returns the result as a DataFrame.
    It uses the standard spark reader patthern of :code:`spark.read.format(*).option(*).load(*)`.

    :param rawPath: rawPath to the tile file on dbfs
    :type rawPath: Column(StringType)
    :rtype: DataFrame

    :example:

.. tabs::
    .. code-tab:: py

        df = spark.read.format("gdal")\
            .option("driverName", "GTiff")\
            .load("dbfs:/rawPath/to/tile.tif")
        df.show()
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+
        |                                                                                                           tile| ySize| xSize| bandCount|             metadata|         subdatasets| srid|              proj4Str|
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+
        | {index_id: 593308294097928191, tile: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" } |  100 |  100 |        1 | {AREA_OR_POINT=Po...|                null| 4326|  +proj=longlat +da...|
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+

    .. code-tab:: scala

        val df = spark.read.format("gdal")
            .option("driverName", "GTiff")
            .load("dbfs:/rawPath/to/tile.tif")
        df.show()
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+
        |                                                                                                           tile| ySize| xSize| bandCount|             metadata|         subdatasets| srid|              proj4Str|
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+
        | {index_id: 593308294097928191, tile: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" } |  100 |  100 |        1 | {AREA_OR_POINT=Po...|                null| 4326|  +proj=longlat +da...|
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+

.. note::
    Keyword options not identified in function signature are converted to a :code:`Map<String,String>`.
    These must be supplied as a :code:`String`.
    Also, you can supply function signature values as :code:`String`.

.. warning::
    Issue 350: https://github.com/databrickslabs/mosaic/issues/350
    The tile reader 'driverName' option has to match the names provided in the above list.
    For example, if you want to read a GeoTiff file, you have to use the following option:
    .option("driverName", "GTiff") instead of .option("driverName", "tif").


mos.read().format("raster_to_grid")
***********************************
Reads a GDAL tile file and converts it to a grid.
It uses a pattern similar to standard :code:`spark.read.format(*).option(*).load(*)` pattern.
The only difference is that it uses :code:`mos.read()` instead of :code:`spark.read()`.
The tile pixels are converted to grid cells using specified combiner operation (default is mean).
If the tile pixels are larger than the grid cells, the cell values can be calculated using interpolation.
The interpolation method used is Inverse Distance Weighting (IDW) where the distance function is a k_ring
distance of the grid.
The reader supports the following options:

    * :code:`extensions` (default "*") - tile file extensions, optionally separated by ";" (StringType),
      e.g. "grib;grb" or "*" or ".tif" or  "tif" (what the file ends with will be tested), case insensitive
    * :code:`'vsizip` (default false) - if the rasters are zipped files, set this to true (BooleanType)
    * :code:`resolution` (default 0) - resolution of the output grid (IntegerType)
    * :code:`combiner` (default "mean") - combiner operation to use when converting tile to grid (StringType), options:
      "mean", "min", "max", "median", "count", "average", "avg"
    * :code:`driverName` (default "") - when the extension of the file is not enough, specify the driver (e.g. .zips) (StringType)
    * :code:`kRingInterpolate` (default 0) - if the tile pixels are larger than the grid cells, use k_ring
      interpolation with n = kRingInterpolate (IntegerType)
    * :code:`nPartitions` (default <spark.conf.get("spark.sql.shuffle.partitions")>) - you can specify the
      starting number of partitions, will grow (x10 up to 10K) for retile and/or tessellate (IntegerType)
    * :code:`retile` (default true) - recommended to re-tile to smaller tiles (BooleanType)
    * :code:`tileSize` (default 256) - size of the re-tiled tiles, tiles are always squares of tileSize x tileSize (IntegerType)
    * :code:`subdatasetName` (default "")- if the tile has subdatasets, select a specific subdataset by name (StringType)
    * :code:`uriDeepCheck` (default "false") - specify whether more extensive testing of known URI parts is needed (StringType)

.. function:: format("raster_to_grid")

    Loads a GDAL tile file and returns the result as a DataFrame.
    It uses the standard spark reader pattern of :code:`mos.read().format(*).option(*).load(*)`.

    :param rawPath: rawPath to the tile file on dbfs
    :type rawPath: Column(StringType)
    :rtype: DataFrame

    :example:

.. tabs::
    .. code-tab:: py

        df = mos.read().format("raster_to_grid")\
            .option("extensions", "tif")\
            .option("resolution", "8")\
            .option("combiner", "mean")\
            .option("retile", "true")\
            .option("tileSize", "1000")\
            .option("kRingInterpolate", "2")\
            .load("dbfs:/rawPath/to/tile.tif")
        df.show()
        +--------+--------+------------------+
        |band_id |cell_id |cell_value        |
        +--------+--------+------------------+
        |       1|       1|0.1400000000000000|
        |       1|       2|0.1400000000000000|
        |       1|       3|0.2464000000000000|
        |       1|       4|0.2464000000000000|
        +--------+--------+------------------+

    .. code-tab:: scala

        val df = MosaicContext.read.format("raster_to_grid")
            .option("extensions", "tif")
            .option("resolution", "8")
            .option("combiner", "mean")
            .option("retile", "true")
            .option("tileSize", "1000")
            .option("kRingInterpolate", "2")
            .load("dbfs:/rawPath/to/tile.tif")
        df.show()
        +--------+--------+------------------+
        |band_id |cell_id |cell_value        |
        +--------+--------+------------------+
        |       1|       1|0.1400000000000000|
        |       1|       2|0.1400000000000000|
        |       1|       3|0.2464000000000000|
        |       1|       4|0.2464000000000000|
        +--------+--------+------------------+

.. note::
    To improve performance, for 0.4.3+ gdal read strategy :code:`as_path` is used and stores interim tiles in the
    configured checkpoint directory; also, retile and/or tessellate phases store interim tiles in the configured
    checkpoint directory, with the combiner phase returning either :code:`BinaryType` or :code:`StringType` for the
    :code:`tile` column tile payload, depending on whether checkpointing configured on/off. Also, raster_to_grid sets the
    following AQE configuration to false: :code:`spark.sql.adaptive.coalescePartitions.enabled`.

    Keyword options not identified in function signature are converted to a :code:`Map<String,String>`.
    These must be supplied as a :code:`String`.
    Also, you can supply function signature values as :code:`String`.

.. warning::
    Issue 350: https://github.com/databrickslabs/mosaic/issues/350
    The option 'fileExtension' expects a wild card mask. Please use the following format: '*.tif' or equivalent for other formats.
    If you use 'tif' without the wildcard the reader wont pick up any files and you will have empty table as a result.
