=====================
Raster Format Readers
=====================


Intro
#####
Mosaic provides spark readers for raster files supported by GDAL OGR drivers.
Only the drivers that are built by default are supported.
Here are some common useful file formats:

    * `GTiff <https://gdal.org/drivers/raster/gtiff.html>`_ (GeoTiff) using .tif file extension
    * `COG <https://gdal.org/drivers/raster/cog.html>`_ (Cloud Optimized GeoTiff) using .tif file extension
    * `HDF4 <https://gdal.org/drivers/raster/hdf4.html>`_ using .hdf file extension
    * `HDF5 <https://gdal.org/drivers/raster/hdf5.html>`_ using .h5 file extension
    * `NetCDF <https://gdal.org/drivers/raster/netcdf.html>`_ using .nc file extension
    * `JP2ECW <https://gdal.org/drivers/raster/jp2ecw.html>`_ using .jp2 file extension
    * `JP2KAK <https://gdal.org/drivers/raster/jp2kak.html>`_ using .jp2 file extension
    * `JP2OpenJPEG <https://gdal.org/drivers/raster/jp2openjpeg.html>`_ using .jp2 file extension
    * `PDF <https://gdal.org/drivers/raster/pdf.html>`_ using .pdf file extension
    * `PNG <https://gdal.org/drivers/raster/png.html>`_ using .png file extension
    * `VRT <https://gdal.org/drivers/raster/vrt.html>`_ using .vrt file extension
    * `XPM <https://gdal.org/drivers/raster/xpm.html>`_ using .xpm file extension
    * `GRIB <https://gdal.org/drivers/raster/grib.html>`_ using .grb file extension
    * `Zarr <https://gdal.org/drivers/raster/zarr.html>`_ using .zarr file extension

For more information please refer to gdal `raster driver <https://gdal.org/drivers/raster/index.html>`_ documentation.

Mosaic provides two flavors of the readers:

    * :code:`spark.read.format("gdal")` for reading 1 file per spark task
    * :code:`mos.read().format("raster_to_grid")` reader that automatically converts raster to grid.


spark.read.format("gdal")
*************************
A base Spark SQL data source for reading GDAL raster data sources.
It reads metadata of the raster and exposes the direct paths for the raster files.
The output of the reader is a DataFrame with the following columns:

    * tile - loaded raster tile (RasterTileType)
    * ySize - height of the raster in pixels (IntegerType)
    * xSize - width of the raster in pixels (IntegerType)
    * bandCount - number of bands in the raster (IntegerType)
    * metadata - raster metadata (MapType(StringType, StringType))
    * subdatasets - raster subdatasets (MapType(StringType, StringType))
    * srid - raster spatial reference system identifier (IntegerType)
    * proj4Str - raster spatial reference system proj4 string (StringType)

.. function:: spark.read.format("gdal").load(path)

    Loads a GDAL raster file and returns the result as a DataFrame.
    It uses standard :code:`spark.read.format(*).option(*).load(*)` pattern.

    :param path: path to the raster file on dbfs
    :type path: Column(StringType)
    :rtype: DataFrame

    :example:

.. tabs::
    .. code-tab:: py

        df = spark.read.format("gdal")\
            .option("driverName", "GTiff")\
            .load("dbfs:/path/to/raster.tif")
        df.show()
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+
        |                                                                                                           tile| ySize| xSize| bandCount|             metadata|         subdatasets| srid|              proj4Str|
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+
        | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" } |  100 |  100 |        1 | {AREA_OR_POINT=Po...|                null| 4326|  +proj=longlat +da...|
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+

    .. code-tab:: scala

        val df = spark.read.format("gdal")
            .option("driverName", "GTiff")
            .load("dbfs:/path/to/raster.tif")
        df.show()
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+
        |                                                                                                           tile| ySize| xSize| bandCount|             metadata|         subdatasets| srid|              proj4Str|
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+
        | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" } |  100 |  100 |        1 | {AREA_OR_POINT=Po...|                null| 4326|  +proj=longlat +da...|
        +---------------------------------------------------------------------------------------------------------------+------+------+----------+---------------------+--------------------+-----+----------------------+

.. note::
    Keyword options not identified in function signature are converted to a :code:`Map<String,String>`.
    These must be supplied as a :code:`String`.
    Also, you can supply function signature values as :code:`String`.

.. warning::
    Issue 350: https://github.com/databrickslabs/mosaic/issues/350
    The raster reader 'driverName' option has to match the names provided in the above list.
    For example, if you want to read a GeoTiff file, you have to use the following option:
    .option("driverName", "GTiff") instead of .option("driverName", "tif").


mos.read().format("raster_to_grid")
***********************************
Reads a GDAL raster file and converts it to a grid.

It uses a pattern similar to standard spark.read.format(*).option(*).load(*) pattern.
The only difference is that it uses :code:`mos.read()` instead of :code:`spark.read()`.

The raster pixels are converted to grid cells using specified combiner operation (default is mean).
If the raster pixels are larger than the grid cells, the cell values can be calculated using interpolation.
The interpolation method used is Inverse Distance Weighting (IDW) where the distance function is a k_ring
distance of the grid.

Rasters can be transformed into different formats as part of this process in order to overcome problems with bands
being translated into subdatasets by some GDAL operations. Our recommendation is to specify :code:`GTiff` if you run into problems here.

Raster checkpointing should be enabled to avoid memory issues when working with large rasters. See :doc:`Checkpointing </usage/raster-checkpointing>` for more information.

The reader supports the following options:

    * fileExtension - file extension of the raster file (StringType) - default is *.*
    * vsizip - if the rasters are zipped files, set this to true (BooleanType)
    * resolution - resolution of the output grid (IntegerType)
    * sizeInMB - size of subdivided rasters in MB. Must be supplied, must be a positive integer (IntegerType)
    * convertToFormat - convert the raster to a different format (StringType)
    * combiner - combiner operation to use when converting raster to grid (StringType) - default is mean
    * retile - if the rasters are too large they can be re-tiled to smaller tiles (BooleanType)
    * tileSize - size of the re-tiled tiles, tiles are always squares of tileSize x tileSize (IntegerType)
    * readSubdatasets - if the raster has subdatasets set this to true (BooleanType)
    * subdatasetNumber - if the raster has subdatasets, select a specific subdataset by index (IntegerType)
    * subdatasetName - if the raster has subdatasets, select a specific subdataset by name (StringType)
    * kRingInterpolate - if the raster pixels are larger than the grid cells, use k_ring interpolation with n = kRingInterpolate (IntegerType)

.. function:: mos.read().format("raster_to_grid").load(path)

    Loads a GDAL raster file and returns the result as a DataFrame.
    It uses standard :code:`mos.read().format(*).option(*).load(*)` pattern.

    :param path: path to the raster file on dbfs
    :type path: Column(StringType)
    :rtype: DataFrame

    :example:

.. tabs::
    .. code-tab:: py

        df = (
            mos.read()
            .format("raster_to_grid")
            .option("sizeInMB", "16")
            .option("convertToFormat", "GTiff")
            .option("resolution", "0")
            .option("readSubdataset", "true")
            .option("subdatasetName", "t2m")
            .option("retile", "true")
            .option("tileSize", "600")
            .option("combiner", "avg")
            .load("dbfs:/path/to/raster.tif")
            )
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

        val df = MosaicContext.read
             .format("raster_to_grid")
             .option("sizeInMB", "16")
             .option("convertToFormat", "GTiff")
             .option("resolution", "0")
             .option("readSubdataset", "true")
             .option("subdatasetName", "t2m")
             .option("retile", "true")
             .option("tileSize", "600")
             .option("combiner", "avg")
             .load("dbfs:/path/to/raster.tif")
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
    Keyword options not identified in function signature are converted to a :code:`Map<String,String>`.
    These must be supplied as a :code:`String`.
    Also, you can supply function signature values as :code:`String`.

.. warning::
    Issue 350: https://github.com/databrickslabs/mosaic/issues/350
    The option 'fileExtension' expects a wild card mask. Please use the following format: '*.tif' or equivalent for other formats.
    If you use 'tif' without the wildcard the reader wont pick up any files and you will have empty table as a result.
