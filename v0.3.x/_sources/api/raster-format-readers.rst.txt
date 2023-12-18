=====================
Raster Format Readers
=====================


Intro
################
Mosaic provides spark readers for the following raster formats:
    * GTiff (GeoTiff) using .tif file extension - https://gdal.org/drivers/raster/gtiff.html
    * COG (Cloud Optimized GeoTiff) using .tif file extension - https://gdal.org/drivers/raster/cog.html
    * HDF4 using .hdf file extension - https://gdal.org/drivers/raster/hdf4.html
    * HDF5 using .h5 file extension - https://gdal.org/drivers/raster/hdf5.html
    * NetCDF using .nc file extension - https://gdal.org/drivers/raster/netcdf.html
    * JP2ECW using .jp2 file extension - https://gdal.org/drivers/raster/jp2ecw.html
    * JP2KAK using .jp2 file extension - https://gdal.org/drivers/raster/jp2kak.html
    * JP2OpenJPEG using .jp2 file extension - https://gdal.org/drivers/raster/jp2openjpeg.html
    * PDF using .pdf file extension - https://gdal.org/drivers/raster/pdf.html
    * PNG using .png file extension - https://gdal.org/drivers/raster/png.html
    * VRT using .vrt file extension - https://gdal.org/drivers/raster/vrt.html
    * XPM using .xpm file extension - https://gdal.org/drivers/raster/xpm.html
    * GRIB using .grb file extension - https://gdal.org/drivers/raster/grib.html
    * Zarr using .zarr file extension - https://gdal.org/drivers/raster/zarr.html
Other formats are supported if supported by GDAL available drivers.

Mosaic provides two flavors of the readers:
    * spark.read.format("gdal") for reading 1 file per spark task
    * mos.read().format("raster_to_grid") reader that automatically converts raster to grid.


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
    It uses standard spark.read.format(*).option(*).load(*) pattern.

    :param path: path to the raster file on dbfs
    :type path: *StringType
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

.. warning::
    Issue 350: https://github.com/databrickslabs/mosaic/issues/350
    The raster reader 'driverName' option has to match the names provided in the above list.
    For example, if you want to read a GeoTiff file, you have to use the following option:
    .option("driverName", "GTiff") instead of .option("driverName", "tif").


mos.read().format("raster_to_grid")
***********************************
Reads a GDAL raster file and converts it to a grid.
It uses a pattern similar to standard spark.read.format(*).option(*).load(*) pattern.
The only difference is that it uses mos.read() instead of spark.read().
The raster pixels are converted to grid cells using specified combiner operation (default is mean).
If the raster pixels are larger than the grid cells, the cell values can be calculated using interpolation.
The interpolation method used is Inverse Distance Weighting (IDW) where the distance function is a k_ring
distance of the grid.
The reader supports the following options:
    * fileExtension - file extension of the raster file (StringType) - default is *.*
    * vsizip - if the rasters are zipped files, set this to true (BooleanType)
    * resolution - resolution of the output grid (IntegerType)
    * combiner - combiner operation to use when converting raster to grid (StringType) - default is mean
    * retile - if the rasters are too large they can be re-tiled to smaller tiles (BooleanType)
    * tileSize - size of the re-tiled tiles, tiles are always squares of tileSize x tileSize (IntegerType)
    * readSubdatasets - if the raster has subdatasets set this to true (BooleanType)
    * subdatasetNumber - if the raster has subdatasets, select a specific subdataset by index (IntegerType)
    * subdatasetName - if the raster has subdatasets, select a specific subdataset by name (StringType)
    * kRingInterpolate - if the raster pixels are larger than the grid cells, use k_ring interpolation with n = kRingInterpolate (IntegerType)

.. function:: mos.read().format("raster_to_grid").load(path)

    Loads a GDAL raster file and returns the result as a DataFrame.
    It uses standard mos.read().format(*).option(*).load(*) pattern.

    :param path: path to the raster file on dbfs
    :type path: *StringType
    :rtype: DataFrame

    :example:

.. tabs::
    .. code-tab:: py

        df = mos.read().format("raster_to_grid")\
            .option("fileExtension", "*.tif")\
            .option("resolution", "8")\
            .option("combiner", "mean")\
            .option("retile", "true")\
            .option("tileSize", "1000")\
            .option("kRingInterpolate", "2")\
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

    .. code-tab:: scala

        val df = MosaicContext.read.format("raster_to_grid")
            .option("fileExtension", "*.tif")
            .option("resolution", "8")
            .option("combiner", "mean")
            .option("retile", "true")
            .option("tileSize", "1000")
            .option("kRingInterpolate", "2")
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

.. warning::
    Issue 350: https://github.com/databrickslabs/mosaic/issues/350
    The option 'fileExtension' expects a wild card mask. Please use the following format: '*.tif' or equivalent for other formats.
    If you use 'tif' without the wildcard the reader wont pick up any files and you will have empty table as a result.