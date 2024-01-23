=================
Raster functions
=================

Intro
################
Raster functions are available in mosaic if you have installed the optional dependency `GDAL`.
Please see :doc:`Install and Enable GDAL with Mosaic </usage/install-gdal>` for installation instructions.
Mosaic provides several unique raster functions that are not available in other Spark packages.
Mainly raster to grid functions, which are useful for reprojecting the raster data into a standard grid index system.
This is useful for performing spatial joins between raster data and vector data.
Mosaic also provides a scalable retiling function that can be used to retile raster data in case of bottlenecking due to large files.
All raster functions respect the \"rst\_\" prefix naming convention.
Mosaic is operating using raster tile objects only since 0.3.11. Tile objects are created using functions such as rst_fromfile(path_to_raster)
or rst_fromcontent(raster_bin, driver). These functions are used as places to start when working with initial data.
If you use spark.read.format("gdal") tiles are automatically generated for you.
Also, scala does not have a df.display method while python does. In practice you would most often call display(df) in
scala for a prettier output, but for brevity, we write df.show in scala.

.. note:: For mosaic versions > 0.4.0 you can use the revamped setup_gdal function or new setup_fuse_install.
    These functions will configure an init script in your preferred Workspace, Volume, or DBFS location to install GDAL on your cluster.
    See :doc:`Install and Enable GDAL with Mosaic </usage/install-gdal>` for more details.

rst_bandmetadata
****************

.. function:: rst_bandmetadata(tile, band)

    Extract the metadata describing the raster band.
    Metadata is return as a map of key value pairs.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param band: The band number to extract metadata for.
    :type band: Column (IntegerType)
    :rtype: Column: MapType(StringType, StringType)

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_bandmetadata("tile", F.lit(1))).limit(1).display()
    +--------------------------------------------------------------------------------------+
    | rst_bandmetadata(tile, 1)                                                            |
    +--------------------------------------------------------------------------------------+
    | {"_FillValue": "251", "NETCDF_DIM_time": "1294315200", "long_name": "bleaching alert |
    | area 7-day maximum composite", "grid_mapping": "crs", "NETCDF_VARNAME":              |
    | "bleaching_alert_area", "coverage_content_type": "thematicClassification",           |
    | "standard_name": "N/A", "comment": "Bleaching Alert Area (BAA) values are coral      |
    | bleaching heat stress levels: 0 - No Stress; 1 - Bleaching Watch; 2 - Bleaching      |
    | Warning; 3 - Bleaching Alert Level 1; 4 - Bleaching Alert Level 2. Product           |
    | description is provided at https://coralreefwatch.noaa.gov/product/5km/index.php.",  |
    | "valid_min": "0", "units": "stress_level", "valid_max": "4", "scale_factor": "1"}    |
    +--------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_bandmetadata(col("tile"), lit(1))).limit(1).show
    +--------------------------------------------------------------------------------------+
    | rst_bandmetadata(tile, 1)                                                            |
    +--------------------------------------------------------------------------------------+
    | {"_FillValue": "251", "NETCDF_DIM_time": "1294315200", "long_name": "bleaching alert |
    | area 7-day maximum composite", "grid_mapping": "crs", "NETCDF_VARNAME":              |
    | "bleaching_alert_area", "coverage_content_type": "thematicClassification",           |
    | "standard_name": "N/A", "comment": "Bleaching Alert Area (BAA) values are coral      |
    | bleaching heat stress levels: 0 - No Stress; 1 - Bleaching Watch; 2 - Bleaching      |
    | Warning; 3 - Bleaching Alert Level 1; 4 - Bleaching Alert Level 2. Product           |
    | description is provided at https://coralreefwatch.noaa.gov/product/5km/index.php.",  |
    | "valid_min": "0", "units": "stress_level", "valid_max": "4", "scale_factor": "1"}    |
    +--------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_bandmetadata(tile, 1) FROM table LIMIT 1
    +--------------------------------------------------------------------------------------+
    | rst_bandmetadata(tile, 1)                                                            |
    +--------------------------------------------------------------------------------------+
    | {"_FillValue": "251", "NETCDF_DIM_time": "1294315200", "long_name": "bleaching alert |
    | area 7-day maximum composite", "grid_mapping": "crs", "NETCDF_VARNAME":              |
    | "bleaching_alert_area", "coverage_content_type": "thematicClassification",           |
    | "standard_name": "N/A", "comment": "Bleaching Alert Area (BAA) values are coral      |
    | bleaching heat stress levels: 0 - No Stress; 1 - Bleaching Watch; 2 - Bleaching      |
    | Warning; 3 - Bleaching Alert Level 1; 4 - Bleaching Alert Level 2. Product           |
    | description is provided at https://coralreefwatch.noaa.gov/product/5km/index.php.",  |
    | "valid_min": "0", "units": "stress_level", "valid_max": "4", "scale_factor": "1"}    |
    +--------------------------------------------------------------------------------------+

rst_boundingbox
***************

.. function:: rst_boundingbox(tile)

    Returns the bounding box of the raster as a polygon geometry.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: StructType(DoubleType, DoubleType, DoubleType, DoubleType)

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_boundingbox("tile")).limit(1).display()
     +------------------------------------------------------------------+
     | rst_boundingbox(tile)                                            |
     +------------------------------------------------------------------+
     | [00 00 ... 00] // WKB representation of the polygon bounding box |
     +------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_boundingbox(col("tile"))).limit(1).show
     +------------------------------------------------------------------+
     | rst_boundingbox(tile)                                            |
     +------------------------------------------------------------------+
     | [00 00 ... 00] // WKB representation of the polygon bounding box |
     +------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_boundingbox(tile) FROM table LIMIT 1
     +------------------------------------------------------------------+
     | rst_boundingbox(tile)                                            |
     +------------------------------------------------------------------+
     | [00 00 ... 00] // WKB representation of the polygon bounding box |
     +------------------------------------------------------------------+

rst_clip
********

.. function:: rst_clip(tile, geometry)

    Clips the raster tile to the supported geometry (WKB, WKT, GeoJSON).
    The geometry is expected to be in the same coordinate reference system as the raster.
    The geometry is expected to be a polygon or a multipolygon.
    The output raster will have the same extent as the input geometry.
    The output raster will have the same number of bands as the input raster.
    The output raster will have the same pixel type as the input raster.
    The output raster will have the same pixel size as the input raster.
    The output raster will have the same coordinate reference system as the input raster.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param geometry: A column containing the geometry to clip the raster to.
    :type geometry: Column (GeometryType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_clip("tile", F.lit("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"))).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_clip(tile, POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0)))                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_clip(col("tile"), lit("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
    | rst_clip(tile, POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0)))                                                         |
    +-----------------------------------------------------------------------------------------------------------------+
    | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" }  |
    +-----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_clip(tile, "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))") FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_clip(tile, POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0)))                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_combineavg
**************

.. function:: rst_combineavg(tiles)

    Combines a collection of raster tiles by averaging the pixel values.
    The rasters must have the same extent, number of bands, and pixel type.
    The rasters must have the same pixel size and coordinate reference system.
    The output raster will have the same extent as the input rasters.
    The output raster will have the same number of bands as the input rasters.
    The output raster will have the same pixel type as the input rasters.
    The output raster will have the same pixel size as the input rasters.
    The output raster will have the same coordinate reference system as the input rasters.

    :param tiles: A column containing an array of raster tiles.
    :type tiles: Column (ArrayType(RasterTileType))
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df\
       .select(F.array("tile1","tile2","tile3")).alias("tiles"))\
       .select(mos.rst_combineavg("tiles")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavg(tiles)                                                                                          |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df
       .select(F.array("tile1","tile2","tile3")).as("tiles"))
       .select(rst_combineavg(col("tiles"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavg(tiles)                                                                                          |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_combineavg(array(tile1,tile2,tile3)) FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavg(array(tile1,tile2,tile3))                                                                       |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_combineavgagg
*****************

.. function:: rst_combineavgagg(tile)

    Combines a group by statement over aggregated raster tiles by averaging the pixel values.
    The rasters must have the same extent, number of bands, and pixel type.
    The rasters must have the same pixel size and coordinate reference system.
    The output raster will have the same extent as the input rasters.
    The output raster will have the same number of bands as the input rasters.
    The output raster will have the same pixel type as the input rasters.
    The output raster will have the same pixel size as the input rasters.
    The output raster will have the same coordinate reference system as the input rasters.

    :param tile: A grouped column containing raster tiles.
    :type tile: Column (RasterTileType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.groupBy()\
       .agg(mos.rst_combineavgagg("tile").limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavgagg(tile)                                                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.groupBy()
       .agg(rst_combineavgagg(col("tile")).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavgagg(tile)                                                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_combineavgagg(tile)
     FROM table
     GROUP BY 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavgagg(tile)                                                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+


rst_derivedband
**************

.. function:: rst_derivedband(tiles, python_func, func_name)

    Combine an array of raster tiles using provided python function.
    The rasters must have the same extent, number of bands, and pixel type.
    The rasters must have the same pixel size and coordinate reference system.
    The output raster will have the same extent as the input rasters.
    The output raster will have the same number of bands as the input rasters.
    The output raster will have the same pixel type as the input rasters.
    The output raster will have the same pixel size as the input rasters.
    The output raster will have the same coordinate reference system as the input rasters.

    :param tiles: A column containing an array of raster tiles.
    :type tiles: Column (ArrayType(RasterTileType))
    :param python_func: A function to evaluate in python.
    :type python_func: Column (StringType)
    :param func_name: name of the function to evaluate in python.
    :type func_name: Column (StringType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df\
       .select(
         F.array("tile1","tile2","tile3")).alias("tiles"),
         F.lit(
           """
           import numpy as np
           def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
              out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
           """).alias("py_func1"),
         F.lit("average").alias("func1_name")
       )\
       .select(mos.rst_deriveband("tiles","py_func1","func1_name")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_derivedband(tiles,py_func1,func1_name)                                                                     |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df
        .select(
            array("tile1","tile2","tile3")).alias("tiles"),
            lit(
                """
                |import numpy as np
                |def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
                |  out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
                |""".stripMargin).as("py_func1"),
            lit("average").as("func1_name")
        )
        .select(mos.rst_deriveband("tiles","py_func1","func1_name")).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_derivedband(tiles,py_func1,func1_name)                                                                     |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql
     SELECT
     rst_derivedband(array(tile1,tile2,tile3)) as tiles,
     """
     import numpy as np
     def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
        out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
     """ as py_func1,
     "average" as funct1_name
     FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_derivedband(tiles,py_func1,func1_name)                                                                     |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+


rst_derivedbandagg
*****************

.. function:: rst_derivedbandagg(tile, python_func, func_name)

    Combines a group by statement over aggregated raster tiles by using the provided python function.
    The rasters must have the same extent, number of bands, and pixel type.
    The rasters must have the same pixel size and coordinate reference system.
    The output raster will have the same extent as the input rasters.
    The output raster will have the same number of bands as the input rasters.
    The output raster will have the same pixel type as the input rasters.
    The output raster will have the same pixel size as the input rasters.
    The output raster will have the same coordinate reference system as the input rasters.

    :param tile: A grouped column containing raster tile(s).
    :type tile: Column (RasterTileType)
    :param python_func: A function to evaluate in python.
    :type python_func: Column (StringType)
    :param func_name: name of the function to evaluate in python.
    :type func_name: Column (StringType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py
     from textwrap import dedent
     df\
       .select(
         "date", "tile",
         F.lit(dedent(
           """
           import numpy as np
           def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
              out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
           """)).alias("py_func1"),
         F.lit("average").alias("func1_name")
       )\
       .groupBy("date", "py_func1", "func1_name")\
         .agg(mos.rst_derivedbandagg("tile","py_func1","func1_name")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_derivedbandagg(tile,py_func1,func1_name)                                                                   |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df
        .select(
            "date", "tile"
            lit(
                """
                |import numpy as np
                |def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
                |  out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
                |""".stripMargin).as("py_func1"),
            lit("average").as("func1_name")
        )
        .groupBy("date", "py_func1", "func1_name")
            .agg(mos.rst_derivedbandagg("tile","py_func1","func1_name")).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_derivedbandagg(tile,py_func1,func1_name)                                                                   |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql
     SELECT
     date, py_func1, func1_name,
     rst_derivedbandagg(tile, py_func1, func1_name)
     FROM SELECT (
     date, tile,
     """
     import numpy as np
     def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
        out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
     """ as py_func1,
     "average" as func1_name
     FROM table
     )
     GROUP BY date, py_func1, func1_name
     LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_derivedbandagg(tile,py_func1,func1_name)                                                                   |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+


rst_frombands
**************

.. function:: rst_frombands(tiles)

    Combines a collection of raster tiles of different bands into a single raster.
    The rasters must have the same extent.
    The rasters must have the same pixel coordinate reference system.
    The output raster will have the same extent as the input rasters.
    The output raster will have the same number of bands as all the input raster bands.
    The output raster will have the same pixel type as the input raster bands.
    The output raster will have the same pixel size as the highest resolution input rasters.
    The output raster will have the same coordinate reference system as the input rasters.

    :param tiles: A column containing an array of raster tiles.
    :type tiles: Column (ArrayType(RasterTileType))
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.select(F.array("tile1", "tile2", "tile3").as("tiles"))\
       .select(mos.rst_frombands("tiles")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_frombands(tiles)                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df
       .select(array("tile1", "tile2", "tile3").as("tiles"))
       .select(rst_frombands(col("tiles"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_frombands(tiles)                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_frombands(array(tile1,tile2,tile3)) FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_frombands(array(tile1,tile2,tile3))                                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_fromcontent
************

.. function:: rst_fromcontent(raster_bin, driver, <size_in_MB>)

    Returns a tile from raster data.
    The raster must be a binary.
    The driver must be one that GDAL can read.
    If the size_in_MB parameter is specified, the raster will be split into tiles of the specified size.
    If the size_in_MB parameter is not specified or if the size_in_Mb < 0, the raster will only be split if
    it exceeds Integer.MAX_VALUE. The split will be at a threshold of 64MB in this case.

    :param raster_bin: A column containing the raster data.
    :type raster_bin: Column (BinaryType)
    :param size_in_MB: Optional parameter to specify the size of the raster tile in MB. Default is not to split the input.
    :type size_in_MB: Column (IntegerType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py
     # binary is python bytearray data type
     df = spark.read.format("binaryFile")\
         .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")\
     df.select(mos.rst_fromcontent("content")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_fromcontent(content)                                                                                       |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala
     //binary is scala/java Array(Byte) data type
     val df = spark.read
          .format("binaryFile")
          .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
     df.select(rst_fromcontent(col("content"))).limit(1).show(false)
     +----------------------------------------------------------------------------------------------------------------+
     | rst_fromcontent(content)                                                                                       |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
          USING binaryFile
          OPTIONS (path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
     SELECT rst_fromcontent(content) FROM coral_netcdf LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_fromcontent(content)                                                                                       |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_fromfile
************

.. function:: rst_fromfile(path, <size_in_MB>)

    Returns a raster tile from a file path.
    The file path must be a string.
    The file path must be a valid path to a raster file.
    The file path must be a path to a file that GDAL can read.
    If the size_in_MB parameter is specified, the raster will be split into tiles of the specified size.
    If the size_in_MB parameter is not specified or if the size_in_Mb < 0, the raster will only be split if
    it exceeds Integer.MAX_VALUE. The split will be at a threshold of 64MB in this case.

    :param path: A column containing the path to a raster file.
    :type path: Column (StringType)
    :param size_in_MB: Optional parameter to specify the size of the raster tile in MB. Default is not to split the input.
    :type size_in_MB: Column (IntegerType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df = spark.read.format("binaryFile")\
                .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")\
                .drop("content")
     df.select(mos.rst_fromfile("path")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_fromfile(path)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     val df = spark.read
          .format("binaryFile")
          .load("dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
          .drop("content")
     df.select(rst_fromfile(col("path"))).limit(1).show(false)
     +----------------------------------------------------------------------------------------------------------------+
     | rst_fromfile(path)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     CREATE TABLE IF NOT EXISTS TABLE coral_netcdf
          USING binaryFile
          OPTIONS (path "dbfs:/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral")
     SELECT rst_fromfile(path) FROM coral_netcdf LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_fromfile(path)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_georeference
****************

.. function:: rst_georeference(raster_tile)

    Returns GeoTransform of the raster tile as a GT array of doubles.
    GT(0) x-coordinate of the upper-left corner of the upper-left pixel.
    GT(1) w-e pixel resolution / pixel width.
    GT(2) row rotation (typically zero).
    GT(3) y-coordinate of the upper-left corner of the upper-left pixel.
    GT(4) column rotation (typically zero).
    GT(5) n-s pixel resolution / pixel height (negative value for a north-up image).

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: MapType(StringType, DoubleType)

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_georeference("tile")).limit(1).display()
    +--------------------------------------------------------------------------------------------+
    | rst_georeference(tile)                                                                     |
    +--------------------------------------------------------------------------------------------+
    | {"scaleY": -0.049999999152053956, "skewX": 0, "skewY": 0, "upperLeftY": 89.99999847369712, |
    | "upperLeftX": -180.00000610436345, "scaleX": 0.050000001695656514}                         |
    +--------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_georeference(col("tile"))).limit(1).show
    +--------------------------------------------------------------------------------------------+
    | rst_georeference(tile)                                                                     |
    +--------------------------------------------------------------------------------------------+
    | {"scaleY": -0.049999999152053956, "skewX": 0, "skewY": 0, "upperLeftY": 89.99999847369712, |
    | "upperLeftX": -180.00000610436345, "scaleX": 0.050000001695656514}                         |
    +--------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_georeference(tile) FROM table LIMIT 1
    +--------------------------------------------------------------------------------------------+
    | rst_georeference(tile)                                                                     |
    +--------------------------------------------------------------------------------------------+
    | {"scaleY": -0.049999999152053956, "skewX": 0, "skewY": 0, "upperLeftY": 89.99999847369712, |
    | "upperLeftX": -180.00000610436345, "scaleX": 0.050000001695656514}                         |
    +--------------------------------------------------------------------------------------------+

rest_getnodata
**************

.. function:: rst_getnodata(tile)

    Returns the nodata value of the raster tile bands.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: ArrayType(DoubleType)

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_getnodata("tile")).limit(1).display()
     +---------------------+
     | rst_getnodata(tile) |
     +---------------------+
     | [0.0, -9999.0, ...] |
     +---------------------+

    .. code-tab:: scala

     df.select(rst_getnodata(col("tile"))).limit(1).show
     +---------------------+
     | rst_getnodata(tile) |
     +---------------------+
     | [0.0, -9999.0, ...] |
     +---------------------+

    .. code-tab:: sql

     SELECT rst_getnodata(tile) FROM table LIMIT 1
     +---------------------+
     | rst_getnodata(tile) |
     +---------------------+
     | [0.0, -9999.0, ...] |
     +---------------------+

rst_getsubdataset
*****************

.. function:: rst_getsubdataset(tile, name)

    Returns the subdataset of the raster tile with a given name.
    The subdataset name must be a string. The name is not a full path.
    The name is the last identifier in the subdataset path (FORMAT:PATH:NAME).
    The subdataset name must be a valid subdataset name for the raster.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param name: A column containing the name of the subdataset to return.
    :type name: Column (StringType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_getsubdataset("tile", "sst")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_getsubdataset(tile, sst)                                                                                   |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_getsubdataset(col("tile"), lit("sst"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_getsubdataset(tile, sst)                                                                                   |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_getsubdataset(tile, "sst") FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_getsubdataset(tile, sst)                                                                                   |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_height
**********

.. function:: rst_height(tile)

    Returns the height of the raster tile in pixels.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_height('tile')).display()
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    | 3600               |
    | 3600               |
    +--------------------+

   .. code-tab:: scala

    df.select(rst_height(col("tile"))).show
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    |3600                |
    |3600                |
    +--------------------+

   .. code-tab:: sql

    SELECT rst_height(tile) FROM table
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    |3600                |
    |3600                |
    +--------------------+

rst_initnodata
**************

.. function:: rst_initnodata(tile)

    Initializes the nodata value of the raster tile bands.
    The nodata value will be set to default values for the pixel type of the raster bands.
    The output raster will have the same extent as the input raster.
    The default nodata value for ByteType is 0.
    The default nodata value for UnsignedShortType is UShort.MaxValue (65535).
    The default nodata value for ShortType is Short.MinValue (-32768).
    The default nodata value for UnsignedIntegerType is Int.MaxValue (4.294967294E9).
    The default nodata value for IntegerType is Int.MinValue (-2147483648).
    The default nodata value for FloatType is Float.MinValue (-3.4028234663852886E38).
    The default nodata value for DoubleType is Double.MinValue (-1.7976931348623157E308).

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_initnodata("tile")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_initnodata(tile)                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_initnodata(col("tile"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_initnodata(tile)                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_initnodata(tile) FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_initnodata(tile)                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_isempty
*************

.. function:: rst_isempty(tile)

    Returns true if the raster tile is empty.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: BooleanType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_isempty('tile')).display()
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    |false               |
    |false               |
    +--------------------+

   .. code-tab:: scala

    df.select(rst_isempty(col("tile"))).show
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    |false               |
    |false               |
    +--------------------+

   .. code-tab:: sql

    SELECT rst_isempty(tile) FROM table
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    |false               |
    |false               |
    +--------------------+


rst_mapalgebra
********

.. function:: rst_mapalgebra(tile, json_spec)

    Performs map algebra on the raster tile.
    Rasters are provided as 'A' to 'Z' values.
    Bands are provided as 0..n values.
    Uses gdal_calc: command line raster calculator with numpy syntax. Use any basic arithmetic supported by numpy
    arrays (such as +, -, *, and /) along with logical operators (such as >, <, =). For this distributed implementation,
    all rasters must have the same dimensions and no projection checking is performed.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param json_spec: A column containing the map algebra operation specification.
    :type json_spec: Column (StringType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_mapalgebra("tile", "{calc: 'A+B', A_index: 0, B_index: 1}").alias("tile").limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | tile                                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(mos.rst_mapalgebra("tile", "{calc: 'A+B', A_index: 0, B_index: 1}").as("tile")).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | tile                                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_mapalgebra(tile, "{calc: 'A+B', A_index: 0, B_index: 1}") as tile FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | tile                                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+


rst_memsize
*************

.. function:: rst_memsize(tile)

    Returns size of the raster tile in bytes.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: LongType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_memsize('tile')).display()
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    |730260              |
    |730260              |
    +--------------------+

   .. code-tab:: scala

    df.select(rst_memsize(col("tile"))).show
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    |730260              |
    |730260              |
    +--------------------+

   .. code-tab:: sql

    SELECT rst_memsize(tile) FROM table
    +--------------------+
    | rst_height(tile)   |
    +--------------------+
    |730260              |
    |730260              |
    +--------------------+

rst_merge
*********

.. function:: rst_merge(tiles)

    Combines a collection of raster tiles into a single raster.
    The rasters do not need to have the same extent.
    The rasters must have the same coordinate reference system.
    The rasters are combined using gdalwarp.
    The noData value needs to be initialised; if not, the non valid pixels may introduce artifacts in the output raster.
    The rasters are stacked in the order they are provided.
    The output raster will have the extent covering all input rasters.
    The output raster will have the same number of bands as the input rasters.
    The output raster will have the same pixel type as the input rasters.
    The output raster will have the same pixel size as the highest resolution input rasters.
    The output raster will have the same coordinate reference system as the input rasters.

    :param tiles: A column containing an array of raster tiles.
    :type tiles: Column (ArrayType(RasterTileType))
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.select(F.array("tile1", "tile2", "tile3").alias("tiles"))\
       .select(mos.rst_merge("tiles")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_merge(tiles)                                                                                               |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(array("tile1", "tile2", "tile3").as("tiles"))
       .select(rst_merge(col("tiles"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_merge(tiles)                                                                                               |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_merge(array(tile1,tile2,tile3)) FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_merge(array(tile1,tile2,tile3))                                                                            |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_mergeagg
************

.. function:: rst_mergeagg(tile)

    Combines a grouped aggregate of raster tiles into a single raster.
    The rasters do not need to have the same extent.
    The rasters must have the same coordinate reference system.
    The rasters are combined using gdalwarp.
    The noData value needs to be initialised; if not, the non valid pixels may introduce artifacts in the output raster.
    The rasters are stacked in the order they are provided.
    This order is randomized since this is an aggregation function.
    If the order of rasters is important please first collect rasters and sort them by metadata information and then use
    rst_merge function.
    The output raster will have the extent covering all input rasters.
    The output raster will have the same number of bands as the input rasters.
    The output raster will have the same pixel type as the input rasters.
    The output raster will have the same pixel size as the highest resolution input rasters.
    The output raster will have the same coordinate reference system as the input rasters.

    :param tile: A column containing raster tiles.
    :type tile: Column (RasterTileType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.groupBy("date")\
       .agg(mos.rst_mergeagg("tile")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_mergeagg(tile)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.groupBy("date")
       .agg(rst_mergeagg(col("tile"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_mergeagg(tile)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_mergeagg(tile)
     FROM table
     GROUP BY date
     +----------------------------------------------------------------------------------------------------------------+
     | rst_mergeagg(tile)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_metadata
*************

.. function:: rst_metadata(tile)

    Extract the metadata describing the raster tile.
    Metadata is return as a map of key value pairs.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: MapType(StringType, StringType)

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_metadata('tile')).display()
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_metadata(tile)                                                                                                 |
    +--------------------------------------------------------------------------------------------------------------------+
    | {"NC_GLOBAL#publisher_url": "https://coralreefwatch.noaa.gov", "NC_GLOBAL#geospatial_lat_units": "degrees_north",  |
    | "NC_GLOBAL#platform_vocabulary": "NOAA NODC Ocean Archive System Platforms", "NC_GLOBAL#creator_type": "group",    |
    | "NC_GLOBAL#geospatial_lon_units": "degrees_east", "NC_GLOBAL#geospatial_bounds": "POLYGON((-90.0 180.0, 90.0       |
    | 180.0, 90.0 -180.0, -90.0 -180.0, -90.0 180.0))", "NC_GLOBAL#keywords": "Oceans > Ocean Temperature > Sea Surface  |
    | Temperature, Oceans > Ocean Temperature > Water Temperature, Spectral/Engineering > Infrared Wavelengths > Thermal |
    | Infrared, Oceans > Ocean Temperature > Bleaching Alert Area", "NC_GLOBAL#geospatial_lat_max": "89.974998",         |
    | .... (truncated).... "NC_GLOBAL#history": "This is a product data file of the NOAA Coral Reef Watch Daily Global   |
    | 5km Satellite Coral Bleaching Heat Stress Monitoring Product Suite Version 3.1 (v3.1) in its NetCDF Version 1.0    |
    | (v1.0).", "NC_GLOBAL#publisher_institution": "NOAA/NESDIS/STAR Coral Reef Watch Program",                          |
    | "NC_GLOBAL#cdm_data_type": "Grid"}                                                                                 |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_metadata(col("tile"))).show
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_metadata(tile)                                                                                                 |
    +--------------------------------------------------------------------------------------------------------------------+
    | {"NC_GLOBAL#publisher_url": "https://coralreefwatch.noaa.gov", "NC_GLOBAL#geospatial_lat_units": "degrees_north",  |
    | "NC_GLOBAL#platform_vocabulary": "NOAA NODC Ocean Archive System Platforms", "NC_GLOBAL#creator_type": "group",    |
    | "NC_GLOBAL#geospatial_lon_units": "degrees_east", "NC_GLOBAL#geospatial_bounds": "POLYGON((-90.0 180.0, 90.0       |
    | 180.0, 90.0 -180.0, -90.0 -180.0, -90.0 180.0))", "NC_GLOBAL#keywords": "Oceans > Ocean Temperature > Sea Surface  |
    | Temperature, Oceans > Ocean Temperature > Water Temperature, Spectral/Engineering > Infrared Wavelengths > Thermal |
    | Infrared, Oceans > Ocean Temperature > Bleaching Alert Area", "NC_GLOBAL#geospatial_lat_max": "89.974998",         |
    | .... (truncated).... "NC_GLOBAL#history": "This is a product data file of the NOAA Coral Reef Watch Daily Global   |
    | 5km Satellite Coral Bleaching Heat Stress Monitoring Product Suite Version 3.1 (v3.1) in its NetCDF Version 1.0    |
    | (v1.0).", "NC_GLOBAL#publisher_institution": "NOAA/NESDIS/STAR Coral Reef Watch Program",                          |
    | "NC_GLOBAL#cdm_data_type": "Grid"}                                                                                 |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_metadata(tile) FROM table LIMIT 1
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_metadata(tile)                                                                                                 |
    +--------------------------------------------------------------------------------------------------------------------+
    | {"NC_GLOBAL#publisher_url": "https://coralreefwatch.noaa.gov", "NC_GLOBAL#geospatial_lat_units": "degrees_north",  |
    | "NC_GLOBAL#platform_vocabulary": "NOAA NODC Ocean Archive System Platforms", "NC_GLOBAL#creator_type": "group",    |
    | "NC_GLOBAL#geospatial_lon_units": "degrees_east", "NC_GLOBAL#geospatial_bounds": "POLYGON((-90.0 180.0, 90.0       |
    | 180.0, 90.0 -180.0, -90.0 -180.0, -90.0 180.0))", "NC_GLOBAL#keywords": "Oceans > Ocean Temperature > Sea Surface  |
    | Temperature, Oceans > Ocean Temperature > Water Temperature, Spectral/Engineering > Infrared Wavelengths > Thermal |
    | Infrared, Oceans > Ocean Temperature > Bleaching Alert Area", "NC_GLOBAL#geospatial_lat_max": "89.974998",         |
    | .... (truncated).... "NC_GLOBAL#history": "This is a product data file of the NOAA Coral Reef Watch Daily Global   |
    | 5km Satellite Coral Bleaching Heat Stress Monitoring Product Suite Version 3.1 (v3.1) in its NetCDF Version 1.0    |
    | (v1.0).", "NC_GLOBAL#publisher_institution": "NOAA/NESDIS/STAR Coral Reef Watch Program",                          |
    | "NC_GLOBAL#cdm_data_type": "Grid"}                                                                                 |
    +--------------------------------------------------------------------------------------------------------------------+

rst_ndvi
********

.. function:: rst_ndvi(tile, red_band_num, nir_band_num)

    Calculates the Normalized Difference Vegetation Index (NDVI) for a raster.
    The NDVI is calculated using the formula: (NIR - RED) / (NIR + RED).
    The output raster will have the same extent as the input raster.
    The output raster will have a single band.
    The output raster will have a pixel type of float64.
    The output raster will have the same coordinate reference system as the input raster.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param red_band_num: A column containing the band number of the red band.
    :type red_band_num: Column (IntegerType)
    :param nir_band_num: A column containing the band number of the near infrared band.
    :type nir_band_num: Column (IntegerType)
    :rtype: Column: RasterTileType

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_ndvi("tile", 1, 2)).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_ndvi(tile, 1, 2)                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_ndvi(col("tile"), lit(1), lit(2))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_ndvi(tile, 1, 2)                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_ndvi(tile, 1, 2) FROM table LIMIT 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_ndvi(tile, 1, 2)                                                                                           |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

rst_numbands
*************

.. function:: rst_numbands(tile)

    Returns number of bands in the raster tile.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_numbands('tile')).display()
    +---------------------+
    | rst_numbands(tile)  |
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: scala

    df.select(rst_metadata(col("tile"))).show
    +---------------------+
    | rst_numbands(tile)  |
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: sql

    SELECT rst_metadata(tile) FROM table
    +---------------------+
    | rst_numbands(tile)  |
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

rst_pixelheight
***************

.. function:: rst_pixelheight(tile)

    Returns the height of the pixel in the raster tile derived via GeoTransform.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_pixelheight('tile')).display()
    +-----------------------+
    | rst_pixelheight(tile) |
    +-----------------------+
    | 1                     |
    | 1                     |
    +-----------------------+

   .. code-tab:: scala

    df.select(rst_pixelheight(col("tile"))).show
    +-----------------------+
    | rst_pixelheight(tile) |
    +-----------------------+
    | 1                     |
    | 1                     |
    +-----------------------+

   .. code-tab:: sql

    SELECT rst_pixelheight(tile) FROM table
    +-----------------------+
    | rst_pixelheight(tile) |
    +-----------------------+
    | 1                     |
    | 1                     |
    +-----------------------+

rst_pixelwidth
**************

.. function:: rst_pixelwidth(tile)

    Returns the width of the pixel in the raster tile derived via GeoTransform.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_pixelwidth('tile')).display()
    +---------------------+
    | rst_pixelwidth(tile)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: scala

    df.select(rst_pixelwidth(col("tile"))).show
    +---------------------+
    | rst_pixelwidth(tile)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

   .. code-tab:: sql

    SELECT rst_pixelwidth(tile) FROM table
    +---------------------+
    | rst_pixelwidth(tile)|
    +---------------------+
    | 1                   |
    | 1                   |
    +---------------------+

rst_rastertogridavg
*******************

.. function:: rst_rastertogridavg(tile, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the average of the pixel values in the cell.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rastertogridavg('tile', F.lit(3))).display()
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridavg(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rastertogridavg(col("tile"), lit(3))).show
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridavg(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rastertogridavg(tile, 3) FROM table
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridavg(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 1. RST_RasterToGridAvg(tile, 3)

rst_rastertogridcount
*********************

.. function:: rst_rastertogridcount(tile, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the average of the pixel values in the cell.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rastertogridcount('tile', F.lit(3))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridcount(tile, 3)                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1},                |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                  |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1},                  |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                  |
    | {"cellID": "593472602366803967", "measure": 3},                                                                  |
    | {"cellID": "593785619583336447", "measure": 3}, {"cellID": "591988330388783103", "measure": 1},                  |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                           |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rastertogridcount(col("tile"), lit(3))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridcount(tile, 3)                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1},                |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                  |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1},                  |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                  |
    | {"cellID": "593472602366803967", "measure": 3},                                                                  |
    | {"cellID": "593785619583336447", "measure": 3}, {"cellID": "591988330388783103", "measure": 1},                  |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                           |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rastertogridcount(tile, 3) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridcount(tile, 3)                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1},                |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                  |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1},                  |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                  |
    | {"cellID": "593472602366803967", "measure": 3},                                                                  |
    | {"cellID": "593785619583336447", "measure": 3}, {"cellID": "591988330388783103", "measure": 1},                  |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                           |
    +------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 2. RST_RasterToGridCount(tile, 3)

rst_rastertogridmax
*******************

.. function:: rst_rastertogridmax(tile, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the maximum pixel value.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rastertogridmax('tile', F.lit(3))).display()
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmax(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rastertogridmax(col("tile"), lit(3))).show
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmax(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rastertogridmax(tile, 3) FROM table
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmax(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 3. RST_RasterToGridMax(tile, 3)

rst_rastertogridmedian
**********************

.. function:: rst_rastertogridmedian(tile, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the median pixel value.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rastertogridmedian('tile', F.lit(3))).display()
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmedian(tile, 3)                                                                                    |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rastertogridmedian(col("tile"), lit(3))).show
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmedian(tile, 3)                                                                                    |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rastertogridmax(tile, 3) FROM table
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmedian(tile, 3)                                                                                    |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 4. RST_RasterToGridMedian(tile, 3)

rst_rastertogridmin
*******************

.. function:: rst_rastertogridmin(tile, resolution)

    The result is a 2D array of cells, where each cell is a struct of (cellID, value).
    For getting the output of cellID->value pairs, please use explode() function twice.
    CellID can be LongType or StringType depending on the configuration of MosaicContext.
    The value/measure for each cell is the median pixel value.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rastertogridmin('tile', F.lit(3))).display()
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmin(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rastertogridmin(col("tile"), lit(3))).show
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmin(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rastertogridmin(tile, 3) FROM table
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_rastertogridmin(tile, 3)                                                                                       |
    +--------------------------------------------------------------------------------------------------------------------+
    | [[{"cellID": "593176490141548543", "measure": 0}, {"cellID": "593386771740360703", "measure": 1.2037735849056603}, |
    | {"cellID": "593308294097928191", "measure": 0}, {"cellID": "593825202001936383", "measure": 0},                    |
    | {"cellID": "593163914477305855", "measure": 2}, {"cellID": "592998781574709247", "measure": 1.1283185840707965},   |
    | {"cellID": "593262526926422015", "measure": 2}, {"cellID": "592370479398911999", "measure": 0},                    |
    | {"cellID": "593472602366803967", "measure": 0.3963963963963964},                                                   |
    | {"cellID": "593785619583336447", "measure": 0.6590909090909091}, {"cellID": "591988330388783103", "measure": 1},   |
    | {"cellID": "592336738135834623", "measure": 1}, ....]]                                                             |
    +--------------------------------------------------------------------------------------------------------------------+

.. figure:: ../images/rst_rastertogridavg/h3.png
   :figclass: doc-figure

   Fig 4. RST_RasterToGridMin(tile, 3)

rst_rastertoworldcoord
**********************

.. function:: rst_rastertoworldcoord(tile, x, y)

    Computes the world coordinates of the raster tile at the given x and y pixel coordinates.
    The result is a WKT point geometry.
    The coordinates are computed using the GeoTransform of the raster to respect the projection.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param x: x coordinate of the pixel.
    :type x: Column (IntegerType)
    :param y: y coordinate of the pixel.
    :type y: Column (IntegerType)
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rastertoworldcoord('tile', F.lit(3), F.lit(3))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoord(tile, 3, 3)                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |POINT (-179.85000609927647 89.84999847624096)                                                                     |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rastertoworldcoord(col("tile"), lit(3), lit(3))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoord(tile, 3, 3)                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |POINT (-179.85000609927647 89.84999847624096)                                                                     |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rastertoworldcoord(tile, 3, 3) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoord(tile, 3, 3)                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    |POINT (-179.85000609927647 89.84999847624096)                                                                     |
    +------------------------------------------------------------------------------------------------------------------+

rst_rastertoworldcoordx
**********************

.. function:: rst_rastertoworldcoord(tile, x, y)

    Computes the world coordinates of the raster tile at the given x and y pixel coordinates.
    The result is the X coordinate of the point after applying the GeoTransform of the raster.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param x: x coordinate of the pixel.
    :type x: Column (IntegerType)
    :param y: y coordinate of the pixel.
    :type y: Column (IntegerType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rastertoworldcoordx('tile', F.lit(3), F.lit(3))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordx(tile, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | -179.85000609927647                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rastertoworldcoordx(col("tile"), lit(3), lit(3))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordx(tile, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | -179.85000609927647                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rastertoworldcoordx(tile, 3, 3) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordx(tile, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | -179.85000609927647                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_rastertoworldcoordy
**********************

.. function:: rst_rastertoworldcoordy(tile, x, y)

    Computes the world coordinates of the raster tile at the given x and y pixel coordinates.
    The result is the X coordinate of the point after applying the GeoTransform of the raster.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param x: x coordinate of the pixel.
    :type x: Column (IntegerType)
    :param y: y coordinate of the pixel.
    :type y: Column (IntegerType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rastertoworldcoordy('tile', F.lit(3), F.lit(3))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordy(tile, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.84999847624096                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rastertoworldcoordy(col("tile"), lit(3), lit(3))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordy(tile, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.84999847624096                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rastertoworldcoordy(tile, 3, 3) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rastertoworldcoordy(tile, 3, 3)                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.84999847624096                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

rst_retile
**********************

.. function:: rst_retile(tile, width, height)

    Retiles the raster tile to the given size. The result is a collection of new raster tiles.
    The new rasters are stored in the checkpoint directory.
    The results are the paths to the new rasters.
    The result set is automatically exploded.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :param width: The width of the tiles.
    :type width: Column (IntegerType)
    :param height: The height of the tiles.
    :type height: Column (IntegerType)
    :rtype: Column: (RasterTileType)

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_retile('tile', F.lit(300), F.lit(300))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_retile(tile, 300, 300)                                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" }   |
    | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" }   |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_retile(col("tile"), lit(300), lit(300))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_retile(tile, 300, 300)                                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" }   |
    | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" }   |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_retile(tile, 300, 300) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_retile(tile, 300, 300)                                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" }   |
    | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" }   |
    +------------------------------------------------------------------------------------------------------------------+

rst_rotation
**********************

.. function:: rst_rotation(tile)

    Computes the rotation of the raster tile in degrees.
    The rotation is the angle between the X axis and the North axis.
    The rotation is computed using the GeoTransform of the raster.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_rotation('tile').display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rotation(tile)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    | 21.2                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_rotation(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rotation(tile)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    | 21.2                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_rotation(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_rotation(tile)                                                                                               |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    | 21.2                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

rst_scalex
**********************

.. function:: rst_scalex(tile)

    Computes the scale of the raster tile in the X direction.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_scalex('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scalex(tile)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_scalex(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scalex(tile)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_scalex(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scalex(tile)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_scaley
**********************

.. function:: rst_scaley(tile)

    Computes the scale of the raster tile in the Y direction.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_scaley('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scaley(path)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_scaley(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scaley(tile)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_scaley(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_scaley(tile)                                                                                                 |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_setnodata
**********************

.. function:: rst_setnodata(tile, nodata)

    Sets the nodata value of the raster tile.
    The result is a new raster tile with the nodata value set.
    The same nodata value is set for all bands of the raster if a single value is passed.
    If an array of values is passed, the nodata value is set for each band of the raster.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :param nodata: The nodata value to set.
    :type nodata: Column (DoubleType) / ArrayType(DoubleType)
    :rtype: Column: (RasterTileType)

    :example:

.. tabs::

    .. code-tab:: py

     df.select(mos.rst_setnodata('tile', F.lit(0))).display()
     +------------------------------------------------------------------------------------------------------------------+
     | rst_setnodata(tile, 0)                                                                                           |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_setnodata(col("tile"), lit(0))).show
     +------------------------------------------------------------------------------------------------------------------+
     | rst_setnodata(tile, 0)                                                                                           |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_setnodata(tile, 0) FROM table
     +------------------------------------------------------------------------------------------------------------------+
     | rst_setnodata(tile, 0)                                                                                           |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

rst_skewx
**********************

.. function:: rst_skewx(tile)

    Computes the skew of the raster tile in the X direction.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_skewx('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewx(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_skewx(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewx(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_skewx(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewx(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_skewy
**********************

.. function:: rst_skewx(tile)

    Computes the skew of the raster tile in the Y direction.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_skewy('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewy(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_skewy(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewy(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_skewy(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_skewy(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 1.2                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_srid
**********************

.. function:: rst_srid(tile)

    Computes the SRID of the raster tile.
    The SRID is the EPSG code of the raster.

    .. note:: For complex CRS definition the EPSG code may default to 0.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_srid('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_srid(tile)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | 9122                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_srid(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_srid(tile)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | 9122                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_srid(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_srid(tile)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | 9122                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

rst_subdatasets
**********************

.. function:: rst_subdatasets(tile)

    Computes the subdatasets of the raster tile.
    The subdatasets are the paths to the subdatasets of the raster.
    The result is a map of the subdataset path to the subdatasets and the description of the subdatasets.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :rtype: Column: MapType(StringType, StringType)

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_subdatasets('tile')).display()
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_subdatasets(tile)                                                                                              |
    +--------------------------------------------------------------------------------------------------------------------+
    | {"NETCDF:\"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_2022010  |
    | 6-1.nc\":bleaching_alert_area": "[1x3600x7200] N/A (8-bit unsigned integer)", "NETCDF:\"/dbfs/FileStore/geospatial |
    | /mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc\":mask": "[1x3600x7200] mask (8 |
    | -bit unsigned integer)"}                                                                                           |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_subdatasets(col("tile"))).show
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_subdatasets(tile)                                                                                              |
    +--------------------------------------------------------------------------------------------------------------------+
    | {"NETCDF:\"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_2022010  |
    | 6-1.nc\":bleaching_alert_area": "[1x3600x7200] N/A (8-bit unsigned integer)", "NETCDF:\"/dbfs/FileStore/geospatial |
    | /mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc\":mask": "[1x3600x7200] mask (8 |
    | -bit unsigned integer)"}                                                                                           |
    +--------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_subdatasets(tile) FROM table
    +--------------------------------------------------------------------------------------------------------------------+
    | rst_subdatasets(tile)                                                                                              |
    +--------------------------------------------------------------------------------------------------------------------+
    | {"NETCDF:\"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_2022010  |
    | 6-1.nc\":bleaching_alert_area": "[1x3600x7200] N/A (8-bit unsigned integer)", "NETCDF:\"/dbfs/FileStore/geospatial |
    | /mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc\":mask": "[1x3600x7200] mask (8 |
    | -bit unsigned integer)"}                                                                                           |
    +--------------------------------------------------------------------------------------------------------------------+

rst_subdivide
**********************

.. function:: rst_subdivide(tile, sizeInMB)

    Subdivides the raster tile to the given tile size in MB. The result is a collection of new raster tiles.
    The tiles are split until the expected size of a tile is < size_in_MB.
    The tile is always split in 4 tiles. This ensures that the tiles are always split in the same way.
    The aspect ratio of the tiles is preserved.
    The result set is automatically exploded.

    .. note:: The size of the tiles is approximate. Due to compressions and other effects we cannot guarantee the size of the tiles in MB.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :param size_in_MB: The size of the tiles in MB.
    :type size_in_MB: Column (IntegerType)

    :example:

.. tabs::

    .. code-tab:: py

     df.select(mos.rst_subdivide('tile', F.lit(10))).display()
     +------------------------------------------------------------------------------------------------------------------+
     | rst_subdivide(tile, 10)                                                                                          |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_subdivide(col("tile"), lit(10))).show
     +------------------------------------------------------------------------------------------------------------------+
     | rst_subdivide(tile, 10)                                                                                          |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_subdivide(tile, 10) FROM table
     +------------------------------------------------------------------------------------------------------------------+
     | rst_subdivide(tile, 10)                                                                                          |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

rst_summary
**********************

.. function:: rst_summary(tile)

    Computes the summary of the raster tile.
    The summary is a map of the statistics of the raster.
    The logic is produced by gdalinfo procedure.
    The result is stored as JSON.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :rtype: Column: MapType(StringType, StringType)

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_summary('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_summary(tile)                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+
    | {   "description":"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1|
    |_20220106-1.nc",   "driverShortName":"netCDF",   "driverLongName":"Network Common Data Format",   "files":[       |
    |"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc"    |
    |],   "size":[     512,     512   ],   "metadata":{     "":{       "NC_GLOBAL#acknowledgement":"NOAA Coral Reef    |
    |Watch Program",       "NC_GLOBAL#cdm_data_type":"Gr...                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_summary(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_summary(tile)                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+
    | {   "description":"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1|
    |_20220106-1.nc",   "driverShortName":"netCDF",   "driverLongName":"Network Common Data Format",   "files":[       |
    |"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc"    |
    |],   "size":[     512,     512   ],   "metadata":{     "":{       "NC_GLOBAL#acknowledgement":"NOAA Coral Reef    |
    |Watch Program",       "NC_GLOBAL#cdm_data_type":"Gr...                                                            |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_summary(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_summary(tile)                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+
    | {   "description":"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1|
    |_20220106-1.nc",   "driverShortName":"netCDF",   "driverLongName":"Network Common Data Format",   "files":[       |
    |"/dbfs/FileStore/geospatial/mosaic/sample_raster_data/binary/netcdf-coral/ct5km_baa_max_7d_v3_1_20220106-1.nc"    |
    |],   "size":[     512,     512   ],   "metadata":{     "":{       "NC_GLOBAL#acknowledgement":"NOAA Coral Reef    |
    |Watch Program",       "NC_GLOBAL#cdm_data_type":"Gr...                                                            |
    +------------------------------------------------------------------------------------------------------------------+

rst_tessellate
**********************

.. function:: rst_tessellate(tile, resolution)

    Tessellates the raster tile to the given resolution of the supported grid (H3, BNG, Custom). The result is a collection of new raster tiles.
    Each tile in the tile set corresponds to a cell that is a part of the tesselation of the bounding box of the raster.
    The result set is automatically exploded.
    If rst_merge is called on the tile set the original raster will be reconstructed.
    The output tiles have same number of bands as the input rasters.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param resolution: The resolution of the supported grid.
    :type resolution: Column (IntegerType)

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_tessellate('tile', F.lit(10))).display()
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tessellate(tile, 10)                                                                                         |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_tessellate(col("tile"), lit(10))).show
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tessellate(tile, 10)                                                                                         |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_tessellate(tile, 10) FROM table
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tessellate(tile, 10)                                                                                         |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

rst_tooverlappingtiles
**********************

.. function:: rst_tooverlappingtiles(tile, width, height, overlap)

    Splits the raster tile into overlapping tiles of the given width and height.
    The overlap is the the percentage of the tile size that the tiles overlap.
    The result is a collection of new raster files.
    The result set is automatically exploded.
    If rst_merge is called on the tile set the original raster will be reconstructed.
    The output tiles have same number of bands as the input rasters.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param width: The width of the tiles in pixels.
    :type width: Column (IntegerType)
    :param height: The height of the tiles in pixels.
    :type height: Column (IntegerType)
    :param overlap: The overlap of the tiles in percentage.
    :type overlap: Column (IntegerType)

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_tooverlappingtiles('tile', F.lit(10), F.lit(10), F.lit(10))).display()
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tooverlappingtiles(tile, 10, 10, 10)                                                                         |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_tooverlappingtiles(col("tile"), lit(10), lit(10), lit(10))).show
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tooverlappingtiles(tile, 10, 10, 10)                                                                         |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_tooverlappingtiles(tile, 10, 10, 10) FROM table
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tooverlappingtiles(tile, 10, 10, 10)                                                                         |
     +------------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     | {index_id: 593308294097928192, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
     +------------------------------------------------------------------------------------------------------------------+

rst_tryopen
**********************

.. function:: rst_tryopen(tile)

    Tries to open the raster tile. If the raster cannot be opened the result is false and if the raster can be opened the result is true.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :rtype: Column: BooleanType

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_tryopen('tile')).display()
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tryopen(tile)                                                                                                |
     +------------------------------------------------------------------------------------------------------------------+
     | true                                                                                                             |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_tryopen(col("tile"))).show
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tryopen(tile)                                                                                                |
     +------------------------------------------------------------------------------------------------------------------+
     | true                                                                                                             |
     +------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_tryopen(tile) FROM table
     +------------------------------------------------------------------------------------------------------------------+
     | rst_tryopen(tile)                                                                                                |
     +------------------------------------------------------------------------------------------------------------------+
     | true                                                                                                             |
     +------------------------------------------------------------------------------------------------------------------+

rst_upperleftx
**********************

.. function:: rst_upperleftx(tile)

    Computes the upper left X coordinate of the raster tile.
    The value is computed based on GeoTransform.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_upperleftx('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperleftx(tile)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | -180.00000610436345                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_upperleftx(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperleftx(tile)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | -180.00000610436345                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_upperleftx(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperleftx(tile)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | -180.00000610436345                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_upperlefty
**********************

.. function:: rst_upperlefty(tile)

    Computes the upper left Y coordinate of the raster tile.
    The value is computed based on GeoTransform.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_upperlefty('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperlefty(tile)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.99999847369712                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_upperlefty(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperlefty(tile)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.99999847369712                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_upperlefty(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_upperlefty(tile)                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+
    | 89.99999847369712                                                                                                |
    +------------------------------------------------------------------------------------------------------------------+

rst_width
**********************

.. function:: rst_width(tile)

    Computes the width of the raster tile in pixels.


    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_width('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_width(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 600                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_width(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_width(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 600                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_width(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_width(tile)                                                                                                  |
    +------------------------------------------------------------------------------------------------------------------+
    | 600                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_worldtorastercoord
**********************

.. function:: rst_worldtorastercoord(tile, xworld, yworld)

    Computes the raster tile coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.

    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :param xworld: X world coordinate.
    :type xworld: Column (DoubleType)
    :param yworld: Y world coordinate.
    :type yworld: Column (DoubleType)
    :rtype: Column: StructType(IntegerType, IntegerType)

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_worldtorastercoord('tile', F.lit(-160.1), F.lit(40.0))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoord(tile, -160.1, 40.0)                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | {"x": 398, "y": 997}                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_worldtorastercoord(col("tile"), lit(-160.1), lit(40.0))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoord(tile, -160.1, 40.0)                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | {"x": 398, "y": 997}                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_worldtorastercoord(tile, -160.1, 40.0) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoord(tile, -160.1, 40.0)                                                                       |
    +------------------------------------------------------------------------------------------------------------------+
    | {"x": 398, "y": 997}                                                                                             |
    +------------------------------------------------------------------------------------------------------------------+

rst_worldtorastercoordx
***********************

.. function:: rst_worldtorastercoordx(tile, xworld, yworld)

    Computes the raster tile coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.
    This method returns the X coordinate.


    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :param xworld: X world coordinate.
    :type xworld: Column (DoubleType)
    :param yworld: Y world coordinate.
    :type yworld: Column (DoubleType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_worldtorastercoord('tile', F.lit(-160.1), F.lit(40.0))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordx(tile, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 398                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_worldtorastercoordx(col("tile"), lit(-160.1), lit(40.0))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordx(tile, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 398                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_worldtorastercoordx(tile, -160.1, 40.0) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordx(tile, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 398                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

rst_worldtorastercoordy
***********************

.. function:: rst_worldtorastercoordy(tile, xworld, yworld)

    Computes the raster tile coordinates of the world coordinates.
    The raster coordinates are the pixel coordinates of the raster.
    The world coordinates are the coordinates in the CRS of the raster.
    The coordinates are resolved using GeoTransform.
    This method returns the Y coordinate.


    :param tile: A column containing the raster tile. For < 0.3.11 string representing the path to a raster file or byte array.A column containing the path to a raster file.
    :type tile: Column (RasterTileType)
    :param xworld: X world coordinate.
    :type xworld: Column (DoubleType)
    :param yworld: Y world coordinate.
    :type yworld: Column (DoubleType)
    :rtype: Column: IntegerType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_worldtorastercoordy('tile', F.lit(-160.1), F.lit(40.0))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordy(tile, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 997                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_worldtorastercoordy(col("tile"), lit(-160.1), lit(40.0))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordy(tile, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 997                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_worldtorastercoordy(tile, -160.1, 40.0) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_worldtorastercoordy(tile, -160.1, 40.0)                                                                      |
    +------------------------------------------------------------------------------------------------------------------+
    | 997                                                                                                              |
    +------------------------------------------------------------------------------------------------------------------+
