=================
Raster functions
=================

#####
Intro
#####
Raster functions are available in mosaic if you have installed the optional dependency `GDAL`.
Please see :doc:`Install and Enable GDAL with Mosaic </usage/install-gdal>` for installation instructions.

    * Mosaic provides several unique raster functions that are not available in other Spark packages.
      Mainly raster to grid functions, which are useful for reprojecting the raster data into a standard grid index
      system. This is useful for performing spatial joins between raster data and vector data.
    * Mosaic also provides a scalable retiling function that can be used to retile raster data in case of bottlenecking
      due to large files.
    * All raster functions respect the :code:`rst_` prefix naming convention.

Tile objects
------------

Mosaic raster functions perform operations on "raster tile" objects. These can be created explicitly using functions
such as :ref:`rst_fromfile` or :ref:`rst_fromcontent` or implicitly when using Mosaic's GDAL datasource reader
e.g. :code:`spark.read.format("gdal")`

**Important changes to tile objects**
    * The Mosaic raster tile schema changed in v0.4.1 to the following:
      :code:`<tile:struct<index_id:bigint, tile:binary, metadata:map<string, string>>`. All APIs that use tiles now follow
      this schema.
    * Mosaic can write rasters from a DataFrame to a target directory in DBFS using the function :ref:`rst_write`

.. note:: For mosaic versions > 0.4.0 you can use the revamped setup_gdal function or new setup_fuse_install.
    These functions will configure an init script in your preferred Workspace, Volume, or DBFS location to install GDAL
    on your cluster. See :doc:`Install and Enable GDAL with Mosaic </usage/install-gdal>` for more details.

.. note:: For complex operations and / or working with large rasters, Mosaic offers the option option of employing checkpointing
    to write intermediate results to disk. Follow the instructions in :doc:`Checkpointing </usage/raster-checkpointing>` to enable this feature.

Functions
#########

rst_avg
*******

.. function:: rst_avg(tile)

    Returns an array containing mean values for each band.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :rtype: Column: ArrayType(DoubleType)

    :example:

.. tabs::
   .. code-tab:: python

    df.selectExpr(mos.rst_avg("tile")).limit(1).display()
    +---------------+
    | rst_avg(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

   .. code-tab:: scala

    df.select(rst_avg(col("tile"))).limit(1).show
    +---------------+
    | rst_avg(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

   .. code-tab:: sql

    SELECT rst_avg(tile) FROM table LIMIT 1
    +---------------+
    | rst_avg(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

rst_bandmetadata
****************

.. function:: rst_bandmetadata(tile, band)

    Extract the metadata describing the raster band.
    Metadata is return as a map of key value pairs.

    :param tile: A column containing the raster tile.
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

    :param tile: A column containing the raster tile.
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

.. function:: rst_clip(tile, geometry, cutline_all_touched)

    Clips :code:`tile` with :code:`geometry`, provided in a supported encoding (WKB, WKT or GeoJSON).

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param geometry: A column containing the geometry to clip the raster to.
    :type geometry: Column (GeometryType)
    :param cutline_all_touched: A column to specify pixels boundary behavior.
    :type cutline_all_touched: Column (BooleanType)
    :rtype: Column: RasterTileType

.. note::
  **Notes**
    Geometry input
      The :code:`geometry` parameter is expected to be a polygon or a multipolygon.

    Cutline handling
      The :code:`cutline_all_touched` parameter:

      - Optional: default is true. This is a GDAL warp config for the operation.
      - If set to true, the pixels touching the geometry are included in the clip,
        regardless of half-in or half-out; this is important for tessellation behaviors.
      - If set to false, only at least half-in pixels are included in the clip.
      - More information can be found `here <https://gdal.org/en/latest/api/gdalwarp_cpp.html>`__

      The actual GDAL command employed to perform the clipping operation is as follows:
      :code:`"gdalwarp -wo CUTLINE_ALL_TOUCHED=<TRUE|FALSE> -cutline <GEOMETRY> -crop_to_cutline"`

    Output
      Output raster tiles will have:

        - the same extent as the input geometry.
        - the same number of bands as the input raster.
        - the same pixel data type as the input raster.
        - the same pixel size as the input raster.
        - the same coordinate reference system as the input raster.
..

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

    :param tiles: A column containing an array of raster tiles.
    :type tiles: Column (ArrayType(RasterTileType))
    :rtype: Column: RasterTileType

.. note::

  Notes
    - Each tile in :code:`tiles` must have the same extent, number of bands, pixel data type, pixel size and coordinate reference system.
    - The output raster will have the same extent, number of bands, pixel data type, pixel size and coordinate reference system as the input tiles.

    Also, see :ref:`rst_combineavg_agg` function.
..

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

rst_convolve
************

.. function:: rst_convolve(tile, kernel)

    Applies a convolution filter to the raster. The result is Mosaic raster tile representing the filtered input :code:`tile`.

    :param tile: A column containing raster tile.
    :type tile: Column (RasterTileType)
    :param kernel: The kernel to apply to the raster.
    :type kernel: Column (ArrayType(ArrayType(DoubleType)))
    :rtype: Column: RasterTileType

.. note::
  Notes
    - The :code:`kernel` can be Array of Array of either Double, Integer, or Decimal but will be cast to Double.
    - This method assumes the kernel is square and has an odd number of rows and columns.
    - Kernel uses the configured GDAL :code:`blockSize` with a stride being :code:`kernelSize/2`.

..

    :example:

.. tabs::
    .. code-tab:: py

      df\
        .withColumn("convolve_arr", array(
          array(lit(1.0), lit(2.0), lit(3.0))
          array(lit(3.0), lit(2.0), lit(1.0)),
          array(lit(1.0), lit(3.0), lit(2.0)))\
       .select(rst_convolve("tile", "convolve_arr").display()
     +---------------------------------------------------------------------------+
     | rst_convolve(tile,convolve_arr)                                           |
     +---------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)",                      |
     |  "metadata":{"path":"... .tif","parentPath":"no_path","driver":"GTiff"}}  |
     +---------------------------------------------------------------------------+

    .. code-tab:: scala

      df
        .withColumn("convolve_arr", array(
          array(lit(1.0), lit(2.0), lit(3.0)),
          array(lit(3.0), lit(2.0), lit(1.0)),
          array(lit(1.0), lit(3.0), lit(2.0)))
          )
        .select(rst_convolve(col("tile"), col("convolve_arr")).show
     +---------------------------------------------------------------------------+
     | rst_convolve(tile,convolve_arr)                                           |
     +---------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)",                      |
     |  "metadata":{"path":"... .tif","parentPath":"no_path","driver":"GTiff"}}  |
     +---------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_convolve(tile, convolve_arr) FROM table LIMIT 1
     +---------------------------------------------------------------------------+
     | rst_convolve(tile,convolve_arr)                                           |
     +---------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)",                      |
     |  "metadata":{"path":"... .tif","parentPath":"no_path","driver":"GTiff"}}  |
     +---------------------------------------------------------------------------+

For clarity, this is ultimately the execution of the kernel.

    .. code-block:: scala

        def convolveAt(x: Int, y: Int, kernel: Array[Array[Double]]): Double = {
            val kernelWidth = kernel.head.length
            val kernelHeight = kernel.length
            val kernelCenterX = kernelWidth / 2
            val kernelCenterY = kernelHeight / 2
            var sum = 0.0
            for (i <- 0 until kernelHeight) {
                for (j <- 0 until kernelWidth) {
                    val xIndex = x + (j - kernelCenterX)
                    val yIndex = y + (i - kernelCenterY)
                    if (xIndex >= 0 && xIndex < width && yIndex >= 0 && yIndex < height) {
                        val maskValue = maskAt(xIndex, yIndex)
                        val value = elementAt(xIndex, yIndex)
                        if (maskValue != 0.0 && num.toDouble(value) != noDataValue) {
                            sum += num.toDouble(value) * kernel(i)(j)
                        }
                    }
                }
            }
            sum
        }

rst_derivedband
***************

.. function:: rst_derivedband(tiles, python_func, func_name)

    Combine an array of raster tiles using provided python function.

    :param tiles: A column containing an array of raster tiles.
    :type tiles: Column (ArrayType(RasterTileType))
    :param python_func: A function to evaluate in python.
    :type python_func: Column (StringType)
    :param func_name: name of the function to evaluate in python.
    :type func_name: Column (StringType)
    :rtype: Column: RasterTileType

.. note::
  Notes
    - Input raster tiles in :code:`tiles` must have the same extent, number of bands, pixel data type, pixel size and coordinate reference system.
    - The output raster will have the same the same extent, number of bands, pixel data type, pixel size and coordinate reference system as the input raster tiles.

  See also: :ref:`rst_derivedband_agg` function.
..


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


rst_dtmfromgeoms
****************

.. function:: rst_dtmfromgeoms(pointsArray, linesArray, mergeTolerance, snapTolerance, splitPointFinder, origin, xWidth, yWidth, xSize, ySize, noData)

    Generate a raster with interpolated elevations across a grid of points described by:

    - :code:`origin`: a point geometry describing the bottom-left corner of the grid,
    - :code:`xWidth` and :code:`yWidth`: the number of points in the grid in x and y directions,
    - :code:`xSize` and :code:`ySize`: the space between grid points in the x and y directions.

    :note: To generate a grid from a "top-left" :code:`origin`, use a negative value for :code:`ySize`.

    The underlying algorithm first creates a surface mesh by triangulating :code:`pointsArray`
    (including :code:`linesArray` as a set of constraint lines) then determines where each point
    in the grid would lie on the surface mesh. Finally, it interpolates the
    elevation of that point based on the surrounding triangle's vertices.

    As with :code:`st_triangulate`, there are two 'tolerance' parameters for the algorithm:

    - :code:`mergeTolerance` sets the point merging tolerance of the triangulation algorithm, i.e. before the initial
      triangulation is performed, nearby points in :code:`pointsArray` can be merged in order to speed up the triangulation
      process. A value of zero means all points are considered for triangulation.
    - :code:`snapTolerance` sets the tolerance for post-processing the results of the triangulation, i.e. matching
      the vertices of the output triangles to input points / lines. This is necessary as the algorithm often returns null
      height / Z values. Setting this to a large value may result in the incorrect Z values being assigned to the
      output triangle vertices (especially when :code:`linesArray` contains very densely spaced segments).
      Setting this value to zero may result in the output triangle vertices being assigned a null Z value.
    Both tolerance parameters are expressed in the same units as the projection of the input point geometries.

    Additionally, you have control over the algorithm used to find split points on the constraint lines. The recommended
    default option here is the "NONENCROACHING" algorithm. You can also use the "MIDPOINT" algorithm if you find the
    constraint fitting process fails to converge. For full details of these options see the JTS reference
    `here <https://locationtech.github.io/jts/javadoc/org/locationtech/jts/triangulate/ConstraintSplitPointFinder.html>`__.

    The :code:`noData` value of the output raster can be set using the :code:`noData` parameter.

    This is a generator expression and the resulting DataFrame will contain one row per point of the grid.

    :param pointsArray: Array of geometries respresenting the points to be triangulated
    :type pointsArray: Column (ArrayType(Geometry))
    :param linesArray: Array of geometries respresenting the lines to be used as constraints
    :type linesArray: Column (ArrayType(Geometry))
    :param mergeTolerance: A tolerance used to coalesce points in close proximity to each other before performing triangulation.
    :type mergeTolerance: Column (DoubleType)
    :param snapTolerance: A snapping tolerance used to relate created points to their corresponding lines for elevation interpolation.
    :type snapTolerance: Column (DoubleType)
    :param splitPointFinder: Algorithm used for finding split points on constraint lines. Options are "NONENCROACHING" and "MIDPOINT".
    :type splitPointFinder: Column (StringType)
    :param origin: A point geometry describing the bottom-left corner of the grid.
    :type origin: Column (Geometry)
    :param xWidth: The number of points in the grid in x direction.
    :type xWidth: Column (IntegerType)
    :param yWidth: The number of points in the grid in y direction.
    :type yWidth: Column (IntegerType)
    :param xSize: The spacing between each point on the grid's x-axis.
    :type xSize: Column (DoubleType)
    :param ySize: The spacing between each point on the grid's y-axis.
    :type ySize: Column (DoubleType)
    :param noData: The no-data value of the output raster.
    :type noData: Column (DoubleType)
    :rtype: Column (RasterTileType)

    :example:

.. tabs::
   .. code-tab:: py

    df = (
        spark.createDataFrame(
            [
                ["POINT Z (2 1 0)"],
                ["POINT Z (3 2 1)"],
                ["POINT Z (1 3 3)"],
                ["POINT Z (0 2 2)"],
            ],
            ["wkt"],
        )
        .groupBy()
        .agg(collect_list("wkt").alias("masspoints"))
        .withColumn("breaklines", array(lit("LINESTRING EMPTY")))
        .withColumn("origin", st_geomfromwkt(lit("POINT (0.6 1.8)")))
        .withColumn("xWidth", lit(12))
        .withColumn("yWidth", lit(6))
        .withColumn("xSize", lit(0.1))
        .withColumn("ySize", lit(0.1))
    )
    df.select(
        rst_dtmfromgeoms(
            "masspoints", "breaklines", lit(0.0), lit(0.01),
            "origin", "xWidth", "yWidth", "xSize", "ySize",
            split_point_finder="NONENCROACHING", no_data_value=-9999.0
        )
    ).show(truncate=False)
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |rst_dtmfromgeoms(masspoints, breaklines, 0.0, 0.01, origin, xWidth, yWidth, xSize, ySize)                                                                                                                                                                                                                                                                                                                                                              |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |{NULL, /dbfs/tmp/mosaic/raster/checkpoint/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, {path -> /dbfs/tmp/mosaic/raster/checkpoint/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, last_error -> , all_parents -> , driver -> GTiff, parentPath -> /tmp/mosaic_tmp/mosaic5678582907307109410/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, last_command -> gdal_rasterize ATTRIBUTE=VALUES -of GTiff -co TILED=YES -co COMPRESS=DEFLATE}}|
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = Seq(
      Seq(
        "POINT Z (2 1 0)", "POINT Z (3 2 1)",
        "POINT Z (1 3 3)", "POINT Z (0 2 2)"
      )
    )
    .toDF("masspoints")
    .withColumn("breaklines", array().cast(ArrayType(StringType)))
    .withColumn("origin", st_geomfromwkt(lit("POINT (0.6 1.8)")))
    .withColumn("xWidth", lit(12))
    .withColumn("yWidth", lit(6))
    .withColumn("xSize", lit(0.1))
    .withColumn("ySize", lit(0.1))

    df.select(
      rst_dtmfromgeoms(
        $"masspoints", $"breaklines",
        lit(0.0), lit(0.01), lit("NONENCROACHING")
        $"origin", $"xWidth", $"yWidth",
        $"xSize", $"ySize", lit(-9999.0)
      )
    ).show(1, false)
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |rst_dtmfromgeoms(masspoints, breaklines, 0.0, 0.01, origin, xWidth, yWidth, xSize, ySize)                                                                                                                                                                                                                                                                                                                                                              |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |{NULL, /dbfs/tmp/mosaic/raster/checkpoint/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, {path -> /dbfs/tmp/mosaic/raster/checkpoint/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, last_error -> , all_parents -> , driver -> GTiff, parentPath -> /tmp/mosaic_tmp/mosaic5678582907307109410/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, last_command -> gdal_rasterize ATTRIBUTE=VALUES -of GTiff -co TILED=YES -co COMPRESS=DEFLATE}}|
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT
      RST_DTMFROMGEOMS(
        ARRAY(
          "POINT Z (2 1 0)",
          "POINT Z (3 2 1)",
          "POINT Z (1 3 3)",
          "POINT Z (0 2 2)"
        ),
        ARRAY("LINESTRING EMPTY"),
        DOUBLE(0.0), DOUBLE(0.01), "NONENCROACHING",
        "POINT (0.6 1.8)", 12, 6,
        DOUBLE(0.1), DOUBLE(0.1), DOUBLE(-9999.0)
      ) AS tile
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |rst_dtmfromgeoms(masspoints, breaklines, 0.0, 0.01, origin, xWidth, yWidth, xSize, ySize)                                                                                                                                                                                                                                                                                                                                                              |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |{NULL, /dbfs/tmp/mosaic/raster/checkpoint/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, {path -> /dbfs/tmp/mosaic/raster/checkpoint/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, last_error -> , all_parents -> , driver -> GTiff, parentPath -> /tmp/mosaic_tmp/mosaic5678582907307109410/raster_d4ab419f_9829_4004_99a3_aaa597a69938.GTiff, last_command -> gdal_rasterize ATTRIBUTE=VALUES -of GTiff -co TILED=YES -co COMPRESS=DEFLATE}}|
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: r R

    sdf <- createDataFrame(
      data.frame(
        points = c(
          "POINT Z (3 2 1)", "POINT Z (2 1 0)",
          "POINT Z (1 3 3)", "POINT Z (0 2 2)"
        )
      )
    )
    sdf <- agg(groupBy(sdf), masspoints = collect_list(column("points")))
    sdf <- withColumn(sdf, "breaklines", expr("array('LINESTRING EMPTY')"))
    sdf <- select(sdf, rst_dtmfromgeoms(
      column("masspoints"), column("breaklines"),
      lit(0.0), lit(0.01), lit("NONENCROACHING"),
      lit("POINT (0.6 1.8)"), lit(12L), lit(6L),
      lit(0.1), lit(0.1), lit(-9999.0)
      )
    )
    showDF(sdf, n=1, truncate=F)
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |rst_dtmfromgeoms(masspoints, breaklines, 0.0, 0.01, POINT (0.6 1.8), 12, 6, 0.1, 0.1)                                                                                                                                                                                                                                                                                                                                                                  |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |{NULL, /dbfs/tmp/mosaic/raster/checkpoint/raster_ab03a97f_9bc3_410c_80e1_adf6f75f46e2.GTiff, {path -> /dbfs/tmp/mosaic/raster/checkpoint/raster_ab03a97f_9bc3_410c_80e1_adf6f75f46e2.GTiff, last_error -> , all_parents -> , driver -> GTiff, parentPath -> /tmp/mosaic_tmp/mosaic8840676907961488874/raster_ab03a97f_9bc3_410c_80e1_adf6f75f46e2.GTiff, last_command -> gdal_rasterize ATTRIBUTE=VALUES -of GTiff -co TILED=YES -co COMPRESS=DEFLATE}}|
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


rst_filter
**********

.. function:: rst_filter(tile,kernel_size,operation)

    Applies a filter to the raster.
    Returns a new raster tile with the filter applied.
    :code:`kernel_size` is the number of pixels to compare; it must be odd.
    :code:`operation` is the op to apply, e.g. 'avg', 'median', 'mode', 'max', 'min'.

    :param tile: Mosaic raster tile struct column.
    :type tile: Column (RasterTileType)
    :param kernel_size: The size of the kernel. Has to be odd.
    :type kernel_size: Column (IntegerType)
    :param operation: The operation to apply to the kernel.
    :type operation: Column (StringType)
    :rtype: Column (RasterTileType) 

    :example:

.. tabs::
    .. code-tab:: py

     df.select(rst_filter('tile', lit(3), lit("mode"))).limit(1).display()
     +-----------------------------------------------------------------------------------------------------------------------------+
     | rst_filter(tile,3,mode)                                                                                                     |
     +-----------------------------------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","parentPath":"no_path","driver":"GTiff"}} |
     +-----------------------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_filter(col("tile"), lit(3), lit("mode"))).limit(1).show
     +-----------------------------------------------------------------------------------------------------------------------------+
     | rst_filter(tile,3,mode)                                                                                                     |
     +-----------------------------------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","parentPath":"no_path","driver":"GTiff"}} |
     +-----------------------------------------------------------------------------------------------------------------------------+


    .. code-tab:: sql

     SELECT rst_filter(tile,3,"mode") FROM table LIMIT 1
     +-----------------------------------------------------------------------------------------------------------------------------+
     | rst_filter(tile,3,mode)                                                                                                     |
     +-----------------------------------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","parentPath":"no_path","driver":"GTiff"}} |
     +-----------------------------------------------------------------------------------------------------------------------------+

rst_frombands
**************

.. function:: rst_frombands(tiles)

    Combines a collection of raster tiles of different bands into a single raster.

    :param tiles: A column containing an array of raster tiles.
    :type tiles: Column (ArrayType(RasterTileType))
    :rtype: Column: RasterTileType

.. note::

  Notes
    - All raster tiles must have the same extent.
    - The tiles must have the same pixel coordinate reference system.
    - The output tile will have the same extent as the input tiles.
    - The output tile will have the a number of bands equivalent to the number of input tiles.
    - The output tile will have the same pixel type as the input tiles.
    - The output tile will have the same pixel size as the highest resolution input tile.
    - The output tile will have the same coordinate reference system as the input tiles.
..

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
***************

.. function:: rst_fromcontent(raster_bin, driver, <size_in_MB>)

    Returns a tile from raster data.


    :param raster_bin: A column containing the raster data.
    :type raster_bin: Column (BinaryType)
    :param driver: GDAL driver to use to open the raster.
    :type driver: Column(StringType)
    :param size_in_MB: Optional parameter to specify the size of the raster tile in MB. Default is not to split the input.
    :type size_in_MB: Column (IntegerType)
    :rtype: Column: RasterTileType

.. note::

  **Notes**
    - The input raster must be a byte array in a BinaryType column.
    - The driver required to read the raster must be one supplied with GDAL.
    - If the size_in_MB parameter is specified, the raster will be split into tiles of the specified size.
    - If the size_in_MB parameter is not specified or if the size_in_Mb < 0, the raster will only be split if it exceeds Integer.MAX_VALUE. The split will be at a threshold of 64MB in this case.


..

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

    :param path: A column containing the path to a raster file.
    :type path: Column (StringType)
    :param size_in_MB: Optional parameter to specify the size of the raster tile in MB. Default is not to split the input.
    :type size_in_MB: Column (IntegerType)
    :rtype: Column: RasterTileType

.. note::

  Notes
    - The file path must be a string.
    - The file path must be a valid path to a raster file.
    - The file path must be a path to a file that GDAL can read.
    - If the size_in_MB parameter is specified, the raster will be split into tiles of the specified size.
    - If the size_in_MB parameter is not specified or if the size_in_Mb < 0, the raster will only be split if it exceeds Integer.MAX_VALUE. The split will be at a threshold of 64MB in this case.
..


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

    Returns GeoTransform of the raster tile as a GT array of doubles. The output takes the form of a MapType with the following keys:

    - :code:`GT(0)` x-coordinate of the upper-left corner of the upper-left pixel.
    - :code:`GT(1)` w-e pixel resolution / pixel width.
    - :code:`GT(2)` row rotation (typically zero).
    - :code:`GT(3)` y-coordinate of the upper-left corner of the upper-left pixel.
    - :code:`GT(4)` column rotation (typically zero).
    - :code:`GT(5)` n-s pixel resolution / pixel height (negative value for a north-up image).

    :param tile: A column containing the raster tile.
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

rst_getnodata
*************

.. function:: rst_getnodata(tile)

    Returns the nodata value of the raster tile bands.

    :param tile: A column containing the raster tile.
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

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param name: A column containing the name of the subdataset to return.
    :type name: Column (StringType)
    :rtype: Column: RasterTileType

.. note::
  Notes
    - :code:`name` should be the last identifier in the standard GDAL subdataset path: :code:`DRIVER:PATH:NAME`.
    - :code:`name` must be a valid subdataset name for the raster, i.e. it must exist within the raster.
..

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

    :param tile: A column containing the raster tile.
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

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :rtype: Column: RasterTileType

.. note::

  Notes
    - The nodata value will be set to a default sentinel values according to the pixel data type of the raster bands.
    - The output raster will have the same extent as the input raster.

    .. list-table:: Default nodata values for raster data types
      :widths: 25 25 50
      :header-rows: 1

      * - Data Type
        - Scala representation
        - Value
      * - ByteType
        -
        - 0
      * - UnsignedShortType
        - :code:`UShort.MaxValue`
        - 65535
      * - ShortType
        - :code:`Short.MinValue`
        - -32768
      * - UnsignedIntegerType
        - :code:`Int.MaxValue`
        - 4.294967294E9
      * - IntegerType
        - :code:`Int.MinValue`
        - -2147483648
      * - FloatType
        - :code:`Float.MinValue`
        - -3.4028234663852886E38
      * - DoubleType
        - :code:`Double.MinValue`
        - -1.7976931348623157E308
    ..
..

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

    :param tile: A column containing the raster tile.
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

rst_maketiles
*************

.. function:: rst_maketiles(input, driver, size, withCheckpoint)

    Tiles the raster into tiles of the given size, optionally writing them to disk in the process.

    :param input: path (StringType) or content (BinaryType)
    :type input: Column
    :param driver: The driver to use for reading the raster. 
    :type driver: Column(StringType)
    :param size_in_mb: The size of the tiles in MB. 
    :type size_in_mb: Column(IntegerType)
    :param with_checkpoint: whether to use configured checkpoint location.
    :type with_checkpoint: Column(BooleanType)
    :rtype: Column: RasterTileType

.. note::

  Notes:

  :code:`input`
    - If the raster is stored on disk, :code:`input` should be the path to the raster, similar to :ref:`rst_fromfile`.
    - If the raster is stored in memory, :code:`input` should be the byte array representation of the raster, similar to :ref:`rst_fromcontent`.

  :code:`driver`
    - If not specified, :code:`driver` is inferred from the file extension
    - If the input is a byte array, the driver must be explicitly specified.

  :code:`size`
    - If :code:`size` is set to -1, the file is loaded and returned as a single tile
    - If set to 0, the file is loaded and subdivided into tiles of size 64MB
    - If set to a positive value, the file is loaded and subdivided into tiles of the specified size
    - If the file is too big to fit in memory, it is subdivided into tiles of size 64MB.

  :code:`with_checkpoint`
    - If :code:`with_checkpoint` set to true, the tiles are written to the checkpoint directory
    - If set to false, the tiles are returned as in-memory byte arrays.

  Once enabled, checkpointing will remain enabled for tiles originating from this function,
  meaning follow-on calls will also use checkpointing. To switch away from checkpointing down the line,
  you could call :ref:`rst_fromfile` using the checkpointed locations as the :code:`path` input.
..

    :example:

.. tabs::
    .. code-tab:: py

     spark.read.format("binaryFile").load(dbfs_dir)\
     .select(rst_maketiles("path")).limit(1).display()
     +------------------------------------------------------------------------+
     | tile                                                                   |
     +------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAMAAA (truncated)","metadata":{         |
     | "parentPath":"no_path","driver":"GTiff","path":"...","last_error":""}} |
     +------------------------------------------------------------------------+

    .. code-tab:: scala

     spark.read.format("binaryFile").load(dbfs_dir)
     .select(rst_maketiles(col("path"))).limit(1).show
     +------------------------------------------------------------------------+
     | tile                                                                   |
     +------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAMAAA (truncated)","metadata":{         |
     | "parentPath":"no_path","driver":"GTiff","path":"...","last_error":""}} |
     +------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_maketiles(path) FROM table LIMIT 1
     +------------------------------------------------------------------------+
     | tile                                                                   |
     +------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAMAAA (truncated)","metadata":{         |
     | "parentPath":"no_path","driver":"GTiff","path":"...","last_error":""}} |
     +------------------------------------------------------------------------+



rst_mapalgebra
**************

.. function:: rst_mapalgebra(tile, json_spec)

    Performs map algebra on the raster tile.

    Employs the :code:`gdal_calc` command line raster calculator with standard numpy syntax.
    Use any basic arithmetic supported by numpy arrays (such as \+, \-, \*, and /) along with
    logical operators (such as >, <, =).

    For this distributed implementation, all rasters must have the same dimensions and no projection checking is performed.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param json_spec: A column containing the map algebra operation specification.
    :type json_spec: Column (StringType)
    :rtype: Column: RasterTileType

.. note::
    The :code:`json_spec` parameter
      - Input rasters to the algebra function are referencable as variables with names :code:`A` through :code:`Z`.
      - Bands from the input :code:`tile` are referencable using ordinal 0..n values.

    Examples of valid :code:`json_spec`


    .. code-block:: text

        (1) '{"calc": "A+B/C"}'
        (2) '{"calc": "A+B/C", "A_index": 0, "B_index": 1, "C_index": 1}'
        (3) '{"calc": "A+B/C", "A_index": 0, "B_index": 1, "C_index": 2, "A_band": 1, "B_band": 1, "C_band": 1}'

  ..

    In these examples:

      1. demonstrates default indexing (i.e. the first three bands in :code:`tile` are assigned A, B and C respectively)
      2. demonstrates reusing an index (B and C represent the same band); and
      3. shows band indexing.
..

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

rst_max
*******

.. function:: rst_max(tile)

    Returns an array containing maximum values for each band.

    :param tile: A column containing the raster tile. 
    :type tile: Column (RasterTileType)
    :rtype: Column: ArrayType(DoubleType)

    :example:

.. tabs::
   .. code-tab:: python

    df.selectExpr(mos.rst_max("tile")).limit(1).display()
    +---------------+
    | rst_max(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

   .. code-tab:: scala

    df.select(rst_max(col("tile"))).limit(1).show
    +---------------+
    | rst_max(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

   .. code-tab:: sql

    SELECT rst_max(tile) FROM table LIMIT 1
    +---------------+
    | rst_max(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

rst_median
**********

.. function:: rst_median(tile)

    Returns an array containing median values for each band.

    :param tile: A column containing the raster tile. 
    :type tile: Column (RasterTileType)
    :rtype: Column: ArrayType(DoubleType)

    :example:

.. tabs::
   .. code-tab:: python

    df.selectExpr(mos.rst_median("tile")).limit(1).display()
    +---------------+
    | rst_median(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

   .. code-tab:: scala

    df.select(rst_median(col("tile"))).limit(1).show
    +---------------+
    | rst_median(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

   .. code-tab:: sql

    SELECT rst_median(tile) FROM table LIMIT 1
    +---------------+
    | rst_median(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

rst_memsize
*************

.. function:: rst_memsize(tile)

    Returns size of the raster tile in bytes.

    :param tile: A column containing the raster tile.
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

    :param tiles: A column containing an array of raster tiles.
    :type tiles: Column (ArrayType(RasterTileType))
    :rtype: Column: RasterTileType

.. note::
  Notes

  Input tiles supplied in :code:`tiles`:
    - are not required to have the same extent.
    - must have the same coordinate reference system.
    - must have the same pixel data type.
    - will be combined using the :code:`gdalwarp` command.
    - require a :code:`noData` value to have been initialised (if this is not the case, the non valid pixels may introduce artifacts in the output raster).
    - will be stacked in the order they are provided.

  The resulting output raster will have:
    - an extent that covers all of the input tiles;
    - the same number of bands as the input tiles;
    - the same pixel type as the input tiles;
    - the same pixel size as the highest resolution input tiles; and
    - the same coordinate reference system as the input tiles.

  See also :ref:`rst_merge_agg` function.
..

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

rst_metadata
*************

.. function:: rst_metadata(tile)

    Extract the metadata describing the raster tile.
    Metadata is return as a map of key value pairs.

    :param tile: A column containing the raster tile.
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

rst_min
*******

.. function:: rst_min(tile)

    Returns an array containing minimum values for each band.

    :param tile: A column containing the raster tile. 
    :type tile: Column (RasterTileType)
    :rtype: Column: ArrayType(DoubleType)

    :example:

.. tabs::
   .. code-tab:: python

    df.selectExpr(mos.rst_min("tile")).limit(1).display()
    +---------------+
    | rst_min(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

   .. code-tab:: scala

    df.select(rst_min(col("tile"))).limit(1).show
    +---------------+
    | rst_min(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

   .. code-tab:: sql

    SELECT rst_min(tile) FROM table LIMIT 1
    +---------------+
    | rst_min(tile) |
    +---------------+
    |        [42.0] |
    +---------------+

rst_ndvi
********

.. function:: rst_ndvi(tile, red_band_num, nir_band_num)

    Calculates the Normalized Difference Vegetation Index (NDVI) for a raster.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param red_band_num: A column containing the band number of the red band.
    :type red_band_num: Column (IntegerType)
    :param nir_band_num: A column containing the band number of the near infrared band.
    :type nir_band_num: Column (IntegerType)
    :rtype: Column: RasterTileType

.. note::
  NDVI is calculated using the formula: (NIR - RED) / (NIR + RED).

  The output raster tiles will have:
    - the same extent as the input raster.
    - a single band.
    - a pixel data type of float64.
    - the same coordinate reference system as the input raster.
..

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

    :param tile: A column containing the raster tile.
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

rst_pixelcount
**************

.. function:: rst_pixelcount(tile, count_nodata, count_all)

    Returns an array containing pixel count values for each band; default excludes mask and nodata pixels.
    
    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param count_nodata: A column to specify whether to count nodata pixels.
    :type count_nodata: Column (BooleanType)
    :param count_all: A column to specify whether to count all pixels.
    :type count_all: Column (BooleanType)
    :rtype: Column: ArrayType(LongType)

.. note::

  Notes:

  If pixel value is noData or mask value is 0.0, the pixel is not counted by default.

  :code:`count_nodata`
    - This is an optional param.
    - if specified as true, include the noData (not mask) pixels in the count (default is false).

  :code:`count_all`
    - This is an optional param; as a positional arg, must also pass :code:`count_nodata`
      (value of :code:`count_nodata` is ignored).
    - if specified as true, simply return bandX * bandY in the count (default is false).
..

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_pixelcount('tile')).display()
    +----------------------+
    | rst_pixelcount(tile) |
    +----------------------+
    |          [120560172] |
    +----------------------+

   .. code-tab:: scala

    df.select(rst_pixelcount(col("tile"))).show
    +----------------------+
    | rst_pixelcount(tile) |
    +----------------------+
    |          [120560172] |
    +----------------------+

   .. code-tab:: sql

    SELECT rst_pixelcount(tile) FROM table
    +----------------------+
    | rst_pixelcount(tile) |
    +----------------------+
    |          [120560172] |
    +----------------------+

rst_pixelheight
***************

.. function:: rst_pixelheight(tile)

    Returns the height of the pixel in the raster tile derived via GeoTransform.

    :param tile: A column containing the raster tile.
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

    :param tile: A column containing the raster tile.
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


    Compute the gridwise mean of the pixel values in :code:`tile`.

    The result is a 2D array of cells, where each cell is a struct of (:code:`cellID`, :code:`value`).

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

.. note::
  Notes
    - To obtain cellID->value pairs, use the Spark SQL explode() function twice.
    - CellID can be LongType or StringType depending on the configuration of MosaicContext.
    - The value/measure for each cell is the average of the pixel values in the cell.
..

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

    Compute the gridwise count of the pixels in :code:`tile`.

    The result is a 2D array of cells, where each cell is a struct of (:code:`cellID`, :code:`value`).

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

.. note::
  Notes
    - To obtain cellID->value pairs, use the Spark SQL explode() function twice.
    - CellID can be LongType or StringType depending on the configuration of MosaicContext.
    - The value/measure for each cell is the count of the pixel values in the cell.
..

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

    Compute the gridwise maximum of the pixels in :code:`tile`.

    The result is a 2D array of cells, where each cell is a struct of (:code:`cellID`, :code:`value`).

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

.. note::
  Notes
    - To obtain cellID->value pairs, use the Spark SQL explode() function twice.
    - CellID can be LongType or StringType depending on the configuration of MosaicContext.
    - The value/measure for each cell is the maximum of the pixel values in the cell.
..

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

    Compute the gridwise median value of the pixels in :code:`tile`.

    The result is a 2D array of cells, where each cell is a struct of (:code:`cellID`, :code:`value`).

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

.. note::
  Notes
    - To obtain cellID->value pairs, use the Spark SQL explode() function twice.
    - CellID can be LongType or StringType depending on the configuration of MosaicContext.
    - The value/measure for each cell is the median of the pixel values in the cell.
..

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

    SELECT rst_rastertogridmedian(tile, 3) FROM table
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

    Compute the gridwise minimum of the pixel values in :code:`tile`.

    The result is a 2D array of cells, where each cell is a struct of (:code:`cellID`, :code:`value`).

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param resolution: A resolution of the grid index system.
    :type resolution: Column (IntegerType)
    :rtype: Column: ArrayType(ArrayType(StructType(LongType|StringType, DoubleType)))

.. note::
  Notes
    - To obtain cellID->value pairs, use the Spark SQL explode() function twice.
    - CellID can be LongType or StringType depending on the configuration of MosaicContext.
    - The value/measure for each cell is the minimum of the pixel values in the cell.
..

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

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param x: x coordinate of the pixel.
    :type x: Column (IntegerType)
    :param y: y coordinate of the pixel.
    :type y: Column (IntegerType)
    :rtype: Column: StringType

.. note::
  Notes
    - The result is a WKT point geometry.
    - The coordinates are computed using the GeoTransform of the raster to respect the projection.
..

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
***********************

.. function:: rst_rastertoworldcoordx(tile, x, y)

    Computes the world coordinates of the raster tile at the given x and y pixel coordinates.

    The result is the X coordinate of the point after applying the GeoTransform of the raster.


    :param tile: A column containing the raster tile.
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
***********************

.. function:: rst_rastertoworldcoordy(tile, x, y)

    Computes the world coordinates of the raster tile at the given x and y pixel coordinates.

    The result is the Y coordinate of the point after applying the GeoTransform of the raster.

    :param tile: A column containing the raster tile.
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
**********

.. function:: rst_retile(tile, width, height)

    Retiles the raster tile to the given size. The result is a collection of new raster tiles.

    :param tile: A column containing the raster tile.
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
************

.. function:: rst_rotation(tile)

    Computes the angle of rotation between the X axis of the raster tile and geographic North in degrees
    using the GeoTransform of the raster.

    :param tile: A column containing the raster tile.
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
**********

.. function:: rst_scalex(tile)

    Computes the scale of the raster tile in the X direction.

    :param tile: A column containing the raster tile.
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
**********

.. function:: rst_scaley(tile)

    Computes the scale of the raster tile in the Y direction.

    :param tile: A column containing the raster tile.
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

rst_separatebands
*****************

.. function:: rst_separatebands(tile)

    Returns a set of new single-band rasters, one for each band in the input raster. The result set will contain one row
    per input band for each :code:`tile` provided.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :rtype: Column: (RasterTileType)

.. note::
  ️⚠️ Before performing this operation, you may want to add an identifier column to the dataframe to trace each band
  back to its original parent raster.
..

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_separatebands('tile')).display()
    +--------------------------------------------------------------------------------------------------------------------------------+
    | tile                                                                                                                           |
    +--------------------------------------------------------------------------------------------------------------------------------+
    | {"index_id":null,"raster":"SUkqAAg...= (truncated)",                                                                           |
    |  "metadata":{"path":"....tif","last_error":"","all_parents":"no_path","driver":"GTiff","bandIndex":"1","parentPath":"no_path", |
    |              "last_command":"gdal_translate -of GTiff -b 1 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}                     |
    +--------------------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_separatebands(col("tile"))).show
    +--------------------------------------------------------------------------------------------------------------------------------+
    | tile                                                                                                                           |
    +--------------------------------------------------------------------------------------------------------------------------------+
    | {"index_id":null,"raster":"SUkqAAg...= (truncated)",                                                                           |
    |  "metadata":{"path":"....tif","last_error":"","all_parents":"no_path","driver":"GTiff","bandIndex":"1","parentPath":"no_path", |
    |              "last_command":"gdal_translate -of GTiff -b 1 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}                     |
    +--------------------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_separatebands(tile) FROM table
    +--------------------------------------------------------------------------------------------------------------------------------+
    | tile                                                                                                                           |
    +--------------------------------------------------------------------------------------------------------------------------------+
    | {"index_id":null,"raster":"SUkqAAg...= (truncated)",                                                                           |
    |  "metadata":{"path":"....tif","last_error":"","all_parents":"no_path","driver":"GTiff","bandIndex":"1","parentPath":"no_path", |
    |              "last_command":"gdal_translate -of GTiff -b 1 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}                     |
    +--------------------------------------------------------------------------------------------------------------------------------+

rst_setnodata
*************

.. function:: rst_setnodata(tile, nodata)

    Returns a new raster tile with the nodata value set to :code:`nodata`.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param nodata: The nodata value to set.
    :type nodata: Column (DoubleType) / ArrayType(DoubleType)
    :rtype: Column: (RasterTileType)

.. note::
  Notes
    - If a single :code:`nodata` value is passed, the same nodata value is set for all bands of :code:`tile`.
    - If an array of values is passed, the respective :code:`nodata` value is set for each band of :code:`tile`.
..

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

rst_setsrid
***********

.. function:: rst_setsrid(tile, srid)

    Set the SRID of the raster tile as an EPSG code.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param srid: The SRID to set
    :type srid: Column (IntegerType)
    :rtype: Column: (RasterTileType)

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_setsrid('tile', F.lit(9122))).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_setsrid(tile, 9122)                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+
    | {index_id: 593308294097928191, tile: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_setsrid(col("tile"), lit(9122))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_setsrid(tile, 9122)                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+
    | {index_id: 593308294097928191, tile: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_setsrid(tile, 9122) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_setsrid(tile, 9122)                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+
    | {index_id: 593308294097928191, tile: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "GTiff" }    |
    +------------------------------------------------------------------------------------------------------------------+

rst_skewx
*********

.. function:: rst_skewx(tile)

    Computes the skew of the raster tile in the X direction.

    :param tile: A column containing the raster tile.
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
*********

.. function:: rst_skewy(tile)

    Computes the skew of the raster tile in the Y direction.

    :param tile: A column containing the raster tile.
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
********

.. function:: rst_srid(tile)

    Returns the SRID of the raster tile as an EPSG code.

    .. note:: For complex CRS definition the EPSG code may default to 0.

    :param tile: A column containing the raster tile.
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

    Returns the subdatasets of the raster tile as a set of paths in the standard GDAL format.

    The result is a map of the subdataset path to the subdatasets and the description of the subdatasets.

    :param tile: A column containing the raster tile.
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
*************

.. function:: rst_subdivide(tile, sizeInMB)

    Subdivides the raster tile to the given tile size in MB. The result is a collection of new raster tiles.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param size_in_MB: The size of the tiles in MB.
    :type size_in_MB: Column (IntegerType)

.. note::
  Notes
    - Each :code:`tile` will be recursively split along two orthogonal axes until the expected size of the last child tile is < :code:`size_in_MB`.
    - The aspect ratio of the tiles is preserved.
    - The result set is automatically exploded.

  The size of the resulting tiles is approximate. Due to compression and other effects we cannot guarantee the size of the tiles in MB.
..

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
***********

.. function:: rst_summary(tile)

    Returns a summary description of the raster tile including metadata and statistics in JSON format.

    Values returned here are produced by the :code:`gdalinfo` procedure.


    :param tile: A column containing the raster tile.
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
**************

.. function:: rst_tessellate(tile, resolution)

    Divides the raster tile into tessellating chips for the given resolution of the supported grid (H3, BNG, Custom).
    The result is a collection of new raster tiles.

    Each tile in the tile set corresponds to an index cell intersecting the bounding box of :code:`tile`.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param resolution: The resolution of the supported grid.
    :type resolution: Column (IntegerType)

.. note::
  Notes
    - The result set is automatically exploded into a row-per-index-cell.
    - If :ref:`rst_merge` is called on output tile set, the original raster will be reconstructed.
    - Each output tile chip will have the same number of bands as its parent :code:`tile`.
..

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

    Splits each :code:`tile` into a collection of new raster tiles of the given width and height,
    with an overlap of :code:`overlap` percent.

    The result set is automatically exploded into a row-per-subtile.


    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param width: The width of the tiles in pixels.
    :type width: Column (IntegerType)
    :param height: The height of the tiles in pixels.
    :type height: Column (IntegerType)
    :param overlap: The overlap of the tiles in percentage.
    :type overlap: Column (IntegerType)

.. note::
  Notes
    - If :ref:`rst_merge` is called on the tile set the original raster will be reconstructed.
    - Each output tile chip will have the same number of bands as its parent :code:`tile`.
..

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

rst_transform
**********************

.. function:: rst_transform(tile,srid)

    Transforms the raster to the given SRID.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param srid: EPSG authority code for the file's projection.
    :type srid: Column (IntegerType)
    :rtype: Column: (RasterTileType)

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_transform('tile', lit(4326))).display()
     +----------------------------------------------------------------------------------------------------+
     | rst_transform(tile,4326)                                                                           |
     +----------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","last_error":"", |
     |  "all_parents":"no_path","driver":"GTiff","parentPath":"no_path",                                  |
     |  "last_command":"gdalwarp -t_srs EPSG:4326 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}         |
     +----------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_transform(col("tile"), lit(4326))).show
     +----------------------------------------------------------------------------------------------------+
     | rst_transform(tile,4326)                                                                           |
     +----------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","last_error":"", |
     |  "all_parents":"no_path","driver":"GTiff","parentPath":"no_path",                                  |
     |  "last_command":"gdalwarp -t_srs EPSG:4326 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}         |
     +----------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_transform(tile,4326) FROM table
     +----------------------------------------------------------------------------------------------------+
     | rst_transform(tile,4326)                                                                           |
     +----------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","last_error":"", |
     |  "all_parents":"no_path","driver":"GTiff","parentPath":"no_path",                                  |
     |  "last_command":"gdalwarp -t_srs EPSG:4326 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}         |
     +----------------------------------------------------------------------------------------------------+


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


rst_type
********

.. function:: rst_type(tile)

    Returns the data type of the raster's bands.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    df.select(mos.rst_type('tile')).display()
    +------------------------------------------------------------------------------------------------------------------+
    | rst_type(tile)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | [Int16]                                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    df.select(rst_type(col("tile"))).show
    +------------------------------------------------------------------------------------------------------------------+
    | rst_type(tile)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | [Int16]                                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT rst_type(tile) FROM table
    +------------------------------------------------------------------------------------------------------------------+
    | rst_type(tile)                                                                                                   |
    +------------------------------------------------------------------------------------------------------------------+
    | [Int16]                                                                                                          |
    +------------------------------------------------------------------------------------------------------------------+


rst_updatetype
**************

.. function:: rst_updatetype(tile, newType)

    Translates the raster to a new data type.

    :param tile: A column containing the raster tile.
    :type tile: Column (RasterTileType)
    :param newType: Data type to translate the raster to.
    :type newType: Column (StringType)
    :rtype: Column: (RasterTileType)

    :example:

.. tabs::
    .. code-tab:: py

     df.select(mos.rst_updatetype('tile', lit('Float32'))).display()
     +----------------------------------------------------------------------------------------------------+
     | rst_updatetype(tile,Float32)                                                                       |
     +----------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","last_error":"", |
     |  "all_parents":"no_path","driver":"GTiff","parentPath":"no_path",                                  |
     |  "last_command":"gdaltranslate -ot Float32 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}         |
     +----------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_updatetype(col("tile"), lit("Float32"))).show
     +----------------------------------------------------------------------------------------------------+
     | rst_updatetype(tile,Float32)                                                                       |
     +----------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","last_error":"", |
     |  "all_parents":"no_path","driver":"GTiff","parentPath":"no_path",                                  |
     |  "last_command":"gdaltranslate -ot Float32 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}         |
     +----------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_updatetype(tile, 'Float32') FROM table
     +----------------------------------------------------------------------------------------------------+
     | rst_updatetype(tile,Float32)                                                                       |
     +----------------------------------------------------------------------------------------------------+
     | {"index_id":null,"raster":"SUkqAAg...= (truncated)","metadata":{"path":"... .tif","last_error":"", |
     |  "all_parents":"no_path","driver":"GTiff","parentPath":"no_path",                                  |
     |  "last_command":"gdaltranslate -ot Float32 -of GTiff -co TILED=YES -co COMPRESS=DEFLATE"}}         |
     +----------------------------------------------------------------------------------------------------+


rst_upperleftx
**********************

.. function:: rst_upperleftx(tile)

    Computes the upper left X coordinate of :code:`tile` based its GeoTransform.

    :param tile: A column containing the raster tile.
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

    Computes the upper left Y coordinate of :code:`tile` based its GeoTransform.

    :param tile: A column containing the raster tile.
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


    :param tile: A column containing the raster tile.
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

    Computes the (j, i) pixel coordinates of :code:`xworld` and :code:`yworld` within :code:`tile`
    using the CRS of :code:`tile`.

    :param tile: A column containing the raster tile.
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

    Computes the j pixel coordinate of :code:`xworld` and :code:`yworld` within :code:`tile`
    using the CRS of :code:`tile`.


    :param tile: A column containing the raster tile.
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

    Computes the i pixel coordinate of :code:`xworld` and :code:`yworld` within :code:`tile`
    using the CRS of :code:`tile`.


    :param tile: A column containing the raster tile.
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

rst_write
*********

.. function:: rst_write(input, dir)

    Writes raster tiles from the input column to a specified directory.

    :param input: A column containing the raster tile.
    :type input: Column
    :param dir: The directory, e.g. fuse, to write the tile's raster as file.
    :type dir: Column(StringType)
    :rtype: Column: RasterTileType

.. note::
  **Notes**
    - Use :code:`RST_Write` to save a 'tile' column to a specified directory (e.g. fuse) location using its
      already populated GDAL driver and tile information.
    - Useful for formalizing the tile 'path' when writing a Lakehouse table. An example might be to turn on checkpointing
      for internal data pipeline phase operations in which multiple interim tiles are populated, but at the end of the phase
      use this function to set the final path to be used in the phase's persisted table. Then, you are free to delete
      the internal tiles that accumulated in the configured checkpointing directory.
..

    :example:

.. tabs::
    .. code-tab:: py

     df.select(rst_write("tile", <write_dir>).alias("tile")).limit(1).display()
     +------------------------------------------------------------------------+
     | tile                                                                   |
     +------------------------------------------------------------------------+
     | {"index_id":null,"tile":"<write_path>","metadata":{                  |
     | "parentPath":"no_path","driver":"GTiff","path":"...","last_error":""}} |
     +------------------------------------------------------------------------+

    .. code-tab:: scala

     df.select(rst_write(col("tile", <write_dir>)).as("tile)).limit(1).show
     +------------------------------------------------------------------------+
     | tile                                                                   |
     +------------------------------------------------------------------------+
     | {"index_id":null,"tile":"<write_path>","metadata":{                  |
     | "parentPath":"no_path","driver":"GTiff","path":"...","last_error":""}} |
     +------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_write(tile, <write_dir>) as tile FROM table LIMIT 1
     +------------------------------------------------------------------------+
     | tile                                                                   |
     +------------------------------------------------------------------------+
     | {"index_id":null,"tile":"<write_path>","metadata":{                  |
     | "parentPath":"no_path","driver":"GTiff","path":"...","last_error":""}} |
     +------------------------------------------------------------------------+

