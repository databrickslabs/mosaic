=============================
Spatial aggregation functions
=============================


st_asgeojsontile_agg
********************

.. function:: st_asgeojsontile_agg(geom, attributes)

    Generates GeoJSON vector tiles from a group by statement over aggregated geometry column.

    - :code:`geom` column is WKB, WKT, or GeoJSON.
    - :code:`attributes` column is a Spark struct; it requires minimally "id".

    :param geom: A grouped column containing geometries.
    :type geom: Column
    :param attributes: The attributes column to aggregate.
    :type attributes: Column(StructType)
    :rtype: Column

    :example:

.. tabs::
    .. code-tab:: py

     df.groupBy()\
       .agg(mos.st_asgeojsontile_agg("geom", struct("id"))).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | st_asgeojsontile_agg(geom, struct(id))                                                                         |
     +----------------------------------------------------------------------------------------------------------------+
     | {"type": "FeatureCollection", "name": "tiles", "crs": {                                                        |
     |     "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } }, "features": [ ... ] }         |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.groupBy()
       .agg(st_asgeojsontile_agg(col("geom"), struct(col("id"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | st_asgeojsontile_agg(geom, struct(id))                                                                         |
     +----------------------------------------------------------------------------------------------------------------+
     | {"type": "FeatureCollection", "name": "tiles", "crs": {                                                        |
     |     "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } }, "features": [ ... ] }         |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT st_asgeojsontile_agg(geom, struct(id))
     FROM table
     GROUP BY 1
     +----------------------------------------------------------------------------------------------------------------+
     | st_asgeojsontile_agg(geom, struct(id))                                                                         |
     +----------------------------------------------------------------------------------------------------------------+
     | {"type": "FeatureCollection", "name": "tiles", "crs": {                                                        |
     |     "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } }, "features": [ ... ] }         |
     +----------------------------------------------------------------------------------------------------------------+


st_asmvttile_agg
********************

.. function:: st_asmvttile_agg(geom, attributes, zxyID)

    Generates Mapbox Vector Tiles from a group by statement over aggregated geometry column.

    :param geom: A grouped column containing geometries.
    :type geom: Column
    :param attributes: the attributes column to aggregate.
    :type attributes: Column(StructType)
    :param zxyID: the zxyID column to aggregate.
    :type attributes: Column(StringType)
    :rtype: Column

.. note::
  Notes
    - :code:`geom` column must be represented using the Mosaic Internal Geometry,
      e.g. using :code:`ST_GeomFrom[WKB|WKT|GeoJSON]`.

      - The geometry used in this operation must have an SRID set.
        Use e.g. :code:`ST_SetSRID` or :code:`ST_UpdateSRID` to achieve this.
      - MVT tiles require the SRID to be set to EPSG::3857.
    - :code:`attributes` column is a Spark struct; it requires at least an "id" member.
..

    :example:

.. tabs::
    .. code-tab:: py

     df.groupBy()\
       .agg(mos.st_asmvttile_agg("geom_3857", struct("id"), "zxyID")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | st_asmvttile_agg(geom_3857, struct(id), zxyID)                                                                 |
     +----------------------------------------------------------------------------------------------------------------+
     | H4sIAAAAAAAAA5Ny5GItycxJLRZSFmJiYJBgVpLmfKXxwySIgYmZg5mJkZGRgYGRiZGFFYgZ+KWYMlOUuDQavk05e+ntl1fCGg0KFUwA...    |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.groupBy()
       .agg(st_asmvttiletile_agg(col("geom_3857"), struct(col("id")), col("zxyID")).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | st_asmvttile_agg(geom_3857, struct(id), zxyID)                                                                 |
     +----------------------------------------------------------------------------------------------------------------+
     | H4sIAAAAAAAAA5Ny5GItycxJLRZSFmJiYJBgVpLmfKXxwySIgYmZg5mJkZGRgYGRiZGFFYgZ+KWYMlOUuDQavk05e+ntl1fCGg0KFUwA...    |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT st_asmvttile_agg(geom_3857, struct(id), zxyID)
     FROM table
     GROUP BY 1
     +----------------------------------------------------------------------------------------------------------------+
     | st_asmvttile_agg(geom_3857, struct(id), zxyID)                                                                 |
     +----------------------------------------------------------------------------------------------------------------+
     | H4sIAAAAAAAAA5Ny5GItycxJLRZSFmJiYJBgVpLmfKXxwySIgYmZg5mJkZGRgYGRiZGFFYgZ+KWYMlOUuDQavk05e+ntl1fCGg0KFUwA...    |
     +----------------------------------------------------------------------------------------------------------------+


rst_combineavg_agg
******************

.. function:: rst_combineavg_agg(tile)

    Aggregates raster tiles by averaging pixel values.

    :param tile: A grouped column containing raster tiles.
    :type tile: Column (RasterTileType)
    :rtype: Column: RasterTileType

.. note::

  Notes
    - Each :code:`tile` must have the same extent, number of bands, pixel data type, pixel size and coordinate reference system.
    - The output raster will have the same extent, number of bands, pixel data type, pixel size and coordinate reference system as the input tiles.

    Also, see :ref:`rst_combineavg_agg` function.
..
    :example:

.. tabs::
    .. code-tab:: py

     df.groupBy()\
       .agg(mos.rst_combineavg_agg("tile").limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavg_agg(tile)                                                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.groupBy()
       .agg(rst_combineavg_agg(col("tile")).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavg_agg(tile)                                                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_combineavg_agg(tile)
     FROM table
     GROUP BY 1
     +----------------------------------------------------------------------------------------------------------------+
     | rst_combineavg_agg(tile)                                                                                        |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+


rst_derivedband_agg
*******************

.. function:: rst_derivedband_agg(tile, python_func, func_name)

    Combines a group by statement over aggregated raster tiles by using the provided python function.

    :param tile: A grouped column containing raster tile(s).
    :type tile: Column (RasterTileType)
    :param python_func: A function to evaluate in python.
    :type python_func: Column (StringType)
    :param func_name: name of the function to evaluate in python.
    :type func_name: Column (StringType)
    :rtype: Column: RasterTileType

.. note::
  Notes
    - Input raster tiles in :code:`tile` must have the same extent, number of bands, pixel data type, pixel size and coordinate reference system.
    - The output raster will have the same the same extent, number of bands, pixel data type, pixel size and coordinate reference system as the input raster tiles.
..

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
         .agg(mos.rst_derivedband_agg("tile","py_func1","func1_name")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_derivedband_agg(tile,py_func1,func1_name)                                                                   |
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
            .agg(mos.rst_derivedband_agg("tile","py_func1","func1_name")).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_derivedband_agg(tile,py_func1,func1_name)                                                                   |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT
     date, py_func1, func1_name,
     rst_derivedband_agg(tile, py_func1, func1_name)
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
     | rst_derivedband_agg(tile,py_func1,func1_name)                                                                   |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+


rst_merge_agg
*************

.. function:: rst_merge_agg(tile)

    Aggregates raster tiles into a single raster.

    :param tile: A column containing raster tiles.
    :type tile: Column (RasterTileType)
    :rtype: Column: RasterTileType

.. note::
  Notes

  Input tiles in :code:`tile`:
    - are not required to have the same extent.
    - must have the same coordinate reference system.
    - must have the same pixel data type.
    - will be combined using the :code:`gdalwarp` command.
    - require a :code:`noData` value to have been initialised (if this is not the case, the non valid pixels may introduce artifacts in the output raster).
    - will be stacked in the order they are provided.
      - This order is randomized since this is an aggregation function.
      - If the order of rasters is important please first collect rasters and sort them by metadata information and then use rst_merge function.

  The resulting output raster will have:
    - an extent that covers all of the input tiles;
    - the same number of bands as the input tiles;
    - the same pixel type as the input tiles;
    - the same pixel size as the highest resolution input tiles; and
    - the same coordinate reference system as the input tiles.

  See also :ref:`rst_merge` function.
..

    :example:

.. tabs::
    .. code-tab:: py

     df.groupBy("date")\
       .agg(mos.rst_merge_agg("tile")).limit(1).display()
     +----------------------------------------------------------------------------------------------------------------+
     | rst_merge_agg(tile)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: scala

     df.groupBy("date")
       .agg(rst_merge_agg(col("tile"))).limit(1).show
     +----------------------------------------------------------------------------------------------------------------+
     | rst_merge_agg(tile)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+

    .. code-tab:: sql

     SELECT rst_merge_agg(tile)
     FROM table
     GROUP BY date
     +----------------------------------------------------------------------------------------------------------------+
     | rst_merge_agg(tile)                                                                                             |
     +----------------------------------------------------------------------------------------------------------------+
     | {index_id: 593308294097928191, raster: [00 01 10 ... 00], parentPath: "dbfs:/path_to_file", driver: "NetCDF" } |
     +----------------------------------------------------------------------------------------------------------------+


st_intersects_agg
*****************

.. function:: st_intersects_agg(leftIndex, rightIndex)

    Returns :code:`true` if any of the :code:`leftIndex` and :code:`rightIndex` pairs intersect.

    :param leftIndex: Geometry
    :type leftIndex: Column
    :param rightIndex: Geometry
    :type rightIndex: Column
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    left_df = (
        spark.createDataFrame([{'geom': 'POLYGON ((0 0, 0 3, 3 3, 3 0))'}])
            .select(grid_tessellateexplode(col("geom"), lit(1)).alias("left_index"))
    )
    right_df = (
        spark.createDataFrame([{'geom': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
            .select(grid_tessellateexplode(col("geom"), lit(1)).alias("right_index"))
    )
    (
        left_df
            .join(right_df, col("left_index.index_id") == col("right_index.index_id"))
            .groupBy()
            .agg(st_intersects_agg(col("left_index"), col("right_index")))
    ).show(1, False)
    +------------------------------------------------+
    |st_intersects_agg(left_index, right_index)|
    +------------------------------------------------+
    |true                                            |
    +------------------------------------------------+

   .. code-tab:: scala

    val leftDf = List("POLYGON ((0 0, 0 3, 3 3, 3 0))").toDF("geom")
        .select(grid_tessellateexplode($"geom", lit(1)).alias("left_index"))
    val rightDf = List("POLYGON ((2 2, 2 4, 4 4, 4 2))").toDF("geom")
        .select(grid_tessellateexplode($"geom", lit(1)).alias("right_index"))
    leftDf
        .join(rightDf, $"left_index.index_id" === $"right_index.index_id")
        .groupBy()
        .agg(st_intersects_agg($"left_index", $"right_index"))
        .show(false)
    +------------------------------------------------+
    |st_intersects_agg(left_index, right_index)|
    +------------------------------------------------+
    |true                                            |
    +------------------------------------------------+

   .. code-tab:: sql

    WITH l AS (SELECT grid_tessellateexplode("POLYGON ((0 0, 0 3, 3 3, 3 0))", 1) AS left_index),
        r AS (SELECT grid_tessellateexplode("POLYGON ((2 2, 2 4, 4 4, 4 2))", 1) AS right_index)
    SELECT st_intersects_agg(l.left_index, r.right_index)
    FROM l INNER JOIN r on l.left_index.index_id = r.right_index.index_id
    +------------------------------------------------+
    |st_intersects_agg(left_index, right_index)|
    +------------------------------------------------+
    |true                                            |
    +------------------------------------------------+

   .. code-tab:: r R

    df.l <- select(
        createDataFrame(data.frame(geom = "POLYGON ((0 0, 0 3, 3 3, 3 0))")),
        alias(grid_tessellateexplode(column("geom"), lit(1L)), "left_index")
    )
    df.r <- select(
        createDataFrame(data.frame(geom = "POLYGON ((2 2, 2 4, 4 4, 4 2))")),
        alias(grid_tessellateexplode(column("geom"), lit(1L)), "right_index")
    )
    showDF(
        select(
            join(df.l, df.r, df.l$left_index.index_id == df.r$right_index.index_id),
            st_intersects_agg(column("left_index"), column("right_index"))
        ), truncate=F
    )
    +------------------------------------------------+
    |st_intersects_agg(left_index, right_index)|
    +------------------------------------------------+
    |true                                            |
    +------------------------------------------------+


st_intersection_agg
*******************

.. function:: st_intersection_agg(leftIndex, rightIndex)

    Computes the intersections of :code:`leftIndex` and :code:`rightIndex` and returns the union of these intersections.

    :param leftIndex: Geometry
    :type leftIndex: Column
    :param rightIndex: Geometry
    :type rightIndex: Column
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    left_df = (
        spark.createDataFrame([{'geom': 'POLYGON ((0 0, 0 3, 3 3, 3 0))'}])
            .select(grid_tessellateexplode(col("geom"), lit(1)).alias("left_index"))
    )
    right_df = (
        spark.createDataFrame([{'geom': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
            .select(grid_tessellateexplode(col("geom"), lit(1)).alias("right_index"))
    )
    (
        left_df
            .join(right_df, col("left_index.index_id") == col("right_index.index_id"))
            .groupBy()
            .agg(st_astext(st_intersection_agg(col("left_index"), col("right_index"))))
    ).show(1, False)
    +--------------------------------------------------------------+
    |convert_to(st_intersection_agg(left_index, right_index))|
    +--------------------------------------------------------------+
    |POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))                           |
    +--------------------------------------------------------------+

   .. code-tab:: scala

    val leftDf = List("POLYGON ((0 0, 0 3, 3 3, 3 0))").toDF("geom")
        .select(grid_tessellateexplode($"geom", lit(1)).alias("left_index"))
    val rightDf = List("POLYGON ((2 2, 2 4, 4 4, 4 2))").toDF("geom")
        .select(grid_tessellateexplode($"geom", lit(1)).alias("right_index"))
    leftDf
        .join(rightDf, $"left_index.index_id" === $"right_index.index_id")
        .groupBy()
        .agg(st_astext(st_intersection_agg($"left_index", $"right_index")))
        .show(false)
    +--------------------------------------------------------------+
    |convert_to(st_intersection_agg(left_index, right_index))|
    +--------------------------------------------------------------+
    |POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))                           |
    +--------------------------------------------------------------+

   .. code-tab:: sql

    WITH l AS (SELECT grid_tessellateexplode("POLYGON ((0 0, 0 3, 3 3, 3 0))", 1) AS left_index),
        r AS (SELECT grid_tessellateexplode("POLYGON ((2 2, 2 4, 4 4, 4 2))", 1) AS right_index)
    SELECT st_astext(st_intersection_agg(l.left_index, r.right_index))
    FROM l INNER JOIN r on l.left_index.index_id = r.right_index.index_id
    +--------------------------------------------------------------+
    |convert_to(st_intersection_agg(left_index, right_index))|
    +--------------------------------------------------------------+
    |POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))                           |
    +--------------------------------------------------------------+

   .. code-tab:: r R

    df.l <- select(
        createDataFrame(data.frame(geom = "POLYGON ((0 0, 0 3, 3 3, 3 0))")),
        alias(grid_tessellateexplode(column("geom"), lit(1L)), "left_index")
    )
    df.r <- select(
        createDataFrame(data.frame(geom = "POLYGON ((2 2, 2 4, 4 4, 4 2))")),
        alias(grid_tessellateexplode(column("geom"), lit(1L)), "right_index")
    )
    showDF(
        select(
            join(df.l, df.r, df.l$left_index.index_id == df.r$right_index.index_id),
            st_astext(st_intersection_agg(column("left_index"), column("right_index")))
        ), truncate=F
    )
    +--------------------------------------------------------------+
    |convert_to(st_intersection_agg(left_index, right_index))|
    +--------------------------------------------------------------+
    |POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))                           |
    +--------------------------------------------------------------+

st_union_agg
************

.. function:: st_union_agg(geom)

    Computes the union of the input geometries.

    :param geom: Geometry
    :type geom: Column
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py


    df = spark.createDataFrame([{'geom': 'POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))'}, {'geom': 'POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))'}])
    df.select(st_astext(st_union_agg(col('geom')))).show()
    +-------------------------------------------------------------------------+
    | st_union_agg(geom)                                                      |
    +-------------------------------------------------------------------------+
    |POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))|
    +-------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = List("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))", "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))").toDF("geom")
    df.select(st_astext(st_union_agg(col('geom')))).show()
    +-------------------------------------------------------------------------+
    | st_union_agg(geom)                                                      |
    +-------------------------------------------------------------------------+
    |POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))|
    +-------------------------------------------------------------------------+

   .. code-tab:: sql

    WITH geoms ('geom') AS (VALUES ('POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))'), ('POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))'))
    SELECT st_astext(st_union_agg(geoms));
    +-------------------------------------------------------------------------+
    | st_union_agg(geom)                                                      |
    +-------------------------------------------------------------------------+
    |POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))|
    +-------------------------------------------------------------------------+

   .. code-tab:: r R

    df.geom <- select(createDataFrame(data.frame(geom = c('POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))'), ('POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))'))))
    showDF(select(st_astext(st_union_agg(column("geom")))), truncate=F)
    +-------------------------------------------------------------------------+
    | st_union_agg(geom)                                                      |
    +-------------------------------------------------------------------------+
    |POLYGON ((20 15, 20 10, 10 10, 10 20, 15 20, 15 25, 25 25, 25 15, 20 15))|
    +-------------------------------------------------------------------------+

grid_cell_intersection_agg
**************************

.. function:: grid_cell_intersection_agg(chips)

    Computes the chip representing the intersection of the input chips.

    :param chips: Chips
    :type chips: Column
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py


    df = df.withColumn("chip", grid_tessellateexplode(...))
    df.groupBy("chip.index_id").agg(grid_cell_intersection_agg("chip").alias("agg_chip")).limit(1).show()
    +--------------------------------------------------------+
    | agg_chip                                               |
    +--------------------------------------------------------+
    |{is_core: false, index_id: 590418571381702655, wkb: ...}|
    +--------------------------------------------------------+

   .. code-tab:: scala

    val df = other_df.withColumn("chip", grid_tessellateexplode(...))
    df.groupBy("chip.index_id").agg(grid_cell_intersection_agg("chip").alias("agg_chip")).limit(1).show()
    +--------------------------------------------------------+
    | agg_chip                                               |
    +--------------------------------------------------------+
    |{is_core: false, index_id: 590418571381702655, wkb: ...}|
    +--------------------------------------------------------+

   .. code-tab:: sql

    WITH chips AS (SELECT grid_tessellateexplode(wkt) AS "chip" FROM ...)
    SELECT grid_cell_intersection_agg(chips) AS agg_chip FROM chips GROUP BY chips.index_id;
    +--------------------------------------------------------+
    | agg_chip                                               |
    +--------------------------------------------------------+
    |{is_core: false, index_id: 590418571381702655, wkb: ...}|
    +--------------------------------------------------------+

   .. code-tab:: r R

    showDF(select(grid_cell_intersection_agg(column("chip"))), truncate=F)
    +--------------------------------------------------------+
    | agg_chip                                               |
    +--------------------------------------------------------+
    |{is_core: false, index_id: 590418571381702655, wkb: ...}|
    +--------------------------------------------------------+

grid_cell_union_agg
*******************

.. function:: grid_cell_union_agg(chips)

    Computes the chip representing the union of the input chips.

    :param chips: Chips
    :type chips: Column
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py


    df = df.withColumn("chip", grid_tessellateexplode(...))
    df.groupBy("chip.index_id").agg(grid_cell_union_agg("chip").alias("agg_chip")).limit(1).show()
    +--------------------------------------------------------+
    | agg_chip                                               |
    +--------------------------------------------------------+
    |{is_core: false, index_id: 590418571381702655, wkb: ...}|
    +--------------------------------------------------------+

   .. code-tab:: scala

    val df = other_df.withColumn("chip", grid_tessellateexplode(...))
    df.groupBy("chip.index_id").agg(grid_cell_union_agg("chip").alias("agg_chip")).limit(1).show()
    +--------------------------------------------------------+
    | agg_chip                                               |
    +--------------------------------------------------------+
    |{is_core: false, index_id: 590418571381702655, wkb: ...}|
    +--------------------------------------------------------+

   .. code-tab:: sql

    WITH chips AS (SELECT grid_tessellateexplode(wkt) AS "chip" FROM ...)
    SELECT grid_cell_union_agg(chips) AS agg_chip FROM chips GROUP BY chips.index_id;
    +--------------------------------------------------------+
    | agg_chip                                               |
    +--------------------------------------------------------+
    |{is_core: false, index_id: 590418571381702655, wkb: ...}|
    +--------------------------------------------------------+

   .. code-tab:: r R

    showDF(select(grid_cell_union_agg(column("chip"))), truncate=F)
    +--------------------------------------------------------+
    | agg_chip                                               |
    +--------------------------------------------------------+
    |{is_core: false, index_id: 590418571381702655, wkb: ...}|
    +--------------------------------------------------------+