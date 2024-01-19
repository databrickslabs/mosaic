=============================
Spatial aggregation functions
=============================


st_intersects_aggregate
***********************

.. function:: st_intersects_aggregate(leftIndex, rightIndex)

    Returns `true` if any of the `leftIndex` and `rightIndex` pairs intersect.

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
            .agg(st_intersects_aggregate(col("left_index"), col("right_index")))
    ).show(1, False)
    +------------------------------------------------+
    |st_intersects_aggregate(left_index, right_index)|
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
        .agg(st_intersects_aggregate($"left_index", $"right_index"))
        .show(false)
    +------------------------------------------------+
    |st_intersects_aggregate(left_index, right_index)|
    +------------------------------------------------+
    |true                                            |
    +------------------------------------------------+

   .. code-tab:: sql

    WITH l AS (SELECT grid_tessellateexplode("POLYGON ((0 0, 0 3, 3 3, 3 0))", 1) AS left_index),
        r AS (SELECT grid_tessellateexplode("POLYGON ((2 2, 2 4, 4 4, 4 2))", 1) AS right_index)
    SELECT st_intersects_aggregate(l.left_index, r.right_index)
    FROM l INNER JOIN r on l.left_index.index_id = r.right_index.index_id
    +------------------------------------------------+
    |st_intersects_aggregate(left_index, right_index)|
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
            st_intersects_aggregate(column("left_index"), column("right_index"))
        ), truncate=F
    )
    +------------------------------------------------+
    |st_intersects_aggregate(left_index, right_index)|
    +------------------------------------------------+
    |true                                            |
    +------------------------------------------------+


st_intersection_aggregate
*************************

.. function:: st_intersection_aggregate(leftIndex, rightIndex)

    Computes the intersections of `leftIndex` and `rightIndex` and returns the union of these intersections.

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
            .agg(st_astext(st_intersection_aggregate(col("left_index"), col("right_index"))))
    ).show(1, False)
    +--------------------------------------------------------------+
    |convert_to(st_intersection_aggregate(left_index, right_index))|
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
        .agg(st_astext(st_intersection_aggregate($"left_index", $"right_index")))
        .show(false)
    +--------------------------------------------------------------+
    |convert_to(st_intersection_aggregate(left_index, right_index))|
    +--------------------------------------------------------------+
    |POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))                           |
    +--------------------------------------------------------------+

   .. code-tab:: sql

    WITH l AS (SELECT grid_tessellateexplode("POLYGON ((0 0, 0 3, 3 3, 3 0))", 1) AS left_index),
        r AS (SELECT grid_tessellateexplode("POLYGON ((2 2, 2 4, 4 4, 4 2))", 1) AS right_index)
    SELECT st_astext(st_intersection_aggregate(l.left_index, r.right_index))
    FROM l INNER JOIN r on l.left_index.index_id = r.right_index.index_id
    +--------------------------------------------------------------+
    |convert_to(st_intersection_aggregate(left_index, right_index))|
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
            st_astext(st_intersection_aggregate(column("left_index"), column("right_index")))
        ), truncate=F
    )
    +--------------------------------------------------------------+
    |convert_to(st_intersection_aggregate(left_index, right_index))|
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
************

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
************

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