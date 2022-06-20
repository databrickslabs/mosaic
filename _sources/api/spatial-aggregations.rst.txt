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

    >>> left_df = (
          spark.createDataFrame([{'geom': 'POLYGON ((0 0, 0 3, 3 3, 3 0))'}])
          .select(mosaic_explode(col("geom"), lit(1)).alias("left_index"))
        )
    >>> right_df = (
          spark.createDataFrame([{'geom': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
          .select(mosaic_explode(col("geom"), lit(1)).alias("right_index"))
        )
    >>> (
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

    >>> val leftDf = List("POLYGON ((0 0, 0 3, 3 3, 3 0))").toDF("geom")
            .select(mosaic_explode($"geom", lit(1)).alias("left_index"))
    >>> val rightDf = List("POLYGON ((2 2, 2 4, 4 4, 4 2))").toDF("geom")
            .select(mosaic_explode($"geom", lit(1)).alias("right_index"))
    >>> leftDf
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

    >>> WITH l AS (SELECT mosaic_explode("POLYGON ((0 0, 0 3, 3 3, 3 0))", 1) AS left_index),
        r AS (SELECT mosaic_explode("POLYGON ((2 2, 2 4, 4 4, 4 2))", 1) AS right_index)
        SELECT st_intersects_aggregate(l.left_index, r.right_index)
        FROM l INNER JOIN r on l.left_index.index_id = r.right_index.index_id
    +------------------------------------------------+
    |st_intersects_aggregate(left_index, right_index)|
    +------------------------------------------------+
    |true                                            |
    +------------------------------------------------+

   .. code-tab:: r R

    >>> df.l <- select(
          createDataFrame(data.frame(geom = "POLYGON ((0 0, 0 3, 3 3, 3 0))")),
          alias(mosaic_explode(column("geom"), lit(1L)), "left_index")
        )
    >>> df.r <- select(
          createDataFrame(data.frame(geom = "POLYGON ((2 2, 2 4, 4 4, 4 2))")),
          alias(mosaic_explode(column("geom"), lit(1L)), "right_index")
        )
    >>> showDF(
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

    >>> left_df = (
          spark.createDataFrame([{'geom': 'POLYGON ((0 0, 0 3, 3 3, 3 0))'}])
          .select(mosaic_explode(col("geom"), lit(1)).alias("left_index"))
        )
    >>> right_df = (
          spark.createDataFrame([{'geom': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
          .select(mosaic_explode(col("geom"), lit(1)).alias("right_index"))
        )
    >>> (
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

    >>> val leftDf = List("POLYGON ((0 0, 0 3, 3 3, 3 0))").toDF("geom")
            .select(mosaic_explode($"geom", lit(1)).alias("left_index"))
    >>> val rightDf = List("POLYGON ((2 2, 2 4, 4 4, 4 2))").toDF("geom")
            .select(mosaic_explode($"geom", lit(1)).alias("right_index"))
    >>> leftDf
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

    >>> WITH l AS (SELECT mosaic_explode("POLYGON ((0 0, 0 3, 3 3, 3 0))", 1) AS left_index),
        r AS (SELECT mosaic_explode("POLYGON ((2 2, 2 4, 4 4, 4 2))", 1) AS right_index)
        SELECT st_astext(st_intersection_aggregate(l.left_index, r.right_index))
        FROM l INNER JOIN r on l.left_index.index_id = r.right_index.index_id
    +--------------------------------------------------------------+
    |convert_to(st_intersection_aggregate(left_index, right_index))|
    +--------------------------------------------------------------+
    |POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))                           |
    +--------------------------------------------------------------+

   .. code-tab:: r R

    >>> df.l <- select(
          createDataFrame(data.frame(geom = "POLYGON ((0 0, 0 3, 3 3, 3 0))")),
          alias(mosaic_explode(column("geom"), lit(1L)), "left_index")
        )
    >>> df.r <- select(
          createDataFrame(data.frame(geom = "POLYGON ((2 2, 2 4, 4 4, 4 2))")),
          alias(mosaic_explode(column("geom"), lit(1L)), "right_index")
        )
    >>> showDF(
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
