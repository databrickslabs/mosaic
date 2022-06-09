=============================
Spatial aggregation functions
=============================

st_intersection_aggregate
*************************

.. function:: st_intersection_aggregate(leftIndex, rightIndex)

    Compute the intersections of `leftIndex` and `rightIndex` and return the resulting composite geometry.

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

    >>> SELECT st_area("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +------------+
    |st_area(wkt)|
    +------------+
    |       550.0|
    +------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_area(column("wkt"))))
    +------------+
    |st_area(wkt)|
    +------------+
    |       550.0|
    +------------+
