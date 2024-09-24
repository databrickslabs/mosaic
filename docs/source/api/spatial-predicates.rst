==================
Spatial predicates
==================


st_contains
***********

.. function:: st_contains(geom1, geom2)

    Returns :code:`true` if :code:`geom1` 'spatially' contains :code:`geom2`.

    :param geom1: Geometry
    :type geom1: Column
    :param geom2: Geometry
    :type geom2: Column
    :rtype: Column: BooleanType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'point': 'POINT (25 15)', 'poly': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    df.select(st_contains('poly', 'point')).show()
    +------------------------+
    |st_contains(poly, point)|
    +------------------------+
    |                    true|
    +------------------------+

   .. code-tab:: scala

    val df = List(("POINT (25 15)", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("point", "poly")
    df.select(st_contains($"poly", $"point")).show()
    +------------------------+
    |st_contains(poly, point)|
    +------------------------+
    |                    true|
    +------------------------+

   .. code-tab:: sql

    SELECT st_contains("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", "POINT (25 15)")
    +------------------------+
    |st_contains(poly, point)|
    +------------------------+
    |                    true|
    +------------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame(point = c( "POINT (25 15)"), poly = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    showDF(select(df, st_contains(column("poly"), column("point"))))
    +------------------------+
    |st_contains(poly, point)|
    +------------------------+
    |                    true|
    +------------------------+

.. note:: ST_Within is the inverse of ST_Contains, where ST_Contains(a, b)==ST_Within(b,a).


st_intersects
*************

.. function:: st_intersects(geom1, geom2)

    Returns true if the geometry :code:`geom1` intersects :code:`geom2`.
    Also, see :ref:`st_intersects_agg` function.

    :param geom1: Geometry
    :type geom1: Column
    :param geom2: Geometry
    :type geom2: Column
    :rtype: Column: BooleanType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'p1': 'POLYGON ((0 0, 0 3, 3 3, 3 0))', 'p2': 'POLYGON ((2 2, 2 4, 4 4, 4 2))'}])
    df.select(st_intersects(col('p1'), col('p2'))).show(1, False)
    +---------------------+
    |st_intersects(p1, p2)|
    +---------------------+
    |                 true|
    +---------------------+

   .. code-tab:: scala

    val df = List(("POLYGON ((0 0, 0 3, 3 3, 3 0))", "POLYGON ((2 2, 2 4, 4 4, 4 2))")).toDF("p1", "p2")
    df.select(st_intersects($"p1", $"p2")).show(false)
    +---------------------+
    |st_intersects(p1, p2)|
    +---------------------+
    |                 true|
    +---------------------+

   .. code-tab:: sql

    SELECT st_intersects("POLYGON ((0 0, 0 3, 3 3, 3 0))", "POLYGON ((2 2, 2 4, 4 4, 4 2))")
    +---------------------+
    |st_intersects(p1, p2)|
    +---------------------+
    |                 true|
    +---------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame(p1 = "POLYGON ((0 0, 0 3, 3 3, 3 0))", p2 = "POLYGON ((2 2, 2 4, 4 4, 4 2))"))
    showDF(select(df, st_intersects(column("p1"), column("p2"))), truncate=F)
    +---------------------+
    |st_intersects(p1, p2)|
    +---------------------+
    |                 true|
    +---------------------+

.. note:: Intersection logic will be dependent on the chosen geometry API (ESRI or JTS). ESRI is only available for mosaic < 0.4.x series, in mosaic >= 0.4.0 JTS is the only geometry API.

st_within
*********

.. function:: st_within(geom1, geom2)

    Returns :code:`true` if :code:`geom1` 'spatially' is within :code:`geom2`.

    :param geom1: Geometry
    :type geom1: Column
    :param geom2: Geometry
    :type geom2: Column
    :rtype: Column: BooleanType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'point': 'POINT (25 15)', 'poly': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    df.select(st_within('point', 'poly')).show()
    +----------------------+
    |st_within(point, poly)|
    +----------------------+
    |                  true|
    +----------------------+

   .. code-tab:: scala

    val df = List(("POINT (25 15)", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("point", "poly")
    df.select(st_within($"point", $"poly")).show()
    +----------------------+
    |st_within(point, poly)|
    +----------------------+
    |                  true|
    +----------------------+

   .. code-tab:: sql

    SELECT st_within("POINT (25 15)", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +----------------------+
    |st_within(point, poly)|
    +----------------------+
    |                  true|
    +----------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame(point = c( "POINT (25 15)"), poly = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    showDF(select(df, st_within(column("point"), column("poly"))))
    +----------------------+
    |st_within(point, poly)|
    +----------------------+
    |                  true|
    +----------------------+

.. note:: ST_Within is the inverse of ST_Contains, where ST_Contains(a, b)==ST_Within(b,a).

