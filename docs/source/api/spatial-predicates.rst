==================
Spatial predicates
==================


st_distance
***********

.. function:: st_distance(geom1, geom2)

    Compute the distance between `geom1` and `geom2`.

    :param geom1: Geometry
    :type geom1: Column
    :param geom2: Geometry
    :type geom2: Column
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'point': 'POINT (5 5)', 'poly': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_distance('poly', 'point')).show()
    +------------------------+
    |st_distance(poly, point)|
    +------------------------+
    |      15.652475842498529|
    +------------------------+

   .. code-tab:: scala

    >>> val df = List(("POINT (5 5)", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("point", "poly")
    >>> df.select(st_distance($"poly", $"point")).show()
    +------------------------+
    |st_distance(poly, point)|
    +------------------------+
    |      15.652475842498529|
    +------------------------+

   .. code-tab:: sql

    >>> SELECT st_distance("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", "POINT (5 5)")
    +------------------------+
    |st_distance(poly, point)|
    +------------------------+
    |      15.652475842498529|
    +------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(point = c( "POINT (5 5)"), poly = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_distance(column("poly"), column("point"))))
    +------------------------+
    |st_distance(poly, point)|
    +------------------------+
    |      15.652475842498529|
    +------------------------+

.. note:: Results of this function are always expressed in the original units of the input geometries.


st_contains
***********

.. function:: st_contains(geom1, geom2)

    Returns `true` if `geom1` 'spatially' contains `geom2`.

    :param geom1: Geometry
    :type geom1: Column
    :param geom2: Geometry
    :type geom2: Column
    :rtype: Column: BooleanType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'point': 'POINT (25 15)', 'poly': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_contains('poly', 'point')).show()
    +------------------------+
    |st_contains(poly, point)|
    +------------------------+
    |                    true|
    +------------------------+

   .. code-tab:: scala

    >>> val df = List(("POINT (25 15)", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("point", "poly")
    >>> df.select(st_contains($"poly", $"point")).show()
    +------------------------+
    |st_contains(poly, point)|
    +------------------------+
    |                    true|
    +------------------------+

   .. code-tab:: sql

    >>> SELECT st_contains("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", "POINT (25 15)")
    +------------------------+
    |st_contains(poly, point)|
    +------------------------+
    |                    true|
    +------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(point = c( "POINT (25 15)"), poly = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_contains(column("poly"), column("point"))))
    +------------------------+
    |st_distance(poly, point)|
    +------------------------+
    |      15.652475842498529|
    +------------------------+