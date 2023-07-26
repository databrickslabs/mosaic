==================
Geometry accessors
==================

st_asbinary
***********

.. function:: st_asbinary(col)

    Translate a geometry into its Well-known Binary (WKB) representation.

    :param col: Geometry column
    :type col: Column: StringType, HexType, JSONType or InternalGeometryType
    :rtype: Column: BinaryType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'wkt': 'POINT (30 10)'}])
    df.select(st_asbinary('wkt').alias('wkb')).show()
    +--------------------+
    |                 wkb|
    +--------------------+
    |[01 01 00 00 00 0...|
    +--------------------+

   .. code-tab:: scala

    val df = List(("POINT (30 10)")).toDF("wkt")
    df.select(st_asbinary($"wkt").alias("wkb")).show()
    +--------------------+
    |                 wkb|
    +--------------------+
    |[01 01 00 00 00 0...|
    +--------------------+

   .. code-tab:: sql

    SELECT st_asbinary("POINT (30 10)") AS wkb
    +--------------------+
    |                 wkb|
    +--------------------+
    |[01 01 00 00 00 0...|
    +--------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame('wkt'= "POINT (30 10)"))
    showDF(select(df, alias(st_asbinary(column("wkt")), "wkb")))
    +--------------------+
    |                 wkb|
    +--------------------+
    |[01 01 00 00 00 0...|
    +--------------------+


.. note:: Alias for :ref:`st_aswkb`.

st_asgeojson
************

.. function:: st_asgeojson(col)

    Translate a geometry into its GeoJSON representation.

    :param col: Geometry column
    :type col: Column: BinaryType, StringType, HexType or InternalGeometryType
    :rtype: Column: JSONType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'wkt': 'POINT (30 10)'}])
    df.select(st_asgeojson('wkt').cast('string').alias('json')).show(truncate=False)
    +------------------------------------------------------------------------------------------------+
    |json                                                                                            |
    +------------------------------------------------------------------------------------------------+
    |{{"type":"Point","coordinates":[30,10],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}}|
    +------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = List(("POINT (30 10)")).toDF("wkt")
    df.select(st_asgeojson($"wkt").cast("string").alias("json")).show(false)
    +------------------------------------------------------------------------------------------------+
    |json                                                                                            |
    +------------------------------------------------------------------------------------------------+
    |{{"type":"Point","coordinates":[30,10],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}}|
    +------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT cast(st_asgeojson("POINT (30 10)") AS string) AS json
    +------------------------------------------------------------------------------------------------+
    |json                                                                                            |
    +------------------------------------------------------------------------------------------------+
    |{{"type":"Point","coordinates":[30,10],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}}|
    +------------------------------------------------------------------------------------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame('wkt'= "POINT (30 10)"))
    showDF(select(df, alias(st_asgeojson(column("wkt")), "json")), truncate=F)
    +------------------------------------------------------------------------------------------------+
    |json                                                                                            |
    +------------------------------------------------------------------------------------------------+
    |{{"type":"Point","coordinates":[30,10],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}}|
    +------------------------------------------------------------------------------------------------+


st_astext
*********

.. function:: st_astext(col)

    Translate a geometry into its Well-known Text (WKT) representation.

    :param col: Geometry column
    :type col: Column: BinaryType, HexType, JSONType or InternalGeometryType
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
    df.select(st_astext(st_point('lon', 'lat')).alias('wkt')).show()
    +-------------+
    |          wkt|
    +-------------+
    |POINT (30 10)|
    +-------------+

   .. code-tab:: scala

    val df = List((30.0, 10.0)).toDF("lon", "lat")
    df.select(st_astext(st_point($"lon", $"lat")).alias("wkt")).show()
    +-------------+
    |          wkt|
    +-------------+
    |POINT (30 10)|
    +-------------+

   .. code-tab:: sql

    SELECT st_astext(st_point(30.0D, 10.0D)) AS wkt
    +-------------+
    |          wkt|
    +-------------+
    |POINT (30 10)|
    +-------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
    showDF(select(df, alias(st_astext(st_point(column("lon"), column("lat"))), "wkt")), truncate=F)
    +-------------+
    |          wkt|
    +-------------+
    |POINT (30 10)|
    +-------------+

.. note:: Alias for :ref:`st_aswkt`.


st_aswkb
********

.. function:: st_aswkb(col)

    Translate a geometry into its Well-known Binary (WKB) representation.

    :param col: Geometry column
    :type col: Column: StringType, HexType, JSONType or InternalGeometryType
    :rtype: Column: BinaryType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'wkt': 'POINT (30 10)'}])
    df.select(st_aswkb('wkt').alias('wkb')).show()
    +--------------------+
    |                 wkb|
    +--------------------+
    |[01 01 00 00 00 0...|
    +--------------------+

   .. code-tab:: scala

    val df = List(("POINT (30 10)")).toDF("wkt")
    df.select(st_aswkb($"wkt").alias("wkb")).show()
    +--------------------+
    |                 wkb|
    +--------------------+
    |[01 01 00 00 00 0...|
    +--------------------+

   .. code-tab:: sql

    SELECT st_aswkb("POINT (30 10)") AS wkb
    +--------------------+
    |                 wkb|
    +--------------------+
    |[01 01 00 00 00 0...|
    +--------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame('wkt'= "POINT (30 10)"))
    showDF(select(df, alias(st_aswkb(column("wkt")), "wkb")))
    +--------------------+
    |                 wkb|
    +--------------------+
    |[01 01 00 00 00 0...|
    +--------------------+

.. note:: Alias for :ref:`st_asbinary`.

st_aswkt
********

.. function:: st_aswkt(col)

    Translate a geometry into its Well-known Text (WKT) representation.

    :param col: Geometry column
    :type col: Column: BinaryType, HexType, JSONType or InternalGeometryType
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
    df.select(st_aswkt(st_point('lon', 'lat')).alias('wkt')).show()
    +-------------+
    |          wkt|
    +-------------+
    |POINT (30 10)|
    +-------------+

   .. code-tab:: scala

    val df = List((30.0, 10.0)).toDF("lon", "lat")
    df.select(st_aswkt(st_point($"lon", $"lat")).alias("wkt")).show()
    +-------------+
    |          wkt|
    +-------------+
    |POINT (30 10)|
    +-------------+

   .. code-tab:: sql

    SELECT st_aswkt(st_point(30.0D, 10.0D)) AS wkt
    +-------------+
    |          wkt|
    +-------------+
    |POINT (30 10)|
    +-------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
    showDF(select(df, alias(st_aswkt(st_point(column("lon"), column("lat"))), "wkt")), truncate=F)
    +-------------+
    |          wkt|
    +-------------+
    |POINT (30 10)|
    +-------------+


.. note:: Alias for :ref:`st_astext`.
