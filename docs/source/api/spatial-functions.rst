=================
Spatial functions
=================

st_area
*******

.. function:: st_area(col)

    Compute the area of a geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_area('wkt')).show()
    +------------+
    |st_area(wkt)|
    +------------+
    |       550.0|
    +------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_area($"wkt")).show()
    +------------+
    |st_area(wkt)|
    +------------+
    |       550.0|
    +------------+

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

.. note:: Results of this function are always expressed in the original units of the input geometry.




st_buffer
*********

.. function:: st_buffer(col)

    Buffer the input geometry by radius `radius` and return a new, buffered geometry.

    :param col: Geometry
    :type col: Column
    :param radius: Double
    :type radius: Column (DoubleType)
    :rtype: Column: Geometry

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_buffer('wkt', lit(2.))).show()
    +--------------------+
    | st_buffer(wkt, 2.0)|
    +--------------------+
    |POLYGON ((29.1055...|
    +--------------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_buffer($"wkt", 2d)).show()
    +--------------------+
    | st_buffer(wkt, 2.0)|
    +--------------------+
    |POLYGON ((29.1055...|
    +--------------------+

   .. code-tab:: sql

    >>> SELECT st_buffer("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", 2d)
    +--------------------+
    | st_buffer(wkt, 2.0)|
    +--------------------+
    |POLYGON ((29.1055...|
    +--------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_buffer(column("wkt"), lit(2))))
    +--------------------+
    | st_buffer(wkt, 2.0)|
    +--------------------+
    |POLYGON ((29.1055...|
    +--------------------+

st_perimeter
************

.. function:: st_perimeter(col)

    Compute the perimeter length of a geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_perimeter('wkt')).show()
    +-----------------+
    |   st_length(wkt)|
    +-----------------+
    |96.34413615167959|
    +-----------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_perimeter($"wkt")).show()
    +-----------------+
    |   st_length(wkt)|
    +-----------------+
    |96.34413615167959|
    +-----------------+

   .. code-tab:: sql

    >>> SELECT st_perimeter("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +-----------------+
    |   st_length(wkt)|
    +-----------------+
    |96.34413615167959|
    +-----------------+
   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_perimeter(column("wkt"))))
    +-----------------+
    |   st_length(wkt)|
    +-----------------+
    |96.34413615167959|
    +-----------------+


.. note:: Results of this function are always expressed in the original units of the input geometry.

.. note:: Alias for :ref:`st_length`.

st_length
************

.. function:: st_length(col)

    Compute the length of a geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_length('wkt')).show()
    +-----------------+
    |   st_length(wkt)|
    +-----------------+
    |96.34413615167959|
    +-----------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_length($"wkt")).show()
    +-----------------+
    |   st_length(wkt)|
    +-----------------+
    |96.34413615167959|
    +-----------------+

   .. code-tab:: sql

    >>> SELECT st_length("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +-----------------+
    |   st_length(wkt)|
    +-----------------+
    |96.34413615167959|
    +-----------------+
   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_length(column("wkt"))))
    +-----------------+
    |   st_length(wkt)|
    +-----------------+
    |96.34413615167959|
    +-----------------+


.. note:: Results of this function are always expressed in the original units of the input geometry.

.. note:: Alias for :ref:`st_perimeter`.


st_convexhull
*************

.. function:: st_convexhull(col)

    Compute the convex hull of a geometry or multi-geometry object.

    :param col: Geometry
    :type col: Column
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
    >>> df.select(st_convexhull('wkt')).show(1, False)
    +---------------------------------------------+
    |st_convexhull(wkt)                           |
    +---------------------------------------------+
    |POLYGON ((10 40, 20 20, 30 10, 40 30, 10 40))|
    +---------------------------------------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")).toDF("wkt")
    >>> df.select(st_convexhull($"wkt")).show(false)
    +---------------------------------------------+
    |st_convexhull(wkt)                           |
    +---------------------------------------------+
    |POLYGON ((10 40, 20 20, 30 10, 40 30, 10 40))|
    +---------------------------------------------+

   .. code-tab:: sql

    >>> SELECT st_convexhull("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")
    +---------------------------------------------+
    |st_convexhull(wkt)                           |
    +---------------------------------------------+
    |POLYGON ((10 40, 20 20, 30 10, 40 30, 10 40))|
    +---------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
    >>> showDF(select(df, st_convexhull(column("wkt"))))
    +---------------------------------------------+
    |st_convexhull(wkt)                           |
    +---------------------------------------------+
    |POLYGON ((10 40, 20 20, 30 10, 40 30, 10 40))|
    +---------------------------------------------+


st_dump
*******

.. function:: st_dump(col)

    Explodes a multi-geometry into one row per constituent geometry.

    :param col: The input multi-geometry
    :type col: Column
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
    >>> df.select(st_dump('wkt')).show(5, False)
    +-------------+
    |element      |
    +-------------+
    |POINT (10 40)|
    |POINT (40 30)|
    |POINT (20 20)|
    |POINT (30 10)|
    +-------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")).toDF("wkt")
    >>> df.select(st_dump($"wkt")).show(false)
    +-------------+
    |element      |
    +-------------+
    |POINT (10 40)|
    |POINT (40 30)|
    |POINT (20 20)|
    |POINT (30 10)|
    +-------------+

   .. code-tab:: sql

    >>> SELECT st_dump("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")
    +-------------+
    |element      |
    +-------------+
    |POINT (10 40)|
    |POINT (40 30)|
    |POINT (20 20)|
    |POINT (30 10)|
    +-------------+
   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
    >>> showDF(select(df, st_dump(column("wkt"))))
    +-------------+
    |element      |
    +-------------+
    |POINT (10 40)|
    |POINT (40 30)|
    |POINT (20 20)|
    |POINT (30 10)|
    +-------------+


st_srid
*******

.. function:: st_srid(geom)

    Looks up the Coordinate Reference System well-known identifier (SRID) for `geom`.

    :param geom: Geometry
    :type geom: Column
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    >>> json_geom = '{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}'
    >>> df = spark.createDataFrame([{'json': json_geom}])
    >>> df.select(st_srid(as_json('json'))).show(1)
    +----------------------+
    |st_srid(as_json(json))|
    +----------------------+
    |                  4326|
    +----------------------+

   .. code-tab:: scala

    >>> val df =
    >>>    List("""{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}""")
    >>>    .toDF("json")
    >>> df.select(st_srid(as_json($"json"))).show(1)
    +----------------------+
    |st_srid(as_json(json))|
    +----------------------+
    |                  4326|
    +----------------------+

   .. code-tab:: sql

    >>> select st_srid(as_json('{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}'))
    +------------+
    |st_srid(...)|
    +------------+
    |4326        |
    +------------+

   .. code-tab:: r R

    >>> json_geom <- '{"type":"MultiPoint","coordinates":[[10,40],[40,30],[20,20],[30,10]],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}'
    >>> df <- createDataFrame(data.frame(json=json_geom))
    >>> showDF(select(df, st_srid(as_json(column('json')))))
    +------------+
    |st_srid(...)|
    +------------+
    |4326        |
    +------------+

.. note::
    ST_SRID can only operate on geometries encoded in GeoJSON or the Mosaic internal format.


st_setsrid
**********

.. function:: st_setsrid(geom, srid)

    Sets the Coordinate Reference System well-known identifier (SRID) for `geom`.

    :param geom: Geometry
    :type geom: Column
    :param srid: The spatial reference identifier of `geom`, expressed as an integer, e.g. `4326` for EPSG:4326 / WGS84
    :type srid: Column (IntegerType)
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
    >>> df.select(st_setsrid(st_geomfromwkt('wkt'), lit(4326))).show(1)
    +---------------------------------+
    |st_setsrid(convert_to(wkt), 4326)|
    +---------------------------------+
    |             {2, 4326, [[[10.0...|
    +---------------------------------+

   .. code-tab:: scala

    >>> val df = List("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").toDF("wkt")
    >>> df.select(st_setsrid(st_geomfromwkt($"wkt"), lit(4326))).show
    +---------------------------------+
    |st_setsrid(convert_to(wkt), 4326)|
    +---------------------------------+
    |             {2, 4326, [[[10.0...|
    +---------------------------------+

   .. code-tab:: sql

    >>> select st_setsrid(st_geomfromwkt("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"), 4326)
    +---------------------------------+
    |st_setsrid(convert_to(wkt), 4326)|
    +---------------------------------+
    |             {2, 4326, [[[10.0...|
    +---------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
    >>> showDF(select(df, st_setsrid(st_geomfromwkt(column("wkt")), lit(4326L))))
    +---------------------------------+
    |st_setsrid(convert_to(wkt), 4326)|
    +---------------------------------+
    |             {2, 4326, [[[10.0...|
    +---------------------------------+

.. note::
    ST_SetSRID does not transform the coordinates of `geom`,
    rather it tells Mosaic the SRID in which the current coordinates are expressed.
    ST_SetSRID can only operate on geometries encoded in GeoJSON or the Mosaic internal format.


st_transform
************

.. function:: st_transform(geom, srid)

    Transforms the horizontal (XY) coordinates of `geom` from the current reference system to that described by `srid`.

    :param geom: Geometry
    :type geom: Column
    :param srid: Target spatial reference system for `geom`, expressed as an integer, e.g. `3857` for EPSG:3857 / Pseudo-Mercator
    :type srid: Column (IntegerType)
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = (
    >>>   spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
    >>>   .withColumn('geom', st_setsrid(st_geomfromwkt('wkt'), lit(4326)))
    >>> )
    >>> df.select(st_astext(st_transform('geom', lit(3857)))).show(1, False)
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |convert_to(st_transform(geom, 3857))                                                                                                                                      |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |MULTIPOINT ((1113194.9079327357 4865942.279503176), (4452779.631730943 3503549.843504374), (2226389.8158654715 2273030.926987689), (3339584.723798207 1118889.9748579597))|
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: scala

    >>> val df = List("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").toDF("wkt")
    >>>   .withColumn("geom", st_setsrid(st_geomfromwkt($"wkt"), lit(4326)))
    >>> df.select(st_astext(st_transform($"geom", lit(3857)))).show(1, false)
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |convert_to(st_transform(geom, 3857))                                                                                                                                      |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |MULTIPOINT ((1113194.9079327357 4865942.279503176), (4452779.631730943 3503549.843504374), (2226389.8158654715 2273030.926987689), (3339584.723798207 1118889.9748579597))|
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: sql

    >>> select st_astext(st_transform(st_setsrid(st_geomfromwkt("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"), 4326), 3857))
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |convert_to(st_transform(geom, 3857))                                                                                                                                      |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |MULTIPOINT ((1113194.9079327357 4865942.279503176), (4452779.631730943 3503549.843504374), (2226389.8158654715 2273030.926987689), (3339584.723798207 1118889.9748579597))|
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
    >>> df <- withColumn(df, 'geom', st_setsrid(st_geomfromwkt(column('wkt')), lit(4326L)))
    >>>
    >>> showDF(select(df, st_astext(st_transform(column('geom'), lit(3857L)))), truncate=F)
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |convert_to(st_transform(geom, 3857))                                                                                                                                      |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |MULTIPOINT ((1113194.9079327357 4865942.279503176), (4452779.631730943 3503549.843504374), (2226389.8158654715 2273030.926987689), (3339584.723798207 1118889.9748579597))|
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

.. note::
    If `geom` does not have an associated SRID, use ST_SetSRID to set this before calling ST_Transform.



st_translate
************

.. function:: st_translate(geom, xd, yd)

    Translates `geom` to a new location using the distance parameters `xd` and `yd`.

    :param geom: Geometry
    :type geom: Column
    :param xd: Offset in the x-direction
    :type xd: Column (DoubleType)
    :param yd: Offset in the y-direction
    :type yd: Column (DoubleType)
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'}])
    >>> df.select(st_translate('wkt', lit(10), lit(-5))).show(1, False)
    +----------------------------------------------+
    |st_translate(wkt, 10, -5)                     |
    +----------------------------------------------+
    |MULTIPOINT ((20 35), (50 25), (30 15), (40 5))|
    +----------------------------------------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))")).toDF("wkt")
    >>> df.select(st_translate($"wkt", lit(10d), lit(-5d))).show(false)
    +----------------------------------------------+
    |st_translate(wkt, 10, -5)                     |
    +----------------------------------------------+
    |MULTIPOINT ((20 35), (50 25), (30 15), (40 5))|
    +----------------------------------------------+

   .. code-tab:: sql

    >>> SELECT st_translate("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", 10d, -5d)
    +----------------------------------------------+
    |st_translate(wkt, 10, -5)                     |
    +----------------------------------------------+
    |MULTIPOINT ((20 35), (50 25), (30 15), (40 5))|
    +----------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"))
    >>> showDF(select(df, st_translate(column('wkt'), lit(10), lit(-5))))
    +----------------------------------------------+
    |st_translate(wkt, 10, -5)                     |
    +----------------------------------------------+
    |MULTIPOINT ((20 35), (50 25), (30 15), (40 5))|
    +----------------------------------------------+


st_scale
********

.. function:: st_scale(geom, xd, yd)

    Scales `geom` using the scaling factors `xd` and `yd`.

    :param geom: Geometry
    :type geom: Column
    :param xd: Scale factor in the x-direction
    :type xd: Column (DoubleType)
    :param yd: Scale factor in the y-direction
    :type yd: Column (DoubleType)
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_scale('wkt', lit(0.5), lit(2))).show(1, False)
    +--------------------------------------------+
    |st_scale(wkt, 0.5, 2)                       |
    +--------------------------------------------+
    |POLYGON ((15 20, 20 80, 10 80, 5 40, 15 20))|
    +--------------------------------------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_scale($"wkt", lit(0.5), lit(2.0))).show(false)
    +--------------------------------------------+
    |st_scale(wkt, 0.5, 2)                       |
    +--------------------------------------------+
    |POLYGON ((15 20, 20 80, 10 80, 5 40, 15 20))|
    +--------------------------------------------+

   .. code-tab:: sql

    >>> SELECT st_scale("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", 0.5d, 2.0d)
    +--------------------------------------------+
    |st_scale(wkt, 0.5, 2)                       |
    +--------------------------------------------+
    |POLYGON ((15 20, 20 80, 10 80, 5 40, 15 20))|
    +--------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_scale(column('wkt'), lit(0.5), lit(2))), truncate=F)
    +--------------------------------------------+
    |st_scale(wkt, 0.5, 2)                       |
    +--------------------------------------------+
    |POLYGON ((15 20, 20 80, 10 80, 5 40, 15 20))|
    +--------------------------------------------+


st_rotate
*********

.. function:: st_rotate(geom, td)

    Rotates `geom` using the rotational factor `td`.

    :param geom: Geometry
    :type geom: Column
    :param td: Rotation (in radians)
    :type td: Column (DoubleType)
    :rtype: Column

    :example:

.. tabs::
   .. code-tab:: py

    >>> from math import pi
    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_rotate('wkt', lit(pi))).show(1, False)
    +-------------------------------------------------------+
    |st_rotate(wkt, 3.141592653589793)                      |
    +-------------------------------------------------------+
    |POLYGON ((-30 -10, -40 -40, -20 -40, -10 -20, -30 -10))|
    +-------------------------------------------------------+

   .. code-tab:: scala

    >>> import math.Pi
    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_rotate($"wkt", lit(Pi))).show(false)
    +-------------------------------------------------------+
    |st_rotate(wkt, 3.141592653589793)                      |
    +-------------------------------------------------------+
    |POLYGON ((-30 -10, -40 -40, -20 -40, -10 -20, -30 -10))|
    +-------------------------------------------------------+

   .. code-tab:: sql

    >>> SELECT st_rotate("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", pi())
    +-------------------------------------------------------+
    |st_rotate(wkt, 3.141592653589793)                      |
    +-------------------------------------------------------+
    |POLYGON ((-30 -10, -40 -40, -20 -40, -10 -20, -30 -10))|
    +-------------------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_rotate(column("wkt"), lit(pi))), truncate=F)
    +-------------------------------------------------------+
    |st_rotate(wkt, 3.141592653589793)                      |
    +-------------------------------------------------------+
    |POLYGON ((-30 -10, -40 -40, -20 -40, -10 -20, -30 -10))|
    +-------------------------------------------------------+


st_centroid2D
*************

.. function:: st_centroid2D(col)

    Returns the x and y coordinates representing the centroid of the input geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: StructType[x: DoubleType, y: DoubleType]

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_centroid2D('wkt')).show()
    +---------------------------------------+
    |st_centroid(wkt)                       |
    +---------------------------------------+
    |{25.454545454545453, 26.96969696969697}|
    +---------------------------------------+

   .. code-tab:: scala

    >>> val df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_centroid2D('wkt')).show()
    +---------------------------------------+
    |st_centroid(wkt)                       |
    +---------------------------------------+
    |{25.454545454545453, 26.96969696969697}|
    +---------------------------------------+

   .. code-tab:: sql

    >>> SELECT st_centroid2D("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +---------------------------------------+
    |st_centroid(wkt)                       |
    +---------------------------------------+
    |{25.454545454545453, 26.96969696969697}|
    +---------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_centroid2D(column("wkt"))), truncate=F)
    +---------------------------------------+
    |st_centroid(wkt)                       |
    +---------------------------------------+
    |{25.454545454545453, 26.96969696969697}|
    +---------------------------------------+

st_centroid3D
*************

.. function:: st_centroid3D(col)

    Returns the x, y and z coordinates representing the centroid of the input geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: StructType[x: DoubleType, y: DoubleType, z: DoubleType]


st_isvalid
**********

.. function:: st_isvalid(col)

    Returns `true` if the geometry is valid.

    :param col: Geometry
    :type col: Column
    :rtype: Column: BooleanType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_isvalid('wkt')).show()
    +---------------+
    |st_isvalid(wkt)|
    +---------------+
    |           true|
    +---------------+

    >>> df = spark.createDataFrame([{
        'wkt': 'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))'
        }])
    >>> df.select(st_isvalid('wkt')).show()
    +---------------+
    |st_isvalid(wkt)|
    +---------------+
    |          false|
    +---------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_isvalid($"wkt")).show()
    +---------------+
    |st_isvalid(wkt)|
    +---------------+
    |           true|
    +---------------+

    >>> val df = List(("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))")).toDF("wkt")
    >>> df.select(st_isvalid($"wkt")).show()
    +---------------+
    |st_isvalid(wkt)|
    +---------------+
    |          false|
    +---------------+

   .. code-tab:: sql

    >>> SELECT st_isvalid("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +---------------+
    |st_isvalid(wkt)|
    +---------------+
    |           true|
    +---------------+

    >>> SELECT st_isvalid("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))")
    +---------------+
    |st_isvalid(wkt)|
    +---------------+
    |          false|
    +---------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_isvalid(column("wkt"))), truncate=F)
    +---------------+
    |st_isvalid(wkt)|
    +---------------+
    |           true|
    +---------------+

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))"))
    >>> showDF(select(df, st_isvalid(column("wkt"))), truncate=F)
    +---------------+
    |st_isvalid(wkt)|
    +---------------+
    |          false|
    +---------------+

.. note:: Validity assertions will be dependent on the chosen geometry API.
    The assertions used in the ESRI geometry API (the default) follow the definitions in the
    "Simple feature access - Part 1" document (OGC 06-103r4) for each geometry type.

st_geometrytype
***************

.. function:: st_geometrytype(col)

    Returns the type of the input geometry ("POINT", "LINESTRING", "POLYGON" etc.).

    :param col: Geometry
    :type col: Column
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_geometrytype('wkt')).show()
    +--------------------+
    |st_geometrytype(wkt)|
    +--------------------+
    |             POLYGON|
    +--------------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_geometrytype($"wkt")).show()
    +--------------------+
    |st_geometrytype(wkt)|
    +--------------------+
    |             POLYGON|
    +--------------------+

   .. code-tab:: sql

    >>> SELECT st_geometrytype("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))")
    +--------------------+
    |st_geometrytype(wkt)|
    +--------------------+
    |             POLYGON|
    +--------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_geometrytype(column("wkt"))), truncate=F)
    +--------------------+
    |st_geometrytype(wkt)|
    +--------------------+
    |             POLYGON|
    +--------------------+


st_xmin
*******

.. function:: st_xmin(col)

    Returns the smallest x coordinate in the input geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_xmin('wkt')).show()
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             10.0|
    +-----------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_xmin($"wkt")).show()
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             10.0|
    +-----------------+

   .. code-tab:: sql

    >>> SELECT st_xmin("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             10.0|
    +-----------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_xmin(column("wkt"))), truncate=F)
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             10.0|
    +-----------------+

st_xmax
*******

.. function:: st_xmax(col)

    Returns the largest x coordinate in the input geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_xmax('wkt')).show()
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             40.0|
    +-----------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_xmax($"wkt")).show()
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             40.0|
    +-----------------+

   .. code-tab:: sql

    >>> SELECT st_xmax("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             40.0|
    +-----------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_xmax(column("wkt"))), truncate=F)
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             40.0|
    +-----------------+

st_ymin
*******

.. function:: st_ymin(col)

    Returns the smallest y coordinate in the input geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_ymin('wkt')).show()
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             10.0|
    +-----------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_ymin($"wkt")).show()
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             10.0|
    +-----------------+

   .. code-tab:: sql

    >>> SELECT st_ymin("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             10.0|
    +-----------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_ymin(column("wkt"))), truncate=F)
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             10.0|
    +-----------------+

st_ymax
*******

.. function:: st_ymax(col)

    Returns the largest y coordinate in the input geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
    >>> df.select(st_ymax('wkt')).show()
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             40.0|
    +-----------------+

   .. code-tab:: scala

    >>> val df = List(("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")).toDF("wkt")
    >>> df.select(st_ymax($"wkt")).show()
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             40.0|
    +-----------------+

   .. code-tab:: sql

    >>> SELECT st_ymax("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             40.0|
    +-----------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))
    >>> showDF(select(df, st_ymax(column("wkt"))), truncate=F)
    +-----------------+
    |st_minmaxxyz(wkt)|
    +-----------------+
    |             40.0|
    +-----------------+


st_zmin
*******

.. function:: st_zmin(col)

    Returns the smallest z coordinate in the input geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType

st_zmax
*******

.. function:: st_zmax(col)

    Returns the largest z coordinate in the input geometry.

    :param col: Geometry
    :type col: Column
    :rtype: Column: DoubleType


flatten_polygons
****************

.. function:: flatten_polygons(col)

    Explodes a MultiPolygon geometry into one row per constituent Polygon.

    :param col: MultiPolygon Geometry
    :type col: Column
    :rtype: Column: StringType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([
        {'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'}
        ])
    >>> df.select(flatten_polygons('wkt')).show(2, False)
    +------------------------------------------+
    |element                                   |
    +------------------------------------------+
    |POLYGON ((30 20, 45 40, 10 40, 30 20))    |
    |POLYGON ((15 5, 40 10, 10 20, 5 10, 15 5))|
    +------------------------------------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
    >>> df.select(flatten_polygons($"wkt")).show(false)
    +------------------------------------------+
    |element                                   |
    +------------------------------------------+
    |POLYGON ((30 20, 45 40, 10 40, 30 20))    |
    |POLYGON ((15 5, 40 10, 10 20, 5 10, 15 5))|
    +------------------------------------------+

   .. code-tab:: sql

    >>> SELECT flatten_polygons("'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'")
    +------------------------------------------+
    |element                                   |
    +------------------------------------------+
    |POLYGON ((30 20, 45 40, 10 40, 30 20))    |
    |POLYGON ((15 5, 40 10, 10 20, 5 10, 15 5))|
    +------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'))
    >>> showDF(select(df, flatten_polygons(column("wkt"))), truncate=F)
    +------------------------------------------+
    |element                                   |
    +------------------------------------------+
    |POLYGON ((30 20, 45 40, 10 40, 30 20))    |
    |POLYGON ((15 5, 40 10, 10 20, 5 10, 15 5))|
    +------------------------------------------+

point_index_lonlat
******************

.. function:: point_index_lonlat(lon, lat, resolution)

    Returns the `resolution` grid index associated with
    the input `lon` and `lat` coordinates.

    :param lon: Longitude
    :type lon: Column: DoubleType
    :param lat: Latitude
    :type lat: Column: DoubleType
    :param resolution: Index resolution
    :type resolution: Column: Integer
    :rtype: Column: LongType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
    >>> df.select(point_index_lonlat('lon', 'lat', lit(10))).show(1, False)
    +----------------------------+
    |h3_point_index(lon, lat, 10)|
    +----------------------------+
    |623385352048508927          |
    +----------------------------+

   .. code-tab:: scala

    >>> val df = List((30.0, 10.0)).toDF("lon", "lat")
    >>> df.select(point_index_lonlat($"lon", $"lat", lit(10))).show()
    +----------------------------+
    |h3_point_index(lat, lon, 10)|
    +----------------------------+
    |623385352048508927          |
    +----------------------------+

   .. code-tab:: sql

    >>> SELECT point_index_lonlat(30d, 10d, 10)
    +----------------------------+
    |h3_point_index(lat, lon, 10)|
    +----------------------------+
    |623385352048508927          |
    +----------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
    >>> showDF(select(df, point_index_lonlat(column("lon"), column("lat"), lit(10L))), truncate=F)
    +----------------------------+
    |h3_point_index(lat, lon, 10)|
    +----------------------------+
    |623385352048508927          |
    +----------------------------+


polyfill
********

.. function:: polyfill(geometry, resolution)

    Returns the set of grid indices covering the input `geometry` at `resolution`.

    :param geometry: Geometry
    :type geometry: Column
    :param resolution: Index resolution
    :type resolution: Column: Integer
    :rtype: Column: ArrayType[LongType]

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{
        'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'
        }])
    >>> df.select(polyfill('wkt', lit(0))).show(1, False)
    +------------------------------------------------------------+
    |h3_polyfill(wkt, 0)                                         |
    +------------------------------------------------------------+
    |[577586652210266111, 578360708396220415, 577269992861466623]|
    +------------------------------------------------------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
    >>> df.select(polyfill($"wkt", lit(0))).show(false)
    +------------------------------------------------------------+
    |h3_polyfill(wkt, 0)                                         |
    +------------------------------------------------------------+
    |[577586652210266111, 578360708396220415, 577269992861466623]|
    +------------------------------------------------------------+

   .. code-tab:: sql

    >>> SELECT polyfill("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)
    +------------------------------------------------------------+
    |h3_polyfill(wkt, 0)                                         |
    +------------------------------------------------------------+
    |[577586652210266111, 578360708396220415, 577269992861466623]|
    +------------------------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"))
    >>> showDF(select(df, polyfill(column("wkt"), lit(0L))), truncate=F)
    +------------------------------------------------------------+
    |h3_polyfill(wkt, 0)                                         |
    +------------------------------------------------------------+
    |[577586652210266111, 578360708396220415, 577269992861466623]|
    +------------------------------------------------------------+


mosaicfill
**********

.. function:: mosaicfill(geometry, resolution, keep_core_geometries)

    Generates:
    - a set of core indices that are fully contained by `geometry`; and
    - a set of border indices and sub-polygons that are partially contained by the input.

    Outputs an array of chip structs for each input row.

    :param geometry: Geometry
    :type geometry: Column
    :param resolution: Index resolution
    :type resolution: Column: Integer
    :param keep_core_geometries: Whether to keep the core geometries or set them to null
    :type keep_core_geometries: Column: Boolean
    :rtype: Column: ArrayType[MosaicType]

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'}])
    >>> df.select(mosaicfill('wkt', lit(0))).printSchema()
    root
     |-- mosaicfill(wkt, 0): mosaic (nullable = true)
     |    |-- chips: array (nullable = true)
     |    |    |-- element: mosaic_chip (containsNull = true)
     |    |    |    |-- is_core: boolean (nullable = true)
     |    |    |    |-- h3: long (nullable = true)
     |    |    |    |-- wkb: binary (nullable = true)


    >>> df.select(mosaicfill('wkt', lit(0))).show()
    +---------------------+
    |mosaicfill(wkt, 0)   |
    +---------------------+
    | {[{false, 5774810...|
    +---------------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
    >>> df.select(mosaicfill($"wkt", lit(0))).printSchema
    root
     |-- mosaicfill(wkt, 0): mosaic (nullable = true)
     |    |-- chips: array (nullable = true)
     |    |    |-- element: mosaic_chip (containsNull = true)
     |    |    |    |-- is_core: boolean (nullable = true)
     |    |    |    |-- h3: long (nullable = true)
     |    |    |    |-- wkb: binary (nullable = true)

    >>> df.select(mosaicfill($"wkt", lit(0))).show()
    +---------------------+
    |mosaicfill(wkt, 0)   |
    +---------------------+
    | {[{false, 5774810...|
    +---------------------+

   .. code-tab:: sql

    >>> SELECT mosaicfill("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)
    +---------------------+
    |mosaicfill(wkt, 0)   |
    +---------------------+
    | {[{false, 5774810...|
    +---------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"))
    >>> schema(select(df, mosaicfill(column("wkt"), lit(0L))))
    root
     |-- mosaicfill(wkt, 0): mosaic (nullable = true)
     |    |-- chips: array (nullable = true)
     |    |    |-- element: mosaic_chip (containsNull = true)
     |    |    |    |-- is_core: boolean (nullable = true)
     |    |    |    |-- h3: long (nullable = true)
     |    |    |    |-- wkb: binary (nullable = true)
    >>> showDF(select(df, mosaicfill(column("wkt"), lit(0L))))
    +---------------------+
    |mosaicfill(wkt, 0)   |
    +---------------------+
    | {[{false, 5774810...|
    +---------------------+


mosaic_explode
**************

.. function:: mosaic_explode(geometry, resolution, keep_core_geometries)

    Returns the set of Mosaic chips covering the input `geometry` at `resolution`.

    In contrast to :ref:`mosaicfill`, `mosaic_explode` generates one result row per chip.

    :param geometry: Geometry
    :type geometry: Column
    :param resolution: Index resolution
    :type resolution: Column: Integer
    :param keep_core_geometries: Whether to keep the core geometries or set them to null
    :type keep_core_geometries: Column: Boolean
    :rtype: Column: MosaicType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'wkt': 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'}])
    >>> df.select(mosaic_explode('wkt', lit(0))).show()
    +-----------------------------------------------+
    |is_core|                h3|                 wkb|
    +-------+------------------+--------------------+
    |  false|577481099093999615|[01 03 00 00 00 0...|
    |  false|578044049047420927|[01 03 00 00 00 0...|
    |  false|578782920861286399|[01 03 00 00 00 0...|
    |  false|577023702256844799|[01 03 00 00 00 0...|
    |  false|577938495931154431|[01 03 00 00 00 0...|
    |  false|577586652210266111|[01 06 00 00 00 0...|
    |  false|577269992861466623|[01 03 00 00 00 0...|
    |  false|578360708396220415|[01 03 00 00 00 0...|
    +-------+------------------+--------------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
    >>> df.select(mosaic_explode($"wkt", lit(0))).show()
    +-----------------------------------------------+
    |is_core|                h3|                 wkb|
    +-------+------------------+--------------------+
    |  false|577481099093999615|[01 03 00 00 00 0...|
    |  false|578044049047420927|[01 03 00 00 00 0...|
    |  false|578782920861286399|[01 03 00 00 00 0...|
    |  false|577023702256844799|[01 03 00 00 00 0...|
    |  false|577938495931154431|[01 03 00 00 00 0...|
    |  false|577586652210266111|[01 06 00 00 00 0...|
    |  false|577269992861466623|[01 03 00 00 00 0...|
    |  false|578360708396220415|[01 03 00 00 00 0...|
    +-------+------------------+--------------------+

   .. code-tab:: sql

    >>> SELECT mosaic_explode("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)
    +-----------------------------------------------+
    |is_core|                h3|                 wkb|
    +-------+------------------+--------------------+
    |  false|577481099093999615|[01 03 00 00 00 0...|
    |  false|578044049047420927|[01 03 00 00 00 0...|
    |  false|578782920861286399|[01 03 00 00 00 0...|
    |  false|577023702256844799|[01 03 00 00 00 0...|
    |  false|577938495931154431|[01 03 00 00 00 0...|
    |  false|577586652210266111|[01 06 00 00 00 0...|
    |  false|577269992861466623|[01 03 00 00 00 0...|
    |  false|578360708396220415|[01 03 00 00 00 0...|
    +-------+------------------+--------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))'))
    >>> showDF(select(df, mosaic_explode(column("wkt"), lit(0L))))
    +--------------------+
    |               index|
    +--------------------+
    |{false, 577481099...|
    |{false, 578044049...|
    |{false, 578782920...|
    |{false, 577023702...|
    |{false, 577938495...|
    |{false, 577586652...|
    |{false, 577269992...|
    |{false, 578360708...|
    +--------------------+

