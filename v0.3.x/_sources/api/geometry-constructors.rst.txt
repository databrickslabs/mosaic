


=====================
Geometry constructors
=====================

st_point
********

.. function:: st_point(x, y)

    Create a new Mosaic Point geometry from two DoubleType values.

    :param x: x coordinate (DoubleType)
    :type x: Column: DoubleType
    :param y: y coordinate (DoubleType)
    :type y: Column: DoubleType
    :rtype: Column: InternalGeometryType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
    df.select(st_point('lon', 'lat').alias('point_geom')).show(1, False)
    +---------------------------+
    |point_geom                 |
    +---------------------------+
    |{1, [[[30.0, 10.0]]], [[]]}|
    +---------------------------+

   .. code-tab:: scala

    val df = List((30.0, 10.0)).toDF("lon", "lat")
    df.select(st_point($"lon", $"lat")).alias("point_geom").show()
    +---------------------------+
    |point_geom                 |
    +---------------------------+
    |{1, [[[30.0, 10.0]]], [[]]}|
    +---------------------------+

   .. code-tab:: sql

    SELECT st_point(30D, 10D) AS point_geom
    +---------------------------+
    |point_geom                 |
    +---------------------------+
    |{1, [[[30.0, 10.0]]], [[]]}|
    +---------------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
    showDF(select(df, alias(st_point(column("lon"), column("lat")), "point_geom")), truncate=F)
    +---------------------------+
    |point_geom                 |
    +---------------------------+
    |{1, [[[30.0, 10.0]]], [[]]}|
    +---------------------------+

st_makeline
***********

.. function:: st_makeline(col)

    Create a new Mosaic LineString geometry from an Array of Mosaic Points.

    :param col: Point array
    :type col: Column: ArrayType[InternalGeometryType]
    :rtype: Column: InternalGeometryType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([
        {'lon': 30., 'lat': 10.},
        {'lon': 10., 'lat': 30.},
        {'lon': 40., 'lat': 40.}
        ])
    (
        df.select(st_point('lon', 'lat').alias('point_geom'))
        .groupBy()
        .agg(collect_list('point_geom').alias('point_array'))
        .select(st_makeline('point_array').alias('line_geom'))
    ).show(1, False)
    +-------------------------------------------------------+
    |line_geom                                              |
    +-------------------------------------------------------+
    |{3, [[[40.0, 40.0], [30.0, 10.0], [10.0, 30.0]]], [[]]}|
    +-------------------------------------------------------+

   .. code-tab:: scala

    val df = List(
          (30.0, 10.0),
          (10.0, 30.0),
          (40.0, 40.0)
          ).toDF("lon", "lat")
    df.select(st_point($"lon", $"lat").alias("point_geom"))
          .groupBy()
          .agg(collect_list($"point_geom").alias("point_array"))
          .select(st_makeline($"point_array").alias("line_geom"))
          .show(false)
    +-------------------------------------------------------+
    |line_geom                                              |
    +-------------------------------------------------------+
    |{3, [[[40.0, 40.0], [30.0, 10.0], [10.0, 30.0]]], [[]]}|
    +-------------------------------------------------------+

   .. code-tab:: sql

    WITH points (
        SELECT st_point(30D, 10D) AS point_geom
        UNION SELECT st_point(10D, 30D) AS point_geom
        UNION SELECT st_point(40D, 40D) AS point_geom)
    SELECT st_makeline(collect_list(point_geom))
    FROM points
    +-------------------------------------------------------+
    |line_geom                                              |
    +-------------------------------------------------------+
    |{3, [[[40.0, 40.0], [30.0, 10.0], [10.0, 30.0]]], [[]]}|
    +-------------------------------------------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame(lon = c(30.0, 10.0, 40.0), lat = c(10.0, 30.0, 40.0)))
    df <- select(df, alias(st_point(column("lon"), column("lat")), "point_geom"))
    df <- groupBy(df)
    df <- agg(df, alias(collect_list(column("point_geom")), "point_array"))
    df <- select(df, alias(st_makeline(column("point_array")), "line_geom"))
    showDF(df, truncate=F)
    +---------------------------------------------------------------+
    |line_geom                                                      |
    +---------------------------------------------------------------+
    |{3, 4326, [[[30.0, 10.0], [10.0, 30.0], [40.0, 40.0]]], [[[]]]}|
    +---------------------------------------------------------------+


st_makepolygon
**************

.. function:: st_makepolygon(col)

    Create a new Mosaic Polygon geometry from a closed LineString.

    :param col: closed LineString
    :type col: Column: InternalGeometryType
    :rtype: Column: InternalGeometryType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'wkt': 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'}])
    df.select(st_makepolygon(st_geomfromwkt('wkt')).alias('polygon_geom')).show(1, False)
    +-----------------------------------------------------------------------------------+
    |polygon_geom                                                                       |
    +-----------------------------------------------------------------------------------+
    |{5, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[]]}|
    +-----------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = List(("LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)")).toDF("wkt")
    df.select(st_makepolygon(st_geomfromwkt($"wkt")).alias("polygon_geom")).show(false)
    +-----------------------------------------------------------------------------------+
    |polygon_geom                                                                       |
    +-----------------------------------------------------------------------------------+
    |{5, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[]]}|
    +-----------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT st_makepolygon(st_geomfromwkt("LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)")) AS polygon_geom
    +-----------------------------------------------------------------------------------+
    |polygon_geom                                                                       |
    +-----------------------------------------------------------------------------------+
    |{5, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[]]}|
    +-----------------------------------------------------------------------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame('wkt' = 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'))
    showDF(select(df, alias(st_makepolygon(st_geomfromwkt(column('wkt'))), 'polygon_geom')), truncate=F)
    +-----------------------------------------------------------------------------------+
    |polygon_geom                                                                       |
    +-----------------------------------------------------------------------------------+
    |{5, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[]]}|
    +-----------------------------------------------------------------------------------+

st_geomfromwkt
**************

.. function:: st_geomfromwkt(col)

    Create a new Mosaic geometry from Well-known Text.

    :param col: Well-known Text Geometry
    :type col: Column: StringType
    :rtype: Column: InternalGeometryType

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.createDataFrame([{'wkt': 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'}])
    df.select(st_geomfromwkt('wkt')).show(1, False)
    +-------------------------------------------------------------------------------------+
    |convert_to(wkt)                                                                      |
    +-------------------------------------------------------------------------------------+
    |{3, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[[]]]}|
    +-------------------------------------------------------------------------------------+

   .. code-tab:: scala

    val df = List(("LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)")).toDF("wkt")
    df.select(st_geomfromwkt($"wkt")).show(false)
    +-------------------------------------------------------------------------------------+
    |convert_to(wkt)                                                                      |
    +-------------------------------------------------------------------------------------+
    |{3, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[[]]]}|
    +-------------------------------------------------------------------------------------+

   .. code-tab:: sql

    SELECT st_geomfromwkt("LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)") AS linestring
    +-------------------------------------------------------------------------------------+
    | linestring                                                                          |
    +-------------------------------------------------------------------------------------+
    |{3, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[[]]]}|
    +-------------------------------------------------------------------------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame('wkt' = 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'))
    showDF(select(df, alias(st_geomfromwkt(column('wkt')), 'linestring')), truncate=F)
    +-------------------------------------------------------------------------------------+
    | linestring                                                                          |
    +-------------------------------------------------------------------------------------+
    |{3, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[[]]]}|
    +-------------------------------------------------------------------------------------+

st_geomfromwkb
**************

.. function:: st_geomfromwkb(col)

    Create a new Mosaic geometry from Well-known Binary.

    :param col: Well-known Binary Geometry
    :type col: Column: BinaryType
    :rtype: Column: InternalGeometryType

    :example:

.. tabs::
   .. code-tab:: py

    import binascii

    hex = '0000000001C052F1F0ED3D859D4041983D46B26BF8'
    binary = binascii.unhexlify(hex)
    df = spark.createDataFrame([{'wkb': binary}])
    df.select(st_geomfromwkb('wkb')).show(1, False)
    +--------------------------------------+
    |convert_to(wkb)                       |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+

   .. code-tab:: scala

    val df = List(("POINT (-75.78033 35.18937)")).toDF("wkt")
    df.select(st_geomfromwkb(st_aswkb($"wkt"))).show(false)
    +--------------------------------------+
    |convert_to(convert_to(wkt))           |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+

   .. code-tab:: sql

    SELECT st_geomfromwkb(st_aswkb("POINT (-75.78033 35.18937)"))
    +--------------------------------------+
    |convert_to(convert_to(wkt))           |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+

   .. code-tab:: r R

    df <- createDataFrame(data.frame('wkt'= "POINT (-75.78033 35.18937)"))
    showDF(select(df, st_geomfromwkb(st_aswkb(column("wkt")))), truncate=F)
    +--------------------------------------+
    |convert_to(wkb)                       |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+

st_geomfromgeojson
******************

.. function:: st_geomfromgeojson(col)

    Create a new Mosaic geometry from GeoJSON.

    :param col: GeoJSON Geometry
    :type col: Column: StringType
    :rtype: Column: InternalGeometryType

    :example:

.. tabs::
   .. code-tab:: py

    import json

    geojson_dict = {
            "type":"Point",
            "coordinates":[
                -75.78033,
                35.18937
            ],
            "crs":{
                "type":"name",
                "properties":{
                    "name":"EPSG:4326"
                }
            }
        }
    df = spark.createDataFrame([{'json': json.dumps(geojson_dict)}])
    df.select(st_geomfromgeojson('json')).show(1, False)
    +--------------------------------------+
    |convert_to(as_json(json))             |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+

   .. code-tab:: scala

    val df = List(
        ("""{
            |   "type":"Point",
            |   "coordinates":[
            |       -75.78033,
            |       35.18937
            |   ],
            |   "crs":{
            |       "type":"name",
            |       "properties":{
            |           "name":"EPSG:4326"
            |       }
            |   }
            |}""".stripMargin)
        )
        .toDF("json")
    df.select(st_geomfromgeojson($"json")).show(false)
    +--------------------------------------+
    |convert_to(as_json(json))             |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+

   .. code-tab:: sql

    SELECT st_geomfromgeojson("{\"type\":\"Point\",\"coordinates\":[-75.78033,35.18937],\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:4326\"}}}")
    +--------------------------------------+
    |convert_to(as_json(json))             |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+

   .. code-tab:: r R

    geojson <- '{
            "type":"Point",
            "coordinates":[
                -75.78033,
                35.18937
            ],
            "crs":{
                "type":"name",
                "properties":{
                    "name":"EPSG:4326"
                }
            }
        }'
    df <- createDataFrame(data.frame('json' = geojson))
    showDF(select(df, st_geomfromgeojson(column('json'))), truncate=F)
    +--------------------------------------+
    |convert_to(as_json(json))             |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+
