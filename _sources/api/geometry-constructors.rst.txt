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

    >>> df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
    >>> df.select(st_point('lon', 'lat').alias('point_geom')).show(1, False)
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

    >>> df = spark.createDataFrame([
            {'lon': 30., 'lat': 10.},
            {'lon': 10., 'lat': 30.},
            {'lon': 40., 'lat': 40.}
            ])
    >>> (
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

st_makepolygon
**************

.. function:: st_makepolygon(col)

    Create a new Mosaic Polygon geometry from a closed LineString.

    :param col: closed LineString
    :type col: Column: InternalGeometryType
    :rtype: Column: InternalGeometryType

    :example:

    >>> df = spark.createDataFrame([{'wkt': 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'}])
    >>> df.select(st_makepolygon(st_geomfromwkt('wkt')).alias('polygon_geom')).show(1, False)
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

    >>> df = spark.createDataFrame([{'wkt': 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'}])
    >>> df.select(st_geomfromwkt('wkt')).show(1, False)
    +-------------------------------------------------------------------------------------+
    |convert_to(wkt)                                                                      |
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

    >>> import binascii
    >>> hex = '0000000001C052F1F0ED3D859D4041983D46B26BF8'
    >>> binary = binascii.unhexlify(hex)
    >>> df = spark.createDataFrame([{'wkb': binary}])
    >>> df.select(st_geomfromwkb('wkb')).show(1, False)
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

    >>> df = spark.createDataFrame([{
        'json': '{"type":"Point","coordinates":[-75.78033,35.18937],"crs":{"type":"name","properties":{"name":"EPSG:0"}}}'
        }])
    >>> df.select(st_geomfromgeojson('json')).show(1, False)
    +--------------------------------------+
    |convert_to(as_json(json))             |
    +--------------------------------------+
    |{1, [[[-75.78033, 35.18937]]], [[[]]]}|
    +--------------------------------------+
