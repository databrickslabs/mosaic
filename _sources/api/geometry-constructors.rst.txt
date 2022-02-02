=====================
Geometry Constructors
=====================

st_point
********

.. function:: st_point(x, y)

    Create a new Mosaic Point geometry from two DoubleType values.

    :param x: x coordinate (e.g. Longitude), DoubleType
    :type lon: Column
    :param y: y coordinate (e.g. Latitude), DoubleType
    :type lat: Column
    :rtype: Column[InternalGeometryType]

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

    :param col: ArrayType[InternalGeometryType]
    :type col: Column
    :rtype: Column[InternalGeometryType]

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

    :param col: closed LineString, InternalGeometryType
    :type col: Column
    :rtype: Column[InternalGeometryType]

    :example:

    >>> df = spark.createDataFrame([{'wkt': 'LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)'}])
    >>> df.select(st_makepolygon(st_geomfromwkt('wkt')).alias('polygon_geom')).show(1, False)
    +-----------------------------------------------------------------------------------+
    |polygon_geom                                                                       |
    +-----------------------------------------------------------------------------------+
    |{5, [[[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]], [[]]}|
    +-----------------------------------------------------------------------------------+
