==================
Geometry Accessors
==================

st_astext
*********

.. function:: st_astext(col)

    Translate a geometry into its Well-known Text (WKT) representation.

    :param col: BinaryType, HexType, JSONType or InternalGeometryType
    :type col: Column
    :rtype: Column[StringType]

    :example:

    >>> df = spark.createDataFrame([(30., 10.)], ['lon', 'lat'])
    >>> df.select(st_astext(st_point('lon', 'lat')).alias('wkt')).collect()
    [Row(wkt='POINT (30 10)')]
