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

    >>> df = spark.createDataFrame([{'wkt': 'POINT (30 10)'}])
    >>> df.select(st_asbinary('wkt').alias('wkb')).collect()
    [Row(wkb=bytearray(b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00$@'))]

.. note:: Alias for :ref:`st_aswkb`.

st_asgeojson
************

.. function:: st_asgeojson(col)

    Translate a geometry into its GeoJSON representation.

    :param col: Geometry column
    :type col: Column: BinaryType, StringType, HexType or InternalGeometryType
    :rtype: Column: JSONType

    :example:

    >>> df = spark.createDataFrame([{'wkt': 'POINT (30 10)'}])
    >>> df.select(st_asgeojson('wkt').cast('string').alias('json')).collect()
    [Row(json='{{"type":"Point","coordinates":[30,10],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}}')]

.. note:: Alias for :ref:`st_astext`.


st_astext
*********

.. function:: st_astext(col)

    Translate a geometry into its Well-known Text (WKT) representation.

    :param col: Geometry column
    :type col: Column: BinaryType, HexType, JSONType or InternalGeometryType
    :rtype: Column: StringType

    :example:

    >>> df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
    >>> df.select(st_astext(st_point('lon', 'lat')).alias('wkt')).collect()
    [Row(wkt='POINT (30 10)')]

.. note:: Alias for :ref:`st_aswkt`.


st_aswkb
********

.. function:: st_aswkb(col)

    Translate a geometry into its Well-known Binary (WKB) representation.

    :param col: Geometry column
    :type col: Column: StringType, HexType, JSONType or InternalGeometryType
    :rtype: Column: BinaryType

    :example:

    >>> df = spark.createDataFrame([{'wkt': 'POINT (30 10)'}])
    >>> df.select(st_aswkb('wkt').alias('wkb')).collect()
    [Row(wkb=bytearray(b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00$@'))]

.. note:: Alias for :ref:`st_asbinary`.

st_aswkt
********

.. function:: st_aswkt(col)

    Translate a geometry into its Well-known Text (WKT) representation.

    :param col: Geometry column
    :type col: Column: BinaryType, HexType, JSONType or InternalGeometryType
    :rtype: Column: StringType

    :example:

    >>> df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
    >>> df.select(st_astext(st_point('lon', 'lat')).alias('wkt')).collect()
    [Row(wkt='POINT (30 10)')]

.. note:: Alias for :ref:`st_astext`.
