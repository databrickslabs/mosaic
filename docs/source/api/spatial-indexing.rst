=====================
Spatial grid indexing
=====================

Spatial grid indexing is the process of mapping a geometry (or a point) to one or more cells (or cell ID)
from the selected spatial grid.

grid_longlatascellid
********************

.. function:: grid_longlatascellid(lon, lat, resolution)

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
    >>> df.select(grid_longlatascellid('lon', 'lat', lit(10))).show(1, False)
    +----------------------------------+
    |grid_longlatascellid(lon, lat, 10)|
    +----------------------------------+
    |                623385352048508927|
    +----------------------------------+

   .. code-tab:: scala

    >>> val df = List((30.0, 10.0)).toDF("lon", "lat")
    >>> df.select(grid_longlatascellid(col("lon"), col("lat"), lit(10))).show()
    +----------------------------------+
    |grid_longlatascellid(lon, lat, 10)|
    +----------------------------------+
    |                623385352048508927|
    +----------------------------------+

   .. code-tab:: sql

    >>> SELECT grid_longlatascellid(30d, 10d, 10)
    +----------------------------------+
    |grid_longlatascellid(lon, lat, 10)|
    +----------------------------------+
    |                623385352048508927|
    +----------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
    >>> showDF(select(df, grid_longlatascellid(column("lon"), column("lat"), lit(10L))), truncate=F)
    +----------------------------------+
    |grid_longlatascellid(lon, lat, 10)|
    +----------------------------------+
    |                623385352048508927|
    +----------------------------------+



grid_pointascellid
******************

.. function:: grid_pointascellid(geometry, resolution)

    Returns the `resolution` grid index associated
    with the input point geometry `geometry`.

    :param geometry: Geometry
    :type geometry: Column
    :param resolution: Index resolution
    :type resolution: Column: Integer
    :rtype: Column: LongType

    :example:

.. tabs::
   .. code-tab:: py

    >>> df = spark.createDataFrame([{'lon': 30., 'lat': 10.}])
    >>> df.select(grid_pointascellid(st_point('lon', 'lat'), lit(10))).show(1, False)
    +------------------------------------------+
    |grid_pointascellid(st_point(lon, lat), 10)|
    +------------------------------------------+
    |623385352048508927                        |
    +------------------------------------------+

   .. code-tab:: scala

    >>> val df = List((30.0, 10.0)).toDF("lon", "lat")
    >>> df.select(grid_pointascellid(st_point(col("lon"), col("lat")), lit(10))).show()
    +------------------------------------------+
    |grid_pointascellid(st_point(lon, lat), 10)|
    +------------------------------------------+
    |623385352048508927                        |
    +------------------------------------------+

   .. code-tab:: sql

    >>> SELECT grid_pointascellid(st_point(30d, 10d), 10)
    +------------------------------------------+
    |grid_pointascellid(st_point(lon, lat), 10)|
    +------------------------------------------+
    |623385352048508927                        |
    +------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(lon = 30.0, lat = 10.0))
    >>> showDF(select(df, grid_pointascellid(st_point(column("lon"), column("lat")), lit(10L))), truncate=F)
    +------------------------------------------+
    |grid_pointascellid(st_point(lon, lat), 10)|
    +------------------------------------------+
    |623385352048508927                        |
    +------------------------------------------+



grid_polyfill
*************

.. function:: grid_polyfill(geometry, resolution)

    Returns the set of grid indices of which centroid is contained in the input `geometry` at `resolution`.

    When using `H3 <https://h3geo.org/>` index system, this is equivalent to the
    `H3 polyfill <https://h3geo.org/docs/api/regions/#polyfill>` method

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
    >>> df.select(grid_polyfill('wkt', lit(0))).show(1, False)
    +------------------------------------------------------------+
    |grid_polyfill(wkt, 0)                                       |
    +------------------------------------------------------------+
    |[577586652210266111, 578360708396220415, 577269992861466623]|
    +------------------------------------------------------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
    >>> df.select(grid_polyfill(col("wkt"), lit(0))).show(false)
    +------------------------------------------------------------+
    |grid_polyfill(wkt, 0)                                       |
    +------------------------------------------------------------+
    |[577586652210266111, 578360708396220415, 577269992861466623]|
    +------------------------------------------------------------+

   .. code-tab:: sql

    >>> SELECT grid_polyfill("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)
    +------------------------------------------------------------+
    |grid_polyfill(wkt, 0)                                       |
    +------------------------------------------------------------+
    |[577586652210266111, 578360708396220415, 577269992861466623]|
    +------------------------------------------------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"))
    >>> showDF(select(df, grid_polyfill(column("wkt"), lit(0L))), truncate=F)
    +------------------------------------------------------------+
    |grid_polyfill(wkt, 0)                                       |
    +------------------------------------------------------------+
    |[577586652210266111, 578360708396220415, 577269992861466623]|
    +------------------------------------------------------------+


grid_tessellate
***************

.. function:: grid_tessellate(geometry, resolution, keep_core_geometries)

    Cuts the original `geometry` into several pieces along the grid index borders at the specified `resolution`.

    Returns an array of Mosaic chips **covering** the input `geometry` at `resolution`.

    A Mosaic chip is a struct type composed of:

    - `is_core`: Identifies if the chip is fully contained within the geometry: Boolean

    - `index_id`: Index ID of the configured spatial indexing (default H3): Integer

    - `wkb`: Geometry in WKB format equal to the intersection of the index shape and the original `geometry`: Binary

    In contrast to :ref:`grid_tessellateexplode`, `grid_tessellate` does not explode the list of shapes.

    In contrast to :ref:`grid_polyfill`, `grid_tessellate` fully covers the original `geometry` even if the index centroid
    falls outside of the original geometry. This makes it suitable to index lines as well.

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
    >>> df.select(grid_tessellate('wkt', lit(0))).printSchema()
    root
     |-- grid_tessellate(wkt, 0): mosaic (nullable = true)
     |    |-- chips: array (nullable = true)
     |    |    |-- element: mosaic_chip (containsNull = true)
     |    |    |    |-- is_core: boolean (nullable = true)
     |    |    |    |-- index_id: long (nullable = true)
     |    |    |    |-- wkb: binary (nullable = true)


    >>> df.select(grid_tessellate('wkt', lit(0))).show()
    +-----------------------+
    |grid_tessellate(wkt, 0)|
    +-----------------------+
    |   {[{false, 5774810...|
    +-----------------------+

   .. code-tab:: scala

    >>> val df = List(("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))")).toDF("wkt")
    >>> df.select(grid_tessellate(col("wkt"), lit(0))).printSchema
    root
     |-- grid_tessellate(wkt, 0): mosaic (nullable = true)
     |    |-- chips: array (nullable = true)
     |    |    |-- element: mosaic_chip (containsNull = true)
     |    |    |    |-- is_core: boolean (nullable = true)
     |    |    |    |-- index_id: long (nullable = true)
     |    |    |    |-- wkb: binary (nullable = true)

    >>> df.select(grid_tessellate(col("wkt"), lit(0))).show()
    +-----------------------+
    |grid_tessellate(wkt, 0)|
    +-----------------------+
    |   {[{false, 5774810...|
    +-----------------------+

   .. code-tab:: sql

    >>> SELECT grid_tessellate("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)
    +-----------------------+
    |grid_tessellate(wkt, 0)|
    +-----------------------+
    |   {[{false, 5774810...|
    +-----------------------+

   .. code-tab:: r R

    >>> df <- createDataFrame(data.frame(wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"))
    >>> schema(select(df, grid_tessellate(column("wkt"), lit(0L))))
    root
     |-- grid_tessellate(wkt, 0): mosaic (nullable = true)
     |    |-- chips: array (nullable = true)
     |    |    |-- element: mosaic_chip (containsNull = true)
     |    |    |    |-- is_core: boolean (nullable = true)
     |    |    |    |-- index_id: long (nullable = true)
     |    |    |    |-- wkb: binary (nullable = true)
    >>> showDF(select(df, grid_tessellate(column("wkt"), lit(0L))))
    +-----------------------+
    |grid_tessellate(wkt, 0)|
    +-----------------------+
    |   {[{false, 5774810...|
    +-----------------------+



grid_tessellateexplode
**********************

.. function:: grid_tessellateexplode(geometry, resolution, keep_core_geometries)

    Cuts the original `geometry` into several pieces along the grid index borders at the specified `resolution`.

    Returns the set of Mosaic chips **covering** the input `geometry` at `resolution`.

    A Mosaic chip is a struct type composed of:

    - `is_core`: Identifies if the chip is fully contained within the geometry: Boolean

    - `index_id`: Index ID of the configured spatial indexing (default H3): Integer

    - `wkb`: Geometry in WKB format equal to the intersection of the index shape and the original `geometry`: Binary

    In contrast to :ref:`grid_tessellate`, `grid_tessellateexplode` generates one result row per chip.

    In contrast to :ref:`grid_polyfill`, `grid_tessellateexplode` fully covers the original `geometry` even if the index centroid
    falls outside of the original geometry. This makes it suitable to index lines as well.

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
    >>> df.select(grid_tessellateexplode('wkt', lit(0))).show()
    +-----------------------------------------------+
    |is_core|          index_id|                 wkb|
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
    >>> df.select(grid_tessellateexplode(col("wkt"), lit(0))).show()
    +-----------------------------------------------+
    |is_core|          index_id|                 wkb|
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

    >>> SELECT grid_tessellateexplode("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))", 0)
    +-----------------------------------------------+
    |is_core|          index_id|                 wkb|
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
    >>> showDF(select(df, grid_tessellateexplode(column("wkt"), lit(0L))))
    +-----------------------------------------------+
    |is_core|          index_id|                 wkb|
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


mosaic_explode [Deprecated]
***************************

.. function:: mosaic_explode(geometry, resolution, keep_core_geometries)

    This is an alias for :ref:`grid_tessellateexplode`


mosaicfill [Deprecated]
************************

.. function:: mosaicfill(geometry, resolution, keep_core_geometries)

    This is an alias for :ref:`grid_tessellate`


point_index_geom [Deprecated]
******************************

.. function:: point_index_geom(point, resolution)

    This is an alias for :ref:`grid_pointascellid`


point_index_lonlat [Deprecated]
********************************

.. function:: point_index_lonlat(point, resolution)

    This is an alias for :ref:`grid_longlatascellid`


polyfill [Deprecated]
**********************

.. function:: polyfill(geom, resolution)

    This is an alias for :ref:`grid_polyfill`
