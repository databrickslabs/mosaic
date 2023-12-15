=====================
Vector Format Readers
=====================


Intro
################
Mosaic provides spark readers for vector files supported by GDAL OGR drivers.
Only the drivers that are built by default are supported.
Here are some common useful file formats:
    * GeoJSON (also ESRIJSON, TopoJSON)
      https://gdal.org/drivers/vector/geojson.html
    * ESRI File Geodatabase (FileGDB) and ESRI File Geodatabase vector (OpenFileGDB)
      Mosaic implements named reader geo_db (described in this doc)
      https://gdal.org/drivers/vector/filegdb.html
    * ESRI Shapefile / DBF (ESRI Shapefile) - Mosaic implements named reader shapefile (described in this doc)
      https://gdal.org/drivers/vector/shapefile.html
    * Network Common Data Form (netCDF) - Mosaic implements raster reader also
      https://gdal.org/drivers/raster/netcdf.html
    * (Geo)Parquet (Parquet) - Mosaic will be implementing a custom reader soon
      https://gdal.org/drivers/vector/parquet.html
    * Spreadsheets (XLSX, XLS, ODS)
      https://gdal.org/drivers/vector/xls.html
    * U.S. Census TIGER/Line (TIGER)
      https://gdal.org/drivers/vector/tiger.html
    * PostgreSQL Dump (PGDump)
      https://gdal.org/drivers/vector/pgdump.html
    * Keyhole Markup Language (KML)
      https://gdal.org/drivers/vector/kml.html
    * Geography Markup Language (GML)
      https://gdal.org/drivers/vector/gml.html
    * GRASS - option for Linear Referencing Systems (LRS)
      https://gdal.org/drivers/vector/grass.html
For more information please refer to gdal documentation: https://gdal.org/drivers/vector/index.html



Mosaic provides two flavors of the readers:
    * spark.read.format("ogr") for reading 1 file per spark task
    * mos.read().format("multi_read_ogr") for reading file in parallel with multiple spark tasks


spark.read.format("ogr")
*************************
A base Spark SQL data source for reading GDAL vector data sources.
The output of the reader is a DataFrame with inferred schema.
The schema is inferred from both features and fields in the vector file.
Each feature will be provided as 2 columns:
    * geometry - geometry of the feature (GeometryType)
    * srid - spatial reference system identifier of the feature (StringType)

The fields of the feature will be provided as columns in the DataFrame.
The types of the fields are coerced to most concrete type that can hold all the values.
The reader supports the following options:
    * driverName - GDAL driver name (StringType)
    * vsizip - if the vector files are zipped files, set this to true (BooleanType)
    * asWKB - if the geometry should be returned as WKB (BooleanType) - default is false
    * layerName - name of the layer to read (StringType)
    * layerNumber - number of the layer to read (IntegerType)


.. function:: read.format("ogr").load(path)

    Loads a vector file and returns the result as a :class:`DataFrame`.

    :param path: the path of the vector file
    :return: :class:`DataFrame`

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("ogr")\
        .option("driverName", "GeoJSON")\
        .option("layerName", "points")\
        .option("asWKB", "false")\
        .load("file:///tmp/points.geojson")
    df.show()
    +--------------------+-------+-----+-----------------+-----------+
    |             field_1|field_2| ... |           geom_1|geom_1_srid|
    +--------------------+-------+-----+-----------------+-----------+
    |       "description"|      1| ... | POINT (1.0 1.0) |       4326|
    |       "description"|      2| ... | POINT (2.0 2.0) |       4326|
    |       "description"|      3| ... | POINT (3.0 3.0) |       4326|
    +--------------------+-------+-----+-----------------+-----------+

   .. code-tab:: scala

    val df = spark.read.format("ogr")
        .option("layerName", "points")
        .option("asWKB", "false")
        .load("file:///tmp/points.geojson")
    df.show()
    +--------------------+-------+-----+-----------------+-----------+
    |             field_1|field_2| ... |           geom_1|geom_1_srid|
    +--------------------+-------+-----+-----------------+-----------+
    |       "description"|      1| ... | POINT (1.0 1.0) |       4326|
    |       "description"|      2| ... | POINT (2.0 2.0) |       4326|
    |       "description"|      3| ... | POINT (3.0 3.0) |       4326|
    +--------------------+-------+-----+-----------------+-----------+


mos.read().format("multi_read_ogr")
***********************************
Mosaic supports reading vector files in parallel with multiple spark tasks.
The amount of data per task is controlled by the chunkSize option.
Chunk size is the number of file rows that will be read per single task.
The output of the reader is a DataFrame with inferred schema.
The schema is inferred from both features and fields in the vector file.
Each feature will be provided as 2 columns:
    * geometry - geometry of the feature (GeometryType)
    * srid - spatial reference system identifier of the feature (StringType)

The fields of the feature will be provided as columns in the DataFrame.
The types of the fields are coerced to most concrete type that can hold all the values.
The reader supports the following options:
    * driverName - GDAL driver name (StringType)
    * vsizip - if the vector files are zipped files, set this to true (BooleanType)
    * asWKB - if the geometry should be returned as WKB (BooleanType) - default is false
    * chunkSize - size of the chunk to read from the file per single task (IntegerType) - default is 5000
    * layerName - name of the layer to read (StringType)
    * layerNumber - number of the layer to read (IntegerType)


.. function:: read.format("multi_read_ogr").load(path)

    Loads a vector file and returns the result as a :class:`DataFrame`.

    :param path: the path of the vector file
    :return: :class:`DataFrame`

    :example:

.. tabs::
   .. code-tab:: py

    df = mos.read().format("multi_read_ogr")\
        .option("driverName", "GeoJSON")\
        .option("layerName", "points")\
        .option("asWKB", "false")\
        .load("file:///tmp/points.geojson")
    df.show()
    +--------------------+-------+-----+-----------------+-----------+
    |             field_1|field_2| ... |           geom_1|geom_1_srid|
    +--------------------+-------+-----+-----------------+-----------+
    |       "description"|      1| ... | POINT (1.0 1.0) |       4326|
    |       "description"|      2| ... | POINT (2.0 2.0) |       4326|
    |       "description"|      3| ... | POINT (3.0 3.0) |       4326|
    +--------------------+-------+-----+-----------------+-----------+

   .. code-tab:: scala

    val df = MosaicContext.read.format("multi_read_ogr")
        .option("layerName", "points")
        .option("asWKB", "false")
        .load("file:///tmp/points.geojson")
    df.show()
    +--------------------+-------+-----+-----------------+-----------+
    |             field_1|field_2| ... |           geom_1|geom_1_srid|
    +--------------------+-------+-----+-----------------+-----------+
    |       "description"|      1| ... | POINT (1.0 1.0) |       4326|
    |       "description"|      2| ... | POINT (2.0 2.0) |       4326|
    |       "description"|      3| ... | POINT (3.0 3.0) |       4326|
    +--------------------+-------+-----+-----------------+-----------+


spark.read().format("geo_db")
*****************************
Mosaic provides a reader for GeoDB files natively in Spark.
The output of the reader is a DataFrame with inferred schema.
Only 1 file per task is read. For parallel reading of large files use the multi_read_ogr reader.
The reader supports the following options:
    * asWKB - if the geometry should be returned as WKB (BooleanType) - default is false
    * layerName - name of the layer to read (StringType)
    * layerNumber - number of the layer to read (IntegerType)
    * vsizip - if the vector files are zipped files, set this to true (BooleanType)

.. function:: read.format("geo_db").load(path)

    Loads a GeoDB file and returns the result as a :class:`DataFrame`.

    :param path: the path of the GeoDB file
    :return: :class:`DataFrame`

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("geo_db")\
        .option("layerName", "points")\
        .option("asWKB", "false")\
        .load("file:///tmp/points.geodb")
    df.show()
    +--------------------+-------+-----+-----------------+-----------+
    |             field_1|field_2| ... |           geom_1|geom_1_srid|
    +--------------------+-------+-----+-----------------+-----------+
    |       "description"|      1| ... | POINT (1.0 1.0) |       4326|
    |       "description"|      2| ... | POINT (2.0 2.0) |       4326|
    |       "description"|      3| ... | POINT (3.0 3.0) |       4326|
    +--------------------+-------+-----+-----------------+-----------+

   .. code-tab:: scala

    val df = spark.read.format("geo_db")
        .option("layerName", "points")
        .option("asWKB", "false")
        .load("file:///tmp/points.geodb")
    df.show()
    +--------------------+-------+-----+-----------------+-----------+
    |             field_1|field_2| ... |           geom_1|geom_1_srid|
    +--------------------+-------+-----+-----------------+-----------+
    |       "description"|      1| ... | POINT (1.0 1.0) |       4326|
    |       "description"|      2| ... | POINT (2.0 2.0) |       4326|
    |       "description"|      3| ... | POINT (3.0 3.0) |       4326|
    +--------------------+-------+-----+-----------------+-----------+


spark.read().format("shapefile")
********************************
Mosaic provides a reader for Shapefiles natively in Spark.
The output of the reader is a DataFrame with inferred schema.
Only 1 file per task is read. For parallel reading of large files use the multi_read_ogr reader.
The reader supports the following options:
    * asWKB - if the geometry should be returned as WKB (BooleanType) - default is false
    * layerName - name of the layer to read (StringType)
    * layerNumber - number of the layer to read (IntegerType)
    * vsizip - if the vector files are zipped files, set this to true (BooleanType)

.. function:: read.format("shapefile").load(path)

    Loads a Shapefile and returns the result as a :class:`DataFrame`.

    :param path: the path of the Shapefile
    :return: :class:`DataFrame`

    :example:

.. tabs::
   .. code-tab:: py

    df = spark.read.format("shapefile")\
        .option("layerName", "points")\
        .option("asWKB", "false")\
        .load("file:///tmp/points.shp")
    df.show()
    +--------------------+-------+-----+-----------------+-----------+
    |             field_1|field_2| ... |           geom_1|geom_1_srid|
    +--------------------+-------+-----+-----------------+-----------+
    |       "description"|      1| ... | POINT (1.0 1.0) |       4326|
    |       "description"|      2| ... | POINT (2.0 2.0) |       4326|
    |       "description"|      3| ... | POINT (3.0 3.0) |       4326|
    +--------------------+-------+-----+-----------------+-----------+

   .. code-tab:: scala

    val df = spark.read.format("shapefile")
        .option("layerName", "points")
        .option("asWKB", "false")
        .load("file:///tmp/points.shp")
    df.show()
    +--------------------+-------+-----+-----------------+-----------+
    |             field_1|field_2| ... |           geom_1|geom_1_srid|
    +--------------------+-------+-----+-----------------+-----------+
    |       "description"|      1| ... | POINT (1.0 1.0) |       4326|
    |       "description"|      2| ... | POINT (2.0 2.0) |       4326|
    |       "description"|      3| ... | POINT (3.0 3.0) |       4326|
    +--------------------+-------+-----+-----------------+-----------+