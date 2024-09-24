=====================
Vector Format Readers
=====================

#####
Intro
#####
Mosaic provides spark readers for vector files supported by GDAL OGR drivers.
Only the drivers that are built by default are supported.
Here are some common useful file formats:

    * `GeoJSON <https://gdal.org/drivers/vector/geojson.html>`_ (also `ESRIJSON <https://gdal.org/drivers/vector/esrijson.html>`_,
      `TopoJSON  <https://gdal.org/drivers/vector/topojson.html>`_)
    * `FileGDB <https://gdal.org/drivers/vector/filegdb.html>`_ (ESRI File Geodatabase) and `OpenFileGDB <https://gdal.org/drivers/vector/openfilegdb.html>`_ (ESRI File Geodatabase vector) - Mosaic implements named reader :ref:`spark.read.format("geo_db")` (described in this doc).
    * `ESRI Shapefile <https://gdal.org/drivers/vector/shapefile.html>`_ (ESRI Shapefile / DBF) - Mosaic implements named reader :ref:`spark.read.format("shapefile")` (described in this doc).
    * `netCDF <https://gdal.org/drivers/raster/netcdf.html>`_ (Network Common Data Form) - Mosaic supports GDAL netCDF raster reader also.
    * `XLSX <https://gdal.org/drivers/vector/xlsx.html>`_, `XLS <https://gdal.org/drivers/vector/xls.html>`_, `ODS <https://gdal.org/drivers/vector/ods.html>`_ spreadsheets
    * `TIGER <https://gdal.org/drivers/vector/tiger.html>`_ (U.S. Census TIGER/Line)
    * `PGDump <https://gdal.org/drivers/vector/pgdump.html>`_ (PostgreSQL Dump)
    * `KML <https://gdal.org/drivers/vector/kml.html>`_ (Keyhole Markup Language)
    * `GML <https://gdal.org/drivers/vector/gml.html>`_ (Geography Markup Language)
    * `GRASS <https://gdal.org/drivers/vector/grass.html>`_ - option for Linear Referencing Systems (LRS)

For more information please refer to gdal `vector driver <https://gdal.org/drivers/vector/index.html>`_ documentation.


Mosaic provides two flavors of the general readers:

    * :code:`spark.read.format("ogr")` for reading 1 file per spark task
    * :code:`mos.read().format("multi_read_ogr")` for reading file in parallel with multiple spark tasks

Additionally, for convenience, Mosaic provides specific readers for Shapefile and File Geodatabases:

    * :code:`spark.read.format("geo_db")` reader for GeoDB files natively in Spark.
    * :code:`spark.read.format("shapefile")` reader for Shapefiles natively in Spark.

spark.read.format("ogr")
************************
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
    * layerNumber - number of the layer to read (IntegerType), zero-indexed


.. function:: load(path)
    :module: spark.read.format("ogr")

    Loads a vector file and returns the result as a :class:`DataFrame`.

    :param path: the path of the vector file
    :type path: Column(StringType)
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

.. note::
    Keyword options not identified in function signature are converted to a :code:`Map<String,String>`.
    These must be supplied as a :code:`String`.
    Also, you can supply function signature values as :code:`String`.


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
ALL options should be passed as String as they are provided as a :code:`Map<String,String>`
and parsed into expected types on execution. The reader supports the following options:

    * driverName - GDAL driver name (StringType)
    * vsizip - if the vector files are zipped files, set this to true (BooleanType) [pass as String]
    * asWKB - if the geometry should be returned as WKB (BooleanType) - default is false [pass as String]
    * chunkSize - size of the chunk to read from the file per single task (IntegerType) - default is 5000 [pass as String]
    * layerName - name of the layer to read (StringType)
    * layerNumber - number of the layer to read (IntegerType), zero-indexed [pass as String]


.. function:: load(path)
    :module: mos.read().format("multi_read_ogr")

    Loads a vector file and returns the result as a :class:`DataFrame`.

    :param path: the path of the vector file
    :type path: Column(StringType)
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

.. note::
    All options are converted to a :code:`Map<String,String>` and must be supplied as a :code:`String`.


spark.read.format("geo_db")
***************************
Mosaic provides a reader for GeoDB files natively in Spark.
The output of the reader is a DataFrame with inferred schema.
Only 1 file per task is read. For parallel reading of large files use the multi_read_ogr reader.
The reader supports the following options:

    * asWKB - if the geometry should be returned as WKB (BooleanType) - default is false
    * layerName - name of the layer to read (StringType)
    * layerNumber - number of the layer to read (IntegerType), zero-indexed
    * vsizip - if the vector files are zipped files, set this to true (BooleanType)

.. function:: load(path)
    :module: spark.read.format("geo_db")

    Loads a GeoDB file and returns the result as a :class:`DataFrame`.

    :param path: the path of the GeoDB file
    :type path: Column(StringType)
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

.. note::
    Keyword options not identified in function signature are converted to a :code:`Map<String,String>`.
    These must be supplied as a :code:`String`.
    Also, you can supply function signature values as :code:`String`.


spark.read.format("shapefile")
******************************
Mosaic provides a reader for Shapefiles natively in Spark.
The output of the reader is a DataFrame with inferred schema.
Only 1 file per task is read. For parallel reading of large files use the multi_read_ogr reader.
The reader supports the following options:

    * asWKB - if the geometry should be returned as WKB (BooleanType) - default is false
    * layerName - name of the layer to read (StringType)
    * layerNumber - number of the layer to read (IntegerType), zero-indexed
    * vsizip - if the vector files are zipped files, set this to true (BooleanType)

.. function:: load(path)
    :module: spark.read.format("shapefile")

    Loads a Shapefile and returns the result as a :class:`DataFrame`.

    :param path: the path of the Shapefile
    :type path: Column(StringType)
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

.. note::
    Keyword options not identified in function signature are converted to a :code:`Map<String,String>`.
    These must be supplied as a :code:`String`.
    Also, you can supply function signature values as :code:`String`.

################
Vector File UDFs
################

It can be of use to perform various exploratory operations on vector file formats to help with processing.
The following UDFs use `fiona <https://fiona.readthedocs.io/en/stable/index.html>`_ which is already provided
as part of the dependencies of Mosaic python bindings.

We are showing the zipped variation for a larger (800MB) shapefile.
This is just one file for example purposes; you can have any number of files in real-world use.
Here is a snippet for downloading.

.. code-block:: bash

    %sh
    mkdir -p /dbfs/home/<username>/data/large_shapefiles
    wget -nv -P /dbfs/home/<username>/data/large_shapefiles -nc https://osmdata.openstreetmap.de/download/land-polygons-split-4326.zip
    ls -lh /dbfs/home/<username>/data/large_shapefiles

Then we create a spark dataframe made up of metadata to drive the examples.

.. code-block:: py

    df = spark.createDataFrame([
      {
        'rel_path': '/land-polygons-split-4326/land_polygons.shp',
        'driver': 'ESRI Shapefile',
        'zip_path': '/dbfs/home/<username>/data/large_shapefiles/land-polygons-split-4326.zip'
      }
    ])

Here is an example UDF to list layers, supporting both zipped and non-zipped.

.. code-block:: py

    from pyspark.sql.functions import udf
    from pyspark.sql.types import *

    @udf(returnType=ArrayType(StringType()))
    def list_layers(in_path, driver, zip_path=None):
      """
      List layer names (in index order).
       - in_path: file location for read; when used with `zip_path`,
          this will be the relative path within a zip to open
       - driver: name of GDAL driver to use
       - zip_path: follows format 'zip:///some/file.zip' (Optional, default is None); zip gets opened something like:
          `with fiona.open('/test/a.shp', vfs='zip:///tmp/dir1/test.zip', driver='<driver>') as f:`
          Note: you can prepend 'zip://' for the param or leave it off in this example
      """
      import fiona

      z_path = zip_path
      if zip_path and not zip_path.startswith("zip:"):
        z_path = f"zip://{zip_path}"
      return fiona.listlayers(in_path, vfs=z_path, driver=driver)

We can call the UDF, e.g.

.. code-block:: py

    import pyspark.sql.functions as F

    display(
      df
        .withColumn(
          "layers",
          list_layers("rel_path", "driver", "zip_path")
        )
        .withColumn("num_layers", F.size("layers"))
    )
    +--------------+--------------------+--------------------+---------------+----------+
    |        driver|            rel_path|            zip_path|         layers|num_layers|
    +--------------+--------------------+--------------------+---------------+----------+
    |ESRI Shapefile|/land-polygons-sp...|/dbfs/home/...      |[land_polygons]|         1|
    +--------------+--------------------+--------------------+---------------+----------+

Here is an example UDF to count rows for a layer, supporting both zipped and non-zipped.

.. code-block:: py

    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType

    @udf(returnType=IntegerType())
    def count_vector_rows(in_path, driver, layer, zip_path=None):
      """
      Count rows for the provided vector file.
       - in_path: file location for read; when used with `zip_path`,
          this will be the relative path within a zip to open
       - driver: name of GDAL driver to use
       - layer: integer (zero-indexed) or string (name)
       - zip_path: follows format 'zip:///some/file.zip' (Optional, default is None); zip gets opened something like:
          `with fiona.open('/test/a.shp', vfs='zip:///tmp/dir1/test.zip', driver='<driver>') as f:`
          Note: you can prepend 'zip://' for the param or leave it off in this example
      """
      import fiona

      cnt = 0
      z_path = zip_path
      if zip_path and not zip_path.startswith("zip:"):
        z_path = f"zip://{zip_path}"
      with fiona.open(in_path, vfs=z_path, driver=driver, layer=layer) as in_file:
        for item in in_file:
          cnt += 1
      return cnt

We can call the UDF, e.g.

.. code-block:: py

    import pyspark.sql.functions as F

    display(
      df
        .withColumn(
          "row_cnt",
          count_vector_rows("rel_path", "driver", F.lit(0), "zip_path")
        )
    )
    +--------------+--------------------+--------------------+-------+
    |        driver|            rel_path|            zip_path|row_cnt|
    +--------------+--------------------+--------------------+-------+
    |ESRI Shapefile|/land-polygons-sp...|/dbfs/home/...      | 789972|
    +--------------+--------------------+--------------------+-------+


Here is an example UDF to get spark friendly schema for a layer, supporting both zipped and non-zipped.

.. code-block:: py

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    @udf(returnType=StringType())
    def layer_schema(in_path, driver, layer, zip_path=None):
      """
      Get the schema for the provided vector file layer.
       - in_path: file location for read; when used with `zip_path`,
          this will be the relative path within a zip to open
       - driver: name of GDAL driver to use
       - layer: integer (zero-indexed) or string (name)
       - zip_path: follows format 'zip:///some/file.zip' (Optional, default is None); zip gets opened something like:
          `with fiona.open('/test/a.shp', vfs='zip:///tmp/dir1/test.zip', driver='<driver>') as f:`
          Note: you can prepend 'zip://' for the param or leave it off in this example
      Returns layer schema json as string
      """
      import fiona
      import json

      cnt = 0
      z_path = zip_path
      if zip_path and not zip_path.startswith("zip:"):
        z_path = f"zip://{zip_path}"
      with fiona.open(in_path, vfs=z_path, driver=driver, layer=layer) as in_file:
        return json.dumps(in_file.schema.copy())

We can call the UDF, e.g.

.. code-block:: py

    import pyspark.sql.functions as F

    display(
      df
        .withColumn(
          "layer_schema",
          layer_schema("rel_path", "driver", F.lit(0), "zip_path")
        )
    )
    +--------------+--------------------+--------------------+--------------------+
    |        driver|            rel_path|            zip_path|        layer_schema|
    +--------------+--------------------+--------------------+--------------------+
    |ESRI Shapefile|/land-polygons-sp...|/dbfs/home/...      |{"properties": {"...|
    +--------------+--------------------+--------------------+--------------------+

Also, it can be useful to standardize collections of zipped vector formats to ensure all are individually zipped to work
with the provided APIs.

.. note::
    Option `vsizip` in the Mosaic GDAL APIs (different API than the above fiona UDF examples) is for individually zipped
    vector files (.e.g File Geodatabase or Shapefile), not collections. If you end up with mixed or unclear zipped files,
    you can test them with a UDF such as shown below.

Here is an example UDF to test for zip of zips.
In this example, we can use :code:`zip_path` from :code:`df` because we left "zip://" out of the name.

.. code-block:: py

    from pyspark.sql.functions import udf
    from pyspark.sql.types import BooleanType

    @udf(returnType=BooleanType())
    def test_double_zip(path):
      """
      Tests whether a zip contains zips, which is not supported by
      Mosaic GDAL APIs.
       - path: to check
      Returns boolean
      """
      import zipfile

      try:
        with zipfile.ZipFile(path, mode="r") as zip:
          for f in zip.namelist():
            if f.lower().endswith(".zip"):
              return True
          return False
      except:
        return False

We can call the UDF, e.g.

.. code-block:: py

    display(
      df
        .withColumn(
          "is_double_zip",
          test_double_zip("zip_path")
        )
    )
    +--------------------+-------------+
    |            zip_path|is_double_zip|
    +--------------------+-------------+
    |/dbfs/home/...      |        false|
    +--------------------+-------------+

Though not shown here, you can then handle unzipping the "double" zips that return `True` by extending
:code:`test_double_zip` UDF to perform unzips (with a provided out_dir) or through an additional UDF, e.g. using ZipFile
`extractall <https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile.extractall>`_ function.