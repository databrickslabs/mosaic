============================
BNG - British National Grid
============================

Intro
###################
The `Ordnance Survey National Grid reference system <https://en.wikipedia.org/wiki/Ordnance_Survey_National_Grid>`__
also known as the British National Grid (BNG) is a grid index system defined over the territory of Great Britain and
is extensively being used by many organizations in UK for indexing their geospatial data.
Mosaic supports multiple different grid indexes. One of the supported index systems is the British National Grid.
All of the operations that can be performed on H3 grid system are transferable to BNG grid system, and in general, are
transferable to a generic index system.

.. image:: ../images/OS_BNG_definition.png
   :width: 600px
   :height: 600px
   :alt: Ordnance Survey British National Grid
   :align: center

Configuring BNG
####################

The default index system for Mosaic is H3. If the users wish to use BNG instead, this can be achieved using spark
configurations. Spark provides an easy way to supply configuration parameters using spark.conf.set API.

.. tabs::
   .. code-tab:: py

    import mosaic as mos

    spark.conf.set("spark.databricks.labs.mosaic.index.system", "BNG")
    mos.enable_mosaic(spark, dbutils)

   .. code-tab:: scala

    import com.databricks.labs.mosaic.functions.MosaicContext
    import com.databricks.labs.mosaic.{BNG, JTS}

    val mosaicContext = MosaicContext.build(BNG, JTS)
    import mosaicContext.functions._

   .. code-tab:: r R

    library(sparkrMosaic)
    enableMosaic("JTS", "BNG")

   .. code-tab:: sql

    -- For SQL please use Automatic SQL Configuration. See: https://databrickslabs.github.io/mosaic/usage/automatic-sql-registration.html
    -- Otherwise, use python configuration in the first cell of your notebook, then use SQL in the rest of the notebook.

Coordinate Reference System
###########################

British National Grid (BNG) expects that coordinates are provided in EPSG:27700 coordinate reference system.
EPSG:27700 CRS is also referred to as "OSGB36 / British National Grid -- United Kingdom Ordnance Survey".
For more information on the CRS you can visit `EPSG:27700 <https://epsg.io/27700>`__.

If your data in is not provided in EPSG:27700 we would recommend to either switch the indexing strategy to H3
or to reproject the data from source CRS to EPSG:27700. Mosaic provides the functionality to set CRS ID
even for geometries that arrive as WKT and WKB (the formats do not capture the CRS information and this information
has to be set manually).

.. tabs::
   .. code-tab:: py

    ### CRS codes assume EPSG
    df = df.withColumn("geometry", mos.st_setsrid("geometry", 4326))
    df = df.withColumn("geometry", mos.st_transform("geometry", 27700))


   .. code-tab:: scala

    ### CRS codes assume EPSG
    val withSrcCRS = df.withColumn("geometry", mos.st_setsrid("geometry", 4326))
    val withReprojected = withSrcCRS.withColumn("geometry", mos.st_transform("geometry", 27700))

   .. code-tab:: r R

    ### CRS codes assume EPSG
    df <- withColumn(df, "geometry", mos.st_setsrid(df$geometry, 4326))
    df <- withColumn(df, "geometry", mos.st_transform(df$geometry, 27700))

Mosaic provides functionality to verify provided geometries have all of their vertices within bounds of the
specified CRS. If the CRS isn't EPSG:4326 then the functionality allows to prefer checks on the coordinates
before and/or after reprojection. This allows the end users to filter out geometries that would not be
possible to index with BNG.

.. tabs::
   .. code-tab:: py

    df = df.withColumn("is_within_bng_bounds", st_hasvalidcoordinates(geometry, 'EPSG:27700', 'reprojected_bounds'))

   .. code-tab:: scala

    val withValidCoords = df.withColumn("is_within_bng_bounds", st_hasvalidcoordinates(geometry, 'EPSG:27700', 'reprojected_bounds'))

   .. code-tab:: r R

    df <- withColumn(df, "is_within_bng_bounds", st_hasvalidcoordinates(geometry, 'EPSG:27700', 'reprojected_bounds'))

   .. code-tab:: sql

    SELECT *, st_hasvalidcoordinates(geometry, 'EPSG:27700', 'reprojected_bounds') as is_within_bng_bounds

Mosaic supports all indexing operations for both H3 and BNG.
Please see :doc:`Spatial Indexing </api/spatial-indexing>` for supported indexing operations.





