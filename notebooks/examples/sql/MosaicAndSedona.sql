-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Mosaic & Sedona
-- MAGIC 
-- MAGIC You can combine the usage of Mosaic with other geospatial libraries. 
-- MAGIC 
-- MAGIC In this example we combine the use of [Sedona](https://sedona.apache.org/setup/databricks/) and Mosaic.
-- MAGIC 
-- MAGIC ## Setup
-- MAGIC 
-- MAGIC This notebook will run if you have both Mosaic and Sedona installed on your cluster.
-- MAGIC 
-- MAGIC ### Install sedona
-- MAGIC 
-- MAGIC To install Sedona, follow the [official Sedona instructions](https://sedona.apache.org/setup/databricks/).

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC // Register Sedona in the 'default' database 
-- MAGIC import org.apache.sedona.sql.utils.SedonaSQLRegistrator
-- MAGIC SedonaSQLRegistrator.registerAll(spark)
-- MAGIC 
-- MAGIC // Register Mosaic in a separate 'mosaic' database
-- MAGIC import com.databricks.labs.mosaic.functions.MosaicContext
-- MAGIC MosaicContext.context.register(spark, Some("mosaic"))

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Example dataset
-- MAGIC spark.createDataFrame(Seq(Tuple1("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"))).toDF("wkt").createOrReplaceTempView("sample")

-- COMMAND ----------

SELECT 
  mosaic.ST_Area(wkt) as mosaic_area,                         -- Mosaic
  ST_Area(ST_GeomFromWKT(wkt)) as sedona_area,                -- Sedona
  ST_FlipCoordinates(ST_GeomFromWKT(wkt)) as sedona_flipped,  -- Sedona
  wkt
FROM sample
