# Databricks notebook source
# MAGIC %md
# MAGIC # Mosaic & Sedona
# MAGIC 
# MAGIC You can combine the usage of Mosaic with other geospatial libraries. 
# MAGIC 
# MAGIC In this example we combine the use of [Sedona](https://sedona.apache.org/setup/databricks/) and Mosaic

# COMMAND ----------

import pyspark.sql.functions as f
from mosaic import enable_mosaic, st_area
from sedona.register.geo_registrator import SedonaRegistrator

enable_mosaic(spark, dbutils)           # Enable Mosaic
SedonaRegistrator.registerAll(spark)    # Register Sedona SQL functions

# COMMAND ----------

df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
(df
   # Mosaic
   .withColumn("mosaic_area", st_area('wkt'))
   # Sedona
   .withColumn("sedona_area", f.expr("ST_Area(ST_GeomFromWKT(wkt))"))
).show()

# COMMAND ----------

spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}]).createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mosaic & Sedona using SQL

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.mosaic.functions.MosaicContext
# MAGIC // Register Mosaic in a separate 'mosaic' database, Sedona uses the 'default' database 
# MAGIC MosaicContext.context.register(spark, Some("mosaic"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   mosaic.ST_Area(wkt) as mosaic_area,           -- Mosaic
# MAGIC   ST_Area(ST_GeomFromWKT(wkt)) as sedona_area,  -- Sedona
# MAGIC   wkt
# MAGIC FROM sample

# COMMAND ----------


