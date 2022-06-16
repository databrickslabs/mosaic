# Databricks notebook source
# MAGIC %md
# MAGIC # Mosaic & Sedona
# MAGIC 
# MAGIC You can combine the usage of Mosaic with other geospatial libraries. 
# MAGIC 
# MAGIC In this example we combine the use of [Sedona](https://sedona.apache.org/setup/databricks/) and Mosaic.
# MAGIC 
# MAGIC ## Setup
# MAGIC 
# MAGIC This notebook will run if you have both Mosaic and Sedona installed on your cluster.
# MAGIC 
# MAGIC ### Install sedona
# MAGIC 
# MAGIC To install Sedona, follow the [official Sedona instructions](https://sedona.apache.org/setup/databricks/).

# COMMAND ----------

import pyspark.sql.functions as f
import mosaic as mos
from sedona.register.geo_registrator import SedonaRegistrator

mos.enable_mosaic(spark, dbutils)       # Enable Mosaic
SedonaRegistrator.registerAll(spark)    # Register Sedona SQL functions

# COMMAND ----------

df = spark.createDataFrame([{'wkt': 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'}])
(df
   # Mosaic
   .withColumn("mosaic_area", mos.st_area('wkt'))
   # Sedona
   .withColumn("sedona_area", f.expr("ST_Area(ST_GeomFromWKT(wkt))"))
   # Sedona function not available in Mosaic
   .withColumn("sedona_flipped", f.expr("ST_FlipCoordinates(ST_GeomFromWKT(wkt))"))
).show()

# COMMAND ----------


