# Databricks notebook source
# MAGIC %md ## Data Ingestion

# COMMAND ----------

from pyspark.sql.functions import *
import mosaic as mos

spark.conf.set("spark.databricks.labs.mosaic.geometry.api", "ESRI")
spark.conf.set("spark.databricks.labs.mosaic.index.system", "H3")
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We begin with loading from a table. Here we use captured `AIS` data.
# MAGIC
# MAGIC - MMSI: unique 9-digit identification code of the ship - numeric
# MAGIC - VesselName: name of the ship - string
# MAGIC - CallSign: unique callsign of the ship - string
# MAGIC - BaseDateTime: timestamp of the AIS message - datetime
# MAGIC - LAT: latitude of the ship (in degree: [-90 ; 90], negative value represents South, 91 indicates ‘not available’) - numeric
# MAGIC - LON: longitude of the ship (in degree: [-180 ; 180], negative value represents West, 181 indicates ‘not available’) - numeric
# MAGIC - SOG: speed over ground, in knots - numeric
# MAGIC - Status: status of the ship - string

# COMMAND ----------

cargos = spark.read.table("ship2ship.AIS")
display(cargos)

# COMMAND ----------

# MAGIC %md ## Data Transformation

# COMMAND ----------

# MAGIC %md ### Indexing
# MAGIC To facilitate downstream analytics it is also possible to create a quick point index leveraging a chosen H3 resolution.
# MAGIC In this case, resolution `9` has an edge length of ~174 metres.

# COMMAND ----------

cargos_indexed = (
    cargos.withColumn("point_geom", mos.st_point("LON", "LAT"))
    .withColumn("ix", mos.point_index_geom("point_geom", resolution=lit(9)))
    .withColumn("sog_kmph", round(col("sog") * 1.852, 2))
)
display(cargos_indexed)

# COMMAND ----------

# MAGIC %md ## Exporting
# MAGIC and we can write the treated output to a new table.

# COMMAND ----------

(
    cargos_indexed.withColumn("point_geom", mos.st_aswkb("point_geom"))
    .write.mode("overwrite")
    .saveAsTable("ship2ship.cargos_indexed")
)

# COMMAND ----------

# DBTITLE 1,We can optimise our table to colocate data and make querying faster
# MAGIC %sql OPTIMIZE ship2ship.cargos_indexed ZORDER by (ix, BaseDateTime)

# COMMAND ----------

# MAGIC %md ## Visualisation
# MAGIC And we can perform a quick visual inspection of the data.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC ship2ship.cargos_indexed "ix" "h3" 10_000
