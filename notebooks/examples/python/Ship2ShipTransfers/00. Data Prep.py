# Databricks notebook source
# MAGIC %md ## We First Prep the data and download it

# COMMAND ----------

import mosaic as mos
from pyspark.sql.functions import *

spark.conf.set("spark.databricks.labs.mosaic.geometry.api", "ESRI")
spark.conf.set("spark.databricks.labs.mosaic.index.system", "H3")
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md ##AIS Data

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/vessels")

# COMMAND ----------

# MAGIC %sh
# MAGIC # see: https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/index.html
# MAGIC # we download data to dbfs:// mountpoint (/dbfs)
# MAGIC cd /dbfs/tmp/vessels/
# MAGIC
# MAGIC wget -np -r -nH -L --cut-dirs=3 https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_31.zip > /dev/null 2>&1
# MAGIC unzip 2018/AIS_2018_01_31.zip

# COMMAND ----------

schema = """
  MMSI int, 
  BaseDateTime timestamp, 
  LAT double, 
  LON double, 
  SOG double, 
  COG double, 
  Heading double, 
  VesselName string, 
  IMO string, 
  CallSign string, 
  VesselType int, 
  Status int, 
  Length int, 
  Width int, 
  Draft double, 
  Cargo int, 
  TranscieverClass string
"""

AIS_df = (
    # Quarantine bad records
    spark.read.option("badRecordsPath", "/tmp/ais_invalid")
    .csv("/tmp/vessels/2018", header=True, schema=schema)
    .filter("VesselType = 70")  # Only select cargos
    .filter("Status IS NOT NULL")
)
display(AIS_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ship2ship

# COMMAND ----------

(AIS_df.write.format("delta").mode("overwrite").saveAsTable("ship2ship.AIS"))

# COMMAND ----------

# MAGIC %md ## Harbours
# MAGIC
# MAGIC This data can be obtained from [here](https://data-usdot.opendata.arcgis.com/datasets/usdot::ports-major/about), and loaded accordingly.
# MAGIC Choosing `(1*0.001 - 1*0.0001)` as being equal to 99.99 metres at the equator
# MAGIC Ref: http://wiki.gis.com/wiki/index.php/Decimal_degrees

# COMMAND ----------

one_metre = 0.00001 - 0.000001
buffer = 10 * 1000 * one_metre

major_ports = (
    spark.read.table("hive_metastore.default.major_ports")
    .withColumn("geom", mos.st_point(x="X", y="Y"))
    .withColumn("geom", mos.st_buffer("geom", lit(buffer)))
)

# COMMAND ----------

display(major_ports)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC major_ports "geom" "geometry"

# COMMAND ----------

(
    major_ports.select(mos.mosaic_explode("geom", lit(9)).alias("mos"))
    .select(col("mos.index_id").alias("h3"))
    .write.mode("overwrite")
    .format("delta")
    .saveAsTable("ship2ship.harbours_h3")
)

# COMMAND ----------

harbours_h3 = spark.read.table("ship2ship.harbours_h3")
display(harbours_h3)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC "harbours_h3" "h3" "h3" 5_000

# COMMAND ----------
