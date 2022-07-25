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

dbutils.fs.mkdirs("/tmp/ship2ship")

# COMMAND ----------

# MAGIC %sh
# MAGIC # see: https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/index.html
# MAGIC # we download data to dbfs:// mountpoint (/dbfs)
# MAGIC mkdir /ship2ship/
# MAGIC cd /ship2ship/
# MAGIC wget -np -r -nH -L --cut-dirs=4 https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_31.zip > /dev/null 2>&1
# MAGIC unzip AIS_2018_01_31.zip
# MAGIC mv AIS_2018_01_31.csv /dbfs/tmp/ship2ship/

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
    spark.read.csv("/tmp/ship2ship", header=True, schema=schema)
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
# MAGIC
# MAGIC We are choosing a buffer of `10 km` around harbours to arbitrarily define an area wherein we do not expect ship-to-ship transfers to take place.
# MAGIC Since our projection is not in metres, we convert from decimal degrees. With `(0.00001 - 0.000001)` as being equal to one metres at the equator
# MAGIC Ref: http://wiki.gis.com/wiki/index.php/Decimal_degrees

# COMMAND ----------

# MAGIC %sh
# MAGIC # we download data to dbfs:// mountpoint (/dbfs)
# MAGIC cd /dbfs/tmp/ship2ship/
# MAGIC wget -np -r -nH -L -q --cut-dirs=7 -O harbours.geojson "https://geo.dot.gov/mapping/rest/services/NTAD/Ports_Major/MapServer/0/query?outFields=*&where=1%3D1&f=geojson"

# COMMAND ----------

one_metre = 0.00001 - 0.000001
buffer = 10 * 1000 * one_metre

major_ports = (
    spark.read.format("json")
    .option("multiline", "true")
    .load("/tmp/ship2ship/harbours.geojson")
    .select("type", explode(col("features")).alias("feature"))
    .select(
        "type",
        col("feature.properties").alias("properties"),
        to_json(col("feature.geometry")).alias("json_geometry"),
    )
    .withColumn("geom", mos.st_aswkt(mos.st_geomfromgeojson("json_geometry")))
    .select(col("properties.PORT_NAME").alias("name"), "geom")
    .withColumn("geom", mos.st_buffer("geom", lit(buffer)))
)
display(major_ports)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC major_ports "geom" "geometry"

# COMMAND ----------

(
    major_ports.select("name", mos.mosaic_explode("geom", lit(9)).alias("mos"))
    .select("name", col("mos.index_id").alias("h3"))
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
