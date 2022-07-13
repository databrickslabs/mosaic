# Databricks notebook source
# MAGIC %md ## We First Prep the data and download it

# COMMAND ----------

import mosaic as mos

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
    spark.read.option("badRecordsPath", "/tmp/ais_invalid")  # Quarantine bad records
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

# MAGIC %md ## Continental US Data

# COMMAND ----------

from pyspark.sql.functions import *

one_metre = 0.00001 - 0.000001
tolerance = 500 * one_metre

# COMMAND ----------

user_name = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

raw_path = f"dbfs:/tmp/ship2ship/{user_name}"
raw_us_outline_path = f"{raw_path}/us_outline"

print(f"The raw data is stored in {raw_path}")

# COMMAND ----------

import requests
import pathlib

us_outline_url = (
    "https://eric.clst.org/assets/wiki/uploads/Stuff/gz_2010_us_outline_500k.json"
)

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes

local_us_outline_path = pathlib.Path(raw_us_outline_path.replace("dbfs:/", "/dbfs/"))
local_us_outline_path.mkdir(parents=True, exist_ok=True)

req = requests.get(us_outline_url)
with open(local_us_outline_path / f"us_outline.geojson", "wb") as f:
    f.write(req.content)

# COMMAND ----------

outlines = (
    spark.read.format("json")
    .option("multiline", "true")
    .load(raw_us_outline_path)
    .select("type", explode(col("features")).alias("feature"))
    .select(
        "type",
        col("feature.properties").alias("properties"),
        to_json(col("feature.geometry")).alias("json_geometry"),
    )
    .withColumn("geometry", mos.st_aswkt(mos.st_geomfromgeojson("json_geometry")))
    .withColumn("coastal_area", mos.st_buffer("geometry", lit(tolerance)))
    .where("properties.type == 'COASTAL'")
    .drop("json_geometry")
)
display(outlines)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC outlines "coastal_area" "geometry" 100

# COMMAND ----------

coastal_h3 = (
    outlines.withColumn("geometry", mos.st_buffer("geometry", lit(tolerance)))
    .select(mos.mosaic_explode("geometry", lit(9)).alias("h3"))
    .select(col("h3.index_id").alias("h3"))
)

coastal_h3.createOrReplaceTempView("coastal")

display(coastal_h3)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC "coastal" "h3" "h3" 100000

# COMMAND ----------

(coastal_h3.write.mode("overwrite").format("delta").saveAsTable("ship2ship.coastal_h3"))

# COMMAND ----------

# MAGIC %md ## Harbours
# MAGIC
# MAGIC This data can be obtained from [here](https://data-usdot.opendata.arcgis.com/datasets/usdot::ports-major/about), and loaded accordingly.

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
